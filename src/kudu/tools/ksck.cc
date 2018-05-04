// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/tools/ksck.h"

#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <mutex>
#include <set>
#include <vector>

#include <boost/optional.hpp> // IWYU pragma: keep
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/human_readable.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/util/atomic.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/tools/color.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/threadpool.h"

#define PUSH_PREPEND_NOT_OK(s, statuses, msg) do { \
  ::kudu::Status _s = (s); \
  if (PREDICT_FALSE(!_s.ok())) { \
    statuses.push_back(_s.CloneAndPrepend(msg)); \
  } \
} while (0);

DEFINE_bool(checksum_scan, false,
            "Perform a checksum scan on data in the cluster.");
DEFINE_int32(checksum_timeout_sec, 3600,
             "Maximum total seconds to wait for a checksum scan to complete "
             "before timing out.");
DEFINE_int32(checksum_scan_concurrency, 4,
             "Number of concurrent checksum scans to execute per tablet server.");
DEFINE_bool(checksum_snapshot, true, "Should the checksum scanner use a snapshot scan");
DEFINE_uint64(checksum_snapshot_timestamp,
              kudu::tools::ChecksumOptions::kCurrentTimestamp,
              "timestamp to use for snapshot checksum scans, defaults to 0, which "
              "uses the current timestamp of a tablet server involved in the scan");

DEFINE_int32(fetch_replica_info_concurrency, 20,
             "Number of concurrent tablet servers to fetch replica info from.");

DEFINE_bool(consensus, true,
            "Whether to check the consensus state from each tablet against the master.");
DEFINE_bool(verbose, false,
            "Output detailed information even if no inconsistency is found.");

using std::cout;
using std::endl;
using std::left;
using std::map;
using std::ostream;
using std::ostringstream;
using std::setw;
using std::shared_ptr;
using std::string;
using std::to_string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {
// Return true if 'str' matches any of the patterns in 'patterns', or if
// 'patterns' is empty.
bool MatchesAnyPattern(const vector<string>& patterns, const string& str) {
  // Consider no filter a wildcard.
  if (patterns.empty()) return true;

  for (const auto& p : patterns) {
    if (MatchPattern(str, p)) return true;
  }
  return false;
}

void BuildKsckConsensusStateForConfigMember(const consensus::ConsensusStatePB& cstate,
                                            KsckConsensusState* ksck_cstate) {
  CHECK(ksck_cstate);
  ksck_cstate->term = cstate.current_term();
  ksck_cstate->type = cstate.has_pending_config() ?
                      KsckConsensusConfigType::PENDING :
                      KsckConsensusConfigType::COMMITTED;
  const auto& config = cstate.has_pending_config() ?
                       cstate.pending_config() :
                       cstate.committed_config();
  if (config.has_opid_index()) {
    ksck_cstate->opid_index = config.opid_index();
  }
  // Test for emptiness rather than mere presence, since Kudu nodes set
  // leader_uuid to "" explicitly when they do not know about a leader.
  if (!cstate.leader_uuid().empty()) {
    ksck_cstate->leader_uuid = cstate.leader_uuid();
  }
  const auto& peers = config.peers();
  for (const auto& pb : peers) {
    if (pb.member_type() == consensus::RaftPeerPB::NON_VOTER) {
      InsertOrDie(&ksck_cstate->non_voter_uuids, pb.permanent_uuid());
    } else {
      InsertOrDie(&ksck_cstate->voter_uuids, pb.permanent_uuid());
    }
  }
}

} // anonymous namespace

ChecksumOptions::ChecksumOptions()
    : timeout(MonoDelta::FromSeconds(FLAGS_checksum_timeout_sec)),
      scan_concurrency(FLAGS_checksum_scan_concurrency),
      use_snapshot(FLAGS_checksum_snapshot),
      snapshot_timestamp(FLAGS_checksum_snapshot_timestamp) {
}

ChecksumOptions::ChecksumOptions(MonoDelta timeout, int scan_concurrency,
                                 bool use_snapshot, uint64_t snapshot_timestamp)
    : timeout(timeout),
      scan_concurrency(scan_concurrency),
      use_snapshot(use_snapshot),
      snapshot_timestamp(snapshot_timestamp) {}

const uint64_t ChecksumOptions::kCurrentTimestamp = 0;

tablet::TabletStatePB KsckTabletServer::ReplicaState(const std::string& tablet_id) const {
  CHECK_EQ(state_, KsckFetchState::FETCHED);
  if (!ContainsKey(tablet_status_map_, tablet_id)) {
    return tablet::UNKNOWN;
  }
  return tablet_status_map_.at(tablet_id).state();
}

std::ostream& operator<<(std::ostream& lhs, KsckFetchState state) {
  switch (state) {
    case KsckFetchState::UNINITIALIZED:
      lhs << "UNINITIALIZED";
      break;
    case KsckFetchState::FETCH_FAILED:
      lhs << "FETCH_FAILED";
      break;
    case KsckFetchState::FETCHED:
      lhs << "FETCHED";
      break;
    default:
      LOG(FATAL) << "unknown KsckFetchState";
  }
  return lhs;
}

Ksck::Ksck(shared_ptr<KsckCluster> cluster, ostream* out)
    : cluster_(std::move(cluster)),
      out_(out == nullptr ? &std::cout : out) {
}

Status Ksck::CheckMasterHealth() {
  int bad_masters = 0;
  vector<KsckServerHealthSummary> master_summaries;
  // There shouldn't be more than 5 masters, so we'll keep it simple and gather
  // info in sequence instead of spreading it across a threadpool.
  for (const auto& master : cluster_->masters()) {
    KsckServerHealthSummary sh;
    Status s = master->FetchInfo().AndThen([&]() {
          return master->FetchConsensusState();
        });
    sh.uuid = master->uuid();
    sh.address = master->address();
    sh.status = s;
    if (!s.ok()) {
      bad_masters++;
      sh.health = KsckServerHealth::UNAVAILABLE;
    }
    master_summaries.push_back(std::move(sh));
  }
  results_.master_summaries.swap(master_summaries);

  int num_masters = cluster_->masters().size();
  if (bad_masters > 0) {
    return Status::NetworkError(
        Substitute("failed to gather info from all masters: $0 of $1 had errors",
                   bad_masters, num_masters));
  }
  return Status::OK();
}

Status Ksck::CheckMasterConsensus() {
  if (!FLAGS_consensus) {
    return Status::OK();
  }
  KsckConsensusStateMap master_cstates;
  for (const KsckCluster::MasterList::value_type& master : cluster_->masters()) {
    if (master->cstate()) {
      KsckConsensusState ksck_cstate;
      BuildKsckConsensusStateForConfigMember(*master->cstate(), &ksck_cstate);
      InsertOrDie(&master_cstates, master->uuid(), ksck_cstate);
    } else {
      results_.master_consensus_conflict = true;
    }
  }
  if (master_cstates.empty()) {
    return Status::NotFound("no master consensus state available");
  }
  // There's no "reference" cstate for masters, so pick an arbitrary master
  // cstate to compare with.
  const KsckConsensusState& base = master_cstates.begin()->second;
  for (const auto& entry : master_cstates) {
    if (!base.Matches(entry.second)) {
      results_.master_consensus_conflict = true;
      break;
    }
  }
  results_.master_consensus_state_map.swap(master_cstates);
  vector<string> uuids;
  std::transform(cluster_->masters().begin(), cluster_->masters().end(),
                 std::back_inserter(uuids),
                 [](const shared_ptr<KsckMaster>& master) { return master->uuid(); });
  results_.master_uuids.swap(uuids);

  if (results_.master_consensus_conflict) {
    return Status::Corruption("there are master consensus conflicts");
  }
  return Status::OK();
}

Status Ksck::CheckClusterRunning() {
  VLOG(1) << "Connecting to the leader master";
  return cluster_->Connect();
}

Status Ksck::FetchTableAndTabletInfo() {
  return cluster_->FetchTableAndTabletInfo();
}

Status Ksck::FetchInfoFromTabletServers() {
  VLOG(1) << "Fetching the list of tablet servers";
  int servers_count = cluster_->tablet_servers().size();
  VLOG(1) << Substitute("List of $0 tablet servers retrieved", servers_count);

  if (servers_count == 0) {
    return Status::NotFound("No tablet servers found");
  }

  gscoped_ptr<ThreadPool> pool;
  RETURN_NOT_OK(ThreadPoolBuilder("ksck-fetch")
                .set_max_threads(FLAGS_fetch_replica_info_concurrency)
                .Build(&pool));

  AtomicInt<int32_t> bad_servers(0);
  VLOG(1) << "Fetching info from all " << servers_count << " tablet servers";

  vector<KsckServerHealthSummary> tablet_server_summaries;
  simple_spinlock tablet_server_summaries_lock;

  for (const auto& entry : cluster_->tablet_servers()) {
    const auto& ts = entry.second;
    CHECK_OK(pool->SubmitFunc([&]() {
          VLOG(1) << "Going to connect to tablet server: " << ts->uuid();
          Status s = ts->FetchInfo().AndThen([&ts]() {
                if (FLAGS_consensus) {
                  return ts->FetchConsensusState();
                }
                return Status::OK();
              });
          KsckServerHealthSummary summary;
          summary.uuid = entry.second->uuid();
          summary.address = entry.second->address();
          summary.status = s;
          if (!s.ok()) {
            bad_servers.Increment();
            if (s.IsRemoteError()) {
              summary.health = KsckServerHealth::WRONG_SERVER_UUID;
            } else {
              summary.health = KsckServerHealth::UNAVAILABLE;
              }
          } else {
            summary.health = KsckServerHealth::HEALTHY;
          }

          std::lock_guard<simple_spinlock> lock(tablet_server_summaries_lock);
          tablet_server_summaries.push_back(std::move(summary));
        }));
  }
  pool->Wait();

  results_.tserver_summaries.swap(tablet_server_summaries);

  if (bad_servers.Load() == 0) {
    return Status::OK();
  }
  return Status::NetworkError(
      Substitute("failed to gather info for all tablet servers: $0 of $1 had errors",
                 bad_servers.Load(), servers_count));
}

const KsckResults& Ksck::results() const {
  return results_;
}

Status Ksck::Run() {
  PUSH_PREPEND_NOT_OK(CheckMasterHealth(), results_.error_messages,
                      "error fetching info from masters");
  PUSH_PREPEND_NOT_OK(CheckMasterConsensus(), results_.error_messages,
                      "master consensus error");

  // CheckClusterRunning and FetchTableAndTabletInfo must succeed for
  // subsequent checks to be runnable.
  const char* const liveness_prefix = "leader master liveness check error";
  Status s = CheckClusterRunning();
  PUSH_PREPEND_NOT_OK(s, results_.error_messages, liveness_prefix);
  RETURN_NOT_OK_PREPEND(s, liveness_prefix);
  const char* const fetch_prefix = "error fetching the cluster metadata "
                                   "from the leader master";
  s = FetchTableAndTabletInfo();
  PUSH_PREPEND_NOT_OK(s, results_.error_messages, fetch_prefix);
  RETURN_NOT_OK_PREPEND(s, fetch_prefix);

  PUSH_PREPEND_NOT_OK(FetchInfoFromTabletServers(), results_.error_messages,
                      "error fetching info from tablet servers");

  PUSH_PREPEND_NOT_OK(CheckTablesConsistency(), results_.error_messages,
                      "table consistency check error");

  if (FLAGS_checksum_scan) {
    PUSH_PREPEND_NOT_OK(ChecksumData(ChecksumOptions()),
                        results_.error_messages, "checksum scan error");
  }

  if (!results_.error_messages.empty()) {
    return Status::RuntimeError("ksck discovered errors");
  }
  return Status::OK();
}

Status Ksck::PrintResults() {
  PrintMode mode = FLAGS_verbose ? PrintMode::VERBOSE : PrintMode::DEFAULT;
  return results_.PrintTo(mode, *out_);
}

Status Ksck::RunAndPrintResults() {
  Status s = Run();
  RETURN_NOT_OK_PREPEND(PrintResults(), "error printing results");
  return s;
}

Status Ksck::CheckTablesConsistency() {
  int bad_tables_count = 0;
  for (const shared_ptr<KsckTable> &table : cluster_->tables()) {
    if (!MatchesAnyPattern(table_filters_, table->name())) {
      VLOG(1) << "Skipping table " << table->name();
      continue;
    }
    if (!VerifyTable(table)) {
      bad_tables_count++;
    }
  }

  if (bad_tables_count > 0) {
      return Status::Corruption(
          Substitute("$0 out of $1 table(s) are not healthy",
                     bad_tables_count, results_.table_summaries.size()));
  }
  return Status::OK();
}

// Class to act as a collector of scan results.
// Provides thread-safe accessors to update and read a hash table of results.
class ChecksumResultReporter : public RefCountedThreadSafe<ChecksumResultReporter> {
 public:
  typedef std::pair<Status, uint64_t> ResultPair;
  typedef std::unordered_map<std::string, ResultPair> ReplicaResultMap;
  typedef std::unordered_map<std::string, ReplicaResultMap> TabletResultMap;

  // Initialize reporter with the number of replicas being queried.
  explicit ChecksumResultReporter(int num_tablet_replicas)
      : expected_count_(num_tablet_replicas),
        responses_(num_tablet_replicas),
        rows_summed_(0),
        disk_bytes_summed_(0) {
  }

  void ReportProgress(int64_t delta_rows, int64_t delta_bytes) {
    rows_summed_.IncrementBy(delta_rows);
    disk_bytes_summed_.IncrementBy(delta_bytes);
  }

  // Write an entry to the result map indicating a response from the remote.
  void ReportResult(const std::string& tablet_id,
                    const std::string& replica_uuid,
                    const Status& status,
                    uint64_t checksum) {
    std::lock_guard<simple_spinlock> guard(lock_);
    unordered_map<string, ResultPair>& replica_results =
        LookupOrInsert(&checksums_, tablet_id, unordered_map<string, ResultPair>());
    InsertOrDie(&replica_results, replica_uuid, ResultPair(status, checksum));
    responses_.CountDown();
  }

  // Blocks until either the number of results plus errors reported equals
  // num_tablet_replicas (from the constructor), or until the timeout expires,
  // whichever comes first. Progress messages are printed to 'out'.
  // Returns false if the timeout expired before all responses came in.
  // Otherwise, returns true.
  bool WaitFor(const MonoDelta& timeout, std::ostream* out) const {
    MonoTime start = MonoTime::Now();
    MonoTime deadline = start + timeout;

    bool done = false;
    while (!done) {
      MonoTime now = MonoTime::Now();
      int rem_ms = (deadline - now).ToMilliseconds();
      if (rem_ms <= 0) return false;

      done = responses_.WaitFor(MonoDelta::FromMilliseconds(std::min(rem_ms, 5000)));
      string status = done ? "finished in " : "running for ";
      int run_time_sec = (MonoTime::Now() - start).ToSeconds();
      (*out) << "Checksum " << status << run_time_sec << "s: "
             << responses_.count() << "/" << expected_count_ << " replicas remaining ("
             << HumanReadableNumBytes::ToString(disk_bytes_summed_.Load()) << " from disk, "
             << HumanReadableInt::ToString(rows_summed_.Load()) << " rows summed)"
             << endl;
    }
    return true;
  }

  // Returns true iff all replicas have reported in.
  bool AllReported() const { return responses_.count() == 0; }

  // Get reported results.
  TabletResultMap checksums() const {
    std::lock_guard<simple_spinlock> guard(lock_);
    return checksums_;
  }

 private:
  friend class RefCountedThreadSafe<ChecksumResultReporter>;
  ~ChecksumResultReporter() {}

  // Report either a success or error response.
  void HandleResponse(const std::string& tablet_id, const std::string& replica_uuid,
                      const Status& status, uint64_t checksum);

  const int expected_count_;
  CountDownLatch responses_;

  mutable simple_spinlock lock_; // Protects 'checksums_'.
  // checksums_ is an unordered_map of { tablet_id : { replica_uuid : checksum } }.
  TabletResultMap checksums_;

  AtomicInt<int64_t> rows_summed_;
  AtomicInt<int64_t> disk_bytes_summed_;
};

// Queue of tablet replicas for an individual tablet server.
typedef shared_ptr<BlockingQueue<std::pair<Schema, std::string>>> SharedTabletQueue;

// A set of callbacks which records the result of a tablet replica's checksum,
// and then checks if the tablet server has any more tablets to checksum. If so,
// a new async checksum scan is started.
class TabletServerChecksumCallbacks : public ChecksumProgressCallbacks {
 public:
  TabletServerChecksumCallbacks(
    scoped_refptr<ChecksumResultReporter> reporter,
    shared_ptr<KsckTabletServer> tablet_server,
    SharedTabletQueue queue,
    std::string tablet_id,
    ChecksumOptions options) :
      reporter_(std::move(reporter)),
      tablet_server_(std::move(tablet_server)),
      queue_(std::move(queue)),
      options_(options),
      tablet_id_(std::move(tablet_id)) {
  }

  void Progress(int64_t rows_summed, int64_t disk_bytes_summed) override {
    reporter_->ReportProgress(rows_summed, disk_bytes_summed);
  }

  void Finished(const Status& status, uint64_t checksum) override {
    reporter_->ReportResult(tablet_id_, tablet_server_->uuid(), status, checksum);

    std::pair<Schema, std::string> table_tablet;
    if (queue_->BlockingGet(&table_tablet)) {
      const Schema& table_schema = table_tablet.first;
      tablet_id_ = table_tablet.second;
      tablet_server_->RunTabletChecksumScanAsync(tablet_id_, table_schema, options_, this);
    } else {
      delete this;
    }
  }

 private:
  const scoped_refptr<ChecksumResultReporter> reporter_;
  const shared_ptr<KsckTabletServer> tablet_server_;
  const SharedTabletQueue queue_;
  const ChecksumOptions options_;

  std::string tablet_id_;
};

Status Ksck::ChecksumData(const ChecksumOptions& opts) {
  // Copy options so that local modifications can be made and passed on.
  ChecksumOptions options = opts;

  typedef unordered_map<shared_ptr<KsckTablet>, shared_ptr<KsckTable>> TabletTableMap;
  TabletTableMap tablet_table_map;

  int num_tables = 0;
  int num_tablets = 0;
  int num_tablet_replicas = 0;
  for (const shared_ptr<KsckTable>& table : cluster_->tables()) {
    VLOG(1) << "Table: " << table->name();
    if (!MatchesAnyPattern(table_filters_, table->name())) continue;
    num_tables += 1;
    num_tablets += table->tablets().size();
    for (const shared_ptr<KsckTablet>& tablet : table->tablets()) {
      VLOG(1) << "Tablet: " << tablet->id();
      if (!MatchesAnyPattern(tablet_id_filters_, tablet->id())) continue;
      InsertOrDie(&tablet_table_map, tablet, table);
      num_tablet_replicas += tablet->replicas().size();
    }
  }

  if (num_tables == 0) {
    string msg = "No table found.";
    if (!table_filters_.empty()) {
      msg += " Filter: table_filters=" + JoinStrings(table_filters_, ",");
    }
    return Status::NotFound(msg);
  }

  if (num_tablets > 0 && num_tablet_replicas == 0) {
    // Warn if the table has tablets, but no replicas. The table may have no
    // tablets if all range partitions have been dropped.
    string msg = "No tablet replicas found.";
    if (!table_filters_.empty() || !tablet_id_filters_.empty()) {
      msg += " Filter: ";
      if (!table_filters_.empty()) {
        msg += "table_filters=" + JoinStrings(table_filters_, ",");
      }
      if (!tablet_id_filters_.empty()) {
        msg += "tablet_id_filters=" + JoinStrings(tablet_id_filters_, ",");
      }
    }
    return Status::NotFound(msg);
  }

  // Map of tablet servers to tablet queue.
  typedef unordered_map<shared_ptr<KsckTabletServer>, SharedTabletQueue> TabletServerQueueMap;

  TabletServerQueueMap tablet_server_queues;
  scoped_refptr<ChecksumResultReporter> reporter(new ChecksumResultReporter(num_tablet_replicas));

  // Create a queue of checksum callbacks grouped by the tablet server.
  for (const TabletTableMap::value_type& entry : tablet_table_map) {
    const shared_ptr<KsckTablet>& tablet = entry.first;
    const shared_ptr<KsckTable>& table = entry.second;
    for (const shared_ptr<KsckTabletReplica>& replica : tablet->replicas()) {
      const shared_ptr<KsckTabletServer>& ts =
          FindOrDie(cluster_->tablet_servers(), replica->ts_uuid());

      const SharedTabletQueue& queue =
          LookupOrInsertNewSharedPtr(&tablet_server_queues, ts, num_tablet_replicas);
      CHECK_EQ(QUEUE_SUCCESS, queue->Put(make_pair(table->schema(), tablet->id())));
    }
  }

  if (options.use_snapshot && options.snapshot_timestamp == ChecksumOptions::kCurrentTimestamp) {
    // Set the snapshot timestamp to the current timestamp of the first healthy tablet server
    // we can find.
    for (const auto& ts : tablet_server_queues) {
      if (ts.first->is_healthy()) {
        options.snapshot_timestamp = ts.first->current_timestamp();
        break;
      }
    }
    if (options.snapshot_timestamp == ChecksumOptions::kCurrentTimestamp) {
      return Status::ServiceUnavailable(
          "No tablet servers were available to fetch the current timestamp");
    }
    results_.checksum_results.snapshot_timestamp = options.snapshot_timestamp;
  }

  // Kick off checksum scans in parallel. For each tablet server, we start
  // scan_concurrency scans. Each callback then initiates one additional
  // scan when it returns if the queue for that TS is not empty.
  for (const TabletServerQueueMap::value_type& entry : tablet_server_queues) {
    const shared_ptr<KsckTabletServer>& tablet_server = entry.first;
    const SharedTabletQueue& queue = entry.second;
    queue->Shutdown(); // Ensures that BlockingGet() will not block.
    for (int i = 0; i < options.scan_concurrency; i++) {
      std::pair<Schema, std::string> table_tablet;
      if (queue->BlockingGet(&table_tablet)) {
        const Schema& table_schema = table_tablet.first;
        const std::string& tablet_id = table_tablet.second;
        auto* cbs = new TabletServerChecksumCallbacks(
            reporter, tablet_server, queue, tablet_id, options);
        // 'cbs' deletes itself when complete.
        tablet_server->RunTabletChecksumScanAsync(tablet_id, table_schema, options, cbs);
      }
    }
  }

  bool timed_out = !reporter->WaitFor(options.timeout, out_);

  // Even if we timed out, for printing collate the checksum results that we did get.
  ChecksumResultReporter::TabletResultMap checksums = reporter->checksums();

  int num_errors = 0;
  int num_mismatches = 0;
  int num_results = 0;
  KsckTableChecksumMap checksum_tables;
  for (const shared_ptr<KsckTable>& table : cluster_->tables()) {
    KsckTableChecksum table_checksum;
    for (const shared_ptr<KsckTablet>& tablet : table->tablets()) {
      if (ContainsKey(checksums, tablet->id())) {
        KsckTabletChecksum tablet_checksum;
        tablet_checksum.tablet_id = tablet->id();
        bool seen_first_replica = false;
        uint64_t first_checksum = 0;

        for (const auto& r : FindOrDie(checksums, tablet->id())) {
          KsckReplicaChecksum replica_checksum;
          const string& replica_uuid = r.first;
          shared_ptr<KsckTabletServer> ts = FindOrDie(cluster_->tablet_servers(), replica_uuid);
          replica_checksum.ts_uuid = ts->uuid();
          replica_checksum.ts_address = ts->address();

          const ChecksumResultReporter::ResultPair& result = r.second;
          const Status& status = result.first;
          replica_checksum.checksum = result.second;
          replica_checksum.status = status;
          if (!status.ok()) {
            num_errors++;
          } else if (!seen_first_replica) {
            seen_first_replica = true;
            first_checksum = replica_checksum.checksum;
          } else if (replica_checksum.checksum != first_checksum) {
            num_mismatches++;
            tablet_checksum.mismatch = true;
          }
          num_results++;
          InsertOrDie(&tablet_checksum.replica_checksums,
                      replica_checksum.ts_uuid,
                      std::move(replica_checksum));
        }
        InsertOrDie(&table_checksum,
                    tablet_checksum.tablet_id,
                    std::move(tablet_checksum));
      }
    }
    InsertOrDie(&checksum_tables, table->name(), std::move(table_checksum));
  }
  results_.checksum_results.tables.swap(checksum_tables);
  if (timed_out) {
    return Status::TimedOut(Substitute("Checksum scan did not complete within the timeout of $0: "
                                       "Received results for $1 out of $2 expected replicas",
                                       options.timeout.ToString(), num_results,
                                       num_tablet_replicas));
  }
  CHECK_EQ(num_results, num_tablet_replicas)
      << Substitute("Unexpected error: only got $0 out of $1 replica results",
                    num_results, num_tablet_replicas);

  if (num_mismatches != 0) {
    return Status::Corruption(Substitute("$0 checksum mismatches were detected.", num_mismatches));
  }
  if (num_errors != 0) {
    return Status::Aborted(Substitute("$0 errors were detected", num_errors));
  }

  return Status::OK();
}

bool Ksck::VerifyTable(const shared_ptr<KsckTable>& table) {
  const auto all_tablets = table->tablets();
  vector<shared_ptr<KsckTablet>> tablets;
  std::copy_if(all_tablets.begin(), all_tablets.end(), std::back_inserter(tablets),
                 [&](const shared_ptr<KsckTablet>& t) {
                   return MatchesAnyPattern(tablet_id_filters_, t->id());
                 });

  if (tablets.empty()) {
    VLOG(1) << Substitute("Skipping table $0 as it has no matching tablets",
                          table->name());
    return true;
  }

  KsckTableSummary ts;
  ts.replication_factor = table->num_replicas();
  VLOG(1) << Substitute("Verifying $0 tablet(s) for table $1 configured with num_replicas = $2",
                        tablets.size(), table->name(), table->num_replicas());
  ts.name = table->name();
  for (const auto& tablet : tablets) {
    auto tablet_result = VerifyTablet(tablet, table->num_replicas());
    switch (tablet_result) {
      case KsckCheckResult::HEALTHY:
        ts.healthy_tablets++;
        break;
      case KsckCheckResult::RECOVERING:
        ts.recovering_tablets++;
        break;
      case KsckCheckResult::UNDER_REPLICATED:
        ts.underreplicated_tablets++;
        break;
      case KsckCheckResult::CONSENSUS_MISMATCH:
        ts.consensus_mismatch_tablets++;
        break;
      case KsckCheckResult::UNAVAILABLE:
        ts.unavailable_tablets++;
        break;
    }
  }
  bool all_healthy = ts.healthy_tablets == ts.TotalTablets();
  if (ts.TotalTablets() > 0) {
    results_.table_summaries.push_back(std::move(ts));
  }
  return all_healthy;
}

KsckCheckResult Ksck::VerifyTablet(const shared_ptr<KsckTablet>& tablet,
                                   int table_num_replicas) {
  const string tablet_str = Substitute("Tablet $0 of table '$1'",
                                 tablet->id(), tablet->table()->name());

  auto leader_it = std::find_if(tablet->replicas().cbegin(), tablet->replicas().cend(),
      [](const shared_ptr<KsckTabletReplica>& r) -> bool { return r->is_leader(); });
  boost::optional<string> leader_uuid;
  if (leader_it != tablet->replicas().cend()) {
    leader_uuid = (*leader_it)->ts_uuid();
  }
  vector<string> voter_uuids_from_master;
  vector<string> non_voter_uuids_from_master;
  for (const auto& replica : tablet->replicas()) {
    if (replica->is_voter()) {
      voter_uuids_from_master.push_back(replica->ts_uuid());
    } else {
      non_voter_uuids_from_master.push_back(replica->ts_uuid());
    }
  }
  KsckConsensusState master_config(KsckConsensusConfigType::MASTER,
                                   boost::none,
                                   boost::none,
                                   leader_uuid,
                                   voter_uuids_from_master,
                                   non_voter_uuids_from_master);

  int leaders_count = 0;
  int running_voters_count = 0;
  int copying_replicas_count = 0;
  int conflicting_states = 0;
  int num_voters = 0;
  vector<KsckReplicaSummary> replica_infos;
  for (const shared_ptr<KsckTabletReplica>& replica : tablet->replicas()) {
    replica_infos.emplace_back();
    auto* repl_info = &replica_infos.back();
    repl_info->ts_uuid = replica->ts_uuid();
    VLOG(1) << Substitute("A replica of tablet $0 is on live tablet server $1",
                          tablet->id(), replica->ts_uuid());

    // Check for agreement on tablet assignment and state between the master
    // and the tablet server.
    auto ts = FindPointeeOrNull(cluster_->tablet_servers(), replica->ts_uuid());
    if (ts) {
      repl_info->ts_address = ts->address();
    }
    if (ts && ts->is_healthy()) {
      repl_info->ts_healthy = true;
      repl_info->state = ts->ReplicaState(tablet->id());
      if (ContainsKey(ts->tablet_status_map(), tablet->id())) {
        repl_info->status_pb = ts->tablet_status_map().at(tablet->id());
      }

      // Organize consensus info for each replica.
      std::pair<string, string> tablet_key = std::make_pair(ts->uuid(), tablet->id());
      if (ContainsKey(ts->tablet_consensus_state_map(), tablet_key)) {
        const auto& cstate = FindOrDieNoPrint(ts->tablet_consensus_state_map(), tablet_key);
        KsckConsensusState ksck_cstate;
        BuildKsckConsensusStateForConfigMember(cstate, &ksck_cstate);
        repl_info->consensus_state = std::move(ksck_cstate);
      }
    }

    repl_info->is_leader = replica->is_leader();
    repl_info->is_voter = replica->is_voter();
    num_voters += replica->is_voter() ? 1 : 0;
    if (replica->is_leader()) {
      leaders_count++;
    }
    if (repl_info->state == tablet::RUNNING && replica->is_voter()) {
      running_voters_count++;
    } else if (repl_info->status_pb &&
               repl_info->status_pb->tablet_data_state() == tablet::TABLET_DATA_COPYING) {
      copying_replicas_count++;
    }
    // Compare the master's and peers' consensus configs.
    for (const auto& r : replica_infos) {
      if (r.consensus_state && !r.consensus_state->Matches(master_config)) {
        conflicting_states++;
      }
    }
  }

  // Determine the overall health state of the tablet.
  KsckCheckResult result = KsckCheckResult::HEALTHY;
  string status;
  int majority_size = consensus::MajoritySize(num_voters);
  if (copying_replicas_count > 0) {
    result = KsckCheckResult::RECOVERING;
    status = Substitute("$0 is $1: $2 on-going tablet copies",
                        tablet_str,
                        Color(AnsiCode::YELLOW, "recovering"),
                        copying_replicas_count);
  } else if (running_voters_count < majority_size) {
    result = KsckCheckResult::UNAVAILABLE;
    status = Substitute("$0 is $1: $2 replica(s) not RUNNING",
                        tablet_str,
                        Color(AnsiCode::RED, "unavailable"),
                        num_voters - running_voters_count);
  } else if (running_voters_count < num_voters) {
    result = KsckCheckResult::UNDER_REPLICATED;
    status = Substitute("$0 is $1: $2 replica(s) not RUNNING",
                        tablet_str,
                        Color(AnsiCode::YELLOW, "under-replicated"),
                        num_voters - running_voters_count);
  } else if (check_replica_count_ && num_voters < table_num_replicas) {
    result = KsckCheckResult::UNDER_REPLICATED;
    status = Substitute("$0 is $1: configuration has $2 replicas vs desired $3",
                        tablet_str,
                        Color(AnsiCode::YELLOW, "under-replicated"),
                        num_voters,
                        table_num_replicas);
  } else if (leaders_count != 1) {
    result = KsckCheckResult::UNAVAILABLE;
    status = Substitute("$0 is $1: expected one LEADER replica",
                        tablet_str, Color(AnsiCode::RED, "unavailable"));
  } else if (conflicting_states > 0) {
    result = KsckCheckResult::CONSENSUS_MISMATCH;
    status = Substitute("$0 is $1: $0 replicas' active configs disagree with the master's",
                        tablet_str,
                        Color(AnsiCode::YELLOW, "conflicted"),
                        conflicting_states);
  } else {
    status = Substitute("$0 is $1.",
                        tablet_str,
                        Color(AnsiCode::GREEN, "healthy"));
  }

  KsckTabletSummary tablet_summary;
  tablet_summary.result = result;
  tablet_summary.status = status;
  tablet_summary.master_cstate = std::move(master_config);
  tablet_summary.replica_infos.swap(replica_infos);
  results_.tablet_summaries.push_back(std::move(tablet_summary));
  return result;
}

} // namespace tools
} // namespace kudu
