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

#include "kudu/util/maintenance_manager.h"

#include <cinttypes>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>

#include <boost/bind.hpp>
#include <gflags/gflags.h>

#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/debug/trace_logging.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/maintenance_manager.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/thread.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

using std::pair;
using std::set;
using std::string;
using strings::Split;
using strings::Substitute;

DEFINE_int32(maintenance_manager_num_threads, 1,
             "Size of the maintenance manager thread pool. "
             "For spinning disks, the number of threads should "
             "not be above the number of devices.");
TAG_FLAG(maintenance_manager_num_threads, stable);

DEFINE_int32(maintenance_manager_privilege_num_threads, 1,
             "Size of the maintenance manager thread pool for privilege OPs. "
             "Maintenance OPs whose table id in --maintenance_manager_privilege_table_ids or "
             "tablet id in --maintenance_manager_privilege_tablet_ids will be launched in this "
             "privilege thread pool.");
TAG_FLAG(maintenance_manager_privilege_num_threads, advanced);
TAG_FLAG(maintenance_manager_privilege_num_threads, experimental);

DEFINE_int32(maintenance_manager_polling_interval_ms, 250,
       "Polling interval for the maintenance manager scheduler, "
       "in milliseconds.");
TAG_FLAG(maintenance_manager_polling_interval_ms, hidden);

DEFINE_int32(maintenance_manager_history_size, 8,
       "Number of completed operations the manager keeps track of.");
TAG_FLAG(maintenance_manager_history_size, hidden);

DEFINE_bool(enable_maintenance_manager, true,
       "Enable the maintenance manager, which runs flush, compaction, and "
       "garbage collection operations on tablets.");
TAG_FLAG(enable_maintenance_manager, unsafe);

DEFINE_int64(log_target_replay_size_mb, 1024,
             "The target maximum size of logs to be replayed at startup. If a tablet "
             "has in-memory operations that are causing more than this size of logs "
             "to be retained, then the maintenance manager will prioritize flushing "
             "these operations to disk.");
TAG_FLAG(log_target_replay_size_mb, experimental);

DEFINE_int64(data_gc_min_size_mb, 0,
             "The (exclusive) minimum number of megabytes of ancient data on "
             "disk, per tablet, needed to prioritize deletion of that data.");
TAG_FLAG(data_gc_min_size_mb, experimental);

DEFINE_double(data_gc_prioritization_prob, 0.5,
             "The probability that we will prioritize data GC over performance "
             "improvement operations. If set to 1.0, we will always prefer to "
             "delete old data before running performance improvement operations "
             "such as delta compaction.");
TAG_FLAG(data_gc_prioritization_prob, experimental);

DEFINE_string(maintenance_manager_privilege_table_ids, "",
              "Table ids to privilege launch for maintenance.");
TAG_FLAG(maintenance_manager_privilege_table_ids, advanced);
TAG_FLAG(maintenance_manager_privilege_table_ids, experimental);
TAG_FLAG(maintenance_manager_privilege_table_ids, runtime);

DEFINE_string(maintenance_manager_privilege_tablet_ids, "",
              "Tablet ids to privilege launch for maintenance.");
TAG_FLAG(maintenance_manager_privilege_tablet_ids, advanced);
TAG_FLAG(maintenance_manager_privilege_tablet_ids, experimental);
TAG_FLAG(maintenance_manager_privilege_tablet_ids, runtime);

namespace kudu {

MaintenanceOpStats::MaintenanceOpStats() {
  Clear();
}

void MaintenanceOpStats::Clear() {
  valid_ = false;
  runnable_ = false;
  ram_anchored_ = 0;
  logs_retained_bytes_ = 0;
  data_retained_bytes_ = 0;
  perf_improvement_ = 0;
  last_modified_ = MonoTime();
}

MaintenanceOp::MaintenanceOp(std::string name, IOUsage io_usage)
    : name_(std::move(name)),
      running_(0),
      cancel_(false),
      io_usage_(io_usage) {
}

MaintenanceOp::~MaintenanceOp() {
  CHECK(!manager_.get()) << "You must unregister the " << name_
                         << " Op before destroying it.";
}

void MaintenanceOp::Unregister() {
  CHECK(manager_.get()) << "Op " << name_ << " is not registered.";
  manager_->UnregisterOp(this);
}

bool MaintenanceOp::IsPrivilege(const set<string>& privilege_table_id,
                                const set<string>& privilege_tablet_id) const {
  return ContainsKey(privilege_table_id, table_id())
    || ContainsKey(privilege_tablet_id, tablet_id());
}

MaintenanceManagerStatusPB_OpInstancePB OpInstance::DumpToPB() const {
  MaintenanceManagerStatusPB_OpInstancePB pb;
  pb.set_as_privilege(as_privilege);
  pb.set_thread_id(thread_id);
  pb.set_name(name);
  if (duration.Initialized()) {
    pb.set_duration_millis(static_cast<int32_t>(duration.ToMilliseconds()));
  }
  MonoDelta delta(MonoTime::Now() - start_mono_time);
  pb.set_millis_since_start(static_cast<int32_t>(delta.ToMilliseconds()));
  return pb;
}

const MaintenanceManager::Options MaintenanceManager::kDefaultOptions = {
  .num_threads = 0,
  .privilege_num_threads = 0,
  .polling_interval_ms = 0,
  .history_size = 0,
};

MaintenanceManager::MaintenanceManager(const Options& options,
                                       std::string server_uuid)
  : server_uuid_(std::move(server_uuid)),
    num_threads_(options.num_threads <= 0 ?
                 FLAGS_maintenance_manager_num_threads : options.num_threads),
    privilege_num_threads_(options.privilege_num_threads <= 0 ?
                 FLAGS_maintenance_manager_privilege_num_threads : options.privilege_num_threads),
    cond_(&lock_),
    shutdown_(false),
    polling_interval_ms_(options.polling_interval_ms <= 0 ?
          FLAGS_maintenance_manager_polling_interval_ms :
          options.polling_interval_ms),
    running_ops_(0),
    running_privilege_ops_(0),
    completed_ops_count_(0),
    rand_(GetRandomSeed32()),
    memory_pressure_func_(&process_memory::UnderMemoryPressure) {
  CHECK_OK(ThreadPoolBuilder("MaintenanceMgr")
               .set_min_threads(num_threads_)
               .set_max_threads(num_threads_)
               .Build(&thread_pool_));
  CHECK_OK(ThreadPoolBuilder("PrivilegeMaintenanceMgr")
               .set_min_threads(privilege_num_threads_)
               .set_max_threads(privilege_num_threads_)
               .Build(&privilege_thread_pool_));
  uint32_t history_size = options.history_size == 0 ?
                          FLAGS_maintenance_manager_history_size :
                          options.history_size;
  completed_ops_.resize(history_size);
}

MaintenanceManager::~MaintenanceManager() {
  Shutdown();
}

Status MaintenanceManager::Start() {
  CHECK(!monitor_thread_);
  RETURN_NOT_OK(Thread::Create("maintenance", "maintenance_scheduler",
      boost::bind(&MaintenanceManager::RunSchedulerThread, this),
      &monitor_thread_));
  return Status::OK();
}

void MaintenanceManager::Shutdown() {
  {
    std::lock_guard<Mutex> guard(lock_);
    if (shutdown_) {
      return;
    }
    shutdown_ = true;
    cond_.Broadcast();
  }
  if (monitor_thread_.get()) {
    CHECK_OK(ThreadJoiner(monitor_thread_.get()).Join());
    monitor_thread_.reset();
    // Wait for all the running and queued tasks before shutting down. Otherwise,
    // Shutdown() can remove a queued task silently. We count on eventually running the
    // queued tasks to decrement their "running" count, which is incremented at the time
    // they are enqueued.
    thread_pool_->Wait();
    thread_pool_->Shutdown();
  }
}

void MaintenanceManager::RegisterOp(MaintenanceOp* op) {
  CHECK(op);
  std::lock_guard<Mutex> guard(lock_);
  CHECK(!op->manager_) << "Tried to register " << op->name()
                       << ", but it is already registered.";
  pair<OpMapTy::iterator, bool> val
    (ops_.insert(OpMapTy::value_type(op, MaintenanceOpStats())));
  CHECK(val.second) << "Tried to register " << op->name()
                    << ", but it already exists in ops_.";
  op->manager_ = shared_from_this();
  op->cond_.reset(new ConditionVariable(&lock_));
  VLOG_AND_TRACE_WITH_PREFIX("maintenance", 1) << "Registered " << op->name();
}

void MaintenanceManager::UnregisterOp(MaintenanceOp* op) {
  {
    std::lock_guard<Mutex> guard(lock_);
    CHECK(op->manager_.get() == this) << "Tried to unregister " << op->name()
        << ", but it is not currently registered with this maintenance manager.";
    auto iter = ops_.find(op);
    CHECK(iter != ops_.end()) << "Tried to unregister " << op->name()
                              << ", but it was never registered";
    // While the op is running, wait for it to be finished.
    if (iter->first->running_ > 0) {
      VLOG_AND_TRACE_WITH_PREFIX("maintenance", 1) << "Waiting for op " << op->name()
                                                   << " to finish so we can unregister it.";
    }
    op->CancelAndDisable();
    while (iter->first->running_ > 0) {
      op->cond_->Wait();
      iter = ops_.find(op);
      CHECK(iter != ops_.end()) << "Tried to unregister " << op->name()
          << ", but another thread unregistered it while we were "
          << "waiting for it to complete";
    }
    ops_.erase(iter);
  }
  VLOG_WITH_PREFIX(1) << "Unregistered op " << op->name();
  op->cond_.reset();
  // Remove the op's shared_ptr reference to us. This might 'delete this'.
  op->manager_.reset();
}

bool MaintenanceManager::disabled_for_tests() const {
  return !ANNOTATE_UNPROTECTED_READ(FLAGS_enable_maintenance_manager);
}

void MaintenanceManager::RunSchedulerThread() {
  if (!FLAGS_enable_maintenance_manager) {
    LOG(INFO) << "Maintenance manager is disabled. Stopping thread.";
    return;
  }

  MonoDelta polling_interval = MonoDelta::FromMilliseconds(polling_interval_ms_);

  std::unique_lock<Mutex> guard(lock_);

  // Set to true if the scheduler runs and finds that there is no work to do.
  bool prev_iter_found_no_work = false;

  while (true) {
    // We'll keep sleeping if:
    //    1) there are no free threads available to perform a maintenance op.
    // or 2) we just tried to schedule an op but found nothing to run.
    // However, if it's time to shut down, we want to do so immediately.
    while (CouldNotLaunchNewOp(prev_iter_found_no_work)) {
      cond_.WaitFor(polling_interval);
      prev_iter_found_no_work = false;
    }
    if (shutdown_) {
      VLOG_AND_TRACE_WITH_PREFIX("maintenance", 1) << "Shutting down maintenance manager.";
      return;
    }

    // TODO(yingchun): move it to SetFlag, callback once as a gflags setter handler.
    UpdatePrivilegeIds();

    // If we found no work to do, then we should sleep before trying again to schedule.
    // Otherwise, we can go right into trying to find the next op.
    bool privilege_op_found = FindAndLaunchOp(&guard, MaintenanceType::kPrivilege);
    bool common_op_found = FindAndLaunchOp(&guard, MaintenanceType::kCommon);
    prev_iter_found_no_work = !common_op_found && !privilege_op_found;
  }
}

bool MaintenanceManager::FindAndLaunchOp(std::unique_lock<Mutex>* guard,
                                         MaintenanceType type) {
  // Find the best op.
  auto best_op_and_why = FindBestOp(type);
  auto* op = best_op_and_why.first;
  const auto& note = best_op_and_why.second;

  if (!op) {
    VLOG_AND_TRACE_WITH_PREFIX("maintenance", 2)
        << "No maintenance operations look worth doing.";
    return false;
  }

  // Prepare the maintenance operation.
  IncreaseOpCount(type, op);
  guard->unlock();
  bool ready = op->Prepare();
  guard->lock();
  if (!ready) {
    LOG_WITH_PREFIX(INFO) << "Prepare failed for " << op->name()
                          << ". Re-running scheduler.";
    DecreaseOpCount(type, op);
    op->cond_->Signal();
    return true;
  }

  LOG_AND_TRACE_WITH_PREFIX("maintenance", INFO)
      << Substitute("Scheduling $0: $1", op->name(), note);
  // Run the maintenance operation.
  auto& thread_pool = (type == MaintenanceType::kCommon ? thread_pool_ : privilege_thread_pool_);
  Status s = thread_pool->SubmitFunc(boost::bind(&MaintenanceManager::LaunchOp, this, type, op));
  CHECK(s.ok());

  return true;
}

// Finding the best operation goes through four filters:
// - If there's an Op that we can run quickly that frees log retention, we run it.
// - If we've hit the overall process memory limit (note: this includes memory that the Ops cannot
//   free), we run the Op with the highest RAM usage.
// - If there are Ops that are retaining logs past our target replay size, we run the one that has
//   the highest retention (and if many qualify, then we run the one that also frees up the
//   most RAM).
// - Finally, if there's nothing else that we really need to do, we run the Op that will improve
//   performance the most.
//
// The reason it's done this way is that we want to prioritize limiting the amount of resources we
// hold on to. Low IO Ops go first since we can quickly run them, then we can look at memory usage.
// Reversing those can starve the low IO Ops when the system is under intense memory pressure.
//
// In the third priority we're at a point where nothing's urgent and there's nothing we can run
// quickly.
// TODO(wdberkeley) We currently optimize for freeing log retention but we could consider having
// some sort of sliding priority between log retention and RAM usage. For example, is an Op that
// frees 128MB of log retention and 12MB of RAM always better than an op that frees 12MB of log
// retention and 128MB of RAM? Maybe a more holistic approach would be better.
pair<MaintenanceOp*, string> MaintenanceManager::FindBestOp(MaintenanceType type) {
  TRACE_EVENT0("maintenance", "MaintenanceManager::FindBestOp");

  if (!HasFreeThreads(type)) {
    return {nullptr, "no free threads"};
  }

  int64_t low_io_most_logs_retained_bytes = 0;
  MaintenanceOp* low_io_most_logs_retained_bytes_op = nullptr;

  int64_t most_ram_anchored = 0;
  MaintenanceOp* most_ram_anchored_op = nullptr;

  int64_t most_logs_retained_bytes = 0;
  int64_t most_logs_retained_bytes_ram_anchored = 0;
  MaintenanceOp* most_logs_retained_bytes_op = nullptr;

  int64_t most_data_retained_bytes = 0;
  MaintenanceOp* most_data_retained_bytes_op = nullptr;

  double best_perf_improvement = 0;
  MaintenanceOp* best_perf_improvement_op = nullptr;
  for (auto& val : ops_) {
    MaintenanceOp* op(val.first);
    MaintenanceOpStats& stats(val.second);
    // Privilege OPs can be submitted into both privilege and common thread pool, while
    // common OPs can only be submitted into common thread pool.
    if (type == MaintenanceType::kPrivilege && !op->IsPrivilege(privilege_table_ids_,
                                                                privilege_tablet_ids_)) {
      continue;
    }

    VLOG_WITH_PREFIX(3) << "Considering MM op " << op->name();
    // Update op stats.
    stats.Clear();
    op->UpdateStats(&stats);
    if (op->cancelled() || !stats.valid() || !stats.runnable()) {
      continue;
    }

    const auto logs_retained_bytes = stats.logs_retained_bytes();
    if (logs_retained_bytes > low_io_most_logs_retained_bytes &&
        op->io_usage() == MaintenanceOp::LOW_IO_USAGE) {
      low_io_most_logs_retained_bytes_op = op;
      low_io_most_logs_retained_bytes = logs_retained_bytes;
      VLOG_AND_TRACE_WITH_PREFIX("maintenance", 2)
          << Substitute("Op $0 can free $1 bytes of logs",
                        op->name(), logs_retained_bytes);
    }

    const auto ram_anchored = stats.ram_anchored();
    if (ram_anchored > most_ram_anchored) {
      most_ram_anchored_op = op;
      most_ram_anchored = ram_anchored;
    }

    // We prioritize ops that can free more logs, but when it's the same we pick
    // the one that also frees up the most memory.
    if (std::make_pair(logs_retained_bytes, ram_anchored) >
        std::make_pair(most_logs_retained_bytes,
                       most_logs_retained_bytes_ram_anchored)) {
      most_logs_retained_bytes_op = op;
      most_logs_retained_bytes = logs_retained_bytes;
      most_logs_retained_bytes_ram_anchored = ram_anchored;
    }

    const auto data_retained_bytes = stats.data_retained_bytes();
    if (data_retained_bytes > most_data_retained_bytes) {
      most_data_retained_bytes_op = op;
      most_data_retained_bytes = data_retained_bytes;
      VLOG_AND_TRACE_WITH_PREFIX("maintenance", 2)
          << Substitute("Op $0 can free $1 bytes of data",
                        op->name(), data_retained_bytes);
    }

    const auto perf_improvement = stats.perf_improvement();
    if ((!best_perf_improvement_op) ||
        (perf_improvement > best_perf_improvement)) {
      best_perf_improvement_op = op;
      best_perf_improvement = perf_improvement;
    }
  }

  // Look at ops that we can run quickly that free up log retention.
  if (low_io_most_logs_retained_bytes_op && low_io_most_logs_retained_bytes > 0) {
    string notes = Substitute("free $0 bytes of WAL", low_io_most_logs_retained_bytes);
    return {low_io_most_logs_retained_bytes_op, std::move(notes)};
  }

  // Look at free memory. If it is dangerously low, we must select something
  // that frees memory-- the op with the most anchored memory.
  double capacity_pct;
  if (memory_pressure_func_(&capacity_pct)) {
    if (!most_ram_anchored_op) {
      std::string msg = StringPrintf("System under memory pressure "
          "(%.2f%% of limit used). However, there are no ops currently "
          "runnable which would free memory.", capacity_pct);
      KLOG_EVERY_N_SECS(WARNING, 5) << msg;
      return {nullptr, msg};
    }
    string note = StringPrintf("under memory pressure (%.2f%% used, "
                               "can flush %" PRIu64 " bytes)",
                               capacity_pct, most_ram_anchored);
    return {most_ram_anchored_op, std::move(note)};
  }

  if (most_logs_retained_bytes_op &&
      most_logs_retained_bytes / 1024 / 1024 >= FLAGS_log_target_replay_size_mb) {
    string note = Substitute("$0 bytes log retention", most_logs_retained_bytes);
    return {most_logs_retained_bytes_op, std::move(note)};
  }

  // Look at ops that we can run quickly that free up data on disk.
  if (most_data_retained_bytes_op &&
      most_data_retained_bytes > FLAGS_data_gc_min_size_mb * 1024 * 1024) {
    if (!best_perf_improvement_op || best_perf_improvement <= 0 ||
        rand_.NextDoubleFraction() <= FLAGS_data_gc_prioritization_prob) {
      string note = Substitute("$0 bytes on disk", most_data_retained_bytes);
      return {most_data_retained_bytes_op, std::move(note)};
    }
    VLOG(1) << "Skipping data GC due to prioritizing perf improvement";
  }

  if (best_perf_improvement_op && best_perf_improvement > 0) {
    string note = StringPrintf("perf score=%.6f", best_perf_improvement);
    return {best_perf_improvement_op, std::move(note)};
  }
  return {nullptr, "no ops with positive improvement"};
}

void MaintenanceManager::LaunchOp(MaintenanceType type, MaintenanceOp* op) {
  int64_t thread_id = Thread::CurrentThreadId();
  OpInstance op_instance;
  op_instance.as_privilege = (type == MaintenanceType::kPrivilege);
  op_instance.thread_id = thread_id;
  op_instance.name = op->name();
  op_instance.start_mono_time = MonoTime::Now();
  op->RunningGauge()->Increment();
  {
    std::lock_guard<Mutex> lock(running_instances_lock_);
    InsertOrDie(&running_instances_, thread_id, &op_instance);
  }

  SCOPED_CLEANUP({
    op->RunningGauge()->Decrement();

    std::lock_guard<Mutex> l(lock_);
    {
      std::lock_guard<Mutex> lock(running_instances_lock_);
      running_instances_.erase(thread_id);
    }
    op_instance.duration = MonoTime::Now() - op_instance.start_mono_time;
    completed_ops_[completed_ops_count_ % completed_ops_.size()] = op_instance;
    completed_ops_count_++;

    op->DurationHistogram()->Increment(op_instance.duration.ToMilliseconds());
    DecreaseOpCount(type, op);
    op->cond_->Signal();
    cond_.Signal(); // Wake up scheduler.
  });

  scoped_refptr<Trace> trace(new Trace);
  Stopwatch sw;
  sw.start();
  {
    ADOPT_TRACE(trace.get());
    TRACE_EVENT1("maintenance", "MaintenanceManager::LaunchOp",
                 "name", op->name());
    op->Perform();
    sw.stop();
  }
  LOG_WITH_PREFIX(INFO) << Substitute("$0 complete. Timing: $1 Metrics: $2",
                                      op->name(),
                                      sw.elapsed().ToString(),
                                      trace->MetricsAsJSON());
}

void MaintenanceManager::GetMaintenanceManagerStatusDump(MaintenanceManagerStatusPB* out_pb) {
  DCHECK(out_pb != nullptr);
  std::lock_guard<Mutex> guard(lock_);
  for (const auto& val : ops_) {
    MaintenanceManagerStatusPB_MaintenanceOpPB* op_pb = out_pb->add_registered_operations();
    MaintenanceOp* op(val.first);
    const MaintenanceOpStats& stats(val.second);
    op_pb->set_name(op->name());
    op_pb->set_running(op->running());
    if (stats.valid()) {
      op_pb->set_runnable(stats.runnable());
      op_pb->set_ram_anchored_bytes(stats.ram_anchored());
      op_pb->set_logs_retained_bytes(stats.logs_retained_bytes());
      op_pb->set_perf_improvement(stats.perf_improvement());
    } else {
      op_pb->set_runnable(false);
      op_pb->set_ram_anchored_bytes(0);
      op_pb->set_logs_retained_bytes(0);
      op_pb->set_perf_improvement(0.0);
    }
  }

  {
    std::lock_guard<Mutex> lock(running_instances_lock_);
    for (const auto& running_instance : running_instances_) {
      *out_pb->add_running_operations() = running_instance.second->DumpToPB();
    }
  }

  // The latest completed op will dump at first.
  for (int n = 1; n <= completed_ops_.size(); n++) {
    int64_t i = completed_ops_count_ - n;
    if (i < 0) break;
    const auto& completed_op = completed_ops_[i % completed_ops_.size()];

    if (!completed_op.name.empty()) {
      *out_pb->add_completed_operations() = completed_op.DumpToPB();
    }
  }
}

std::string MaintenanceManager::LogPrefix() const {
  return Substitute("P $0: ", server_uuid_);
}

bool MaintenanceManager::HasFreeThreads(MaintenanceType type) {
  if (type == MaintenanceType::kCommon) {
    return num_threads_ - running_ops_ > 0;
  }

  CHECK(type == MaintenanceType::kPrivilege);
  return privilege_num_threads_ - running_privilege_ops_ > 0;
}

bool MaintenanceManager::CouldNotLaunchNewOp(bool prev_iter_found_no_work) {
  return ((!HasFreeThreads(MaintenanceType::kPrivilege)
    && !HasFreeThreads(MaintenanceType::kCommon))
    || prev_iter_found_no_work || disabled_for_tests()) && !shutdown_;
}

void MaintenanceManager::UpdatePrivilegeIds() {
  string table_ids;
  if (google::GetCommandLineOption("maintenance_manager_privilege_table_ids", &table_ids)) {
    privilege_table_ids_ = set<string>(Split(table_ids, ",", strings::SkipEmpty()));
  }
  string tablet_ids;
  if (google::GetCommandLineOption("maintenance_manager_privilege_tablet_ids", &tablet_ids)) {
    privilege_tablet_ids_ = set<string>(Split(tablet_ids, ",", strings::SkipEmpty()));
  }
}

void MaintenanceManager::IncreaseOpCount(MaintenanceType type, MaintenanceOp *op) {
  op->running_++;
  if (type == MaintenanceType::kCommon) {
    running_ops_++;
  } else {
    CHECK(type == MaintenanceType::kPrivilege);
    running_privilege_ops_++;
  }
}

void MaintenanceManager::DecreaseOpCount(MaintenanceType type, MaintenanceOp *op) {
  op->running_--;
  if (type == MaintenanceType::kCommon) {
    running_ops_--;
  } else {
    CHECK(type == MaintenanceType::kPrivilege);
    running_privilege_ops_--;
  }
}

} // namespace kudu
