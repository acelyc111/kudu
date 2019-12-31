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

#include "kudu/collector/collector.h"

#include <ostream>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/collector/cluster_rebalancer.h"
#include "kudu/collector/falcon_reporter.h"
#include "kudu/collector/local_reporter.h"
#include "kudu/collector/metrics_collector.h"
#include "kudu/collector/nodes_checker.h"
#include "kudu/collector/reporter_base.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/init.h"
#include "kudu/util/env.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

DEFINE_string(collector_cluster_name, "",
              "Cluster name of this collector to operate");
DEFINE_string(collector_master_addrs, "",
              "Comma-separated list of Kudu master addresses where each address is of "
              "form 'hostname:port");
DEFINE_uint32(collector_interval_sec, 60,
              "Number of interval seconds to collect metrics");
DEFINE_string(collector_report_method, "falcon",
              "Which monitor system the metrics reported to. Now supported system: local, falcon");
DEFINE_uint32(collector_timeout_sec, 10,
              "Number of seconds to wait for a master, tserver, or CLI tool to return metrics");
DEFINE_uint32(collector_warn_threshold_ms, 1000,
              "If a task takes more than this number of milliseconds, issue a warning with a "
              "trace.");

DECLARE_string(principal);
DECLARE_string(keytab_file);

using std::string;
using strings::Substitute;

namespace kudu {
namespace collector {

Collector::Collector()
  : initialized_(false),
    stop_background_threads_latch_(1) {
}

Collector::~Collector() {
  Shutdown();
}

Status Collector::Init() {
  CHECK(!initialized_);

  RETURN_NOT_OK(ValidateFlags());
  RETURN_NOT_OK(security::InitKerberosForServer(FLAGS_principal, FLAGS_keytab_file));

  if (FLAGS_collector_report_method == "falcon") {
    reporter_.reset(new FalconReporter());
  } else if (FLAGS_collector_report_method == "local") {
    reporter_.reset(new LocalReporter());
  } else {
    __builtin_unreachable();
  }

  CHECK_OK(reporter_->Init());
  nodes_checker_.reset(new NodesChecker(reporter_));
  CHECK_OK(nodes_checker_->Init());
  metrics_collector_.reset(new MetricsCollector(nodes_checker_, reporter_));
  CHECK_OK(metrics_collector_->Init());
  cluster_rebalancer_.reset(new ClusterRebalancer());
  CHECK_OK(cluster_rebalancer_->Init());

  initialized_ = true;
  return Status::OK();
}

Status Collector::Start() {
  CHECK(initialized_);

  google::FlushLogFiles(google::INFO); // Flush the startup messages.

  RETURN_NOT_OK(StartExcessLogFileDeleterThread());

  reporter_->Start();
  nodes_checker_->Start();
  metrics_collector_->Start();
  cluster_rebalancer_->Start();

  return Status::OK();
}

void Collector::Shutdown() {
  if (initialized_) {
    string name = ToString();
    LOG(INFO) << name << " shutting down...";

    reporter_->Shutdown();
    metrics_collector_->Shutdown();
    nodes_checker_->Shutdown();
    cluster_rebalancer_->Shutdown();

    stop_background_threads_latch_.CountDown();

    if (excess_log_deleter_thread_) {
      excess_log_deleter_thread_->Join();
    }

    LOG(INFO) << name << " shutdown complete.";
  }
}

string Collector::ToString() const {
  return "Collector";
}

Status Collector::StartExcessLogFileDeleterThread() {
  // Try synchronously deleting excess log files once at startup to make sure it
  // works, then start a background thread to continue deleting them in the
  // future.
  if (!FLAGS_logtostderr) {
    RETURN_NOT_OK_PREPEND(DeleteExcessLogFiles(Env::Default()),
                          "Unable to delete excess log files");
  }
  return Thread::Create("server", "excess-log-deleter", &Collector::ExcessLogFileDeleterThread,
                        this, &excess_log_deleter_thread_);
}

void Collector::ExcessLogFileDeleterThread() {
  // How often to attempt to clean up excess glog files.
  const MonoDelta kWait = MonoDelta::FromSeconds(60);
  while (!stop_background_threads_latch_.WaitFor(kWait)) {
    WARN_NOT_OK(DeleteExcessLogFiles(Env::Default()), "Unable to delete excess log files");
  }
}

Status Collector::ValidateFlags() {
  if (FLAGS_collector_interval_sec < 10 ||
      FLAGS_collector_interval_sec > 60 ||
      FLAGS_collector_timeout_sec < 1 ||
      FLAGS_collector_timeout_sec >= FLAGS_collector_interval_sec) {
    return Status::InvalidArgument("--collector_interval_sec should in range [10, 60], and "
                                   "--collector_timeout_sec should in range "
                                   "(0, collector_interval_sec)");
  }

  if (FLAGS_collector_report_method != "local" &&
      FLAGS_collector_report_method != "falcon") {
    return Status::InvalidArgument("--collector_report_method only support 'local' and 'falcon'.");
  }

  if (FLAGS_collector_cluster_name.empty() ||
      FLAGS_collector_master_addrs.empty()) {
    return Status::InvalidArgument("--collector_cluster_name and --collector_master_addrs should "
                                   "not be empty.");
  }

  return Status::OK();
}
} // namespace collector
} // namespace kudu
