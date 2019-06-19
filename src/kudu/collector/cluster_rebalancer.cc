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

#include "kudu/collector/cluster_rebalancer.h"

#include <stdio.h>
#include <time.h>

#include <ostream>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/collector/collector_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

DEFINE_bool(auto_rebalance, true, "Whether to rebalance cluster automatically");
DEFINE_string(rebalance_time, "00:00",
              "Time to perform cluster rebalance, format in HH:MM");

DECLARE_string(collector_cluster_name);
DECLARE_string(collector_master_addrs);
DECLARE_int32(collector_interval_sec);
DECLARE_int32(collector_timeout_sec);

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace collector {

ClusterRebalancer::ClusterRebalancer()
  : initialized_(false),
    stop_background_threads_latch_(1) {
}

ClusterRebalancer::~ClusterRebalancer() {
  Shutdown();
}

Status ClusterRebalancer::Init() {
  CHECK(!initialized_);

  RETURN_NOT_OK(ValidateHMTime(FLAGS_rebalance_time));

  initialized_ = true;
  return Status::OK();
}

Status ClusterRebalancer::Start() {
  CHECK(initialized_);

  if (!FLAGS_auto_rebalance) {
    return Status::OK();
  }

  RETURN_NOT_OK(StartClusterRebalancerThread());

  return Status::OK();
}

void ClusterRebalancer::Shutdown() {
  if (initialized_) {
    string name = ToString();
    LOG(INFO) << name << " shutting down...";

    stop_background_threads_latch_.CountDown();

    if (cluster_rebalancer_thread_) {
      cluster_rebalancer_thread_->Join();
    }

    LOG(INFO) << name << " shutdown complete.";
  }
}

string ClusterRebalancer::ToString() const {
  return "ClusterRebalancer";
}

Status ClusterRebalancer::StartClusterRebalancerThread() {
  return Thread::Create("server", "cluster-rebalancer", &ClusterRebalancer::ClusterRebalancerThread,
                        this, &cluster_rebalancer_thread_);
}

void ClusterRebalancer::ClusterRebalancerThread() {
  const MonoDelta kWait = MonoDelta::FromSeconds(60);
  while (!RunOnceMode() && !stop_background_threads_latch_.WaitFor(kWait)) {
    string dst;
    StringAppendStrftime(&dst, "%H:%M", time(nullptr), true);
    if (dst == FLAGS_rebalance_time) {
      WARN_NOT_OK(RebalanceCluster(), "Unable to rebalance cluster");
    }
  }
}

Status ClusterRebalancer::RebalanceCluster() {
  vector<string> args = {
    "cluster",
    "rebalance",
    FLAGS_collector_master_addrs
  };
  string tool_stdout;
  string tool_stderr;
  RETURN_NOT_OK_PREPEND(tools::RunKuduTool(args, &tool_stdout, &tool_stderr),
                        Substitute("out: $0, err: $1", tool_stdout, tool_stderr));
  LOG(INFO) << std::endl
            << tool_stdout;
  return Status::OK();
}

Status ClusterRebalancer::ValidateHMTime(const string& time) {
  Status err = Status::InvalidArgument(
      Substitute("Invalid time format '$0', should in format 'HH:MM'", time));
  if (time.size() != 5) {
    return err;
  }

  int hour, minute;
  int count = sscanf(time.c_str(), "%d:%d", &hour, &minute);
  if (count == 2 &&
      0 <= hour && hour < 24 &&
      0 <= minute && minute < 60) {
    return Status::OK();
  }

  return err;
}
} // namespace collector
} // namespace kudu
