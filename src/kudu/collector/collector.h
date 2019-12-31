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
#pragma once

#include <string>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"

namespace kudu {
class Thread;
}  // namespace kudu

namespace kudu {

namespace collector {

class ClusterRebalancer;
class MetricsCollector;
class NodesChecker;
class ReporterBase;

class Collector {
 public:
  Collector();
  ~Collector();

  Status Init();
  Status Start();
  void Shutdown();

  std::string ToString() const;

 private:
  FRIEND_TEST(TestCollector, TestValidateIntervalAndTimeout);

  // Start thread to remove excess glog files.
  Status StartExcessLogFileDeleterThread();
  void ExcessLogFileDeleterThread();

  static Status ValidateFlags();

  bool initialized_;

  scoped_refptr<ReporterBase> reporter_;
  scoped_refptr<MetricsCollector> metrics_collector_;
  scoped_refptr<NodesChecker> nodes_checker_;
  scoped_refptr<ClusterRebalancer> cluster_rebalancer_;

  CountDownLatch stop_background_threads_latch_;
  scoped_refptr<Thread> excess_log_deleter_thread_;

  DISALLOW_COPY_AND_ASSIGN(Collector);
};
} // namespace collector
} // namespace kudu
