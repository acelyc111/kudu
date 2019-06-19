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

#include <cstdint>
#include <memory>
#include <string>
#include <bits/unordered_map.h>
#include <unordered_map>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/server/server_base.h"
#include "kudu/collector/collector_options.h"
#include "kudu/util/status.h"

namespace kudu {

class DnsResolver;

namespace collector {

class Collector {
 public:
  explicit Collector(const CollectorOptions& opts);
  ~Collector();

  // Initializes the tablet server, including the bootstrapping of all
  // existing tablets.
  // Some initialization tasks are asynchronous, such as the bootstrapping
  // of tablets. Caller can block, waiting for the initialization to fully
  // complete by calling WaitInited().
  Status Init();

  Status Start();
  void Shutdown();

  DnsResolver* dns_resolver() { return dns_resolver_.get(); }

  std::string ToString() const;

 private:
  // Start thread to update nodes in the cluster.
  Status StartNodesUpdaterThread();
  void NodesUpdaterThread();
  Status UpdateNodes();

  // Start thread to collect metrics from servers.
  Status StartMetricCollectorThread();
  void MetricCollectorThread();
  Status CollectMetrics();

  // Start thread to remove excess glog and minidump files.
  Status StartExcessLogFileDeleterThread();
  void ExcessLogFileDeleterThread();

  Status InitMetrics(const std::string& tserver_http_addr);

  Status GetMetrics(const std::string& tserver_http_addr, string* result);

  bool initted_;

  std::string master_addrs_;
  std::vector<std::string> tserver_http_addrs_;
  std::unordered_map<std::string, std::string> type_by_metric_name_;

  // The options passed at construction time.
  const CollectorOptions opts_;

  CountDownLatch stop_background_threads_latch_;

  // Utility object for DNS name resolutions.
  std::unique_ptr<DnsResolver> dns_resolver_;

  scoped_refptr<Thread> nodes_updater_thread_;
  scoped_refptr<Thread> metric_collector_thread_;
  scoped_refptr<Thread> excess_log_deleter_thread_;

  DISALLOW_COPY_AND_ASSIGN(Collector);
};

} // namespace collector
} // namespace kudu
