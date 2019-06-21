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
#include <unordered_map>

#include <rapidjson/document.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/server/server_base.h"
#include "kudu/collector/collector_options.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/threadpool.h"
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
  typedef std::unordered_map<std::string, int64_t> Metrics;
  typedef std::unordered_map<std::string, Metrics> TablesMetrics;
  struct SimpleHistogram {
    int64_t count;
    int64_t value;
    SimpleHistogram(int64_t c, int64_t v) : count(c), value(v) {
    }
  };
  typedef std::unordered_map<std::string, std::vector<SimpleHistogram>> HistMetrics;
  typedef std::unordered_map<std::string, HistMetrics> TablesHistMetrics;
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

  Status InitMetrics(const std::string& url);
  Status InitFilters();

  Status GetAndMergeMetrics(const std::string& url, string* host_name);
  Status GetMetrics(const std::string& url, std::string* resp);
  Status ParseServerMetrics(const JsonReader& r,
                            const rapidjson::Value* entity);
  Status ParseTableMetrics(const JsonReader& r,
                           const rapidjson::Value* entity,
                           TablesMetrics* tables_metrics,
                           Metrics* host_metrics,
                           TablesHistMetrics* tables_hist_metrics,
                           HistMetrics* host_hist_metrics);
  Status ParseTabletMetrics(const JsonReader& r,
                            const rapidjson::Value* entity);

  std::string ExtractHostName(const std::string& url);
  struct FalconItem {
    FalconItem(std::string ep, std::string m, std::string t,
               uint64_t ts, int32_t s, int64_t v, std::string ct)
    : endpoint(std::move(ep)),
      metric(std::move(m)),
      tags(std::move(t)),
      timestamp(ts),
      step(s),
      value(v),
      counterType(std::move(ct)) {
    }
    std::string endpoint;
    std::string metric;
    std::string tags;
    uint64_t timestamp;
    int32_t step;
    int64_t value;
    std::string counterType;
  };
  FalconItem ContructFalconItem(std::string endpoint,
                                std::string metric,
                                std::string level,
                                uint64_t timestamp,
                                int64_t value,
                                std::string counter_type,
                                std::string extra_tags = "");

  enum class MetricValueType {
    kInt = 0,
    kString
  };
  bool FilterByAttribute(const JsonReader& r,
                         const rapidjson::Value* entity);
  MetricValueType GetMetricValueType(const std::string& metric_name);
  Status GetIntMetricValue(const JsonReader& r,
                           const rapidjson::Value* metric,
                           const std::string& metric_name,
                           int64_t* result);
  Status GetStringMetricValue(const JsonReader& r,
                              const rapidjson::Value* metric,
                              const std::string& metric_name,
                              int64_t* result);

  Status UpdateThreadPool(uint32_t thread_count);

  static const std::set<std::string> string_value_metrics_;
  static const std::set<std::string> rigister_percentiles_;

  bool initialized_;

  std::string master_addrs_;
  std::vector<std::string> tserver_http_addrs_;
  std::unordered_map<std::string, std::string> type_by_metric_name_;
  std::unordered_map<std::string, std::set<std::string>> entity_values_filter_by_entity_name_;

  // The options passed at construction time.
  const CollectorOptions opts_;

  CountDownLatch stop_background_threads_latch_;

  // Utility object for DNS name resolutions.
  std::unique_ptr<DnsResolver> dns_resolver_;

  scoped_refptr<Thread> nodes_updater_thread_;
  scoped_refptr<Thread> metric_collector_thread_;
  scoped_refptr<Thread> excess_log_deleter_thread_;
  gscoped_ptr<ThreadPool> host_metric_collector_thread_pool_;

  DISALLOW_COPY_AND_ASSIGN(Collector);
};

} // namespace collector
} // namespace kudu
