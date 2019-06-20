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
#include <set>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <kudu/util/curl_util.h>

#include "kudu/collector/tool_util.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/init.h"
#include "kudu/security/security_flags.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

DEFINE_int64(collector_timeout_sec, 10,
             "Number of seconds to wait for a master/tserver to return metrics");
DEFINE_string(collector_metrics, "",
              "Metrics to collect (comma-separated list of metric names)");
DEFINE_string(collector_attributes, "",
              "Entity attributes to collect (semicolon-separated list of entity attribute "
              "name and values). e.g. attr_name1:attr_val1,attr_val2;attr_name2:attr_val3");
DEFINE_bool(collector_local_stat, false,
            "Whether to calculate statistics on local host");

DECLARE_int32(dns_resolver_max_threads_num);
DECLARE_uint32(dns_resolver_cache_capacity_mb);
DECLARE_uint32(dns_resolver_cache_ttl_sec);
DECLARE_string(principal);
DECLARE_string(keytab_file);

using rapidjson::Value;
using std::set;
using std::string;
using std::vector;
using std::unordered_map;
using strings::Substitute;

namespace kudu {
namespace collector {

const set<string> Collector::string_value_metrics_ = {"state"};
const set<string> Collector::rigister_percentiles_ = {"percentile_99"};

Collector::Collector(const CollectorOptions& opts)
  : initialized_(false),
    opts_(opts),
    stop_background_threads_latch_(1),
    dns_resolver_(new DnsResolver(
        FLAGS_dns_resolver_max_threads_num,
        FLAGS_dns_resolver_cache_capacity_mb * 1024 * 1024,
        MonoDelta::FromSeconds(FLAGS_dns_resolver_cache_ttl_sec))) {
}

Collector::~Collector() {
  Shutdown();
}

string Collector::ToString() const {
  return "Collector";
}

Status Collector::StartNodesUpdaterThread() {
  return Thread::Create("server", "nodes-updater", &Collector::NodesUpdaterThread,
                        this, &nodes_updater_thread_);
}

void Collector::NodesUpdaterThread() {
  const MonoDelta kWait = MonoDelta::FromSeconds(60);
  while (!stop_background_threads_latch_.WaitFor(kWait)) {
    WARN_NOT_OK(UpdateNodes(), "Unable to update nodes");
  }
}

Status Collector::UpdateNodes() {
  vector<string> args = {
    "tserver",
    "list",
    master_addrs_,
    "-columns=http-addresses",
    "-format=json",
    Substitute("-timeout_ms=$0", FLAGS_collector_timeout_sec)
  };
  string tool_stdout;
  string tool_stderr;
  RETURN_NOT_OK(RunKuduTool(args, &tool_stdout, &tool_stderr));
  JsonReader r(tool_stdout);
  RETURN_NOT_OK(r.Init());
  vector<const Value*> objs;
  CHECK_OK(r.ExtractObjectArray(r.root(), nullptr, &objs));
  vector<string> tserver_http_addrs;
  for (const Value* v : objs) {
    string http_address;
    CHECK_OK(r.ExtractString(v, "http-addresses", &http_address));
    tserver_http_addrs.emplace_back(http_address);
  }
  tserver_http_addrs_.swap(tserver_http_addrs);

  return Status::OK();
}

Status Collector::StartMetricCollectorThread() {
  return Thread::Create("server", "metric-collector", &Collector::MetricCollectorThread,
                        this, &metric_collector_thread_);
}

void Collector::MetricCollectorThread() {
  const MonoDelta kWait = MonoDelta::FromSeconds(60);
  while (!stop_background_threads_latch_.WaitFor(kWait)) {
    WARN_NOT_OK(CollectMetrics(), "Unable to collect metrics");
  }
}

Status Collector::CollectMetrics() {
  RETURN_NOT_OK(UpdateThreadPool(tserver_http_addrs_.size()));
  string parameters = "/metrics?compact=1";
  if (!FLAGS_collector_metrics.empty()) {
    parameters += "&metrics=" + FLAGS_collector_metrics;
  }
  if (!FLAGS_collector_local_stat) {
    parameters += "&origin=false&merge=true";
  }
  for (const auto& tserver_http_addr : tserver_http_addrs_) {
    RETURN_NOT_OK(host_metric_collector_thread_pool_->SubmitFunc(
      boost::bind(&Collector::GetAndMergeMetrics,
                  this,
                  tserver_http_addr + parameters)));
  }
  host_metric_collector_thread_pool_->Wait();
  return Status::OK();
}

Status Collector::StartExcessLogFileDeleterThread() {
  // Try synchronously deleting excess log files once at startup to make sure it
  // works, then start a background thread to continue deleting them in the
  // future.
  if (!FLAGS_logtostderr) {
    RETURN_NOT_OK_PREPEND(DeleteExcessLogFiles(opts_.env),
                          "Unable to delete excess log files");
  }
  return Thread::Create("server", "excess-log-deleter", &Collector::ExcessLogFileDeleterThread,
                        this, &excess_log_deleter_thread_);
}

void Collector::ExcessLogFileDeleterThread() {
  // How often to attempt to clean up excess glog files.
  const MonoDelta kWait = MonoDelta::FromSeconds(60);
  while (!stop_background_threads_latch_.WaitFor(kWait)) {
    WARN_NOT_OK(DeleteExcessLogFiles(opts_.env), "Unable to delete excess log files");
  }
}

Status Collector::InitMetrics(const std::string& url) {
  string resp;
  RETURN_NOT_OK(GetMetrics(url, &resp));
  JsonReader r(resp);
  RETURN_NOT_OK(r.Init());
  vector<const Value*> entities;
  RETURN_NOT_OK(r.ExtractObjectArray(r.root(), nullptr, &entities));
  unordered_map<string, string> type_by_metric_name;
  for (const Value* entity : entities) {
    string entity_type;
    CHECK_OK(r.ExtractString(entity, "type", &entity_type));
    if (entity_type != "tablet" && entity_type != "table") {
      continue;
    }
    vector<const Value*> metrics;
    CHECK_OK(r.ExtractObjectArray(entity, "metrics", &metrics));
    for (const Value* metric : metrics) {
      string name;
      CHECK_OK(r.ExtractString(metric, "name", &name));
      string type;
      CHECK_OK(r.ExtractString(metric, "type", &type));
      type_by_metric_name.emplace(name, type);
    }
  }
  type_by_metric_name_.swap(type_by_metric_name);
  return Status::OK();
}

Status Collector::InitFilters() {
  unordered_map<string, set<string>> entity_values_filter_by_entity_name;
  vector<string> attribute_values_by_name =
      Split(FLAGS_collector_attributes, ";", strings::SkipEmpty());
  for (const auto& seg : attribute_values_by_name) {
    vector<string> key_and_values = Split(seg, ":", strings::SkipEmpty());
    CHECK_EQ(key_and_values.size(), 2);
    vector<string> values = Split(key_and_values[1], ",", strings::SkipEmpty());
    CHECK_GT(values.size(), 0);
    auto& attrs = entity_values_filter_by_entity_name.insert(std::make_pair(key_and_values[0], set<string>())).first->second;
    for (const auto& value : values) {
      attrs.emplace(value);
    }
  }
  entity_values_filter_by_entity_name_.swap(entity_values_filter_by_entity_name);
  return Status::OK();
}

Status Collector::UpdateThreadPool(uint32_t thread_count) {
  if (host_metric_collector_thread_pool_->num_threads() == thread_count) {
    return Status::OK();
  }
  host_metric_collector_thread_pool_->Shutdown();
  RETURN_NOT_OK(ThreadPoolBuilder("host_metric_collector")
      .set_max_threads(thread_count)
      .set_idle_timeout(MonoDelta::FromMilliseconds(1))
      .Build(&host_metric_collector_thread_pool_));
  return Status::OK();
}

Collector::MetricValueType Collector::GetMetricValueType(const std::string& metric_name) {
  if (ContainsKey(string_value_metrics_, metric_name)) {
    return MetricValueType::kString;
  }
  return MetricValueType::kInt;
}

Status Collector::GetIntMetricValue(const JsonReader& r,
                                    const rapidjson::Value* metric,
                                    const std::string& metric_name,
                                    int64_t* result) {
  CHECK(result);
  return r.ExtractInt64(metric, "value", result);
}

Status Collector::GetStringMetricValue(const JsonReader& r,
                                       const Value* metric,
                                       const std::string& metric_name,
                                       int64_t* result) {
  CHECK(result);
  string value;
  RETURN_NOT_OK(r.ExtractString(metric, "value", &value));
  if (metric_name == "state") {
    if (value == "RUNNING") {
      *result = 1;
    } else {
      *result = 0;
    }
  }
  return Status::OK();
}

bool Collector::FilterByAttribute(const JsonReader& r,
                                  const rapidjson::Value* entity) {
  if (entity_values_filter_by_entity_name_.empty()) {
    return false;
  }
  const Value* attributes;
  CHECK_OK(r.ExtractObject(entity, "attributes", &attributes));
  for (const auto& name_values : entity_values_filter_by_entity_name_) {
    string value;
    Status s = r.ExtractString(attributes, name_values.first.c_str(), &value);
    if (s.ok() && ContainsKey(name_values.second, value)) {
      return false;
    }
  }
  return true;
}

Status Collector::ParseServerMetrics(const JsonReader& r,
                                     const rapidjson::Value* entity) {
  return Status::OK();
}

Status Collector::ParseTableMetrics(const JsonReader& r,
                                    const rapidjson::Value* entity,
                                    TablesMetrics* tables_metrics,
                                    Metrics* host_metrics,
                                    TablesHistMetrics* tables_hist_metrics,
                                    HistMetrics* host_hist_metrics) {
  CHECK(tables_metrics);
  CHECK(host_metrics);
  CHECK(tables_hist_metrics);
  CHECK(host_hist_metrics);
  string table_name;
  CHECK_OK(r.ExtractString(entity, "id", &table_name));
  CHECK(!ContainsKey(*tables_metrics, table_name));
  CHECK(!ContainsKey(*tables_hist_metrics, table_name));
  auto& table_metrics = tables_metrics->insert(std::make_pair(table_name, Metrics())).first->second;
  auto& table_hist_metrics = tables_hist_metrics->insert(std::make_pair(table_name, HistMetrics())).first->second;

  vector<const Value*> metrics;
  CHECK_OK(r.ExtractObjectArray(entity, "metrics", &metrics));
  for (const Value* metric : metrics) {
    string name;
    CHECK_OK(r.ExtractString(metric, "name", &name));
    const auto* known_type = FindOrNull(type_by_metric_name_, name);
    if (!known_type) {
      continue;
    }
    if (*known_type == "GAUGE" || *known_type ==  "COUNTER") {
      int64_t result;
      MetricValueType type = GetMetricValueType(name);
      switch (type) {
        case MetricValueType::kString:
          CHECK_OK(GetStringMetricValue(r, metric, name, &result));
          break;
        case MetricValueType::kInt:
          CHECK_OK(GetIntMetricValue(r, metric, name, &result));
          break;
        default:
          LOG(FATAL) << "Unknown type, metrics name: " << name;
      }
      CHECK(!ContainsKey(table_metrics, name));
      table_metrics.insert({{name, result}});
      auto& host_metric = host_metrics->insert(std::make_pair(name, 0)).first->second;
      host_metric += result;
    } else if (*known_type == "HISTOGRAM") {
      for (const auto& percentile : rigister_percentiles_) {
        string hist_metric_name = name + percentile;
        int64_t total_count;
        CHECK_OK(r.ExtractInt64(metric, "total_count", &total_count));
        int64_t value;
        CHECK_OK(r.ExtractInt64(metric, percentile.c_str(), &value));
        CHECK(!ContainsKey(table_hist_metrics, hist_metric_name));
        table_hist_metrics.insert({{hist_metric_name, {SimpleHistogram(total_count, value)}}});
        std::vector<SimpleHistogram> tmp({SimpleHistogram(total_count, value)});
        auto& host_hist_metric = host_hist_metrics->insert(std::make_pair(hist_metric_name, tmp)).first->second;
        host_hist_metric.emplace_back(SimpleHistogram(total_count, value));
      }
    } else {
      LOG(FATAL) << "Unknown metric type: " << *known_type;
    }
  }
  return Status::OK();
}

Status Collector::ParseTabletMetrics(const JsonReader& r,
                                     const rapidjson::Value* entity) {
  vector<const Value*> metrics;
  CHECK_OK(r.ExtractObjectArray(entity, "metrics", &metrics));
  for (const Value* metric : metrics) {
    string name;
    CHECK_OK(r.ExtractString(metric, "name", &name));
    const auto* known_type = FindOrNull(type_by_metric_name_, name);
    if (!known_type) {
      continue;
    }
    if (*known_type == "GAUGE" || *known_type ==  "COUNTER") {
      int64_t result;
      MetricValueType type = GetMetricValueType(name);
      switch (type) {
        case MetricValueType::kString:
          CHECK_OK(GetStringMetricValue(r, metric, name, &result));
          break;
        case MetricValueType::kInt:
          CHECK_OK(GetIntMetricValue(r, metric, name, &result));
          break;
        default:
          LOG(FATAL) << "Unknown type, metrics nameL " << name;
      }
    } else if (*known_type == "HISTOGRAM") {

    } else {

    }
  }
  return Status::OK();
}

Status Collector::GetAndMergeMetrics(const std::string& url) {
  string resp;
  RETURN_NOT_OK(GetMetrics(url, &resp));
  JsonReader r(resp);
  RETURN_NOT_OK(r.Init());
  vector<const Value*> entities;
  RETURN_NOT_OK(r.ExtractObjectArray(r.root(), nullptr, &entities));

  TablesMetrics tables_metrics;
  Metrics host_metrics;
  TablesHistMetrics tables_hist_metrics;
  HistMetrics host_hist_metrics;
  for (const Value* entity : entities) {
    if (FilterByAttribute(r, entity)) {
      continue;
    }
    string entity_type;
    CHECK_OK(r.ExtractString(entity, "type", &entity_type));
    if (entity_type == "server") {
      ParseServerMetrics(r, entity);
    } else if (entity_type == "table") {
      ParseTableMetrics(r, entity,
                        &tables_metrics, &host_metrics,
                        &tables_hist_metrics, &host_hist_metrics);
    } else if (entity_type == "tablet") {
      ParseTabletMetrics(r, entity);
    } else {
    }
  }
  return Status::OK();
}

Status Collector::GetMetrics(const std::string& url, string* resp) {
  CHECK(resp);
  EasyCurl curl;
  faststring dst;
  RETURN_NOT_OK(curl.FetchURL(url, &dst));
  *resp = dst.ToString();
  return Status::OK();
}

Status Collector::Init() {
  CHECK(!initialized_);

  // Validate that the passed master address actually resolves.
  // We don't validate that we can connect at this point -- it should
  // be allowed to start the collector and the master in whichever order --
  // our collect thread will loop until successfully connecting.
  for (const auto& addr : opts_.master_addresses) {
    RETURN_NOT_OK_PREPEND(dns_resolver()->ResolveAddresses(addr, nullptr),
        strings::Substitute("couldn't resolve master service address '$0'",
                            addr.ToString()));
  }

  master_addrs_ = HostPort::ToCommaSeparatedString(opts_.master_addresses);

  RETURN_NOT_OK(security::InitKerberosForServer(FLAGS_principal, FLAGS_keytab_file));

  RETURN_NOT_OK(UpdateNodes());
  CHECK(!tserver_http_addrs_.empty());
  RETURN_NOT_OK(InitMetrics(tserver_http_addrs_[0] + "&include_schema=1"));
  RETURN_NOT_OK(UpdateThreadPool(tserver_http_addrs_.size()));
  RETURN_NOT_OK(InitFilters());

  RETURN_NOT_OK(StartExcessLogFileDeleterThread());
  RETURN_NOT_OK(StartNodesUpdaterThread());
  RETURN_NOT_OK(StartMetricCollectorThread());

  initialized_ = true;
  return Status::OK();
}

Status Collector::Start() {
  CHECK(initialized_);

  google::FlushLogFiles(google::INFO); // Flush the startup messages.

  return Status::OK();
}

void Collector::Shutdown() {
  if (initialized_) {
    string name = ToString();
    LOG(INFO) << name << " shutting down...";

    // Next, shut down remaining server components.
    stop_background_threads_latch_.CountDown();

    if (nodes_updater_thread_) {
      nodes_updater_thread_->Join();
    }

    if (metric_collector_thread_) {
      metric_collector_thread_->Join();
    }

    if (excess_log_deleter_thread_) {
      excess_log_deleter_thread_->Join();
    }

    LOG(INFO) << name << " shutdown complete.";
  }
}

} // namespace collector
} // namespace kudu