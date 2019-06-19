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
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <kudu/util/curl_util.h>

#include "kudu/collector/tool_util.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/init.h"
#include "kudu/security/security_flags.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

DEFINE_int64(collector_timeout_sec, 10,
            "Number of seconds to wait for a master/tserver to return metrics");

DECLARE_int32(dns_resolver_max_threads_num);
DECLARE_uint32(dns_resolver_cache_capacity_mb);
DECLARE_uint32(dns_resolver_cache_ttl_sec);
DECLARE_string(principal);
DECLARE_string(keytab_file);

using rapidjson::Value;
using std::string;
using std::vector;
using std::unordered_map;
using strings::Substitute;

namespace kudu {
namespace collector {

Collector::Collector(const CollectorOptions& opts)
  : initted_(false),
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

Status Collector::InitMetrics(const std::string& tserver_http_addr) {
  string result;
  RETURN_NOT_OK(GetMetrics(tserver_http_addr + "&include_schema=1", &result));
  JsonReader r(result);
  RETURN_NOT_OK(r.Init());
  vector<const Value*> entities;
  RETURN_NOT_OK(r.ExtractObjectArray(r.root(), nullptr, &entities));
  unordered_map<string, string> type_by_metric_name;
  for (const Value* entity : entities) {
    string entity_type;
    CHECK_OK(r.ExtractString(entity, "name", &entity_type));
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

Status Collector::GetMetrics(const std::string& tserver_http_addr, string* result) {
  CHECK(result);
  EasyCurl curl;
  faststring dst;
  RETURN_NOT_OK(curl.FetchURL(tserver_http_addr, &dst));
  *result = dst.ToString();
  return Status::OK();
}

Status Collector::Init() {
  CHECK(!initted_);

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
  RETURN_NOT_OK(InitMetrics(tserver_http_addrs_[0]));

  RETURN_NOT_OK(StartExcessLogFileDeleterThread());
  RETURN_NOT_OK(StartNodesUpdaterThread());
  RETURN_NOT_OK(StartMetricCollectorThread());

  initted_ = true;
  return Status::OK();
}

Status Collector::Start() {
  CHECK(initted_);

  google::FlushLogFiles(google::INFO); // Flush the startup messages.

  return Status::OK();
}

void Collector::Shutdown() {
  if (initted_) {
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
