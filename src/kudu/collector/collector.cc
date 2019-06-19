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

#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/security/init.h"
#include "kudu/security/security_flags.h"
#include "kudu/util/logging.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

DECLARE_int32(dns_resolver_max_threads_num);
DECLARE_uint32(dns_resolver_cache_capacity_mb);
DECLARE_uint32(dns_resolver_cache_ttl_sec);
DECLARE_string(principal);
DECLARE_string(keytab_file);

using std::string;
using std::vector;

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

Status Collector::StartExcessLogFileDeleterThread() {
  // Try synchronously deleting excess log files once at startup to make sure it
  // works, then start a background thread to continue deleting them in the
  // future. Same with minidumps.
  if (!FLAGS_logtostderr) {
    RETURN_NOT_OK_PREPEND(DeleteExcessLogFiles(opts_.env),
                          "Unable to delete excess log files");
  }
  return Thread::Create("server", "excess-log-deleter", &Collector::ExcessLogFileDeleterThread,
                        this, &excess_log_deleter_thread_);
}

void Collector::ExcessLogFileDeleterThread() {
  // How often to attempt to clean up excess glog and minidump files.
  const MonoDelta kWait = MonoDelta::FromSeconds(60);
  while (!stop_background_threads_latch_.WaitUntil(MonoTime::Now() + kWait)) {
    WARN_NOT_OK(DeleteExcessLogFiles(opts_.env), "Unable to delete excess log files");
  }
}


Status Collector::Init() {
  CHECK(!initted_);

  UnorderedHostPortSet master_addrs;
  for (auto addr : opts_.master_addresses) {
    master_addrs.emplace(std::move(addr));
  }
  // If we deduplicated some masters addresses, log something about it.
  if (master_addrs.size() < opts_.master_addresses.size()) {
    vector<HostPort> addr_list;
    for (const auto& addr : master_addrs) {
      addr_list.emplace_back(addr);
    }
    LOG(INFO) << "deduplicated master addresses: "
              << HostPort::ToCommaSeparatedString(addr_list);
  }
  // Validate that the passed master address actually resolves.
  // We don't validate that we can connect at this point -- it should
  // be allowed to start the collector and the master in whichever order --
  // our collect thread will loop until successfully connecting.
  for (const auto& addr : master_addrs) {
    RETURN_NOT_OK_PREPEND(dns_resolver()->ResolveAddresses(addr, nullptr),
        strings::Substitute("couldn't resolve master service address '$0'",
                            addr.ToString()));
  }

  RETURN_NOT_OK(security::InitKerberosForServer(FLAGS_principal, FLAGS_keytab_file));
  RETURN_NOT_OK(StartExcessLogFileDeleterThread());

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

    if (excess_log_deleter_thread_) {
      excess_log_deleter_thread_->Join();
    }

    LOG(INFO) << name << " shutdown complete.";
  }
}

} // namespace collector
} // namespace kudu
