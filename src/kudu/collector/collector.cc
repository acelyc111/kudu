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

#include "kudu/cfile/block_cache.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/service_if.h"
#include "kudu/tserver/heartbeater.h"
#include "kudu/tserver/scanners.h"
#include "kudu/tserver/tablet_copy_service.h"
#include "kudu/tserver/tablet_service.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver_path_handlers.h"
#include "kudu/util/maintenance_manager.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

using std::string;
using std::vector;

namespace kudu {
namespace collector {

Collector::Collector(const CollectorOptions& opts)
  : KuduServer("Collector", opts, "collector"),
    initted_(false),
    opts_(opts),
    path_handlers_(new TabletServerPathHandlers(this)) {
}

Collector::~Collector() {
  Shutdown();
}

string Collector::ToString() const {
  return "Collector";
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

  RETURN_NOT_OK(KuduServer::Init());

  initted_ = true;
  return Status::OK();
}

Status Collector::Start() {
  CHECK(initted_);

  RETURN_NOT_OK(KuduServer::Start());
  google::FlushLogFiles(google::INFO); // Flush the startup messages.

  return Status::OK();
}

void Collector::Shutdown() {
  if (initted_) {
    string name = ToString();
    LOG(INFO) << name << " shutting down...";

    // 1. Shut down generic subsystems.
    KuduServer::Shutdown();

    LOG(INFO) << name << " shutdown complete.";
  }
}

} // namespace collector
} // namespace kudu
