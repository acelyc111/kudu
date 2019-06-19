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

#include "kudu/collector/local_reporter.h"

#include <iostream>

#include <glog/logging.h>

#include "kudu/util/status.h"

using std::list;
using std::string;

namespace kudu {
namespace collector {

LocalReporter::LocalReporter()
  : initialized_(false) {
}

LocalReporter::~LocalReporter() {
  Shutdown();
}

Status LocalReporter::Init() {
  CHECK(!initialized_);

  initialized_ = true;
  return Status::OK();
}

Status LocalReporter::Start() {
  CHECK(initialized_);

  return Status::OK();
}

void LocalReporter::Shutdown() {
  if (initialized_) {
    string name = ToString();
    LOG(INFO) << name << " shutting down...";

    LOG(INFO) << name << " shutdown complete.";
  }
}

string LocalReporter::ToString() const {
  return "LocalReporter";
}

scoped_refptr<ItemBase> LocalReporter::ConstructItem(string endpoint,
                                                     string metric,
                                                     string level,
                                                     uint64_t /*timestamp*/,
                                                     int64_t value,
                                                     string /*counter_type*/,
                                                     string extra_tags) {
  MutexLock l(output_lock_);
  std::cout << level << " " << metric << " " << endpoint << " "
      << (extra_tags.empty() ? "" : extra_tags + " ") << value << std::endl;
  return nullptr;
}

Status LocalReporter::PushItems(list<scoped_refptr<ItemBase>> /*items*/) {
  return Status::OK();
}

} // namespace collector
} // namespace kudu
