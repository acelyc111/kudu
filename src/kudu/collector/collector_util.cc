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
//
// Utility functions for generating data for use by tools and tests.

#include "kudu/collector/collector_util.h"

#include <stddef.h>

#include <gflags/gflags_declare.h>

DECLARE_string(collector_report_method);

using std::string;

namespace kudu {
namespace collector {

string ExtractHostName(const string& url) {
  size_t pos = url.find(':');
  if (pos == string::npos) {
    return url;
  }
  return url.substr(0, pos);
}

bool RunOnceMode() {
  static bool run_once = (FLAGS_collector_report_method == "local");
  return run_once;
}
} // namespace collector
} // namespace kudu
