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

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DECLARE_string(collector_cluster_name);
DECLARE_string(collector_master_addrs);
DECLARE_uint32(collector_interval_sec);
DECLARE_uint32(collector_timeout_sec);

using std::vector;

namespace kudu {
namespace collector {

TEST(TestCollector, TestValidateIntervalAndTimeout) {
  FLAGS_collector_cluster_name = "test";
  FLAGS_collector_master_addrs = "127.0.0.1:1234";
  vector<std::pair<uint32_t, uint32_t>> invalid_arguments({{9, 1},
                                                           {61, 1},
                                                           {10, 0},
                                                           {10, 10}});
  for (const auto& arguments : invalid_arguments) {
    FLAGS_collector_interval_sec = arguments.first;
    FLAGS_collector_timeout_sec = arguments.second;
    ASSERT_TRUE(Collector::ValidateFlags().IsInvalidArgument())
        << FLAGS_collector_interval_sec << ", " << FLAGS_collector_timeout_sec;
  }

  vector<std::pair<uint32_t, uint32_t>> valid_arguments({{10, 9},
                                                         {60, 9},
                                                         {60, 59}});
  for (const auto& arguments : valid_arguments) {
    FLAGS_collector_interval_sec = arguments.first;
    FLAGS_collector_timeout_sec = arguments.second;
    ASSERT_OK(Collector::ValidateFlags());
  }
}
}  // namespace collector
}  // namespace kudu

