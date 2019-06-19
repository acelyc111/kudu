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

#include "kudu/collector/nodes_checker.h"

#include <gtest/gtest.h>

#include "kudu/rebalance/cluster_status.h"

using kudu::cluster_summary::ServerHealth;
using kudu::cluster_summary::HealthCheckResult;

namespace kudu {
namespace collector {

TEST(TestNodesChecker, TestExtractServerHealthStatus) {
  ASSERT_EQ(ServerHealth::HEALTHY,
            NodesChecker::ExtractServerHealthStatus("HEALTHY"));
  ASSERT_EQ(ServerHealth::UNAUTHORIZED,
            NodesChecker::ExtractServerHealthStatus("UNAUTHORIZED"));
  ASSERT_EQ(ServerHealth::UNAVAILABLE,
            NodesChecker::ExtractServerHealthStatus("UNAVAILABLE"));
  ASSERT_EQ(ServerHealth::WRONG_SERVER_UUID,
            NodesChecker::ExtractServerHealthStatus("WRONG_SERVER_UUID"));
}

TEST(TestNodesChecker, TestExtractTableHealthStatus) {
  ASSERT_EQ(HealthCheckResult::HEALTHY,
            NodesChecker::ExtractTableHealthStatus("HEALTHY"));
  ASSERT_EQ(HealthCheckResult::RECOVERING,
            NodesChecker::ExtractTableHealthStatus("RECOVERING"));
  ASSERT_EQ(HealthCheckResult::UNDER_REPLICATED,
            NodesChecker::ExtractTableHealthStatus("UNDER_REPLICATED"));
  ASSERT_EQ(HealthCheckResult::UNAVAILABLE,
            NodesChecker::ExtractTableHealthStatus("UNAVAILABLE"));
  ASSERT_EQ(HealthCheckResult::CONSENSUS_MISMATCH,
            NodesChecker::ExtractTableHealthStatus("CONSENSUS_MISMATCH"));
}
}  // namespace collector
}  // namespace kudu

