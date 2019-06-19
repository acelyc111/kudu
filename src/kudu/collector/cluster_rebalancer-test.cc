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

#include "kudu/collector/cluster_rebalancer.h"

#include <gtest/gtest.h>

#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

namespace kudu {
namespace collector {

TEST(TestClusterRebalancer, TestValidateHMTime) {
  // 'time' in error format.
  ASSERT_TRUE(ClusterRebalancer::ValidateHMTime("12:34:56").IsInvalidArgument());
  ASSERT_TRUE(ClusterRebalancer::ValidateHMTime("1:23").IsInvalidArgument());
  ASSERT_TRUE(ClusterRebalancer::ValidateHMTime("12:3").IsInvalidArgument());
  ASSERT_TRUE(ClusterRebalancer::ValidateHMTime("12:").IsInvalidArgument());
  ASSERT_TRUE(ClusterRebalancer::ValidateHMTime(":3").IsInvalidArgument());
  ASSERT_TRUE(ClusterRebalancer::ValidateHMTime("12.34").IsInvalidArgument());
  ASSERT_TRUE(ClusterRebalancer::ValidateHMTime("-1:30").IsInvalidArgument());
  ASSERT_TRUE(ClusterRebalancer::ValidateHMTime("24:30").IsInvalidArgument());
  ASSERT_TRUE(ClusterRebalancer::ValidateHMTime("12:-1").IsInvalidArgument());
  ASSERT_TRUE(ClusterRebalancer::ValidateHMTime("12:60").IsInvalidArgument());

  // 'time' in valid format.
  ASSERT_OK(ClusterRebalancer::ValidateHMTime("12:34"));
  ASSERT_OK(ClusterRebalancer::ValidateHMTime("00:00"));
  ASSERT_OK(ClusterRebalancer::ValidateHMTime("00:59"));
  ASSERT_OK(ClusterRebalancer::ValidateHMTime("23:00"));
  ASSERT_OK(ClusterRebalancer::ValidateHMTime("23:59"));
}
}  // namespace collector
}  // namespace kudu

