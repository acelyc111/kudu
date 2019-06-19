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

#include <gtest/gtest.h>

#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

namespace kudu {
namespace collector {

TEST(TestCollector, TestValidateIntervalAndTimeout) {
  // 'interval' in error range.
  ASSERT_TRUE(Collector::ValidateIntervalAndTimeout(9, 1).IsInvalidArgument());
  ASSERT_TRUE(Collector::ValidateIntervalAndTimeout(61, 1).IsInvalidArgument());

  // 'timeout' in error range.
  ASSERT_TRUE(Collector::ValidateIntervalAndTimeout(10, 0).IsInvalidArgument());
  ASSERT_TRUE(Collector::ValidateIntervalAndTimeout(10, 10).IsInvalidArgument());

  // Both 'interval' and 'timeout' are in valid range.
  ASSERT_OK(Collector::ValidateIntervalAndTimeout(10, 9));
  ASSERT_OK(Collector::ValidateIntervalAndTimeout(60, 9));
  ASSERT_OK(Collector::ValidateIntervalAndTimeout(60, 59));
}
}  // namespace collector
}  // namespace kudu

