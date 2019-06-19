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

#include "kudu/collector/falcon_reporter.h"

#include <list>
#include <string>
#include <utility>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/collector/reporter_base.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/test_macros.h"

DECLARE_string(collector_cluster_name);
DECLARE_int32(collector_falcon_metrics_version);
DECLARE_int32(collector_interval_sec);

using std::list;
using std::string;
using strings::Substitute;

namespace kudu {
namespace collector {

TEST(TestFalconReporter, TestSerializeItems) {
  FLAGS_collector_interval_sec = 30;
  FLAGS_collector_cluster_name = "test";
  FLAGS_collector_falcon_metrics_version = 8;
  scoped_refptr<FalconReporter> reporter(new FalconReporter());
  list<scoped_refptr<ItemBase>> falcon_items;
  string data;
  ASSERT_OK(FalconReporter::SerializeItems(falcon_items, &data));
  ASSERT_EQ(data, "");

  falcon_items.emplace_back(reporter->ConstructItem(
    "tserver1",
    "scan_count",
    "host",
    1234567890,
    12345,
    "COUNTER",
    ""));
  ASSERT_OK(FalconReporter::SerializeItems(falcon_items, &data));
  ASSERT_EQ(data, Substitute(
                  R"*([{"endpoint":"tserver1","metric":"scan_count","timestamp":1234567890,)*"
                  R"*("step":$0,"value":12345,"counterType":"COUNTER",)*"
                  R"*("tags":"service=kudu,cluster=$1,level=host,v=$2"}])*",
                  FLAGS_collector_interval_sec,
                  FLAGS_collector_cluster_name,
                  FLAGS_collector_falcon_metrics_version));

  falcon_items.emplace_back(reporter->ConstructItem(
    "table1",
    "disk_size",
    "table",
    1234567891,
    67890,
    "GAUGE",
    ""));
  ASSERT_OK(FalconReporter::SerializeItems(falcon_items, &data));
  ASSERT_EQ(data, Substitute(
                  R"*([{"endpoint":"tserver1","metric":"scan_count","timestamp":1234567890,)*"
                  R"*("step":$0,"value":12345,"counterType":"COUNTER",)*"
                  R"*("tags":"service=kudu,cluster=$1,level=host,v=$2"},)*"
                  R"*({"endpoint":"table1","metric":"disk_size","timestamp":1234567891,)*"
                  R"*("step":$0,"value":67890,"counterType":"GAUGE",)*"
                  R"*("tags":"service=kudu,cluster=$1,level=table,v=$2"}])*",
                  FLAGS_collector_interval_sec,
                  FLAGS_collector_cluster_name,
                  FLAGS_collector_falcon_metrics_version));
}

void GenerateItems(const scoped_refptr<FalconReporter>& reporter, int count) {
  list<scoped_refptr<ItemBase>> items;
  for (int i = 0; i < count; ++i) {
    items.emplace_back(reporter->ConstructItem("endpoint", "metric", "level", 0, i, "GAUGE", ""));
  }
  reporter->PushItems(std::move(items));
}

TEST(TestFalconReporter, TestPushAndPopItems) {
  scoped_refptr<FalconReporter> reporter(new FalconReporter());
  ASSERT_FALSE(reporter->HasItems());
  NO_FATALS(GenerateItems(reporter, 1));
  ASSERT_TRUE(reporter->HasItems());
  NO_FATALS(GenerateItems(reporter, 9));
  ASSERT_TRUE(reporter->HasItems());

  list<scoped_refptr<ItemBase>> falcon_items;
  reporter->PopItems(&falcon_items);
  ASSERT_FALSE(reporter->HasItems());
  ASSERT_EQ(falcon_items.size(), 10);

  NO_FATALS(GenerateItems(reporter, 5));
  ASSERT_TRUE(reporter->HasItems());

  list<scoped_refptr<ItemBase>> falcon_items2;
  reporter->PopItems(&falcon_items2);
  ASSERT_FALSE(reporter->HasItems());
  ASSERT_EQ(falcon_items2.size(), 5);
}
}  // namespace collector
}  // namespace kudu

