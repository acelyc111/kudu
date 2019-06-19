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
#pragma once

#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include <gtest/gtest_prod.h>

#include "kudu/collector/reporter_base.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"

namespace kudu {
class ThreadPool;
}  // namespace kudu

namespace kudu {

namespace collector {

// Open-Falcon is a distributed and high-performance monitoring system,
// see more details http://open-falcon.com
struct FalconItem : public ItemBase {
  FalconItem(std::string ep, std::string m, std::string t,
             uint64_t ts, int32_t s, int64_t v, std::string ct)
  : endpoint(std::move(ep)),
    metric(std::move(m)),
    tags(std::move(t)),
    timestamp(ts),
    step(s),
    value(v),
    counter_type(std::move(ct)) {
  }
  ~FalconItem() override = default;

  std::string endpoint;
  std::string metric;
  std::string tags;
  uint64_t timestamp;
  int32_t step;
  int64_t value;
  std::string counter_type;
};

class FalconReporter : public ReporterBase {
 public:
  FalconReporter();
  ~FalconReporter() override;

  Status Init() override;
  Status Start() override;
  void Shutdown() override;

  std::string ToString() const override;

  scoped_refptr<ItemBase> ConstructItem(std::string endpoint,
                                        std::string metric,
                                        std::string level,
                                        uint64_t timestamp,
                                        int64_t value,
                                        std::string counter_type,
                                        std::string extra_tags) override;

  Status PushItems(std::list<scoped_refptr<ItemBase>> items) override;

 private:
  FRIEND_TEST(TestFalconReporter, TestSerializeItems);
  FRIEND_TEST(TestFalconReporter, TestPushAndPopItems);

  Status StartFalconPusherThreadPool();
  void FalconPusher();

  bool HasItems() const;
  void ReportItems();
  void PopItems(std::list<scoped_refptr<ItemBase>>* falcon_items);
  static Status PushToAgent(std::list<scoped_refptr<ItemBase>> falcon_items);
  static Status SerializeItems(std::list<scoped_refptr<ItemBase>> items, std::string* data);

  bool initialized_;

  CountDownLatch stop_background_threads_latch_;
  std::unique_ptr<ThreadPool> pusher_thread_pool_;

  mutable RWMutex items_lock_;
  std::list<scoped_refptr<ItemBase>> buffer_items_;
};
} // namespace collector
} // namespace kudu
