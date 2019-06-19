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

#include <string>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"

namespace kudu {
class Thread;
}  // namespace kudu

namespace kudu {

namespace collector {

class ClusterRebalancer : public RefCounted<ClusterRebalancer> {
 public:
  ClusterRebalancer();
  ~ClusterRebalancer();

  Status Init();
  Status Start();
  void Shutdown();

  std::string ToString() const;

 private:
  friend class RefCounted<ClusterRebalancer>;

  FRIEND_TEST(TestClusterRebalancer, TestValidateHMTime);

  // Start thread to rebalance cluster.
  Status StartClusterRebalancerThread();
  void ClusterRebalancerThread();
  static Status RebalanceCluster();

  static Status ValidateHMTime(const std::string& time);

  bool initialized_;

  CountDownLatch stop_background_threads_latch_;

  scoped_refptr<Thread> cluster_rebalancer_thread_;

  DISALLOW_COPY_AND_ASSIGN(ClusterRebalancer);
};
} // namespace collector
} // namespace kudu
