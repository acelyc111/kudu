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
#include <memory>
#include <string>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/server/server_base.h"
#include "kudu/collector/collector_options.h"
#include "kudu/util/status.h"

namespace kudu {

class DnsResolver;

namespace collector {

class Collector {
 public:
  explicit Collector(const CollectorOptions& opts);
  ~Collector();

  // Initializes the tablet server, including the bootstrapping of all
  // existing tablets.
  // Some initialization tasks are asynchronous, such as the bootstrapping
  // of tablets. Caller can block, waiting for the initialization to fully
  // complete by calling WaitInited().
  Status Init();

  Status Start();
  void Shutdown();

  DnsResolver* dns_resolver() { return dns_resolver_.get(); }

  std::string ToString() const;

 private:
  // Start thread to remove excess glog and minidump files.
  Status StartExcessLogFileDeleterThread();
  void ExcessLogFileDeleterThread();

  bool initted_;

  // The options passed at construction time.
  const CollectorOptions opts_;

  CountDownLatch stop_background_threads_latch_;

  // Utility object for DNS name resolutions.
  std::unique_ptr<DnsResolver> dns_resolver_;

  scoped_refptr<Thread> excess_log_deleter_thread_;

  DISALLOW_COPY_AND_ASSIGN(Collector);
};

} // namespace collector
} // namespace kudu
