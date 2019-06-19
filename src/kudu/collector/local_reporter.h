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
#include <string>

#include "kudu/collector/reporter_base.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace kudu {

namespace collector {

class LocalReporter : public ReporterBase {
 public:
  LocalReporter();
  ~LocalReporter() override;

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
  bool initialized_;
  Mutex output_lock_;
};
} // namespace collector
} // namespace kudu
