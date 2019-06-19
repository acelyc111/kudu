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
#include <unordered_map>

#include <rapidjson/document.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/server/server_base.h"
#include "kudu/tools/ksck_results.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"

namespace kudu {

namespace collector {

struct ItemBase : public RefCounted<ItemBase> {
  virtual ~ItemBase() = default;

 private:
  friend class RefCounted<ItemBase>;
};

class ReporterBase : public RefCounted<ReporterBase> {
 public:
  virtual ~ReporterBase() = default;

  virtual Status Init() = 0;
  virtual Status Start() = 0;
  virtual void Shutdown() = 0;

  virtual std::string ToString() const = 0;

  // TODO(yingchun) This function is not generic enough for base class.
  virtual scoped_refptr<ItemBase> ConstructItem(std::string endpoint,
                                                std::string metric,
                                                std::string level,
                                                uint64_t timestamp,
                                                int64_t value,
                                                std::string counter_type,
                                                std::string extra_tags) = 0;
  virtual Status PushItems(std::list<scoped_refptr<ItemBase>> items) = 0;

 protected:
  friend class RefCounted<ReporterBase>;
};
} // namespace collector
} // namespace kudu

