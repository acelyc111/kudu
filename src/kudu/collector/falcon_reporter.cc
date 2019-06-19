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

#include <kudu/util/curl_util.h>
#include <stddef.h>

#include <algorithm>
#include <functional>
#include <iterator>
#include <mutex>
#include <ostream>
#include <utility>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/faststring.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

DEFINE_bool(collector_direct_push, false,
            "Whether to push collected items to falcon agent directly, "
            "otherwise items will be cached and then pushed to falcon "
            "agent asynchronous");
DEFINE_string(collector_falcon_agent, "http://127.0.0.1:1988/v1/push",
              "The falcon agent URL to push metrics to");
DEFINE_int32(collector_falcon_metrics_version, 4,
             "Version of metrics pushed to falcon, it will be tagged in "
             "'tag' section of an item");
DEFINE_int32(collector_falcon_pusher_count, 4,
             "Thread count to push collected items to falcon agent");
DEFINE_int32(collector_report_batch_size, 1000,
            "Count of items will be pushed to falcon agent by batch");
DEFINE_int32(collector_push_timeout_ms, 20,
             "Timeout for pushing items to falcon agent");

DECLARE_string(collector_cluster_name);
DECLARE_int32(collector_interval_sec);
DECLARE_int32(collector_timeout_sec);
DECLARE_int32(collector_warn_threshold_ms);

using std::list;
using std::string;
using strings::Substitute;

namespace kudu {
namespace collector {

FalconReporter::FalconReporter()
  : initialized_(false),
    stop_background_threads_latch_(1) {
}

FalconReporter::~FalconReporter() {
  Shutdown();
}

Status FalconReporter::Init() {
  CHECK(!initialized_);

  // Simple test falcon agent.
  EasyCurl curl;
  faststring dst;
  RETURN_NOT_OK(curl.PostToURL(FLAGS_collector_falcon_agent, "", &dst));

  initialized_ = true;
  return Status::OK();
}

Status FalconReporter::Start() {
  CHECK(initialized_);

  if (!FLAGS_collector_direct_push) {
    RETURN_NOT_OK(StartFalconPusherThreadPool());
  }

  return Status::OK();
}

void FalconReporter::Shutdown() {
  if (initialized_) {
    string name = ToString();
    LOG(INFO) << name << " shutting down...";

    stop_background_threads_latch_.CountDown();

    pusher_thread_pool_->Wait();

    LOG(INFO) << name << " shutdown complete.";
  }
}

string FalconReporter::ToString() const {
  return "FalconReporter";
}

scoped_refptr<ItemBase> FalconReporter::ConstructItem(string endpoint,
                                                      string metric,
                                                      string level,
                                                      uint64_t timestamp,
                                                      int64_t value,
                                                      string counter_type,
                                                      string extra_tags) {
  scoped_refptr<ItemBase> tmp(new FalconItem(std::move(endpoint),
                        std::move(metric),
                        Substitute("service=kudu,cluster=$0,level=$1,v=$2$3",
                                   FLAGS_collector_cluster_name, level,
                                   FLAGS_collector_falcon_metrics_version,
                                   extra_tags.empty() ? "" : "," + extra_tags),
                        timestamp,
                        FLAGS_collector_interval_sec,
                        value,
                        std::move(counter_type)));
  return tmp;
}

Status FalconReporter::PushItems(list<scoped_refptr<ItemBase>> items) {
  if (FLAGS_collector_direct_push) {
    RETURN_NOT_OK(PushToAgent(std::move(items)));
  } else {
    std::lock_guard<RWMutex> l(items_lock_);
    buffer_items_.splice(buffer_items_.end(), std::move(items));
  }
  return Status::OK();
}

Status FalconReporter::StartFalconPusherThreadPool() {
  RETURN_NOT_OK(ThreadPoolBuilder("falcon-pusher")
      .set_min_threads(FLAGS_collector_falcon_pusher_count)
      .set_max_threads(FLAGS_collector_falcon_pusher_count)
      .set_idle_timeout(MonoDelta::FromMilliseconds(1))
      .Build(&pusher_thread_pool_));
  for (int i = 0; i < FLAGS_collector_falcon_pusher_count; ++i) {
    RETURN_NOT_OK(pusher_thread_pool_->SubmitFunc(std::bind(&FalconReporter::FalconPusher,
                                                            this)));
  }

  return Status::OK();
}

void FalconReporter::FalconPusher() {
  while (HasItems() || !stop_background_threads_latch_.WaitFor(MonoDelta::FromSeconds(1))) {
    ReportItems();
  }
}

void FalconReporter::ReportItems() {
  MonoTime start(MonoTime::Now());
  scoped_refptr<Trace> trace(new Trace);
  ADOPT_TRACE(trace.get());
  TRACE_EVENT0("collector", "FalconReporter::ReportItems");
  TRACE("init");

  list<scoped_refptr<ItemBase>> falcon_items;
  PopItems(&falcon_items);
  WARN_NOT_OK(PushToAgent(std::move(falcon_items)), "PushToAgent failed");
  int64_t elapsed_ms = (MonoTime::Now() - start).ToMilliseconds();
  if (elapsed_ms > FLAGS_collector_warn_threshold_ms) {
    if (Trace::CurrentTrace()) {
      LOG(WARNING) << "Trace:" << std::endl
                   << Trace::CurrentTrace()->DumpToString();
    }
  }
}

bool FalconReporter::HasItems() const {
  std::lock_guard<RWMutex> l(items_lock_);
  return !buffer_items_.empty();
}

void FalconReporter::PopItems(list<scoped_refptr<ItemBase>>* falcon_items) {
  int items_left = 0;
  CHECK(falcon_items);
  {
    std::lock_guard<RWMutex> l(items_lock_);
    auto end_item = buffer_items_.begin();
    std::advance(end_item, std::min(buffer_items_.size(),
                                    static_cast<size_t>(FLAGS_collector_report_batch_size)));
    falcon_items->splice(falcon_items->end(), buffer_items_, buffer_items_.begin(), end_item);
    items_left = buffer_items_.size();
  }
  if (items_left > 1000000) {
    LOG(INFO) << "Items left " << items_left << std::endl;
  }
  TRACE(Substitute("Pop items, count $0", falcon_items->size()));
}

Status FalconReporter::PushToAgent(list<scoped_refptr<ItemBase>> falcon_items) {
  string data;
  RETURN_NOT_OK(SerializeItems(std::move(falcon_items), &data));

  EasyCurl curl;
  faststring dst;
  curl.set_timeout(MonoDelta::FromMilliseconds(FLAGS_collector_push_timeout_ms));
  RETURN_NOT_OK(curl.PostToURL(FLAGS_collector_falcon_agent, data, &dst));
  TRACE(Substitute("Pushed items to agent, size $0", data.size()));
  return Status::OK();
}

Status FalconReporter::SerializeItems(list<scoped_refptr<ItemBase>> items, string* data) {
  CHECK(data);
  if (items.empty()) {
    return Status::OK();
  }
  std::ostringstream str;
  JsonWriter jw(&str, JsonWriter::COMPACT);
  jw.StartArray();
  for (const auto& item : items) {
    scoped_refptr<FalconItem> falcon_item = dynamic_cast<FalconItem*>(item.get());
    jw.StartObject();
    jw.String("endpoint");
    jw.String(falcon_item->endpoint);
    jw.String("metric");
    jw.String(falcon_item->metric);
    jw.String("timestamp");
    jw.Uint64(falcon_item->timestamp);
    jw.String("step");
    jw.Int(falcon_item->step);
    jw.String("value");
    jw.Int64(falcon_item->value);
    jw.String("counterType");
    jw.String(falcon_item->counter_type);
    jw.String("tags");
    jw.String(falcon_item->tags);
    jw.EndObject();
  }
  jw.EndArray();
  *data = str.str();
  TRACE(Substitute("SerializeItems done, count $0, size $1", items.size(), data->size()));
  return Status::OK();
}
} // namespace collector
} // namespace kudu
