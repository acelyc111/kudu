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
#include <unordered_map>
#include <vector>

#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

class Thread;

namespace client {
class KuduClient;
class KuduTable;
} // namespace client

namespace collector {

class ReporterBase;

class ServiceMonitor : public RefCounted<ServiceMonitor> {
 public:
  explicit ServiceMonitor(scoped_refptr<ReporterBase> reporter);
  ~ServiceMonitor();

  Status Init();
  Status Start();
  void Shutdown();

  std::string ToString() const;

 private:
  friend class RefCounted<ServiceMonitor>;

  Status StartServiceMonitorThread();
  void ServiceMonitorThread();
  void CheckService();

  client::KuduSchema CreateTableSchema();

  Status CallLeaderStepDown(const std::string& tablet_id, const std::string& ts_uuid);
  Status CheckMonitorTable();
  Status CreateMonitorTable(const std::string& table_name);
  Status InitCilent();
  Status RebalanceMonitorTable();
  Status UpsertAndScanRows(const client::sp::shared_ptr<client::KuduTable>& table);

  // Find a tablet from 'tablets', whose leader replica host has n leader replicas,
  // n > least_num_of_leader_replicas.
  std::string FindLeaderStepDownTablet(
      const std::unordered_map<std::string, int>& ts_leader_replica_count,
      const std::vector<std::string>& tablets,
      int least_num_of_leader_replicas);

  // Get leader host uuid for a given tablet id.
  Status GetLeaderHost(const std::string& tablet_id, std::string* leader_host);

  bool initialized_;

  client::sp::shared_ptr<client::KuduClient> client_;
  scoped_refptr<ReporterBase> reporter_;
  CountDownLatch stop_background_threads_latch_;
  scoped_refptr<Thread> service_monitor_thread_;
  MonoTime last_check_table_time_;

  DISALLOW_COPY_AND_ASSIGN(ServiceMonitor);
};
} // namespace collector
} // namespace kudu
