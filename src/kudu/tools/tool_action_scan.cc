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

#include <atomic>
#include <iostream>
#include <thread>

#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/client/value.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strtoint.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/monotime.h"
#include "kudu/util/stopwatch.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using kudu::MonoDelta;
using kudu::Status;

using std::cout;
using std::endl;
using std::map;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

DEFINE_string(key_column_name, "",
              "Key column name of the existing table, which will be used "
              "to limit the lower and upper bounds when scan rows.");
DEFINE_string(key_column_type, "",
              "Type of the above key column.");
DEFINE_string(include_lower_bound, "",
              "Include lower bound of the above key column.");
DEFINE_string(exclude_upper_bound, "",
              "Exclude upper bound of the above key column.");
DEFINE_int64(count, 0,
             "Count limit for scan rows. <= 0 mean no limit.");
DEFINE_bool(show_value, true,
            "Whether to show value of scanned items.");
DECLARE_int32(num_threads);

namespace kudu {
namespace tools {

namespace {

// Constants for parameters and descriptions.
extern const char* const kTableNameArg = "table_name";

Status CreateClient(const vector<string>& master_addrs,
                           shared_ptr<KuduClient>* client) {
  return KuduClientBuilder()
      .master_server_addrs(master_addrs)
      .default_admin_operation_timeout(MonoDelta::FromSeconds(20))
      .Build(client);
}

Status DoesTableExist(const shared_ptr<KuduClient>& client,
                      const string& table_name,
                      bool *exists) {
  shared_ptr<KuduTable> table;
  Status s = client->OpenTable(table_name, &table);
  if (s.ok()) {
    *exists = true;
  } else if (s.IsNotFound()) {
    *exists = false;
    s = Status::OK();
  }
  return s;
}

std::atomic<uint64_t> total_count(0);
volatile size_t worker_count = 0;

void ScannerThread(const vector<KuduScanToken*>& tokens) {
  for (auto token : tokens) {
      KuduScanner *scanner_ptr;
      DCHECK_OK(token->IntoKuduScanner(&scanner_ptr));
      unique_ptr<KuduScanner> scanner(scanner_ptr);
      DCHECK_OK(scanner->Open());

      int count = 0;
      KuduScanBatch batch;
      while (scanner->HasMoreRows()) {
          DCHECK_OK(scanner->NextBatch(&batch));
          for (auto it = batch.begin(); it != batch.end(); ++it) {
              KuduScanBatch::RowPtr row(*it);
              if (FLAGS_show_value) {
                  LOG(INFO) << row.ToString() << endl;
              }
          }
          total_count.fetch_add(batch.NumRows());
          if (total_count.load() >= FLAGS_count && FLAGS_count > 0) {   // TODO maybe larger than FLAGS_count
              LOG(INFO) << "Scanned count(maybe not the total count in specified range): " << count << endl;
              return;
          }
      }
  }
}

void MonitorThread() {
    while (worker_count > 0) {
        LOG(INFO) << "Scanned count: " << total_count.load() << endl;
        SleepFor(MonoDelta::FromSeconds(5));
    }
}

Status ScanRows(const shared_ptr<KuduTable>& table,
                const string& key_column_name, const string& key_column_type,
                const string& include_lower_bound, const string& exclude_upper_bound) {
  KuduScanTokenBuilder builder(table.get());
  RETURN_NOT_OK(builder.SetCacheBlocks(false));
  // RETURN_NOT_OK(scanner.SetFaultTolerant());
  RETURN_NOT_OK(builder.SetSelection(KuduClient::LEADER_ONLY));
  RETURN_NOT_OK(builder.SetReadMode(KuduScanner::READ_LATEST));
  // RETURN_NOT_OK(builder.SetLimit(4096));

  if (!key_column_name.empty() && !key_column_type.empty()) {
    KuduColumnSchema::DataType type = KuduColumnSchema::StringToDataType(key_column_type);
    KuduValue *lower = nullptr;
    KuduValue *upper = nullptr;
    switch (type) {
    case KuduColumnSchema::DataType::INT8:
    case KuduColumnSchema::DataType::INT16:
    case KuduColumnSchema::DataType::INT32:
    case KuduColumnSchema::DataType::INT64:
      if (!include_lower_bound.empty()) {
        lower = KuduValue::FromInt(atoi64(include_lower_bound));
      }
      if (!exclude_upper_bound.empty()) {
        upper = KuduValue::FromInt(atoi64(exclude_upper_bound));
      }
      break;
    case KuduColumnSchema::DataType::STRING:
      if (!include_lower_bound.empty()) {
        lower = KuduValue::CopyString(include_lower_bound);
      }
      if (!exclude_upper_bound.empty()) {
        upper = KuduValue::CopyString(exclude_upper_bound);
      }
      break;
    case KuduColumnSchema::DataType::FLOAT:
    case KuduColumnSchema::DataType::DOUBLE:
      if (!include_lower_bound.empty()) {
        lower = KuduValue::FromDouble(strtod(include_lower_bound.c_str(), nullptr));
      }
      if (!include_lower_bound.empty()) {
        upper = KuduValue::FromDouble(strtod(exclude_upper_bound.c_str(), nullptr));
      }
      break;
    default:
      LOG(FATAL) << "Unhandled type " << type;
    }
    if (lower) {
        RETURN_NOT_OK(builder.AddConjunctPredicate(table->NewComparisonPredicate(
                key_column_name, KuduPredicate::GREATER_EQUAL, lower)));
    }
    if (upper) {
        RETURN_NOT_OK(builder.AddConjunctPredicate(table->NewComparisonPredicate(
                key_column_name, KuduPredicate::LESS, upper)));
    }
  }

  vector<KuduScanToken*> tokens;
  ElementDeleter DeleteTable(&tokens);
  RETURN_NOT_OK(builder.Build(&tokens));

  map<int, vector<KuduScanToken*>> thread_tokens;
  int i = 0;
  for (auto token : tokens) {
    thread_tokens[i++ % FLAGS_num_threads].push_back(token);
  }

  worker_count = FLAGS_num_threads;
  vector<thread> threads;
  for (int i = 0; i < FLAGS_num_threads; ++i) {
      threads.emplace_back(&ScannerThread, thread_tokens[i]);
  }
  threads.emplace_back(&MonitorThread);

  for (auto& t : threads) {
    t.join();
    --worker_count;
  }

  LOG(INFO) << "Total count: " << total_count;

  return Status::OK();
}

Status RowsScanner(const RunnerContext& context) {
  const string& master_addresses_str =
      FindOrDie(context.required_args, kMasterAddressesArg);

  // Create kudu client.
  vector<string> master_addrs(strings::Split(master_addresses_str, ","));
  if (master_addrs.empty()) {
    return Status::InvalidArgument(
        "At least one master address must be specified");
  }
  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateClient(master_addrs, &client));

  const string& table_name =
      FindOrDie(context.required_args, kTableNameArg);

  // Check the table is exist.
  bool exists = false;
  RETURN_NOT_OK(DoesTableExist(client, table_name, &exists));
  CHECK(exists);

  // Open the table.
  shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client->OpenTable(table_name, &table));

  // Scan some rows.
  RETURN_NOT_OK(ScanRows(table, FLAGS_key_column_name, FLAGS_key_column_type,
                         FLAGS_include_lower_bound, FLAGS_exclude_upper_bound));

  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildScanMode() {
  unique_ptr<Action> scan =
      ActionBuilder("table", &RowsScanner)
      .Description("Scan rows from an exist table")
      .ExtraDescription(
          "Scan rows from an exist table, you can specify "
          "one key column's lower and upper bounds.")
      .AddRequiredParameter({ kMasterAddressesArg,
          "Comma-separated list of master addresses to run against. "
          "Addresses are in 'hostname:port' form where port may be omitted "
          "if a master server listens at the default port." })
      .AddRequiredParameter({ kTableNameArg,
          "Key column name of the existing table, which will be used "
          "to limit the lower and upper bounds when scan rows."})
      .AddOptionalParameter("key_column_name")
      .AddOptionalParameter("key_column_type")
      .AddOptionalParameter("include_lower_bound")
      .AddOptionalParameter("exclude_upper_bound")
      .AddOptionalParameter("count")
      .AddOptionalParameter("show_value")
      .Build();

  return ModeBuilder("scan")
      .Description("Scan rows")
      .AddAction(std::move(scan))
      .Build();
}

} // namespace tools
} // namespace kudu
