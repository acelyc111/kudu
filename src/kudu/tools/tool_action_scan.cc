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

#include <iostream>

#include "kudu/client/client.h"
#include "kudu/client/value.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strtoint.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/monotime.h"

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduTable;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using kudu::MonoDelta;
using kudu::Status;

using std::cout;
using std::endl;
using std::string;
using std::unique_ptr;
using std::vector;

DEFINE_string(key_column_name, "",
              "Key column name of the existing table, which will be used "
              "to limit the lower and upper bounds when scan rows.");
DEFINE_string(key_column_type, "",
              "Type of the above key column.");
DEFINE_string(included_lower_bound, "",
              "Included lower bound of the above key column.");
DEFINE_string(included_upper_bound, "",
              "Included upper bound of the above key column.");
DEFINE_int64(count, 0,
             "Count limit for scan rows. <= 0 mean no limit.");

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

Status ScanRows(const shared_ptr<KuduTable>& table,
                const string& key_column_name, const string& key_column_type,
                const string& include_lower_bound, const string& include_upper_bound) {
  KuduScanner scanner(table.get());

  // To be guaranteed results are returned in primary key order, make the
  // scanner fault-tolerant. This also means the scanner can recover if,
  // for example, the server it is scanning fails in the middle of a scan.
  RETURN_NOT_OK(scanner.SetFaultTolerant());
  RETURN_NOT_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));

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
      if (!include_upper_bound.empty()) {
        upper = KuduValue::FromInt(atoi64(include_upper_bound));
      }
      break;
    case KuduColumnSchema::DataType::STRING:
      if (!include_lower_bound.empty()) {
        lower = KuduValue::CopyString(include_lower_bound);
      }
      if (!include_upper_bound.empty()) {
        upper = KuduValue::CopyString(include_upper_bound);
      }
      break;
    case KuduColumnSchema::DataType::FLOAT:
    case KuduColumnSchema::DataType::DOUBLE:
      if (!include_lower_bound.empty()) {
        lower = KuduValue::FromDouble(strtod(include_lower_bound.c_str(), nullptr));
      }
      if (!include_lower_bound.empty()) {
        upper = KuduValue::FromDouble(strtod(include_upper_bound.c_str(), nullptr));
      }
      break;
    default:
      LOG(FATAL) << "Unhandled type " << type;
    }
    if (lower) {
        RETURN_NOT_OK(scanner.AddConjunctPredicate(table->NewComparisonPredicate(
                key_column_name, KuduPredicate::GREATER_EQUAL, lower)));
    }
    if (upper) {
        RETURN_NOT_OK(scanner.AddConjunctPredicate(table->NewComparisonPredicate(
                key_column_name, KuduPredicate::LESS_EQUAL, upper)));
    }
  }
  RETURN_NOT_OK(scanner.Open());

  int count = 0;
  KuduScanBatch batch;
  while (scanner.HasMoreRows()) {
    RETURN_NOT_OK(scanner.NextBatch(&batch));
    for (KuduScanBatch::const_iterator it = batch.begin();
         it != batch.end();
         ++it) {
      KuduScanBatch::RowPtr row(*it);
      cout << row.ToString() << endl;
      if (++count >= FLAGS_count && FLAGS_count > 0) {
        cout << "Scanned count(maybe not the total count in specified range): "
        << count << endl;
        return Status::OK();
      }
    }
  }
  cout << "Total count: " << count << endl;

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
                         FLAGS_included_lower_bound, FLAGS_included_upper_bound));

  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildScanMode() {
  unique_ptr<Action> insert =
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
      .AddOptionalParameter("included_lower_bound")
      .AddOptionalParameter("included_upper_bound")
      .AddOptionalParameter("count")
      .Build();

  return ModeBuilder("scan")
      .Description("Scan rows from an exist table")
      .AddAction(std::move(insert))
      .Build();
}

} // namespace tools
} // namespace kudu
