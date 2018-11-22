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
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include "kudu/client/client.h"
#include "kudu/client/replica_controller-internal.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strtoint.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include <kudu/util/atomic.h>
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"

DECLARE_string(columns);
DEFINE_bool(list_tablets, false,
            "Include tablet and replica UUIDs in the output");
DEFINE_bool(modify_external_catalogs, true,
            "Whether to modify external catalogs, such as the Hive Metastore, "
            "when renaming or dropping a table.");
DECLARE_int32(num_threads);
DEFINE_string(predicates, "",
              "Query predicates on columns, support three types of predicates, "
              "include 'Comparison', 'InList' and 'WhetherNull'."
              "  The 'Comparison' type support <=, <, ==, > and >=, "
              "    which can be represented by one character '[', '(', '=', ')' and ']'"
              "  The 'InList' type means values are in certains list, "
              "    which can be represented by one character '@'"
              "  The 'WhetherNull' type means whether the value is a NULL or not, "
              "    which can be represented by one character 'i'(is) and '!'(is not)"
              "One predicate entry can be represented as <column name>:<predicate type>:<value(s)>, "
              "  e.g. 'col1:[:lower;col1:]:upper;col2:@:v1,v2,v3;col3:!:NULL'");
DEFINE_int64(scan_count, 0,
             "Count limit for scan rows. <= 0 mean no limit.");
DEFINE_bool(show_value, false,
            "Whether to show value of scanned items.");
DECLARE_string(tables);
DEFINE_string(target_table, "",
              "The name of the target table the data will copy to");

namespace kudu {
namespace tools {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduError;
using client::KuduInsert;
using client::KuduPredicate;
using client::KuduScanBatch;
using client::KuduScanToken;
using client::KuduScanTokenBuilder;
using client::KuduScanner;
using client::KuduSchema;
using client::KuduSchemaBuilder;
using client::KuduSession;
using client::KuduTable;
using client::KuduTableAlterer;
using client::KuduTableCreator;
using client::KuduValue;
using client::internal::ReplicaController;
using client::sp::shared_ptr;
using std::cout;
using std::endl;
using std::map;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

// This class only exists so that ListTables() can easily be friended by
// KuduReplica, KuduReplica::Data, and KuduClientBuilder.
class TableLister {
 public:
  static Status ListTablets(const vector<string>& master_addresses) {
    KuduClientBuilder builder;
    ReplicaController::SetVisibility(&builder, ReplicaController::Visibility::ALL);
    shared_ptr<KuduClient> client;
    RETURN_NOT_OK(builder
                  .master_server_addrs(master_addresses)
                  .Build(&client));
    vector<string> table_names;
    RETURN_NOT_OK(client->ListTables(&table_names));

    vector<string> table_filters = Split(FLAGS_tables, ",", strings::SkipEmpty());
    for (const auto& tname : table_names) {
      if (!MatchesAnyPattern(table_filters, tname)) continue;
      cout << tname << endl;
      if (!FLAGS_list_tablets) {
        continue;
      }
      shared_ptr<KuduTable> client_table;
      RETURN_NOT_OK(client->OpenTable(tname, &client_table));
      vector<KuduScanToken*> tokens;
      ElementDeleter deleter(&tokens);
      KuduScanTokenBuilder builder(client_table.get());
      RETURN_NOT_OK(builder.Build(&tokens));

      for (const auto* token : tokens) {
        cout << "  T " << token->tablet().id() << endl;
        for (const auto* replica : token->tablet().replicas()) {
          const bool is_voter = ReplicaController::is_voter(*replica);
          const bool is_leader = replica->is_leader();
          cout << strings::Substitute("    $0 $1 $2:$3",
              is_leader ? "L" : (is_voter ? "V" : "N"), replica->ts().uuid(),
              replica->ts().hostname(), replica->ts().port()) << endl;
        }
        cout << endl;
      }
      cout << endl;
    }
    return Status::OK();
  }
};

namespace {

const char* const kTableNameArg = "table_name";
const char* const kNewTableNameArg = "new_table_name";
const char* const kColumnNameArg = "column_name";
const char* const kNewColumnNameArg = "new_column_name";
const char* const kTargetMasterAddressesArg = "target_master_addresses";

AtomicInt<uint64_t> total_count(0);
AtomicInt<int32_t> worker_count(0);

Status CreateKuduClient(const RunnerContext& context,
                        const char* const master_addresses_arg,
                        shared_ptr<KuduClient>* client) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 master_addresses_arg);
  vector<string> master_addresses = Split(master_addresses_str, ",");
  return KuduClientBuilder()
             .master_server_addrs(master_addresses)
             .Build(client);
}

Status CreateKuduClient(const RunnerContext& context,
                        shared_ptr<KuduClient>* client) {
  return CreateKuduClient(context, kMasterAddressesArg, client);
}

Status CreateTargetKuduClient(const RunnerContext& context,
                        shared_ptr<KuduClient>* client) {
  return CreateKuduClient(context, kTargetMasterAddressesArg, client);
}

Status DeleteTable(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  return client->DeleteTableInCatalogs(table_name, FLAGS_modify_external_catalogs);
}

Status RenameTable(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& new_table_name = FindOrDie(context.required_args, kNewTableNameArg);

  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  return alterer->RenameTo(new_table_name)
                ->modify_external_catalogs(FLAGS_modify_external_catalogs)
                ->Alter();
}

Status RenameColumn(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& column_name = FindOrDie(context.required_args, kColumnNameArg);
  const string& new_column_name = FindOrDie(context.required_args, kNewColumnNameArg);

  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  unique_ptr<KuduTableAlterer> alterer(client->NewTableAlterer(table_name));
  alterer->AlterColumn(column_name)->RenameTo(new_column_name);
  return alterer->Alter();
}

Status ListTables(const RunnerContext& context) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 kMasterAddressesArg);
  return TableLister::ListTablets(Split(master_addresses_str, ","));
}

Status AddRow(const shared_ptr<KuduTable>& table, const KuduSchema& table_schema,
              const KuduScanBatch::RowPtr& row, const shared_ptr<KuduSession>& session) {
  std::unique_ptr<KuduInsert> insert(table->NewInsert());
  KuduPartialRow* insert_row = insert->mutable_row();
  for (size_t i = 0; i < table_schema.num_columns(); ++i) {
    const KuduColumnSchema& col_schema = table_schema.Column(i);
    const std::string& col_name = col_schema.name();
    switch (col_schema.type()) {
      case KuduColumnSchema::DataType::INT8: {
        int8_t v;
        if (row.GetInt8(col_name, &v).ok()) {
          RETURN_NOT_OK(insert_row->SetInt8(col_name, v));
        }
        break;
      }
      case KuduColumnSchema::DataType::INT16: {
        int16_t v;
        if (row.GetInt16(col_name, &v).ok()) {
          RETURN_NOT_OK(insert_row->SetInt16(col_name, v));
        }
        break;
      }
      case KuduColumnSchema::DataType::INT32: {
        int32_t v;
        if (row.GetInt32(col_name, &v).ok()) {
          RETURN_NOT_OK(insert_row->SetInt32(col_name, v));
        }
        break;
      }
      case KuduColumnSchema::DataType::INT64: {
        int64_t v;
        if (row.GetInt64(col_name, &v).ok()) {
          RETURN_NOT_OK(insert_row->SetInt64(col_name, v));
        }
        break;
      }
      case KuduColumnSchema::DataType::STRING: {
        Slice v;
        if (row.GetString(col_name, &v).ok()) {
          RETURN_NOT_OK(insert_row->SetString(col_name, v));
        }
        break;
      }
      case KuduColumnSchema::DataType::BOOL: {
        bool v;
        if (row.GetBool(col_name, &v).ok()) {
          RETURN_NOT_OK(insert_row->SetBool(col_name, v));
        }
        break;
      }
      case KuduColumnSchema::DataType::FLOAT: {
        float v;
        if (row.GetFloat(col_name, &v).ok()) {
          RETURN_NOT_OK(insert_row->SetFloat(col_name, v));
        }
        break;
      }
      case KuduColumnSchema::DataType::DOUBLE: {
        double v;
        if (row.GetDouble(col_name, &v).ok()) {
          RETURN_NOT_OK(insert_row->SetDouble(col_name, v));
        }
        break;
      }
      case KuduColumnSchema::DataType::BINARY: {
        Slice v;
        if (row.GetBinary(col_name, &v).ok()) {
          RETURN_NOT_OK(insert_row->SetBinary(col_name, v));
        }
        break;
      }
      case KuduColumnSchema::DataType::UNIXTIME_MICROS: {
        int64_t v;
        if (row.GetUnixTimeMicros(col_name, &v).ok()) {
          RETURN_NOT_OK(insert_row->SetUnixTimeMicros(col_name, v));
        }
        break;
      }
      case KuduColumnSchema::DataType::DECIMAL: {
        int128_t v;
        if (row.GetUnscaledDecimal(col_name, &v).ok()) {
          RETURN_NOT_OK(insert_row->SetUnscaledDecimal(col_name, v));
        }
        break;
      }
      default:
        LOG(FATAL) << "Unknown type: " << col_schema.type();
    }
  }

  return session->Apply(insert.release());
}

Status CheckFlush(const shared_ptr<KuduSession>& session, const Status& s) {
  if (s.ok()) {
    return s;
  }

  std::vector<KuduError*> errors;
  session->GetPendingErrors(&errors, nullptr);
  for (const auto& it : errors) {
    if (!it->status().IsAlreadyPresent()) {
      LOG(ERROR) << it->status().ToString() << endl;
      return s;
    }
  }

  return Status::OK();
}

void CopyThread(const RunnerContext& context, const KuduSchema& table_schema, const vector<KuduScanToken*>& tokens) {
  // target table
  const string& target_table_name = FLAGS_target_table.empty() ? FindOrDie(context.required_args, kTableNameArg) : FLAGS_target_table;
  shared_ptr<KuduClient> target_client;
  DCHECK_OK(CreateTargetKuduClient(context, &target_client));
  shared_ptr<KuduTable> target_table;
  DCHECK_OK(target_client->OpenTable(target_table_name, &target_table));

  shared_ptr<KuduSession> session(target_client->NewSession());
  DCHECK_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND));
  DCHECK_OK(session->SetErrorBufferSpace(1024));
  session->SetTimeoutMillis(30000);

  for (auto token : tokens) {
    KuduScanner *scanner_ptr;
    DCHECK_OK(token->IntoKuduScanner(&scanner_ptr));
    unique_ptr<KuduScanner> scanner(scanner_ptr);
    DCHECK_OK(scanner->Open());

    KuduScanBatch batch;
    while (scanner->HasMoreRows()) {
      DCHECK_OK(scanner->NextBatch(&batch));
      for (auto it = batch.begin(); it != batch.end(); ++it) {
        KuduScanBatch::RowPtr row(*it);

        DCHECK_OK(AddRow(target_table, table_schema, row, session));
      }
      Status s = session->Flush();
      DCHECK_OK(CheckFlush(session, s));

      total_count.IncrementBy(batch.NumRows());
    }
  }
}

void MonitorThread() {
    while (worker_count.Load() > 0) {
        LOG(INFO) << "Scanned count: " << total_count.Load() << endl;
        SleepFor(MonoDelta::FromSeconds(5));
    }
}

KuduValue* ParseValue(KuduColumnSchema::DataType type,
                      const string& str_value) {
  switch (type) {
    case KuduColumnSchema::DataType::INT8:
    case KuduColumnSchema::DataType::INT16:
    case KuduColumnSchema::DataType::INT32:
    case KuduColumnSchema::DataType::INT64:
      if (!str_value.empty()) {
        return KuduValue::FromInt(atoi64(str_value));
      }
      break;
    case KuduColumnSchema::DataType::STRING:
      if (!str_value.empty()) {
        return KuduValue::CopyString(str_value);
      }
      break;
    case KuduColumnSchema::DataType::FLOAT:
    case KuduColumnSchema::DataType::DOUBLE:
      if (!str_value.empty()) {
        return KuduValue::FromDouble(strtod(str_value.c_str(), nullptr));
      }
      break;
    default:
      CHECK(false) << Substitute("Unhandled type $0", type);
  }

  return nullptr;
}

Status NewComparisonPredicate(const shared_ptr<KuduTable>& table,
                              const string& name,
                              KuduColumnSchema::DataType type,
                              char op,
                              const string& value,
                              KuduPredicate** predicate) {
  KuduValue* lower = ParseValue(type, value);
  client::KuduPredicate::ComparisonOp cop;
  switch (op) {
    case '[':
      cop = client::KuduPredicate::ComparisonOp::GREATER_EQUAL;
      break;
    case '(':
      cop = client::KuduPredicate::ComparisonOp::GREATER;
      break;
    case '=':
      cop = client::KuduPredicate::ComparisonOp::EQUAL;
      break;
    case ')':
      cop = client::KuduPredicate::ComparisonOp::LESS;
      break;
    case ']':
      cop = client::KuduPredicate::ComparisonOp::LESS_EQUAL;
      break;
    default:
      return Status::InvalidArgument(Substitute("invalid op: $0", op));
  }
  *predicate = table->NewComparisonPredicate(name, cop, lower);

  return Status::OK();
}

Status NewInPredicate(const shared_ptr<KuduTable>& table,
                      const string& name,
                      KuduColumnSchema::DataType type,
                      char op,
                      const string& value,
                      KuduPredicate** predicate) {
  switch (op) {
    case '@': {
      std::vector<KuduValue *> values;
      vector<string> str_values = Split(value, ",", strings::SkipEmpty());
      for (const auto& str_value : str_values) {
        values.emplace_back(ParseValue(type, str_value));
      }
      *predicate = table->NewInListPredicate(name, &values);
      break;
    }
    default:
      return Status::InvalidArgument(Substitute("invalid op: $0", op));
  }

  return Status::OK();
}

Status NewNullPredicate(const shared_ptr<KuduTable>& table,
                        const string& name,
                        char op,
                        const string& value,
                        KuduPredicate** predicate) {
  std::string value_upper;
  ToUpperCase(value, &value_upper);
  if (value_upper != "NULL") {
    return Status::OK();
  }

  switch (op) {
    case 'i':
      *predicate = table->NewIsNullPredicate(name);
      break;
    case '!':
      *predicate = table->NewIsNotNullPredicate(name);
      break;
    default:
      return Status::InvalidArgument(Substitute("invalid op: $0", op));
  }

  return Status::OK();
}

enum class PredicateType {
  Invalid = 0,
  Comparison,
  InList,
  WhetherNull
};

PredicateType ParsePredicateType(const string& op) {
  if (op.size() != 1) {
    return PredicateType::Invalid;
  }

  switch (op[0]) {
    case '[':
    case '(':
    case '=':
    case ')':
    case ']':
      return PredicateType::Comparison;
    case '@':
      return PredicateType::InList;
    case 'i':
    case '!':
      return PredicateType::WhetherNull;
    default:
      return PredicateType::Invalid;
  }

  return PredicateType::Invalid;
}

Status AddPredicate(const shared_ptr<KuduTable>& table,
                    const string& name,
                    const string& op,
                    const string& value,
                    KuduScanTokenBuilder& builder) {
  if (name.empty() || op.empty()) {
    return Status::OK();
  }

  for (size_t i = 0; i < table->schema().num_columns(); ++i) {
    if (table->schema().Column(i).name() == name) {
      auto type = table->schema().Column(i).type();
      KuduPredicate* predicate = nullptr;
      PredicateType pt = ParsePredicateType(op);
      switch (pt) {
        case PredicateType::Comparison:
          RETURN_NOT_OK(NewComparisonPredicate(table, name, type, op[0], value, &predicate));
          break;
        case PredicateType::InList:
          RETURN_NOT_OK(NewInPredicate(table, name, type, op[0], value, &predicate));
          break;
        case PredicateType::WhetherNull:
          RETURN_NOT_OK(NewNullPredicate(table, name, op[0], value, &predicate));
          break;
        default:
          return Status::InvalidArgument("Invalid op: $1", op);
      }
      RETURN_NOT_OK(builder.AddConjunctPredicate(predicate));

      return Status::OK();
    }
  }

  return Status::OK();
}

Status AddPredicates(const shared_ptr<KuduTable>& table,
                     const string& predicates,
                     KuduScanTokenBuilder& builder) {
  vector<string> column_predicates = Split(predicates, ";", strings::SkipWhitespace());
  for (const auto& column_predicate : column_predicates) {
    vector<string> name_op_value = Split(column_predicate, ":", strings::SkipWhitespace());
    if (name_op_value.size() == 3) {
      RETURN_NOT_OK(AddPredicate(table, name_op_value[0], name_op_value[1], name_op_value[2], builder));
    }
  }

  return Status::OK();
}

Status CopyTable(const RunnerContext& context) {
  const string& src_table_name = FindOrDie(context.required_args, kTableNameArg);
  shared_ptr<KuduClient> src_client;
  RETURN_NOT_OK(CreateKuduClient(context, &src_client));
  shared_ptr<KuduTable> src_table;
  RETURN_NOT_OK(src_client->OpenTable(src_table_name, &src_table));

  KuduScanTokenBuilder builder(src_table.get());
  RETURN_NOT_OK(builder.SetCacheBlocks(false));
  RETURN_NOT_OK(builder.SetSelection(KuduClient::LEADER_ONLY));
  RETURN_NOT_OK(builder.SetReadMode(KuduScanner::READ_LATEST));
  RETURN_NOT_OK(AddPredicates(src_table, FLAGS_predicates, builder));

  const KuduSchema& table_schema = src_table->schema();

  vector<KuduScanToken*> tokens;
  ElementDeleter DeleteTable(&tokens);
  RETURN_NOT_OK(builder.Build(&tokens));

  map<int, vector<KuduScanToken*>> thread_tokens;
  int i = 0;
  for (auto token : tokens) {
    thread_tokens[i++ % FLAGS_num_threads].push_back(token);
  }

  worker_count.Store(FLAGS_num_threads);
  vector<thread> threads;
  for (i = 0; i < FLAGS_num_threads; ++i) {
      threads.emplace_back(&CopyThread, context, table_schema, thread_tokens[i]);
  }
  threads.emplace_back(&MonitorThread);

  for (auto& t : threads) {
    t.join();
    worker_count.IncrementBy(-1);
  }

  LOG(INFO) << "Total count: " << total_count.Load();

  return Status::OK();
}

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
          total_count.IncrementBy(batch.NumRows());
          if (total_count.Load() >= FLAGS_scan_count && FLAGS_scan_count > 0) {   // TODO maybe larger than FLAGS_scan_count
              LOG(INFO) << "Scanned count(maybe not the total count in specified range): " << count << endl;
              return;
          }
      }
  }
}

Status ScanRows(const shared_ptr<KuduTable>& table, const string& predicates, const string& columns) {
  KuduScanTokenBuilder builder(table.get());
  RETURN_NOT_OK(builder.SetCacheBlocks(false));
  RETURN_NOT_OK(builder.SetTimeoutMillis(30000));
  RETURN_NOT_OK(builder.SetSelection(KuduClient::LEADER_ONLY));
  RETURN_NOT_OK(builder.SetReadMode(KuduScanner::READ_LATEST));
  vector<string> projected_column_names = Split(columns, ",", strings::SkipWhitespace());
  if (!projected_column_names.empty()) {
    RETURN_NOT_OK(builder.SetProjectedColumnNames(projected_column_names));
  }
  RETURN_NOT_OK(AddPredicates(table, predicates, builder));

  vector<KuduScanToken*> tokens;
  ElementDeleter DeleteTable(&tokens);
  RETURN_NOT_OK(builder.Build(&tokens));

  map<int, vector<KuduScanToken*>> thread_tokens;
  int i = 0;
  for (auto token : tokens) {
    thread_tokens[i++ % FLAGS_num_threads].push_back(token);
  }

  worker_count.Store(FLAGS_num_threads);
  vector<thread> threads;
  for (i = 0; i < FLAGS_num_threads; ++i) {
      threads.emplace_back(&ScannerThread, thread_tokens[i]);
  }
  threads.emplace_back(&MonitorThread);

  for (auto& t : threads) {
    t.join();
    worker_count.IncrementBy(-1);
  }

  LOG(INFO) << "Total count: " << total_count.Load();

  return Status::OK();
}

Status ScanTable(const RunnerContext &context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));

  shared_ptr<KuduTable> table;
  RETURN_NOT_OK(client->OpenTable(table_name, &table));

  RETURN_NOT_OK(ScanRows(table, FLAGS_predicates, FLAGS_columns));

  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildTableMode() {
  unique_ptr<Action> delete_table =
      ActionBuilder("delete", &DeleteTable)
      .Description("Delete a table")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to delete" })
      .AddOptionalParameter("modify_external_catalogs")
      .Build();

  unique_ptr<Action> rename_table =
      ActionBuilder("rename_table", &RenameTable)
      .Description("Rename a table")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to rename" })
      .AddRequiredParameter({ kNewTableNameArg, "New table name" })
      .AddOptionalParameter("modify_external_catalogs")
      .Build();

  unique_ptr<Action> rename_column =
      ActionBuilder("rename_column", &RenameColumn)
          .Description("Rename a column")
          .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
          .AddRequiredParameter({ kTableNameArg, "Name of the table to alter" })
          .AddRequiredParameter({ kColumnNameArg, "Name of the table column to rename" })
          .AddRequiredParameter({ kNewColumnNameArg, "New column name" })
          .Build();

  unique_ptr<Action> list_tables =
      ActionBuilder("list", &ListTables)
      .Description("List tables")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddOptionalParameter("tables")
      .AddOptionalParameter("list_tablets")
      .Build();

  unique_ptr<Action> copy_table =
      ActionBuilder("copy", &CopyTable)
      .Description("Copy a table data to another cluster")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to copy" })
      .AddRequiredParameter({ kTargetMasterAddressesArg, "target cluster master_addresses of this table copy to" })
      .AddOptionalParameter("target_table")
      .AddOptionalParameter("predicates")
      .Build();

  unique_ptr<Action> scan_table =
      ActionBuilder("scan", &ScanTable)
      .Description("Scan rows from a table")
      .ExtraDescription(
          "Scan rows from an exist table, you can specify "
          "one column's lower and upper bounds.")
      .AddRequiredParameter({ kMasterAddressesArg,
          "Comma-separated list of master addresses to run against. "
          "Addresses are in 'hostname:port' form where port may be omitted "
          "if a master server listens at the default port." })
      .AddRequiredParameter({ kTableNameArg,
          "Key column name of the existing table, which will be used "
          "to limit the lower and upper bounds when scan rows."})
      .AddOptionalParameter("predicates")
      .AddOptionalParameter("columns")
      .AddOptionalParameter("scan_count")
      .AddOptionalParameter("show_value")
      .Build();

  return ModeBuilder("table")
      .Description("Operate on Kudu tables")
      .AddAction(std::move(delete_table))
      .AddAction(std::move(rename_table))
      .AddAction(std::move(rename_column))
      .AddAction(std::move(list_tables))
      .AddAction(std::move(copy_table))
      .AddAction(std::move(scan_table))
      .Build();
}

} // namespace tools
} // namespace kudu

