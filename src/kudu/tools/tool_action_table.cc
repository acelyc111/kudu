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
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include "kudu/client/client.h"
#include "kudu/client/replica_controller-internal.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/status.h"

DECLARE_string(tables);
DECLARE_double(buffer_flush_watermark_pct);
DECLARE_int32(buffer_size_bytes);
DECLARE_int32(buffers_num);
DECLARE_int32(error_buffer_size_bytes);
DECLARE_int32(flush_per_n_rows);
DEFINE_bool(modify_external_catalogs, true,
            "Whether to modify external catalogs, such as the Hive Metastore, "
            "when renaming or dropping a table.");
DEFINE_bool(list_tablets, false,
            "Include tablet and replica UUIDs in the output");

namespace kudu {
namespace tools {

using client::KuduClient;
using client::KuduClientBuilder;
using client::KuduColumnSchema;
using client::KuduInsert;
using client::KuduScanToken;
using client::KuduScanTokenBuilder;
using client::KuduScanBatch;
using client::KuduScanner;
using client::KuduSchema;
using client::KuduSchemaBuilder;
using client::KuduSession;
using client::KuduTable;
using client::KuduTableAlterer;
using client::KuduTableCreator;
using client::internal::ReplicaController;
using std::cout;
using std::endl;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;

// This class only exists so that ListTables() can easily be friended by
// KuduReplica, KuduReplica::Data, and KuduClientBuilder.
class TableLister {
 public:
  static Status ListTablets(const vector<string>& master_addresses) {
    KuduClientBuilder builder;
    ReplicaController::SetVisibility(&builder, ReplicaController::Visibility::ALL);
    client::sp::shared_ptr<KuduClient> client;
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
      client::sp::shared_ptr<KuduTable> client_table;
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
const char* const kDestMasterAddressesArg = "dest_master_addresses";

Status CreateKuduClient(const RunnerContext& context,
                        const char* const master_addresses_arg,
                        client::sp::shared_ptr<KuduClient>* client) {
  const string& master_addresses_str = FindOrDie(context.required_args,
                                                 master_addresses_arg);
  vector<string> master_addresses = Split(master_addresses_str, ",");
  return KuduClientBuilder()
             .master_server_addrs(master_addresses)
             .Build(client);
}

Status CreateKuduClient(const RunnerContext& context,
                        client::sp::shared_ptr<KuduClient>* client) {
  return CreateKuduClient(context, kMasterAddressesArg, client);
}

Status CreateDestKuduClient(const RunnerContext& context,
                        client::sp::shared_ptr<KuduClient>* client) {
  return CreateKuduClient(context, kDestMasterAddressesArg, client);
}

Status DeleteTable(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));
  return client->DeleteTableInCatalogs(table_name, FLAGS_modify_external_catalogs);
}

Status RenameTable(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  const string& new_table_name = FindOrDie(context.required_args, kNewTableNameArg);

  client::sp::shared_ptr<KuduClient> client;
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

  client::sp::shared_ptr<KuduClient> client;
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

Status CreateTable(const RunnerContext& context) {
  const string& table_name = FindOrDie(context.required_args, kTableNameArg);
  client::sp::shared_ptr<KuduClient> client;
  RETURN_NOT_OK(CreateKuduClient(context, &client));

  std::string col1 = "distinct_id";
  std::string col2 = "olap_date";
  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn(col1)->Type(KuduColumnSchema::STRING)->NotNull();
  b.AddColumn(col2)->Type(KuduColumnSchema::INT32)->NotNull();
  b.SetPrimaryKey({col1, col2});
  RETURN_NOT_OK(b.Build(&schema));

  unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  table_creator->table_name(table_name).schema(&schema);
  table_creator->num_replicas(3);
  table_creator->set_range_partition_columns({col1, col2});

  RETURN_NOT_OK(table_creator->Create());
}

Status CopyTable(const RunnerContext& context) {
  // src table
  const string& src_table_name = FindOrDie(context.required_args, kTableNameArg);
  client::sp::shared_ptr<KuduClient> src_client;
  RETURN_NOT_OK(CreateKuduClient(context, &src_client));
  client::sp::shared_ptr<KuduTable> src_table;
  RETURN_NOT_OK(src_client->OpenTable(src_table_name, &src_table));

  // dst table
  const string& dst_table_name = src_table_name;
  client::sp::shared_ptr<KuduClient> dst_client;
  RETURN_NOT_OK(CreateDestKuduClient(context, &dst_client));
  client::sp::shared_ptr<KuduTable> dst_table;
  RETURN_NOT_OK(src_client->OpenTable(dst_table_name, &dst_table));

  const KuduSchema& table_schema = src_table->schema();
  size_t col_cnt = table_schema.num_columns();

  KuduScanner scanner(src_table.get());
  RETURN_NOT_OK(scanner.SetFaultTolerant());
  RETURN_NOT_OK(scanner.SetSelection(KuduClient::LEADER_ONLY));
  RETURN_NOT_OK(scanner.Open());

  client::sp::shared_ptr<KuduSession> session(dst_client->NewSession());
  const size_t flush_per_n_rows = FLAGS_flush_per_n_rows;
  RETURN_NOT_OK(session->SetMutationBufferFlushWatermark(
                   FLAGS_buffer_flush_watermark_pct));
  RETURN_NOT_OK(session->SetMutationBufferSpace(
                   FLAGS_buffer_size_bytes));
  RETURN_NOT_OK(session->SetMutationBufferMaxNum(FLAGS_buffers_num));
  RETURN_NOT_OK(session->SetErrorBufferSpace(FLAGS_error_buffer_size_bytes));
  RETURN_NOT_OK(session->SetFlushMode(
      flush_per_n_rows == 0 ? KuduSession::AUTO_FLUSH_BACKGROUND
                            : KuduSession::MANUAL_FLUSH));

  int count = 0;
  KuduScanBatch batch;
  while (scanner.HasMoreRows()) {
    RETURN_NOT_OK(scanner.NextBatch(&batch));
    for (auto it = batch.begin(); it != batch.end(); ++it) {
      KuduScanBatch::RowPtr row(*it);

      std::unique_ptr<KuduInsert> insert(dst_table->NewInsert());
      KuduPartialRow* insert_row = insert->mutable_row();
      for (int i = 0; i < col_cnt; ++i) {
        KuduColumnSchema col_schema = table_schema.Column(i);
        switch (col_schema.type()) {
          case KuduColumnSchema::DataType::INT8: {
            int8_t v;
            if (row.GetInt8(i, &v).ok()) {
              insert_row->SetInt8(i, v);
            }
            break;
          }
          case KuduColumnSchema::DataType::INT16: {
            int16_t v;
            if (row.GetInt16(i, &v).ok()) {
              insert_row->SetInt16(i, v);
            }
            break;
          }
          case KuduColumnSchema::DataType::INT32: {
            int32_t v;
            if (row.GetInt32(i, &v).ok()) {
              insert_row->SetInt32(i, v);
            }
            break;
          }
          case KuduColumnSchema::DataType::INT64: {
            int64_t v;
            if (row.GetInt64(i, &v).ok()) {
              insert_row->SetInt64(i, v);
            }
            break;
          }
          case KuduColumnSchema::DataType::STRING: {
            Slice v;
            if (row.GetString(i, &v).ok()) {
              insert_row->SetString(i, v);
            }
            break;
          }
          case KuduColumnSchema::DataType::BOOL: {
            bool v;
            if (row.GetBool(i, &v).ok()) {
              insert_row->SetBool(i, v);
            }
            break;
          }
          case KuduColumnSchema::DataType::FLOAT: {
            float v;
            if (row.GetFloat(i, &v).ok()) {
              insert_row->SetFloat(i, v);
            }
            break;
          }
          case KuduColumnSchema::DataType::DOUBLE: {
            double v;
            if (row.GetDouble(i, &v).ok()) {
              insert_row->SetDouble(i, v);
            }
            break;
          }
          case KuduColumnSchema::DataType::BINARY: {
            Slice v;
            if (row.GetBinary(i, &v).ok()) {
              insert_row->SetBinary(i, v);
            }
            break;
          }
          case KuduColumnSchema::DataType::UNIXTIME_MICROS: {
            int64_t v;
            if (row.GetUnixTimeMicros(i, &v).ok()) {
              insert_row->SetUnixTimeMicros(i, v);
            }
            break;
          }
          case KuduColumnSchema::DataType::DECIMAL: {
            int128_t v;
            if (row.GetUnscaledDecimal(i, &v).ok()) {
              insert_row->SetUnscaledDecimal(i, v);
            }
            break;
          }
          default:
            return Status::InvalidArgument("unknown data type: $1",
                                           std::to_string(col_schema.type()));
        }
      }
      RETURN_NOT_OK(session->Apply(insert.release()));
      ++count;
      if (flush_per_n_rows != 0 && count % flush_per_n_rows == 0) {
        session->FlushAsync(nullptr);
      }
    }
    RETURN_NOT_OK(session->Flush());
  }

  cout << "Total count: " << count << endl;

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

  unique_ptr<Action> create_fenqun_table =
      ActionBuilder("create_fenqun_table", &CreateTable)
      .Description("Create 'fenqun' table")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to create" })
      .Build();

  unique_ptr<Action> copy_table =
      ActionBuilder("copy", &CopyTable)
      .Description("Copy table")
      .AddRequiredParameter({ kMasterAddressesArg, kMasterAddressesArgDesc })
      .AddRequiredParameter({ kTableNameArg, "Name of the table to copy" })
      .AddRequiredParameter({ kDestMasterAddressesArg, "target cluster master_addresses of this table copy to" })
      // TODO schema change suopport
      .Build();

  return ModeBuilder("table")
      .Description("Operate on Kudu tables")
      .AddAction(std::move(delete_table))
      .AddAction(std::move(rename_table))
      .AddAction(std::move(rename_column))
      .AddAction(std::move(list_tables))
      .AddAction(std::move(create_fenqun_table))
      .AddAction(std::move(copy_table))
      .Build();
}

} // namespace tools
} // namespace kudu

