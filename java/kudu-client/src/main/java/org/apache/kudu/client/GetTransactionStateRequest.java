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

package org.apache.kudu.client;

import static org.apache.kudu.transactions.TxnManager.GetTransactionStateResponsePB;

import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import io.netty.util.Timer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.transactions.TxnManager;
import org.apache.kudu.util.Pair;

/**
 * A wrapper class for kudu.transactions.TxnManagerService.GetTransactionState RPC.
 */
@InterfaceAudience.Private
class GetTransactionStateRequest extends KuduRpc<GetTransactionStateResponse> {
  private static final List<Integer> featureFlags = ImmutableList.of();
  final long txnId;

  GetTransactionStateRequest(
      KuduTable masterTable, Timer timer, long timeoutMillis, long txnId) {
    super(masterTable, timer, timeoutMillis);
    Preconditions.checkArgument(txnId > AsyncKuduClient.INVALID_TXN_ID);
    this.txnId = txnId;
  }

  @Override
  Message createRequestPB() {
    final TxnManager.GetTransactionStateRequestPB.Builder b =
        TxnManager.GetTransactionStateRequestPB.newBuilder();
    b.setTxnId(txnId);
    return b.build();
  }

  @Override
  String serviceName() {
    return TXN_MANAGER_SERVICE_NAME;
  }

  @Override
  String method() {
    return "GetTransactionState";
  }

  @Override
  Pair<GetTransactionStateResponse, Object> deserialize(
      final CallResponse callResponse, String serverUUID) throws KuduException {
    final GetTransactionStateResponsePB.Builder b =
        GetTransactionStateResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), b);
    if (!b.hasError()) {
      Preconditions.checkState(b.hasState());
    }
    OptionalLong ts = b.hasCommitTimestamp() ? OptionalLong.of(b.getCommitTimestamp())
                                             : OptionalLong.empty();
    GetTransactionStateResponse response = new GetTransactionStateResponse(
        timeoutTracker.getElapsedMillis(), serverUUID, b.getState(), ts);
    return new Pair<>(response, b.hasError() ? b.getError() : null);
  }

  @Override
  Collection<Integer> getRequiredFeatures() {
    return featureFlags;
  }
}
