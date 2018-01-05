/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.replication.logging;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.replication.IReplicationDestination;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TxnAckTracker {

    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<Long, TxnAck> txnsAcks = new HashMap<>();

    public synchronized void track(ILogRecord logRecord, Set<IReplicationDestination> replicas) {
        if (replicas.isEmpty()) {
            logRecord.setReplicated(true);
            return;
        }
        final long txnId = logRecord.getTxnId();
        //TODO use LSN instead of txnId when all logs have LSN
        txnsAcks.put(txnId, new TxnAck(logRecord, replicas));
    }

    public synchronized void ack(long txnId, IReplicationDestination replica) {
        if (!txnsAcks.containsKey(txnId)) {
            LOGGER.warn("Received ack for unknown txn {}", txnId);
            return;
        }
        TxnAck txnAcks = txnsAcks.get(txnId);
        txnAcks.ack(replica);
        if (txnAcks.allAcked()) {
            txnsAcks.remove(txnId);
        }
    }

    public synchronized void unregister(IReplicationDestination replica) {
        // assume the ack was received from leaving replicas
        final HashSet<Long> pendingTxn = new HashSet<>(txnsAcks.keySet());
        pendingTxn.forEach(txnId -> ack(txnId, replica));
    }
}
