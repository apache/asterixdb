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
package org.apache.asterix.common.utils;

import java.nio.ByteBuffer;

import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogSource;
import org.apache.asterix.common.transactions.LogType;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class TransactionUtil {

    private TransactionUtil() {
    }

    public static void formJobTerminateLogRecord(ITransactionContext txnCtx, LogRecord logRecord, boolean isCommit) {
        logRecord.setTxnCtx(txnCtx);
        TransactionUtil.formJobTerminateLogRecord(logRecord, txnCtx.getTxnId().getId(), isCommit);
    }

    public static void formJobTerminateLogRecord(LogRecord logRecord, long txnId, boolean isCommit) {
        logRecord.setLogType(isCommit ? LogType.JOB_COMMIT : LogType.ABORT);
        logRecord.setDatasetId(-1);
        logRecord.setPKHashValue(-1);
        logRecord.setTxnId(txnId);
        logRecord.computeAndSetLogSize();
    }

    public static void formFlushLogRecord(LogRecord logRecord, int datasetId, int resourcePartition,
            long flushingComponentMinId, long flushingComponentMaxId, PrimaryIndexOperationTracker opTracker) {
        logRecord.setLogType(LogType.FLUSH);
        logRecord.setTxnId(-1);
        logRecord.setDatasetId(datasetId);
        logRecord.setResourcePartition(resourcePartition);
        logRecord.setFlushingComponentMinId(flushingComponentMinId);
        logRecord.setFlushingComponentMaxId(flushingComponentMaxId);
        logRecord.setOpTracker(opTracker);
        logRecord.computeAndSetLogSize();
    }

    public static void formEntityCommitLogRecord(LogRecord logRecord, ITransactionContext txnCtx, int datasetId,
            int pKHashValue, ITupleReference pKValue, int[] pKFields, int resourcePartition, byte entityCommitType) {
        logRecord.setTxnCtx(txnCtx);
        logRecord.setLogType(entityCommitType);
        logRecord.setTxnId(txnCtx.getTxnId().getId());
        logRecord.setDatasetId(datasetId);
        logRecord.setPKHashValue(pKHashValue);
        logRecord.setPKFieldCnt(pKFields.length);
        logRecord.setPKValue(pKValue);
        logRecord.setPKFields(pKFields);
        logRecord.setResourcePartition(resourcePartition);
        logRecord.computeAndSetPKValueSize();
        logRecord.computeAndSetLogSize();
    }

    public static void formMarkerLogRecord(LogRecord logRecord, ITransactionContext txnCtx, int datasetId,
            int resourcePartition, ByteBuffer marker) {
        logRecord.setTxnCtx(txnCtx);
        logRecord.setLogSource(LogSource.LOCAL);
        logRecord.setLogType(LogType.MARKER);
        logRecord.setTxnId(txnCtx.getTxnId().getId());
        logRecord.setDatasetId(datasetId);
        logRecord.setResourcePartition(resourcePartition);
        marker.get(); // read the first byte since it is not part of the marker object
        logRecord.setMarker(marker);
        logRecord.computeAndSetLogSize();
    }
}
