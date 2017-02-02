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

    public static final boolean PROFILE_MODE = false;

    private TransactionUtil() {
    }

    public static void formJobTerminateLogRecord(ITransactionContext txnCtx, LogRecord logRecord, boolean isCommit) {
        logRecord.setTxnCtx(txnCtx);
        TransactionUtil.formJobTerminateLogRecord(logRecord, txnCtx.getJobId().getId(), isCommit);
    }

    public static void formJobTerminateLogRecord(LogRecord logRecord, int jobId, boolean isCommit) {
        logRecord.setLogType(isCommit ? LogType.JOB_COMMIT : LogType.ABORT);
        logRecord.setDatasetId(-1);
        logRecord.setPKHashValue(-1);
        logRecord.setJobId(jobId);
        logRecord.computeAndSetLogSize();
    }

    public static void formFlushLogRecord(LogRecord logRecord, int datasetId, PrimaryIndexOperationTracker opTracker,
            String nodeId, int numberOfIndexes) {
        logRecord.setLogType(LogType.FLUSH);
        logRecord.setJobId(-1);
        logRecord.setDatasetId(datasetId);
        logRecord.setOpTracker(opTracker);
        logRecord.setNumOfFlushedIndexes(numberOfIndexes);
        logRecord.setNodeId(nodeId);
        logRecord.computeAndSetLogSize();
    }

    public static void formEntityCommitLogRecord(LogRecord logRecord, ITransactionContext txnCtx, int datasetId,
            int PKHashValue, ITupleReference PKValue, int[] PKFields, int resourcePartition, byte entityCommitType) {
        logRecord.setTxnCtx(txnCtx);
        logRecord.setLogType(entityCommitType);
        logRecord.setJobId(txnCtx.getJobId().getId());
        logRecord.setDatasetId(datasetId);
        logRecord.setPKHashValue(PKHashValue);
        logRecord.setPKFieldCnt(PKFields.length);
        logRecord.setPKValue(PKValue);
        logRecord.setPKFields(PKFields);
        logRecord.setResourcePartition(resourcePartition);
        logRecord.computeAndSetPKValueSize();
        logRecord.computeAndSetLogSize();
    }

    public static void formMarkerLogRecord(LogRecord logRecord, ITransactionContext txnCtx, int datasetId,
            int resourcePartition, ByteBuffer marker) {
        logRecord.setTxnCtx(txnCtx);
        logRecord.setLogSource(LogSource.LOCAL);
        logRecord.setLogType(LogType.MARKER);
        logRecord.setJobId(txnCtx.getJobId().getId());
        logRecord.setDatasetId(datasetId);
        logRecord.setResourcePartition(resourcePartition);
        marker.get(); // read the first byte since it is not part of the marker object
        logRecord.setMarker(marker);
        logRecord.computeAndSetLogSize();
    }
}
