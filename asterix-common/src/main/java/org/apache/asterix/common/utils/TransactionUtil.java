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

import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogType;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class TransactionUtil {

    public static void formJobTerminateLogRecord(ITransactionContext txnCtx, LogRecord logRecord, boolean isCommit) {
        logRecord.setTxnCtx(txnCtx);
        TransactionUtil.formJobTerminateLogRecord(logRecord, txnCtx.getJobId().getId(), isCommit,
                logRecord.getNodeId());
    }

    public static void formJobTerminateLogRecord(LogRecord logRecord, int jobId, boolean isCommit, String nodeId) {
        logRecord.setLogType(isCommit ? LogType.JOB_COMMIT : LogType.ABORT);
        logRecord.setDatasetId(-1);
        logRecord.setPKHashValue(-1);
        logRecord.setJobId(jobId);
        logRecord.setNodeId(nodeId);
        logRecord.computeAndSetLogSize();
    }

    public static void formFlushLogRecord(LogRecord logRecord, int datasetId, PrimaryIndexOperationTracker opTracker,
            int numOfFlushedIndexes) {
        formFlushLogRecord(logRecord, datasetId, opTracker, null, numOfFlushedIndexes);
    }

    public static void formFlushLogRecord(LogRecord logRecord, int datasetId, PrimaryIndexOperationTracker opTracker,
            String nodeId, int numberOfIndexes) {
        logRecord.setLogType(LogType.FLUSH);
        logRecord.setJobId(-1);
        logRecord.setDatasetId(datasetId);
        logRecord.setOpTracker(opTracker);
        logRecord.setNumOfFlushedIndexes(numberOfIndexes);
        if (nodeId != null) {
            logRecord.setNodeId(nodeId);
        }
        logRecord.computeAndSetLogSize();
    }

    public static void formEntityCommitLogRecord(LogRecord logRecord, ITransactionContext txnCtx, int datasetId,
            int PKHashValue, ITupleReference PKValue, int[] PKFields) {
        logRecord.setTxnCtx(txnCtx);
        logRecord.setLogType(LogType.ENTITY_COMMIT);
        logRecord.setJobId(txnCtx.getJobId().getId());
        logRecord.setDatasetId(datasetId);
        logRecord.setPKHashValue(PKHashValue);
        logRecord.setPKFieldCnt(PKFields.length);
        logRecord.setPKValue(PKValue);
        logRecord.setPKFields(PKFields);
        logRecord.computeAndSetPKValueSize();
        logRecord.computeAndSetLogSize();
    }

    public static void formEntityUpsertCommitLogRecord(LogRecord logRecord, ITransactionContext txnCtx, int datasetId,
            int PKHashValue, ITupleReference PKValue, int[] PKFields) {
        logRecord.setTxnCtx(txnCtx);
        logRecord.setLogType(LogType.UPSERT_ENTITY_COMMIT);
        logRecord.setJobId(txnCtx.getJobId().getId());
        logRecord.setDatasetId(datasetId);
        logRecord.setPKHashValue(PKHashValue);
        logRecord.setPKFieldCnt(PKFields.length);
        logRecord.setPKValue(PKValue);
        logRecord.setPKFields(PKFields);
        logRecord.computeAndSetPKValueSize();
        logRecord.computeAndSetLogSize();
    }
}
