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
package org.apache.asterix.transaction.management.opcallbacks;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.AbstractOperationCallback;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogType;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleWriter;

public abstract class AbstractIndexModificationOperationCallback extends AbstractOperationCallback {

    protected final long resourceId;
    protected final byte resourceType;
    protected final IndexOperation indexOp;
    protected final ITransactionSubsystem txnSubsystem;
    protected final SimpleTupleWriter tupleWriter;
    protected final ILogRecord logRecord;

    protected AbstractIndexModificationOperationCallback(int datasetId, int[] primaryKeyFields,
            ITransactionContext txnCtx, ILockManager lockManager, ITransactionSubsystem txnSubsystem, long resourceId,
            byte resourceType, IndexOperation indexOp) {
        super(datasetId, primaryKeyFields, txnCtx, lockManager);
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        this.indexOp = indexOp;
        this.txnSubsystem = txnSubsystem;
        tupleWriter = new SimpleTupleWriter();
        logRecord = new LogRecord();
        logRecord.setTxnCtx(txnCtx);
        logRecord.setLogType(LogType.UPDATE);
        logRecord.setJobId(txnCtx.getJobId().getId());
        logRecord.setDatasetId(datasetId);
        logRecord.setResourceId(resourceId);
        logRecord.setNewOp((byte) (indexOp.ordinal()));
        logRecord.setNodeId(txnSubsystem.getId());
    }

    protected void log(int PKHash, ITupleReference newValue) throws ACIDException {
        logRecord.setPKHashValue(PKHash);
        logRecord.setPKFields(primaryKeyFields);
        logRecord.setPKValue(newValue);
        logRecord.computeAndSetPKValueSize();
        if (newValue != null) {
            logRecord.setNewValueSize(tupleWriter.bytesRequired(newValue));
            logRecord.setNewValue(newValue);
        } else {
            logRecord.setNewValueSize(0);
        }
        logRecord.computeAndSetLogSize();
        txnSubsystem.getLogManager().log(logRecord);
    }
}
