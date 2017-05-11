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
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ILogRecord;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleWriter;
import org.apache.hyracks.storage.common.IModificationOperationCallback;

public abstract class AbstractIndexModificationOperationCallback extends AbstractOperationCallback
        implements IModificationOperationCallback {
    public static final byte INSERT_BYTE = 0x01;
    public static final byte DELETE_BYTE = 0x02;
    public static final byte UPSERT_BYTE = 0x03;

    public enum Operation {
        INSERT(INSERT_BYTE),
        DELETE(DELETE_BYTE),
        UPSERT(UPSERT_BYTE);
        private byte value;

        Operation(byte value) {
            this.value = value;
        }

        byte value() {
            return value;
        }

        public static Operation get(IndexOperation op) {
            switch (op) {
                case DELETE:
                    return DELETE;
                case INSERT:
                    return INSERT;
                case UPSERT:
                    return UPSERT;
                default:
                    throw new IllegalArgumentException();

            }
        }
    }

    protected final long resourceId;
    protected final byte resourceType;
    protected final Operation indexOp;
    protected final ITransactionSubsystem txnSubsystem;
    protected final ILogRecord logRecord;

    protected AbstractIndexModificationOperationCallback(DatasetId datasetId, int[] primaryKeyFields,
            ITransactionContext txnCtx, ILockManager lockManager, ITransactionSubsystem txnSubsystem, long resourceId,
            int resourcePartition, byte resourceType, Operation indexOp) {
        super(datasetId, primaryKeyFields, txnCtx, lockManager);
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        this.indexOp = indexOp;
        this.txnSubsystem = txnSubsystem;
        logRecord = new LogRecord();
        logRecord.setTxnCtx(txnCtx);
        logRecord.setLogType(LogType.UPDATE);
        logRecord.setJobId(txnCtx.getJobId().getId());
        logRecord.setDatasetId(datasetId.getId());
        logRecord.setResourceId(resourceId);
        logRecord.setResourcePartition(resourcePartition);
        logRecord.setNewOp(indexOp.value());
    }

    protected void log(int PKHash, ITupleReference newValue, ITupleReference oldValue) throws ACIDException {
        logRecord.setPKHashValue(PKHash);
        logRecord.setPKFields(primaryKeyFields);
        logRecord.setPKValue(newValue);
        logRecord.computeAndSetPKValueSize();
        if (newValue != null) {
            logRecord.setNewValueSize(SimpleTupleWriter.INSTANCE.bytesRequired(newValue));
            logRecord.setNewValue(newValue);
        } else {
            logRecord.setNewValueSize(0);
        }
        if (oldValue != null) {
            logRecord.setOldValueSize(SimpleTupleWriter.INSTANCE.bytesRequired(oldValue));
            logRecord.setOldValue(oldValue);
        } else {
            logRecord.setOldValueSize(0);
        }
        logRecord.computeAndSetLogSize();
        txnSubsystem.getLogManager().log(logRecord);
    }

    /**
     * This call specifies the next operation to be performed. It is used to allow
     * a single operator to perform different operations per tuple
     *
     * @param op
     * @throws HyracksDataException
     */
    public void setOp(Operation op) throws HyracksDataException {
        logRecord.setNewOp(op.value());
    }
}
