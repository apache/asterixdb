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
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleWriter;

public abstract class AbstractIndexModificationOperationCallback extends AbstractOperationCallback
        implements IExtendedModificationOperationCallback {
    public static final byte INSERT_BYTE = 0x01;
    public static final byte DELETE_BYTE = 0x02;
    public static final byte UPSERT_BYTE = 0x03;
    public static final byte FILTER_BYTE = 0x04;

    public enum Operation {
        INSERT(INSERT_BYTE),
        DELETE(DELETE_BYTE),
        UPSERT(UPSERT_BYTE),
        FILTER_MOD(FILTER_BYTE);

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
                case FILTER_MOD:
                    return FILTER_MOD;
                case UPSERT:
                    return UPSERT;
                default:
                    throw new IllegalArgumentException();

            }
        }
    }

    protected final byte resourceType;
    protected final Operation indexOp;
    protected final ITransactionSubsystem txnSubsystem;
    protected final ILogRecord indexRecord;
    protected final ILogRecord filterRecord;

    protected AbstractIndexModificationOperationCallback(DatasetId datasetId, int[] primaryKeyFields,
            ITransactionContext txnCtx, ILockManager lockManager, ITransactionSubsystem txnSubsystem, long resourceId,
            int resourcePartition, byte resourceType, Operation indexOp) {
        super(datasetId, resourceId, primaryKeyFields, txnCtx, lockManager);
        this.resourceType = resourceType;
        this.indexOp = indexOp;
        this.txnSubsystem = txnSubsystem;
        indexRecord = new LogRecord();
        indexRecord.setTxnCtx(txnCtx);
        indexRecord.setLogType(LogType.UPDATE);
        indexRecord.setTxnId(txnCtx.getTxnId().getId());
        indexRecord.setDatasetId(datasetId.getId());
        indexRecord.setResourceId(resourceId);
        indexRecord.setResourcePartition(resourcePartition);
        indexRecord.setNewOp(indexOp.value());
        filterRecord = new LogRecord();
        filterRecord.setTxnCtx(txnCtx);
        filterRecord.setLogType(LogType.FILTER);
        filterRecord.setDatasetId(datasetId.getId());
        filterRecord.setTxnId(txnCtx.getTxnId().getId());
        filterRecord.setResourceId(resourceId);
        filterRecord.setResourcePartition(resourcePartition);
        filterRecord.setNewOp(Operation.FILTER_MOD.value());
    }

    protected void log(int PKHash, ITupleReference newValue, ITupleReference oldValue) throws ACIDException {
        indexRecord.setPKHashValue(PKHash);
        indexRecord.setPKFields(primaryKeyFields);
        indexRecord.setPKValue(newValue);
        indexRecord.computeAndSetPKValueSize();
        if (newValue != null) {
            indexRecord.setNewValueSize(SimpleTupleWriter.INSTANCE.bytesRequired(newValue));
            indexRecord.setNewValue(newValue);
        } else {
            indexRecord.setNewValueSize(0);
        }
        if (oldValue != null) {
            indexRecord.setOldValueSize(SimpleTupleWriter.INSTANCE.bytesRequired(oldValue));
            indexRecord.setOldValue(oldValue);
        } else {
            indexRecord.setOldValueSize(0);
        }
        indexRecord.computeAndSetLogSize();
        txnSubsystem.getLogManager().log(indexRecord);
    }

    public void after(ITupleReference newValue) throws HyracksDataException {
        if (newValue != null) {
            filterRecord.setNewValueSize(SimpleTupleWriter.INSTANCE.bytesRequired(newValue));
            filterRecord.setNewValue(newValue);
            filterRecord.computeAndSetLogSize();
            txnSubsystem.getLogManager().log(filterRecord);
        }
    }

    /**
     * This call specifies the next operation to be performed. It is used to allow
     * a single operator to perform different operations per tuple
     *
     * @param op
     */
    public void setOp(Operation op) {
        indexRecord.setNewOp(op.value());
    }
}
