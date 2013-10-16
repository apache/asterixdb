/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.transaction.management.opcallbacks;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.AbstractOperationCallback;
import edu.uci.ics.asterix.common.transactions.ILockManager;
import edu.uci.ics.asterix.common.transactions.ILogRecord;
import edu.uci.ics.asterix.common.transactions.IRecoveryManager.ResourceType;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.asterix.transaction.management.service.logging.LogRecord;
import edu.uci.ics.asterix.transaction.management.service.logging.LogType;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.tuples.SimpleTupleWriter;

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
        logRecord.setResourceType(resourceType);
        logRecord.setNewOp((byte) (indexOp.ordinal()));
    }

    protected void log(int PKHash, ITupleReference newValue, IndexOperation oldOp, ITupleReference oldValue)
            throws ACIDException {
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
        if (resourceType == ResourceType.LSM_BTREE) {
            logRecord.setOldOp((byte) (oldOp.ordinal()));
            if (oldValue != null) {
                logRecord.setOldValueSize(tupleWriter.bytesRequired(oldValue));
                logRecord.setOldValue(oldValue);
            } else {
                logRecord.setOldValueSize(0);
            }
        }
        logRecord.computeAndSetLogSize();
        txnSubsystem.getLogManager().log(logRecord);
    }
}
