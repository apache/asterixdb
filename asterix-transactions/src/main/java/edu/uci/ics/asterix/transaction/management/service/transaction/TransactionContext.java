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
package edu.uci.ics.asterix.transaction.management.service.transaction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import edu.uci.ics.asterix.common.context.BaseOperationTracker;
import edu.uci.ics.asterix.common.context.PrimaryIndexOperationTracker;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.AbstractOperationCallback;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionManager.TransactionState;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.common.transactions.MutableLong;
import edu.uci.ics.asterix.transaction.management.opcallbacks.PrimaryIndexModificationOperationCallback;
import edu.uci.ics.asterix.transaction.management.service.logging.LogRecord;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMOperationType;

/**
 * Represents a holder object that contains all information related to a
 * transaction. A TransactionContext instance can be used as a token and
 * provided to Transaction sub-systems (Log/Lock/Recovery/Transaction)Manager to
 * initiate an operation on the behalf of the transaction associated with the
 * context.
 */
public class TransactionContext implements ITransactionContext, Serializable {

    private static final long serialVersionUID = -6105616785783310111L;
    private TransactionSubsystem transactionSubsystem;
    private final AtomicLong firstLSN;
    private final AtomicLong lastLSN;
    private TransactionState txnState;
    private long startWaitTime;
    private int status;
    private TransactionType transactionType = TransactionType.READ;
    private JobId jobId;
    private boolean exlusiveJobLevelCommit;
    private final Map<MutableLong, BaseOperationTracker> indexMap;
    private ILSMIndex primaryIndex;
    private PrimaryIndexModificationOperationCallback primaryIndexCallback;
    private PrimaryIndexOperationTracker primaryIndexOpTracker;
    private final MutableLong tempResourceIdForRegister;
    private final MutableLong tempResourceIdForSetLSN;
    private final LogRecord logRecord;

    public TransactionContext(JobId jobId, TransactionSubsystem transactionSubsystem) throws ACIDException {
        this.jobId = jobId;
        this.transactionSubsystem = transactionSubsystem;
        firstLSN = new AtomicLong(-1);
        lastLSN = new AtomicLong(-1);
        txnState = TransactionState.ACTIVE;
        startWaitTime = INVALID_TIME;
        status = ACTIVE_STATUS;
        indexMap = new HashMap<MutableLong, BaseOperationTracker>();
        primaryIndex = null;
        tempResourceIdForRegister = new MutableLong();
        tempResourceIdForSetLSN = new MutableLong();
        logRecord = new LogRecord();
    }

    public void registerIndexAndCallback(long resourceId, ILSMIndex index, AbstractOperationCallback callback,
            boolean isPrimaryIndex) {
        synchronized (indexMap) {
            if (isPrimaryIndex && primaryIndex == null) {
                primaryIndex = index;
                primaryIndexCallback = (PrimaryIndexModificationOperationCallback) callback;
                primaryIndexOpTracker = (PrimaryIndexOperationTracker) index.getOperationTracker();
            }
            tempResourceIdForRegister.set(resourceId);
            if (!indexMap.containsKey(tempResourceIdForRegister)) {
                indexMap.put(new MutableLong(resourceId), ((BaseOperationTracker) index.getOperationTracker()));
            }
        }
    }

    //[Notice] 
    //This method is called sequentially by the LogAppender threads. 
    //However, the indexMap is concurrently read and modified through this method and registerIndexAndCallback()
    //TODO: fix issues - 591, 609, 612, and 614.
    @Override
    public void setLastLSN(long resourceId, long LSN) {
        synchronized (indexMap) {
            firstLSN.compareAndSet(-1, LSN);
            lastLSN.set(Math.max(lastLSN.get(), LSN));
            tempResourceIdForSetLSN.set(resourceId);
            //TODO; create version number tracker and keep LSNs there. 
            BaseOperationTracker opTracker = indexMap.get(tempResourceIdForSetLSN);
            opTracker.updateLastLSN(LSN);
        }
    }

    @Override
    public void notifyOptracker(boolean isJobLevelCommit) {
        try {
            if (isJobLevelCommit && exlusiveJobLevelCommit) {
                primaryIndexOpTracker.exclusiveJobCommitted();
            } else if (!isJobLevelCommit){         
                primaryIndexOpTracker
                        .completeOperation(null, LSMOperationType.MODIFICATION, null, primaryIndexCallback);
            }
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    @Override
    public long getFirstLSN() {
        return firstLSN.get();
    }

    @Override
    public long getLastLSN() {
        return lastLSN.get();
    }

    public void setLastLSN(long LSN) {
        if (firstLSN.get() == -1) {
            firstLSN.set(LSN);
        }
        lastLSN.set(LSN);
    }

    public JobId getJobId() {
        return jobId;
    }

    public void setStartWaitTime(long time) {
        this.startWaitTime = time;
    }

    public long getStartWaitTime() {
        return startWaitTime;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public void setTxnState(TransactionState txnState) {
        this.txnState = txnState;
    }

    public TransactionState getTxnState() {
        return txnState;
    }

    @Override
    public int hashCode() {
        return jobId.getId();
    }

    @Override
    public boolean equals(Object o) {
        return (o == this);
    }

    @Override
    public void setExclusiveJobLevelCommit() {
        exlusiveJobLevelCommit = true;
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n" + jobId + "\n");
        sb.append("transactionType: " + transactionType);
        sb.append("firstLSN: " + firstLSN.get() + "\n");
        sb.append("lastLSN: " + lastLSN.get() + "\n");
        sb.append("TransactionState: " + txnState + "\n");
        sb.append("startWaitTime: " + startWaitTime + "\n");
        sb.append("status: " + status + "\n");
        return sb.toString();
    }

    public LogRecord getLogRecord() {
        return logRecord;
    }
}
