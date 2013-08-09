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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import edu.uci.ics.asterix.common.context.BaseOperationTracker;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.AbstractOperationCallback;
import edu.uci.ics.asterix.common.transactions.ICloseable;
import edu.uci.ics.asterix.common.transactions.ITransactionContext;
import edu.uci.ics.asterix.common.transactions.ITransactionManager.TransactionState;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.common.transactions.LogUtil;
import edu.uci.ics.asterix.common.transactions.LogicalLogLocator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
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
    private LogicalLogLocator firstLogLocator;//firstLSN of the Job
    private LogicalLogLocator lastLogLocator;//lastLSN of the Job
    private TransactionState txnState;
    private long startWaitTime;
    private int status;
    private Set<ICloseable> resources = new HashSet<ICloseable>();
    private TransactionType transactionType = TransactionType.READ;
    private JobId jobId;
    private boolean exlusiveJobLevelCommit;

    // List of indexes on which operations were performed on behalf of this transaction.
    private final Set<ILSMIndex> indexes = new HashSet<ILSMIndex>();

    // List of operation callbacks corresponding to the operand indexes. In particular, needed to track
    // the number of active operations contributed by this transaction.
    private final Set<AbstractOperationCallback> callbacks = new HashSet<AbstractOperationCallback>();

    public TransactionContext(JobId jobId, TransactionSubsystem transactionSubsystem) throws ACIDException {
        this.jobId = jobId;
        this.transactionSubsystem = transactionSubsystem;
        init();
    }

    private void init() throws ACIDException {
        firstLogLocator = LogUtil.getDummyLogicalLogLocator(transactionSubsystem.getLogManager());
        lastLogLocator = LogUtil.getDummyLogicalLogLocator(transactionSubsystem.getLogManager());
        txnState = TransactionState.ACTIVE;
        startWaitTime = INVALID_TIME;
        status = ACTIVE_STATUS;
    }

    public void registerIndexAndCallback(ILSMIndex index, AbstractOperationCallback callback) {
        synchronized (indexes) {
            indexes.add(index);
            callbacks.add(callback);
        }
    }

    public void updateLastLSNForIndexes(long lastLSN) {
        synchronized (indexes) {
            for (ILSMIndex index : indexes) {
                ((BaseOperationTracker) index.getOperationTracker()).updateLastLSN(lastLSN);
            }
        }
    }

    public void decreaseActiveTransactionCountOnIndexes() throws HyracksDataException {
        synchronized (indexes) {
            Set<BaseOperationTracker> opTrackers = new HashSet<BaseOperationTracker>();
            Iterator<ILSMIndex> indexIt = indexes.iterator();
            Iterator<AbstractOperationCallback> cbIt = callbacks.iterator();
            while (indexIt.hasNext()) {
                ILSMIndex index = indexIt.next();
                opTrackers.add((BaseOperationTracker) index.getOperationTracker());
                assert cbIt.hasNext();
            }
            Iterator<BaseOperationTracker> trackerIt = opTrackers.iterator();
            while (trackerIt.hasNext()) {
                IModificationOperationCallback modificationCallback = (IModificationOperationCallback) cbIt.next();
                BaseOperationTracker opTracker = (BaseOperationTracker) trackerIt.next();
                if (exlusiveJobLevelCommit) {
                    // For metadata transactions only
                    opTracker.exclusiveJobCommitted();
                } else {
                    opTracker.completeOperation(null, LSMOperationType.MODIFICATION, null, modificationCallback);
                }
            }
        }
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void addCloseableResource(ICloseable resource) {
        resources.add(resource);
    }

    public LogicalLogLocator getFirstLogLocator() {
        return firstLogLocator;
    }

    public LogicalLogLocator getLastLogLocator() {
        return lastLogLocator;
    }

    public void setLastLSN(long lsn) {
        if (firstLogLocator.getLsn() == -1) {
            firstLogLocator.setLsn(lsn);
        }
        lastLogLocator.setLsn(lsn);
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

    public void releaseResources() throws ACIDException {
        for (ICloseable closeable : resources) {
            closeable.close(this);
        }
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

    @Override
    public boolean isExlusiveJobLevelCommit() {
        return exlusiveJobLevelCommit;
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n" + jobId + "\n");
        sb.append("transactionType: " + transactionType);
        sb.append("firstLogLocator: " + firstLogLocator.getLsn() + "\n");
        sb.append("lastLogLocator: " + lastLogLocator.getLsn() + "\n");
        sb.append("TransactionState: " + txnState + "\n");
        sb.append("startWaitTime: " + startWaitTime + "\n");
        sb.append("status: " + status + "\n");
        return sb.toString();
    }
}
