/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.opcallbacks.AbstractOperationCallback;
import edu.uci.ics.asterix.transaction.management.opcallbacks.IndexOperationTracker;
import edu.uci.ics.asterix.transaction.management.resource.ICloseable;
import edu.uci.ics.asterix.transaction.management.service.logging.LogUtil;
import edu.uci.ics.asterix.transaction.management.service.logging.LogicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.transaction.ITransactionManager.TransactionState;
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
public class TransactionContext implements Serializable {

    public static final long INVALID_TIME = -1l; // used for showing a
    // transaction is not waiting.
    public static final int ACTIVE_STATUS = 0;
    public static final int TIMED_OUT_STATUS = 1;

    public enum TransactionType {
        READ,
        READ_WRITE
    }

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

    // List of indexes on which operations were performed on behalf of this transaction.
    private final List<ILSMIndex> indexes = new ArrayList<ILSMIndex>();

    // List of operation callbacks corresponding to the operand indexes. In particular, needed to track
    // the number of active operations contributed by this transaction.
    private final List<AbstractOperationCallback> callbacks = new ArrayList<AbstractOperationCallback>();

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
                ((IndexOperationTracker) index.getOperationTracker()).updateLastLSN(lastLSN);
            }
        }
    }

    public void decreaseActiveTransactionCountOnIndexes() throws HyracksDataException {
        synchronized (indexes) {
            for (int i = 0; i < indexes.size(); i++) {
                ILSMIndex index = indexes.get(i);
                IModificationOperationCallback modificationCallback = (IModificationOperationCallback) callbacks.get(i);
                ((IndexOperationTracker) index.getOperationTracker()).completeOperation(LSMOperationType.MODIFICATION,
                        null, modificationCallback);
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

}
