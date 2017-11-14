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
package org.apache.asterix.transaction.management.service.transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.asterix.common.context.ITransactionOperationTracker;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.AbstractOperationCallback;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IModificationOperationCallback;

/*
 * An object of TransactionContext is created and accessed(read/written) by multiple threads which work for
 * a single job identified by a txnId. Thus, the member variables in the object can be read/written
 * concurrently. Please see each variable declaration to know which one is accessed concurrently and
 * which one is not.
 */
public class TransactionContext implements ITransactionContext {

    private static final long serialVersionUID = -6105616785783310111L;

    // txnId is set once and read concurrently.
    private final TxnId txnId;

    // There are no concurrent writers on both firstLSN and lastLSN
    // since both values are updated by serialized log appenders.
    // But readers and writers can be different threads,
    // so both LSNs are atomic variables in order to be read and written
    // atomically.
    private final AtomicLong firstLSN;
    private final AtomicLong lastLSN;

    // txnState is read and written concurrently.
    private final AtomicInteger txnState;

    // isTimeout is read and written under the lockMgr's tableLatch
    // Thus, no other synchronization is required separately.
    private boolean isTimeout;

    // isWriteTxn can be set concurrently by multiple threads.
    private final AtomicBoolean isWriteTxn;

    // isMetadataTxn is accessed by a single thread since the metadata is not
    // partitioned
    private boolean isMetadataTxn;

    // indexMap is concurrently accessed by multiple threads,
    // so those threads are synchronized on indexMap object itself
    private final Map<Long, ITransactionOperationTracker> indexMap;

    // TODO: fix ComponentLSNs' issues.
    // primaryIndex, primaryIndexCallback, and primaryIndexOptracker will be
    // modified accordingly
    // when the issues of componentLSNs are fixed.
    private ILSMIndex primaryIndex;
    private AbstractOperationCallback primaryIndexCallback;
    private PrimaryIndexOperationTracker primaryIndexOpTracker;

    // The following three variables are used as temporary variables in order to
    // avoid object creations.
    // Those are used in synchronized methods.
    private final LogRecord logRecord;

    private final AtomicInteger transactorNumActiveOperations;

    // TODO: implement transactionContext pool in order to avoid object
    // creations.
    // also, the pool can throttle the number of concurrent active jobs at every
    // moment.
    public TransactionContext(TxnId txnId) throws ACIDException {
        this.txnId = txnId;
        firstLSN = new AtomicLong(-1);
        lastLSN = new AtomicLong(-1);
        txnState = new AtomicInteger(ITransactionManager.ACTIVE);
        isTimeout = false;
        isWriteTxn = new AtomicBoolean(false);
        isMetadataTxn = false;
        indexMap = new HashMap<>();
        primaryIndex = null;
        logRecord = new LogRecord();
        transactorNumActiveOperations = new AtomicInteger(0);
    }

    @Override
    public void registerIndexAndCallback(long resourceId, ILSMIndex index, AbstractOperationCallback callback,
            boolean isPrimaryIndex) {
        synchronized (indexMap) {
            if (isPrimaryIndex && primaryIndex == null) {
                primaryIndex = index;
                primaryIndexCallback = callback;
                primaryIndexOpTracker = (PrimaryIndexOperationTracker) index.getOperationTracker();
            }
            if (!indexMap.containsKey(resourceId)) {
                final ITransactionOperationTracker txnOpTracker =
                        (ITransactionOperationTracker) index.getOperationTracker();
                indexMap.put(resourceId, txnOpTracker);
                txnOpTracker.beforeTransaction(resourceId);
            }
        }
    }

    public PrimaryIndexOperationTracker getPrimaryIndexOpTracker() {
        synchronized (indexMap) {
            return primaryIndexOpTracker;
        }
    }

    // [Notice]
    // This method is called sequentially by the LogAppender threads.
    @Override
    public void setLastLSN(long LSN) {
        firstLSN.compareAndSet(-1, LSN);
        lastLSN.set(Math.max(lastLSN.get(), LSN));
    }

    @Override
    public void notifyOptracker(boolean isJobLevelCommit) {
        try {
            /**
             * in case of transaction abort {@link TransactionContext#cleanupForAbort()} will
             * clean the primaryIndexOpTracker state.
             */
            if (isJobLevelCommit && isMetadataTxn && txnState.get() != ITransactionManager.ABORTED) {
                primaryIndexOpTracker.exclusiveJobCommitted();
            } else if (!isJobLevelCommit) {
                primaryIndexOpTracker.completeOperation(null, LSMOperationType.MODIFICATION, null,
                        (IModificationOperationCallback) primaryIndexCallback);
            }
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void setWriteTxn(boolean isWriteTxn) {
        this.isWriteTxn.set(isWriteTxn);
    }

    @Override
    public boolean isWriteTxn() {
        return isWriteTxn.get();
    }

    @Override
    public long getFirstLSN() {
        return firstLSN.get();
    }

    @Override
    public long getLastLSN() {
        return lastLSN.get();
    }

    @Override
    public TxnId getTxnId() {
        return txnId;
    }

    @Override
    public void setTimeout(boolean isTimeout) {
        this.isTimeout = isTimeout;
    }

    @Override
    public boolean isTimeout() {
        return isTimeout;
    }

    @Override
    public void setTxnState(int txnState) {
        this.txnState.set(txnState);
    }

    @Override
    public int getTxnState() {
        return txnState.get();
    }

    @Override
    public int hashCode() {
        return Long.hashCode(txnId.getId());
    }

    @Override
    public boolean equals(Object o) {
        return (o == this);
    }

    @Override
    public void setMetadataTransaction(boolean isMetadataTxn) {
        this.isMetadataTxn = isMetadataTxn;
    }

    @Override
    public boolean isMetadataTransaction() {
        return isMetadataTxn;
    }

    @Override
    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n" + txnId + "\n");
        sb.append("isWriteTxn: " + isWriteTxn + "\n");
        sb.append("firstLSN: " + firstLSN.get() + "\n");
        sb.append("lastLSN: " + lastLSN.get() + "\n");
        sb.append("TransactionState: " + txnState + "\n");
        sb.append("isTimeout: " + isTimeout + "\n");
        return sb.toString();
    }

    public LogRecord getLogRecord() {
        return logRecord;
    }

    @Override
    public void incrementNumActiveOperations() {
        transactorNumActiveOperations.incrementAndGet();
    }

    @Override
    public void decrementNumActiveOperations() {
        transactorNumActiveOperations.decrementAndGet();
    }

    @Override
    public void complete() {
        try {
            if (txnState.get() == ITransactionManager.ABORTED) {
                cleanupForAbort();
            }
        } finally {
            synchronized (indexMap) {
                indexMap.forEach((resource, opTracker) -> opTracker.afterTransaction(resource));
            }
        }
    }

    private void cleanupForAbort() {
        if (primaryIndexOpTracker != null) {
            primaryIndexOpTracker.cleanupNumActiveOperationsForAbortedJob(transactorNumActiveOperations.get());
        }
    }
}