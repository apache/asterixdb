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
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.util.annotations.ThreadSafe;

@ThreadSafe
public abstract class AbstractTransactionContext implements ITransactionContext {

    protected final TxnId txnId;
    private final Map<Long, ITransactionOperationTracker> txnOpTrackers;
    private final AtomicLong firstLSN;
    private final AtomicLong lastLSN;
    private final AtomicInteger txnState;
    private final AtomicBoolean isWriteTxn;
    private volatile boolean isTimeout;

    protected AbstractTransactionContext(TxnId txnId) {
        this.txnId = txnId;
        firstLSN = new AtomicLong(-1);
        lastLSN = new AtomicLong(-1);
        txnState = new AtomicInteger(ITransactionManager.ACTIVE);
        isTimeout = false;
        isWriteTxn = new AtomicBoolean();
        txnOpTrackers = new HashMap<>();
    }

    @Override
    public long getFirstLSN() {
        return firstLSN.get();
    }

    @Override
    public void setLastLSN(long newValue) {
        firstLSN.compareAndSet(-1, newValue);
        lastLSN.set(Math.max(lastLSN.get(), newValue));
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
    public void setWriteTxn(boolean isWriteTxn) {
        this.isWriteTxn.set(isWriteTxn);
    }

    @Override
    public boolean isWriteTxn() {
        return isWriteTxn.get();
    }

    @Override
    public long getLastLSN() {
        return lastLSN.get();
    }

    @Override
    public void complete() {
        try {
            if (isWriteTxn()) {
                cleanup();
            }
        } finally {
            synchronized (txnOpTrackers) {
                txnOpTrackers.forEach((resource, opTracker) -> opTracker.afterTransaction(resource));
            }
        }
    }

    @Override
    public void register(long resourceId, int partition, ILSMIndex index, IModificationOperationCallback callback,
            boolean primaryIndex) {
        synchronized (txnOpTrackers) {
            if (!txnOpTrackers.containsKey(resourceId)) {
                final ITransactionOperationTracker txnOpTracker =
                        (ITransactionOperationTracker) index.getOperationTracker();
                txnOpTrackers.put(resourceId, txnOpTracker);
                txnOpTracker.beforeTransaction(resourceId);
            }
        }
    }

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

    protected abstract void cleanup();
}
