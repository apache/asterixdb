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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.util.annotations.ThreadSafe;

@ThreadSafe
public class EntityLevelTransactionContext extends AbstractTransactionContext {

    private PrimaryIndexOperationTracker primaryIndexOpTracker;
    private IModificationOperationCallback primaryIndexCallback;
    private final AtomicInteger pendingOps;

    public EntityLevelTransactionContext(TxnId txnId) {
        super(txnId);
        pendingOps = new AtomicInteger(0);
    }

    @Override
    public void register(long resourceId, ILSMIndex index, IModificationOperationCallback callback,
            boolean primaryIndex) {
        super.register(resourceId, index, callback, primaryIndex);
        synchronized (txnOpTrackers) {
            if (primaryIndex && primaryIndexOpTracker == null) {
                primaryIndexCallback = callback;
                primaryIndexOpTracker = (PrimaryIndexOperationTracker) index.getOperationTracker();
            }
        }
    }

    @Override
    public void beforeOperation(long resourceId) {
        pendingOps.incrementAndGet();
    }

    @Override
    public void notifyUpdateCommitted(long resourceId) {
        // no op
    }

    @Override
    public void notifyEntityCommitted() {
        try {
            primaryIndexOpTracker.completeOperation(null, LSMOperationType.MODIFICATION, null, primaryIndexCallback);
        } catch (HyracksDataException e) {
            throw new ACIDException(e);
        }
    }

    @Override
    public void afterOperation(long resourceId) {
        pendingOps.decrementAndGet();
    }

    @Override
    protected void cleanupForAbort() {
        if (primaryIndexOpTracker != null) {
            primaryIndexOpTracker.cleanupNumActiveOperationsForAbortedJob(pendingOps.get());
        }
    }

    @Override
    public int hashCode() {
        return Long.hashCode(txnId.getId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EntityLevelTransactionContext that = (EntityLevelTransactionContext) o;
        return this.txnId.equals(that.txnId);
    }
}