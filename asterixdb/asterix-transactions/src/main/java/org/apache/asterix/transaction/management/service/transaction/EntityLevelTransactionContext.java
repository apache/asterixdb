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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.util.annotations.ThreadSafe;

@ThreadSafe
public class EntityLevelTransactionContext extends AbstractTransactionContext {

    private final Map<Integer, Pair<PrimaryIndexOperationTracker, IModificationOperationCallback>> primaryIndexTrackers;
    private final Map<Long, AtomicInteger> resourcePendingOps;
    private final Map<Integer, AtomicInteger> partitionPendingOps;

    public EntityLevelTransactionContext(TxnId txnId) {
        super(txnId);
        this.primaryIndexTrackers = new ConcurrentHashMap<>();
        this.resourcePendingOps = new ConcurrentHashMap<>();
        this.partitionPendingOps = new ConcurrentHashMap<>();
    }

    @Override
    public void register(long resourceId, int partition, ILSMIndex index, IModificationOperationCallback callback,
            boolean primaryIndex) {
        super.register(resourceId, partition, index, callback, primaryIndex);
        AtomicInteger pendingOps = partitionPendingOps.computeIfAbsent(partition, p -> new AtomicInteger(0));
        resourcePendingOps.put(resourceId, pendingOps);
        if (primaryIndex) {
            Pair<PrimaryIndexOperationTracker, IModificationOperationCallback> pair =
                    new Pair<>((PrimaryIndexOperationTracker) index.getOperationTracker(), callback);
            primaryIndexTrackers.put(partition, pair);
        }
    }

    @Override
    public void beforeOperation(long resourceId) {
        resourcePendingOps.get(resourceId).incrementAndGet();
    }

    @Override
    public void notifyEntityCommitted(int partition) {
        try {
            Pair<PrimaryIndexOperationTracker, IModificationOperationCallback> pair =
                    primaryIndexTrackers.get(partition);
            pair.first.completeOperation(null, LSMOperationType.MODIFICATION, null, pair.second);
        } catch (HyracksDataException e) {
            throw new ACIDException(e);
        }
    }

    @Override
    public void afterOperation(long resourceId) {
        resourcePendingOps.get(resourceId).decrementAndGet();
    }

    @Override
    protected void cleanup() {
        if (getTxnState() == ITransactionManager.ABORTED) {
            primaryIndexTrackers.forEach((partitionId, opTracker) -> {
                int pendingOps = partitionPendingOps.get(partitionId).intValue();
                for (int i = 0; i < pendingOps; i++) {
                    try {
                        opTracker.first.completeOperation(null, LSMOperationType.MODIFICATION, null, opTracker.second);
                    } catch (HyracksDataException ex) {
                        throw new ACIDException(ex);
                    }
                }
            });
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
