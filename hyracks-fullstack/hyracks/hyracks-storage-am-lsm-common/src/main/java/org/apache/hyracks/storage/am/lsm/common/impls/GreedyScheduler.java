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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

import org.apache.hyracks.storage.am.lsm.common.api.IIoOperationFailedCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerFactory;

/**
 * Under the greedy scheduler, a merge operation has the following lifecycles. When the merge policy submits a
 * merge operation to the greedy scheduler, the merge operation is SCHEDULED if the number of scheduled merge
 * operations is smaller than maxNumScheduledMergeOperations; otherwise, the merge operation is WAITING and is
 * stored into a queue. WAITING merge operations will be scheduled after some existing merge operations finish
 * in a FIFO order.
 *
 * The greedy scheduler always runs at most one (and smallest) merge operation for each LSM-tree. The maximum number of
 * running merge operations is controlled by maxNumRunningMergeOperations. A SCHEDULED merge operation can become
 * RUNNING if the greedy scheduler resumes this merge operation, and a RUNNING merge operation can become SCHEDULED
 * if the greedy scheduler pauses this merge operation.
 *
 */
public class GreedyScheduler extends AbstractAsynchronousScheduler {
    public static ILSMIOOperationSchedulerFactory FACTORY = new ILSMIOOperationSchedulerFactory() {
        @Override
        public ILSMIOOperationScheduler createIoScheduler(ThreadFactory threadFactory,
                IIoOperationFailedCallback callback, int maxNumRunningFlushes, int maxNumScheduledMerges,
                int maxNumRunningMerges) {
            return new GreedyScheduler(threadFactory, callback, maxNumRunningFlushes, maxNumScheduledMerges,
                    maxNumRunningMerges);
        }

        @Override
        public String getName() {
            return "greedy";
        }
    };

    private final int maxNumScheduledMerges;
    private final int maxNumRunningMerges;

    private int numScheduledMerges;
    private final Map<String, Set<ILSMIOOperation>> scheduledMergeOperations = new HashMap<>();
    private final Map<String, ILSMIOOperation> runningMergeOperations = new HashMap<>();

    public GreedyScheduler(ThreadFactory threadFactory, IIoOperationFailedCallback callback, int maxNumRunningFlushes,
            int maxNumScheduledMerges, int maxNumRunningMerges) {
        super(threadFactory, callback, maxNumRunningFlushes);
        this.maxNumScheduledMerges = maxNumScheduledMerges;
        this.maxNumRunningMerges = maxNumRunningMerges;
    }

    @Override
    protected void scheduleMerge(ILSMIOOperation operation) {
        operation.pause();
        synchronized (executor) {
            if (numScheduledMerges >= maxNumScheduledMerges) {
                waitingMergeOperations.add(operation);
            } else {
                doScheduleMerge(operation);
            }
        }
    }

    private void doScheduleMerge(ILSMIOOperation operation) {
        String indexIdentier = operation.getIndexIdentifier();
        Set<ILSMIOOperation> mergeOps = scheduledMergeOperations.computeIfAbsent(indexIdentier, k -> new HashSet<>());
        mergeOps.add(operation);
        executor.submit(operation);
        numScheduledMerges++;

        dispatchMergeOperation(indexIdentier, mergeOps);
    }

    private void dispatchMergeOperation(String indexIdentier, Set<ILSMIOOperation> mergeOps) {
        if (!runningMergeOperations.containsKey(indexIdentier)
                && runningMergeOperations.size() >= maxNumRunningMerges) {
            return;
        }
        ILSMIOOperation runningOp = null;
        ILSMIOOperation smallestMergeOp = null;
        for (ILSMIOOperation op : mergeOps) {
            if (op.isActive()) {
                runningOp = op;
            }
            if (smallestMergeOp == null || op.getRemainingPages() < smallestMergeOp.getRemainingPages()) {
                smallestMergeOp = op;
            }
        }
        if (smallestMergeOp != runningOp) {
            if (runningOp != null) {
                runningOp.pause();
            }
            smallestMergeOp.resume();
            runningMergeOperations.put(indexIdentier, smallestMergeOp);
        }
    }

    @Override
    protected void completeMerge(ILSMIOOperation op) {
        String id = op.getIndexIdentifier();
        synchronized (executor) {
            Set<ILSMIOOperation> mergeOperations = scheduledMergeOperations.get(id);
            mergeOperations.remove(op);
            if (mergeOperations.isEmpty()) {
                scheduledMergeOperations.remove(id);
            }
            runningMergeOperations.remove(id);
            numScheduledMerges--;

            if (!waitingMergeOperations.isEmpty() && numScheduledMerges < maxNumScheduledMerges) {
                doScheduleMerge(waitingMergeOperations.poll());
            }
            if (runningMergeOperations.size() < maxNumRunningMerges) {
                String indexWithMostScheduledMerges = findIndexWithMostScheduledMerges();
                if (indexWithMostScheduledMerges != null) {
                    dispatchMergeOperation(indexWithMostScheduledMerges,
                            scheduledMergeOperations.get(indexWithMostScheduledMerges));
                }
            }
        }
    }

    private String findIndexWithMostScheduledMerges() {
        String targetIndex = null;
        int maxMerges = 0;
        for (Map.Entry<String, Set<ILSMIOOperation>> e : scheduledMergeOperations.entrySet()) {
            if (!runningMergeOperations.containsKey(e.getKey())
                    && (targetIndex == null || maxMerges < e.getValue().size())) {
                targetIndex = e.getKey();
                maxMerges = e.getValue().size();
            }
        }
        return targetIndex;
    }
}
