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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

import org.apache.hyracks.storage.am.lsm.common.api.IIoOperationFailedCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerFactory;

/**
 * This is a greedy asynchronous scheduler that always allocates the full bandwidth for the merge operation
 * with the smallest required disk bandwidth to minimize the number of disk components. It has been proven
 * that if the number of components in all merge operations are the same, then this scheduler is optimal
 * by always minimizing the number of disk components over time; if not, this is still a good heuristic
 *
 */
public class GreedyScheduler extends AbstractAsynchronousScheduler {
    public static final ILSMIOOperationSchedulerFactory FACTORY = new ILSMIOOperationSchedulerFactory() {
        @Override
        public ILSMIOOperationScheduler createIoScheduler(ThreadFactory threadFactory,
                IIoOperationFailedCallback callback) {
            return new GreedyScheduler(threadFactory, callback);
        }

        public String getName() {
            return "greedy";
        }
    };

    private final Map<String, List<ILSMIOOperation>> mergeOperations = new HashMap<>();

    public GreedyScheduler(ThreadFactory threadFactory, IIoOperationFailedCallback callback) {
        super(threadFactory, callback);
    }

    protected void scheduleMerge(ILSMIOOperation operation) {
        operation.pause();
        String id = operation.getIndexIdentifier();
        synchronized (executor) {
            List<ILSMIOOperation> mergeOpList = mergeOperations.computeIfAbsent(id, key -> new ArrayList<>());
            mergeOpList.add(operation);
            dispatchMergeOperation(mergeOpList);
        }
        executor.submit(operation);
    }

    private void dispatchMergeOperation(List<ILSMIOOperation> mergeOps) {
        ILSMIOOperation activeOp = null;
        ILSMIOOperation smallestMergeOp = null;
        for (ILSMIOOperation op : mergeOps) {
            if (op.isActive()) {
                activeOp = op;
            }
            if (smallestMergeOp == null || op.getRemainingPages() < smallestMergeOp.getRemainingPages()) {
                smallestMergeOp = op;
            }
        }
        if (smallestMergeOp != activeOp) {
            if (activeOp != null) {
                activeOp.pause();
            }
            smallestMergeOp.resume();
        }
    }

    @Override
    public void completeOperation(ILSMIOOperation op) {
        if (op.getIOOpertionType() == LSMIOOperationType.MERGE) {
            String id = op.getIndexIdentifier();
            synchronized (executor) {
                List<ILSMIOOperation> mergeOpList = mergeOperations.get(id);
                mergeOpList.remove(op);
                if (!mergeOpList.isEmpty()) {
                    dispatchMergeOperation(mergeOpList);
                }
            }
        }
    }
}
