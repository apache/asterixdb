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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.IIoOperationFailedCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;

public abstract class AbstractAsynchronousScheduler implements ILSMIOOperationScheduler, Closeable {
    protected final ExecutorService executor;

    private final int maxNumFlushes;
    protected final Map<String, ILSMIOOperation> runningFlushOperations = new HashMap<>();
    protected final Map<String, ILSMIOOperation> runningReplicateOperations = new HashMap<>();
    protected final Deque<ILSMIOOperation> waitingFlushOperations = new ArrayDeque<>();
    protected final Deque<ILSMIOOperation> waitingMergeOperations = new ArrayDeque<>();
    protected final Deque<ILSMIOOperation> waitingReplicateOperations = new ArrayDeque<>();
    protected final Map<String, Throwable> failedGroups = new HashMap<>();

    public AbstractAsynchronousScheduler(ThreadFactory threadFactory, final IIoOperationFailedCallback callback,
            int maxNumFlushes) {
        executor = new IoOperationExecutor(threadFactory, this, callback, runningFlushOperations, failedGroups);
        this.maxNumFlushes = maxNumFlushes;
    }

    @Override
    public void scheduleOperation(ILSMIOOperation operation) {
        switch (operation.getIOOpertionType()) {
            case FLUSH:
                scheduleFlush(operation);
                break;
            case MERGE:
                scheduleMerge(operation);
                break;
            case REPLICATE:
                scheduleReplicate(operation);
                break;
            case NOOP:
                break;
            default:
                // this should never happen
                // just guard here to avoid silent failures in case of future extensions
                throw new IllegalArgumentException("Unknown operation type " + operation.getIOOpertionType());
        }
    }

    @Override
    public void completeOperation(ILSMIOOperation operation) throws HyracksDataException {
        switch (operation.getIOOpertionType()) {
            case FLUSH:
                completeFlush(operation);
                break;
            case MERGE:
                completeMerge(operation);
                break;
            case REPLICATE:
                completeReplicate(operation);
                break;
            case NOOP:
                return;
            default:
                // this should never happen
                // just guard here to avoid silent failures in case of future extensions
                throw new IllegalArgumentException("Unknown operation type " + operation.getIOOpertionType());
        }
    }

    protected abstract void scheduleMerge(ILSMIOOperation operation);

    protected abstract void completeMerge(ILSMIOOperation operation);

    protected void scheduleFlush(ILSMIOOperation operation) {
        String id = operation.getIndexIdentifier();
        synchronized (executor) {
            if (checkFailedFlush(operation)) {
                return;
            }
            if (runningFlushOperations.size() >= maxNumFlushes || runningFlushOperations.containsKey(id)) {
                waitingFlushOperations.add(operation);
            } else {
                runningFlushOperations.put(id, operation);
                executor.submit(operation);
            }
        }
    }

    private boolean checkFailedFlush(ILSMIOOperation operation) {
        String id = operation.getIndexIdentifier();
        if (failedGroups.containsKey(id)) {
            // Group failure. Fail the operation right away
            operation.setStatus(LSMIOOperationStatus.FAILURE);
            operation.setFailure(
                    new RuntimeException("Operation group " + id + " has permanently failed", failedGroups.get(id)));
            operation.complete();
            return true;
        } else {
            return false;
        }
    }

    private void completeFlush(ILSMIOOperation operation) {
        String id = operation.getIndexIdentifier();
        synchronized (executor) {
            runningFlushOperations.remove(id);

            // Schedule flushes in FIFO order. Must make sure that there is at most one scheduled flush for each index.
            for (ILSMIOOperation flushOp : waitingFlushOperations) {
                String flushOpId = flushOp.getIndexIdentifier();
                if (runningFlushOperations.size() < maxNumFlushes) {
                    if (!runningFlushOperations.containsKey(flushOpId) && !flushOp.isCompleted()
                            && !checkFailedFlush(flushOp)) {
                        runningFlushOperations.put(flushOpId, flushOp);
                        executor.submit(flushOp);
                    }
                } else {
                    break;
                }
            }

            // cleanup scheduled flushes
            while (!waitingFlushOperations.isEmpty()) {
                ILSMIOOperation top = waitingFlushOperations.peek();
                if (top.isCompleted() || runningFlushOperations.get(top.getIndexIdentifier()) == top) {
                    waitingFlushOperations.poll();
                } else {
                    break;
                }
            }

        }
    }

    private void scheduleReplicate(ILSMIOOperation operation) {
        String id = operation.getIndexIdentifier();
        synchronized (executor) {
            if (runningReplicateOperations.size() >= maxNumFlushes || runningReplicateOperations.containsKey(id)) {
                waitingReplicateOperations.add(operation);
            } else {
                runningReplicateOperations.put(id, operation);
                executor.submit(operation);
            }
        }
    }

    private void completeReplicate(ILSMIOOperation operation) {
        String id = operation.getIndexIdentifier();
        synchronized (executor) {
            runningReplicateOperations.remove(id);
            // Schedule replicate in FIFO order. Must make sure that there is at most one scheduled replicate for each index.
            for (ILSMIOOperation replicateOp : waitingReplicateOperations) {
                String replicateOpId = replicateOp.getIndexIdentifier();
                if (runningReplicateOperations.size() < maxNumFlushes) {
                    if (!runningReplicateOperations.containsKey(replicateOpId) && !replicateOp.isCompleted()) {
                        runningReplicateOperations.put(replicateOpId, replicateOp);
                        executor.submit(replicateOp);
                    }
                } else {
                    break;
                }
            }
            // cleanup scheduled replicate
            while (!waitingReplicateOperations.isEmpty()) {
                ILSMIOOperation top = waitingReplicateOperations.peek();
                if (top.isCompleted() || runningReplicateOperations.get(top.getIndexIdentifier()) == top) {
                    waitingReplicateOperations.poll();
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
    }
}
