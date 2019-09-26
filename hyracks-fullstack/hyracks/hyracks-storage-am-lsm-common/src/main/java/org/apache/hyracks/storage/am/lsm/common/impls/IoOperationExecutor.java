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

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.IIoOperationFailedCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;

public class IoOperationExecutor extends ThreadPoolExecutor {

    private final ILSMIOOperationScheduler scheduler;
    private final IIoOperationFailedCallback callback;
    private final Map<String, ILSMIOOperation> runningFlushOperations;
    private final Map<String, Throwable> failedGroups;
    private final Map<String, Deque<ILSMIOOperation>> waitingFlushOperations;

    public IoOperationExecutor(ThreadFactory threadFactory, ILSMIOOperationScheduler scheduler,
            IIoOperationFailedCallback callback, Map<String, ILSMIOOperation> runningFlushOperations,
            Map<String, Deque<ILSMIOOperation>> waitingFlushOperations, Map<String, Throwable> failedGroups) {
        super(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory);
        this.scheduler = scheduler;
        this.callback = callback;
        this.runningFlushOperations = runningFlushOperations;
        this.waitingFlushOperations = waitingFlushOperations;
        this.failedGroups = failedGroups;
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return new LSMIOOperationTask<>(callable);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        LSMIOOperationTask<?> task = (LSMIOOperationTask<?>) r;
        ILSMIOOperation executedOp = task.getOperation();
        try {
            doAfterExecute(executedOp, t);
        } catch (Throwable th) { // NOSONAR must catch all
            callback.schedulerFailed(scheduler, th);
            shutdown();
        }
    }

    private void doAfterExecute(ILSMIOOperation executedOp, Throwable t) throws HyracksDataException {
        final boolean failed = (t != null) || (executedOp.getStatus() == LSMIOOperationStatus.FAILURE);
        if (failed) {
            fail(executedOp, t != null ? t : executedOp.getFailure());
        }
        if (!failed || executedOp.getIOOpertionType() != LSMIOOperationType.FLUSH) {
            executedOp.complete(); // destroy if merge or successful flush
        }
        scheduler.completeOperation(executedOp);
        if (executedOp.getIOOpertionType() == LSMIOOperationType.FLUSH) {
            String id = executedOp.getIndexIdentifier();
            synchronized (this) {
                runningFlushOperations.remove(id);
                if (waitingFlushOperations.containsKey(id)) {
                    ILSMIOOperation op = waitingFlushOperations.get(id).poll();
                    if (op != null) {
                        scheduler.scheduleOperation(op);
                    } else {
                        waitingFlushOperations.remove(id);
                    }
                }
            }
        }
    }

    private void fail(ILSMIOOperation executedOp, Throwable t) throws HyracksDataException {
        callback.operationFailed(executedOp, t);
        if (executedOp.getIOOpertionType() == LSMIOOperationType.FLUSH) {
            executedOp.complete();
            // Doesn't make sense to process further flush requests... Mark the operation group permanently failed
            // Fail other scheduled operations
            synchronized (this) {
                String id = executedOp.getIndexIdentifier();
                failedGroups.put(id, t);
                runningFlushOperations.remove(id);
                if (waitingFlushOperations.containsKey(id)) {
                    Deque<ILSMIOOperation> ops = waitingFlushOperations.remove(id);
                    ILSMIOOperation next = ops.poll();
                    while (next != null) {
                        next.setFailure(new RuntimeException("Operation group " + id + " has permanently failed", t));
                        next.setStatus(LSMIOOperationStatus.FAILURE);
                        next.complete();
                        next = ops.poll();
                    }
                }
            }
        }
    }
}
