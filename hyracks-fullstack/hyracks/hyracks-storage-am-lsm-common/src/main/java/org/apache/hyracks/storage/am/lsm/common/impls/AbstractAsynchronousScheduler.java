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

import org.apache.hyracks.storage.am.lsm.common.api.IIoOperationFailedCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;

public abstract class AbstractAsynchronousScheduler implements ILSMIOOperationScheduler, Closeable {
    protected final ExecutorService executor;
    protected final Map<String, ILSMIOOperation> runningFlushOperations = new HashMap<>();
    protected final Map<String, Deque<ILSMIOOperation>> waitingFlushOperations = new HashMap<>();
    protected final Map<String, Throwable> failedGroups = new HashMap<>();

    public AbstractAsynchronousScheduler(ThreadFactory threadFactory, final IIoOperationFailedCallback callback) {
        executor = new IoOperationExecutor(threadFactory, this, callback, runningFlushOperations,
                waitingFlushOperations, failedGroups);
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
            case NOOP:
                return;
            default:
                // this should never happen
                // just guard here to avoid silent failures in case of future extensions
                throw new IllegalArgumentException("Unknown operation type " + operation.getIOOpertionType());
        }
    }

    protected abstract void scheduleMerge(ILSMIOOperation operation);

    protected void scheduleFlush(ILSMIOOperation operation) {
        String id = operation.getIndexIdentifier();
        synchronized (executor) {
            if (failedGroups.containsKey(id)) {
                // Group failure. Fail the operation right away
                operation.setStatus(LSMIOOperationStatus.FAILURE);
                operation.setFailure(new RuntimeException("Operation group " + id + " has permanently failed",
                        failedGroups.get(id)));
                operation.complete();
                return;
            }
            if (runningFlushOperations.containsKey(id)) {
                if (waitingFlushOperations.containsKey(id)) {
                    waitingFlushOperations.get(id).offer(operation);
                } else {
                    Deque<ILSMIOOperation> q = new ArrayDeque<>();
                    q.offer(operation);
                    waitingFlushOperations.put(id, q);
                }
            } else {
                runningFlushOperations.put(id, operation);
                executor.submit(operation);
            }
        }
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
    }
}
