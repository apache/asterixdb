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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.IIoOperationFailedCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SynchronousScheduler implements ILSMIOOperationScheduler {
    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<String, ILSMIOOperation> runningFlushOperations = new ConcurrentHashMap<>();
    private final Map<String, Throwable> failedGroups = new ConcurrentHashMap<>();
    private final IIoOperationFailedCallback failureCallback;

    public SynchronousScheduler(IIoOperationFailedCallback failureCallback) {
        this.failureCallback = failureCallback;
    }

    @Override
    public void scheduleOperation(ILSMIOOperation operation) {
        try {
            before(operation);
            if (operation.getStatus() == LSMIOOperationStatus.FAILURE) {
                return;
            }
            run(operation);
        } catch (Throwable e) { // NOSONAR: Must catch them all
            throw new IllegalStateException(e);
        } finally {
            after(operation);
        }
    }

    @Override
    public void completeOperation(ILSMIOOperation operation) throws HyracksDataException {
        // no op
    }

    private void run(ILSMIOOperation operation) {
        try {
            operation.call();
        } catch (Throwable th) { // NOSONAR Must catch all
            LOGGER.log(Level.ERROR, "IO Operation failed", th);
            operation.setStatus(LSMIOOperationStatus.FAILURE);
            operation.setFailure(th);
        }
        if (operation.getStatus() == LSMIOOperationStatus.FAILURE) {
            failureCallback.operationFailed(operation, operation.getFailure());
        }
    }

    private void after(ILSMIOOperation operation) {
        if (operation.getIOOpertionType() == LSMIOOperationType.FLUSH) {
            synchronized (runningFlushOperations) {
                runningFlushOperations.remove(operation.getIndexIdentifier());
                if (operation.getStatus() == LSMIOOperationStatus.FAILURE) {
                    failedGroups.putIfAbsent(operation.getIndexIdentifier(), operation.getFailure());
                }
                operation.complete();
                runningFlushOperations.notifyAll();
            }
        } else {
            operation.complete();
        }
    }

    private void before(ILSMIOOperation operation) throws InterruptedException {
        String id = operation.getIndexIdentifier();
        if (operation.getIOOpertionType() == LSMIOOperationType.FLUSH) {
            synchronized (runningFlushOperations) {
                while (true) {
                    if (failedGroups.containsKey(id)) {
                        operation.setStatus(LSMIOOperationStatus.FAILURE);
                        operation.setFailure(new RuntimeException("Operation group " + id + " has permanently failed",
                                failedGroups.get(id)));
                        return;
                    }
                    if (runningFlushOperations.containsKey(id)) {
                        runningFlushOperations.wait();
                    } else {
                        runningFlushOperations.put(id, operation);
                        break;
                    }
                }
            }
        }
    }
}
