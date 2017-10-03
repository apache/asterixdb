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
package org.apache.asterix.test.dataflow;

import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.EmptyComponent;

public class TestLsmBtreeIoOpCallbackFactory implements ILSMIOOperationCallbackFactory {

    private static final long serialVersionUID = 1L;

    public static TestLsmBtreeIoOpCallbackFactory INSTANCE = new TestLsmBtreeIoOpCallbackFactory();
    private static volatile int completedFlushes = 0;
    private static volatile int completedMerges = 0;
    private static volatile int rollbackFlushes = 0;
    private static volatile int rollbackMerges = 0;
    private static volatile int failedFlushes = 0;
    private static volatile int failedMerges = 0;

    private TestLsmBtreeIoOpCallbackFactory() {
    }

    @Override
    public synchronized ILSMIOOperationCallback createIoOpCallback() {
        completedFlushes = 0;
        completedMerges = 0;
        rollbackFlushes = 0;
        rollbackMerges = 0;
        // Whenever this is called, it resets the counter
        // However, the counters for the failed operations are never reset since we expect them
        // To be always 0
        return new TestLsmBtreeIoOpCallback();
    }

    public int getTotalFlushes() {
        return completedFlushes + rollbackFlushes;
    }

    public int getTotalMerges() {
        return completedMerges + rollbackMerges;
    }

    public int getTotalIoOps() {
        return getTotalFlushes() + getTotalMerges();
    }

    public int getRollbackFlushes() {
        return rollbackFlushes;
    }

    public int getRollbackMerges() {
        return rollbackMerges;
    }

    public int getCompletedFlushes() {
        return completedFlushes;
    }

    public int getCompletedMerges() {
        return completedMerges;
    }

    public static int getFailedFlushes() {
        return failedFlushes;
    }

    public static int getFailedMerges() {
        return failedMerges;
    }

    public class TestLsmBtreeIoOpCallback extends LSMBTreeIOOperationCallback {
        @Override
        public void afterFinalize(LSMOperationType opType, ILSMDiskComponent newComponent) {
            super.afterFinalize(opType, newComponent);
            synchronized (INSTANCE) {
                if (newComponent != null) {
                    if (newComponent == EmptyComponent.INSTANCE) {
                        if (opType == LSMOperationType.FLUSH) {
                            rollbackFlushes++;
                        } else {
                            rollbackMerges++;
                        }
                    } else {
                        if (opType == LSMOperationType.FLUSH) {
                            completedFlushes++;
                        } else {
                            completedMerges++;
                        }
                    }
                } else {
                    recordFailure(opType);
                }
                INSTANCE.notifyAll();
            }
        }

        private void recordFailure(LSMOperationType opType) {
            if (opType == LSMOperationType.FLUSH) {
                failedFlushes++;
            } else {
                failedMerges++;
            }
        }
    }
}
