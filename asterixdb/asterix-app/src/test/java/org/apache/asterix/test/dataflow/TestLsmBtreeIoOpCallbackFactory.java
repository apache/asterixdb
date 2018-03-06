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
import org.apache.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGeneratorFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.EmptyComponent;

public class TestLsmBtreeIoOpCallbackFactory extends LSMBTreeIOOperationCallbackFactory {

    private static final long serialVersionUID = 1L;

    private static volatile int completedFlushes = 0;
    private static volatile int completedMerges = 0;
    private static volatile int rollbackFlushes = 0;
    private static volatile int rollbackMerges = 0;
    private static volatile int failedFlushes = 0;
    private static volatile int failedMerges = 0;

    public TestLsmBtreeIoOpCallbackFactory(ILSMComponentIdGeneratorFactory idGeneratorFactory) {
        super(idGeneratorFactory);
    }

    @Override
    public synchronized ILSMIOOperationCallback createIoOpCallback(ILSMIndex index) throws HyracksDataException {
        completedFlushes = 0;
        completedMerges = 0;
        rollbackFlushes = 0;
        rollbackMerges = 0;
        // Whenever this is called, it resets the counter
        // However, the counters for the failed operations are never reset since we expect them
        // To be always 0
        return new TestLsmBtreeIoOpCallback(index, getComponentIdGenerator(), getIndexCheckpointManagerProvider());
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
        private final TestLsmBtree lsmBtree;

        public TestLsmBtreeIoOpCallback(ILSMIndex index, ILSMComponentIdGenerator idGenerator,
                IIndexCheckpointManagerProvider checkpointManagerProvider) {
            super(index, idGenerator, checkpointManagerProvider);
            lsmBtree = (TestLsmBtree) index;
        }

        @Override
        public void beforeOperation(ILSMIndexOperationContext opCtx) throws HyracksDataException {
            lsmBtree.beforeIoOperationCalled();
            super.beforeOperation(opCtx);
            lsmBtree.beforeIoOperationReturned();
        }

        @Override
        public void afterOperation(ILSMIndexOperationContext opCtx) throws HyracksDataException {
            lsmBtree.afterIoOperationCalled();
            super.afterOperation(opCtx);
            lsmBtree.afterIoOperationReturned();
        }

        @Override
        public void afterFinalize(ILSMIndexOperationContext opCtx) throws HyracksDataException {
            lsmBtree.afterIoFinalizeCalled();
            super.afterFinalize(opCtx);
            synchronized (TestLsmBtreeIoOpCallbackFactory.this) {
                if (opCtx.getNewComponent() != null) {
                    if (opCtx.getNewComponent() == EmptyComponent.INSTANCE) {
                        if (opCtx.getIoOperationType() == LSMIOOperationType.FLUSH) {
                            rollbackFlushes++;
                        } else {
                            rollbackMerges++;
                        }
                    } else {
                        if (opCtx.getIoOperationType() == LSMIOOperationType.FLUSH) {
                            completedFlushes++;
                        } else {
                            completedMerges++;
                        }
                    }
                } else {
                    recordFailure(opCtx.getIoOperationType());
                }
                TestLsmBtreeIoOpCallbackFactory.this.notifyAll();
            }
            lsmBtree.afterIoFinalizeReturned();
        }

        @Override
        public void recycled(ILSMMemoryComponent component, boolean advance) throws HyracksDataException {
            lsmBtree.recycledCalled(component);
            super.recycled(component, advance);
            lsmBtree.recycledReturned(component);
        }

        @Override
        public void allocated(ILSMMemoryComponent component) throws HyracksDataException {
            lsmBtree.allocatedCalled(component);
            super.allocated(component);
            lsmBtree.allocatedReturned(component);
        }

        private void recordFailure(LSMIOOperationType opType) {
            if (opType == LSMIOOperationType.FLUSH) {
                failedFlushes++;
            } else {
                failedMerges++;
            }
        }
    }
}
