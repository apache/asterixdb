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

import org.apache.asterix.common.api.IDatasetInfoProvider;
import org.apache.asterix.common.api.ILSMComponentIdGeneratorFactory;
import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.common.ioopcallbacks.LSMIndexIOOperationCallbackFactory;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.EmptyComponent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TestLsmIoOpCallbackFactory extends LSMIndexIOOperationCallbackFactory {

    private static final long serialVersionUID = 1L;

    private static volatile int completedFlushes = 0;
    private static volatile int completedMerges = 0;
    private static volatile int rollbackFlushes = 0;
    private static volatile int rollbackMerges = 0;
    private static volatile int failedFlushes = 0;
    private static volatile int failedMerges = 0;

    public TestLsmIoOpCallbackFactory(ILSMComponentIdGeneratorFactory idGeneratorFactory,
            IDatasetInfoProvider datasetInfoProvider) {
        super(idGeneratorFactory, datasetInfoProvider);
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
        return new TestLsmIoOpCallback(datasetInfoProvider.getDatasetInfo(ncCtx), index, getComponentIdGenerator(),
                getIndexCheckpointManagerProvider());
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

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.set("idGeneratorFactory", idGeneratorFactory.toJson(registry));
        json.set("datasetInfoProvider", datasetInfoProvider.toJson(registry));
        return json;
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final ILSMComponentIdGeneratorFactory idGeneratorFactory =
                (ILSMComponentIdGeneratorFactory) registry.deserialize(json.get("idGeneratorFactory"));
        final IDatasetInfoProvider datasetInfoProvider =
                (IDatasetInfoProvider) registry.deserialize(json.get("datasetInfoProvider"));
        return new TestLsmIoOpCallbackFactory(idGeneratorFactory, datasetInfoProvider);
    }

    public class TestLsmIoOpCallback extends LSMIOOperationCallback {
        private final TestLsmBtree lsmBtree;

        public TestLsmIoOpCallback(DatasetInfo dsInfo, ILSMIndex index, ILSMComponentIdGenerator idGenerator,
                IIndexCheckpointManagerProvider checkpointManagerProvider) {
            super(dsInfo, index, idGenerator.getId(), checkpointManagerProvider);
            lsmBtree = (TestLsmBtree) index;
        }

        @Override
        public void scheduled(ILSMIOOperation op) throws HyracksDataException {
            lsmBtree.ioScheduledCalled();
            super.scheduled(op);
            lsmBtree.ioScheduledReturned();
        }

        @Override
        public void beforeOperation(ILSMIOOperation op) throws HyracksDataException {
            lsmBtree.beforeIoOperationCalled();
            super.beforeOperation(op);
            lsmBtree.beforeIoOperationReturned();
        }

        @Override
        public void afterOperation(ILSMIOOperation op) throws HyracksDataException {
            lsmBtree.afterIoOperationCalled();
            super.afterOperation(op);
            lsmBtree.afterIoOperationReturned();
        }

        @Override
        public void afterFinalize(ILSMIOOperation op) throws HyracksDataException {
            lsmBtree.afterIoFinalizeCalled();
            super.afterFinalize(op);
            synchronized (TestLsmIoOpCallbackFactory.this) {
                if (op.getNewComponent() != null) {
                    if (op.getNewComponent() == EmptyComponent.INSTANCE) {
                        if (op.getIOOpertionType() == LSMIOOperationType.FLUSH) {
                            rollbackFlushes++;
                        } else {
                            rollbackMerges++;
                        }
                    } else {
                        if (op.getIOOpertionType() == LSMIOOperationType.FLUSH) {
                            completedFlushes++;
                        } else {
                            completedMerges++;
                        }
                    }
                } else {
                    recordFailure(op.getIOOpertionType());
                }
                TestLsmIoOpCallbackFactory.this.notifyAll();
            }
            lsmBtree.afterIoFinalizeReturned();
        }

        @Override
        public void completed(ILSMIOOperation operation) {
            try {
                lsmBtree.ioCompletedCalled();
                super.completed(operation);
                lsmBtree.ioCompletedReturned();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void recycled(ILSMMemoryComponent component) throws HyracksDataException {
            lsmBtree.recycledCalled(component);
            super.recycled(component);
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
