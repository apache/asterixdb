/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.common.context;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMOperationType;

public class PrimaryIndexOperationTracker extends BaseOperationTracker {

    private final DatasetLifecycleManager datasetLifecycleManager;
    private final List<IVirtualBufferCache> datasetBufferCaches;
    private final int datasetID;
    // Number of active operations on a ILSMIndex instance.
    private AtomicInteger[] numActiveOperations;

    public PrimaryIndexOperationTracker(DatasetLifecycleManager datasetLifecycleManager, int datasetID,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory) {
        super(ioOpCallbackFactory);
        this.datasetLifecycleManager = datasetLifecycleManager;
        this.datasetID = datasetID;
        this.datasetBufferCaches = datasetLifecycleManager.getVirtualBufferCaches(datasetID);
        this.numActiveOperations = new AtomicInteger[datasetBufferCaches.size()];
        for (int i = 0; i < numActiveOperations.length; i++) {
            this.numActiveOperations[i] = new AtomicInteger(0);
        }
    }

    @Override
    public void beforeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        numActiveOperations[index.getCurrentMutableComponentId()].incrementAndGet();
    }

    @Override
    public void afterOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        // Searches are immediately considered complete, because they should not prevent the execution of flushes.
        if (opType == LSMOperationType.SEARCH || opType == LSMOperationType.NOOP || opType == LSMOperationType.FLUSH
                || opType == LSMOperationType.MERGE) {
            completeOperation(index, opType, searchCallback, modificationCallback);
        }
    }

    @Override
    public void completeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        int nActiveOps = numActiveOperations[index.getCurrentMutableComponentId()].decrementAndGet();

        if (opType != LSMOperationType.FLUSH) {
            flushIfFull(nActiveOps);
        }
    }

    private void flushIfFull(int componentId, int nActiveOps) throws HyracksDataException {
        // If we need a flush, and this is the last completing operation, then schedule the flush.
        if (datasetBufferCaches.get(componentId).isFull() && nActiveOps == 0) {
            Set<ILSMIndex> indexes = datasetLifecycleManager.getDatasetIndexes(datasetID);
            for (ILSMIndex lsmIndex : indexes) {
                ILSMIndexAccessor accessor = (ILSMIndexAccessor) lsmIndex.createAccessor(
                        NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
                accessor.scheduleFlush(((BaseOperationTracker) lsmIndex.getOperationTracker()).getIOOperationCallback());
            }

        }
    }

    public void exclusiveJobCommitted() throws HyracksDataException {
        for (int i = 0; i < numActiveOperations.length; i++) {
            numActiveOperations[i].set(0);
            flushIfFull(i, 0);   
        }
    }

    public boolean isActiveDataset() {
        for (int i = 0; i < numActiveOperations.length; i++) {
            if (numActiveOperations[i].get() > 0) {
                return false;
            }
        }
        return true;
    }
}