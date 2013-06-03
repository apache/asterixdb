/*
 * Copyright 2009-2012 by The Regents of the University of California
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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.asterix.common.transactions.AbstractOperationCallback;
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
    private final IVirtualBufferCache datasetBufferCache;
    private final int datasetID;
    // Number of active operations on a ILSMIndex instance.
    private AtomicInteger numActiveOperations;

    public PrimaryIndexOperationTracker(DatasetLifecycleManager datasetLifecycleManager, int datasetID,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory) {
        super(ioOpCallbackFactory);
        this.datasetLifecycleManager = datasetLifecycleManager;
        this.numActiveOperations = new AtomicInteger(0);
        this.datasetID = datasetID;
        datasetBufferCache = datasetLifecycleManager.getVirtualBufferCache(datasetID);
    }

    @Override
    public void beforeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        numActiveOperations.incrementAndGet();

        // Increment transactor-local active operations count.
        AbstractOperationCallback opCallback = getOperationCallback(searchCallback, modificationCallback);
        if (opCallback != null) {
            opCallback.incrementLocalNumActiveOperations();
        }
    }

    @Override
    public void afterOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        // Searches are immediately considered complete, because they should not prevent the execution of flushes.
        if ((searchCallback != null && searchCallback != NoOpOperationCallback.INSTANCE)
                || opType == LSMOperationType.FLUSH || opType == LSMOperationType.MERGE) {
            completeOperation(index, opType, searchCallback, modificationCallback);
        }
    }

    @Override
    public void completeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        int nActiveOps = numActiveOperations.decrementAndGet();
        // Decrement transactor-local active operations count.
        AbstractOperationCallback opCallback = getOperationCallback(searchCallback, modificationCallback);
        if (opCallback != null) {
            opCallback.decrementLocalNumActiveOperations();
        }
        if (opType != LSMOperationType.FLUSH) {
            flushIfFull(nActiveOps);
        }
    }

    private void flushIfFull(int nActiveOps) throws HyracksDataException {
        // If we need a flush, and this is the last completing operation, then schedule the flush.
        if (datasetBufferCache.isFull() && nActiveOps == 0) {
            Set<ILSMIndex> indexes = datasetLifecycleManager.getDatasetIndexes(datasetID);
            for (ILSMIndex lsmIndex : indexes) {
                ILSMIndexAccessor accessor = (ILSMIndexAccessor) lsmIndex.createAccessor(
                        NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
                accessor.scheduleFlush(ioOpCallback);
            }

        }
    }

    public void exclusiveJobCommitted() throws HyracksDataException {
        numActiveOperations.set(0);
        flushIfFull(0);
    }

    private AbstractOperationCallback getOperationCallback(ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) {

        if (modificationCallback == NoOpOperationCallback.INSTANCE || modificationCallback == null) {
            return null;
        }
        if (searchCallback != null && searchCallback != NoOpOperationCallback.INSTANCE) {
            return (AbstractOperationCallback) searchCallback;
        } else {
            return (AbstractOperationCallback) modificationCallback;
        }
    }

}
