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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.asterix.common.transactions.AbstractOperationCallback;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexInternal;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMOperationType;

public class PrimaryIndexOperationTracker extends BaseOperationTracker {

    // Number of active operations on an ILSMIndex instance.
    private final AtomicInteger numActiveOperations;

    public PrimaryIndexOperationTracker(DatasetLifecycleManager datasetLifecycleManager, int datasetID) {
        super(datasetLifecycleManager, datasetID);
        this.numActiveOperations = new AtomicInteger();
    }

    @Override
    public void beforeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        if (opType == LSMOperationType.MODIFICATION || opType == LSMOperationType.FORCE_MODIFICATION) {
            incrementNumActiveOperations(modificationCallback);
        } else if (opType == LSMOperationType.FLUSH || opType == LSMOperationType.MERGE) {
            datasetLifecycleManager.declareActiveIOOperation(datasetID);
        }
    }

    @Override
    public void afterOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        // Searches are immediately considered complete, because they should not prevent the execution of flushes.
        if (opType == LSMOperationType.SEARCH || opType == LSMOperationType.FLUSH || opType == LSMOperationType.MERGE) {
            completeOperation(index, opType, searchCallback, modificationCallback);
        }
    }

    @Override
    public void completeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        if (opType == LSMOperationType.MODIFICATION || opType == LSMOperationType.FORCE_MODIFICATION) {
            decrementNumActiveOperations(modificationCallback);
            flushIfFull();
        } else if (opType == LSMOperationType.FLUSH || opType == LSMOperationType.MERGE) {
            datasetLifecycleManager.undeclareActiveIOOperation(datasetID);
        }
    }

    private void flushIfFull() throws HyracksDataException {
        // If we need a flush, and this is the last completing operation, then schedule the flush. 
        // TODO: Is there a better way to check if we need to flush instead of communicating with the datasetLifecycleManager each time?
        Set<ILSMIndex> indexes = datasetLifecycleManager.getDatasetIndexes(datasetID);
        boolean needsFlush = false;
        for (ILSMIndex lsmIndex : indexes) {
            if (((ILSMIndexInternal) lsmIndex).hasFlushRequestForCurrentMutableComponent()) {
                needsFlush = true;
                break;
            }
        }
        synchronized (this) {
            if (needsFlush && numActiveOperations.get() == 0) {
                for (ILSMIndex lsmIndex : indexes) {
                    ILSMIndexAccessor accessor = (ILSMIndexAccessor) lsmIndex.createAccessor(
                            NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
                    accessor.scheduleFlush(lsmIndex.getIOOperationCallback());
                }
            }
        }
    }

    public void exclusiveJobCommitted() throws HyracksDataException {
        numActiveOperations.set(0);
        flushIfFull();
    }

    public int getNumActiveOperations() {
        return numActiveOperations.get();
    }

    private void incrementNumActiveOperations(IModificationOperationCallback modificationCallback) {
        //modificationCallback can be NoOpOperationCallback when redo/undo operations are executed. 
        if (modificationCallback != NoOpOperationCallback.INSTANCE) {
            numActiveOperations.incrementAndGet();
            ((AbstractOperationCallback) modificationCallback).incrementLocalNumActiveOperations();
        }
    }

    private void decrementNumActiveOperations(IModificationOperationCallback modificationCallback) {
        //modificationCallback can be NoOpOperationCallback when redo/undo operations are executed.
        if (modificationCallback != NoOpOperationCallback.INSTANCE) {
            numActiveOperations.decrementAndGet();
            ((AbstractOperationCallback) modificationCallback).decrementLocalNumActiveOperations();
        }
    }

    public void cleanupNumActiveOperationsForAbortedJob(AbstractOperationCallback callback) {
        int delta = callback.getLocalNumActiveOperations() * -1;
        numActiveOperations.getAndAdd(delta);
        callback.resetLocalNumActiveOperations();
    }

}