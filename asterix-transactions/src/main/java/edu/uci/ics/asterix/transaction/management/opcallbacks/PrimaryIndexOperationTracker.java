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

package edu.uci.ics.asterix.transaction.management.opcallbacks;

import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.asterix.common.transactions.AbstractOperationCallback;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMOperationType;

public class PrimaryIndexOperationTracker extends BaseOperationTracker {

    // Number of active operations on a ILSMIndex instance.
    private AtomicInteger numActiveOperations;
    private ILSMIndexAccessor accessor;

    public PrimaryIndexOperationTracker(ILSMIndex index, ILSMIOOperationCallbackFactory ioOpCallbackFactory) {
        super(index, ioOpCallbackFactory);
        this.numActiveOperations = new AtomicInteger(0);
    }

    @Override
    public void beforeOperation(LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        if (opType != LSMOperationType.FORCE_MODIFICATION) {
            numActiveOperations.incrementAndGet();

            // Increment transactor-local active operations count.
            AbstractOperationCallback opCallback = getOperationCallback(searchCallback, modificationCallback);
            if (opCallback != null) {
                opCallback.incrementLocalNumActiveOperations();
            }
        }
    }

    @Override
    public void afterOperation(LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        // Searches are immediately considered complete, because they should not prevent the execution of flushes.
        if (searchCallback != null) {
            completeOperation(opType, searchCallback, modificationCallback);
        }
    }

    @Override
    public void completeOperation(LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        int nActiveOps = numActiveOperations.decrementAndGet();
        // Decrement transactor-local active operations count.
        AbstractOperationCallback opCallback = getOperationCallback(searchCallback, modificationCallback);
        if (opCallback != null) {
            opCallback.decrementLocalNumActiveOperations();
        }
        // If we need a flush, and this is the last completing operation, then schedule the flush.
        // Once the flush has completed notify all waiting operations.
        if (index.getFlushStatus() && nActiveOps == 0 && opType != LSMOperationType.FLUSH) {
            if (accessor == null) {
                accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                        NoOpOperationCallback.INSTANCE);
            }
            accessor.scheduleFlush(ioOpCallback);
        }
    }

    private AbstractOperationCallback getOperationCallback(ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) {

        if (searchCallback == NoOpOperationCallback.INSTANCE || modificationCallback == NoOpOperationCallback.INSTANCE) {
            return null;
        }
        if (searchCallback != null) {
            return (AbstractOperationCallback) searchCallback;
        } else {
            return (AbstractOperationCallback) modificationCallback;
        }
    }

}
