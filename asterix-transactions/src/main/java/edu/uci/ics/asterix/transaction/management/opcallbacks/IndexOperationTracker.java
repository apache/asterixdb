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

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;

public class IndexOperationTracker implements ILSMOperationTracker {

    // Number of active operations on a ILSMIndex instance.
    private int numActiveOperations = 0;
    private long lastLsn;
    private final ILSMIndex index;
    private final ILSMIOOperationCallback ioOpCallback;
    private ILSMIndexAccessor accessor;

    public IndexOperationTracker(ILSMIndex index, ILSMIOOperationCallbackFactory ioOpCallbackFactory) {
        this.index = index;
        //TODO 
        //This code is added to avoid NullPointException when the index's comparatorFactory is null.
        //The null comparator factory is set in the constructor of the IndexDropOperatorDescriptor.
        if (ioOpCallbackFactory != null) {
            ioOpCallback = ioOpCallbackFactory.createIOOperationCallback(this);
        } else {
            ioOpCallback = null;
        }
    }

    @Override
    public synchronized boolean beforeOperation(ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback, boolean tryOperation) throws HyracksDataException {
        // Wait for pending flushes to complete.
        // If flushFlag is set, then the flush is queued to occur by the last completing operation.
        // This operation should wait for that flush to occur before proceeding.
        if (index.getFlushStatus(index)) {
            if (tryOperation) {
                return false;
            }
            try {
                this.wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
        numActiveOperations++;

        // Increment transactor-local active operations count.
        AbstractOperationCallback opCallback = getOperationCallback(searchCallback, modificationCallback);
        if (opCallback != null) {
            opCallback.incrementLocalNumActiveOperations();
        }
        return true;
    }

    @Override
    public void afterOperation(ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        // Searches are immediately considered complete, because they should not prevent the execution of flushes.
        if (searchCallback != null) {
            completeOperation(searchCallback, modificationCallback);
        }
    }

    @Override
    public synchronized void completeOperation(ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        numActiveOperations--;

        // Decrement transactor-local active operations count.
        AbstractOperationCallback opCallback = getOperationCallback(searchCallback, modificationCallback);
        if (opCallback != null) {
            opCallback.decrementLocalNumActiveOperations();
        }
        // If we need a flush, and this is the last completing operation, then schedule the flush.
        // Once the flush has completed notify all waiting operations.
        if (index.getFlushStatus(index) && numActiveOperations == 0) {
            if (accessor == null) {
                accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                        NoOpOperationCallback.INSTANCE);
            }
            index.getIOScheduler().scheduleOperation(accessor.createFlushOperation(ioOpCallback));
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

    public long getLastLSN() {
        return lastLsn;
    }

    public void setLastLSN(long lastLsn) {
        this.lastLsn = lastLsn;
    }
}
