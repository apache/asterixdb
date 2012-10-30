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
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;

public class IndexOperationTracker implements ILSMOperationTracker {

    // Number of active operations on a ILSMIndex instance.
    private int numActiveOperations = 0;
    private long lastLsn;
    private final ILSMIndex index;
    private final ILSMIndexAccessor accessor;    
    private final FlushOperationCallback FLUSHCALLBACK_INSTANCE = new FlushOperationCallback();
    
    public IndexOperationTracker(ILSMIndex index) {
        this.index = index;
        accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
    }     

    @Override
    public synchronized void beforeOperation(ILSMIndexOperationContext opCtx) throws HyracksDataException {
        // Wait for pending flushes to complete.
        // If flushFlag is set, then the flush is queued to occur by the last completing operation.
        // This operation should wait for that flush to occur before proceeding.
        if (index.getFlushController().getFlushStatus(index)) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
        numActiveOperations++;
        
        // Increment transactor-local active operations count.
        AbstractOperationCallback opCallback = getOperationCallback(opCtx);
        opCallback.incrementLocalNumActiveOperations();
    }

    @Override
    public void afterOperation(ILSMIndexOperationContext opCtx) throws HyracksDataException {
        // Searches are immediately considered complete, because they should not prevent the execution of flushes.
        IndexOperation op = opCtx.getOperation();
        if (op == IndexOperation.SEARCH || op == IndexOperation.DISKORDERSCAN) {
            completeOperation(opCtx);
        }
    }

    @Override
    public synchronized void completeOperation(ILSMIndexOperationContext opCtx) throws HyracksDataException {
        numActiveOperations--;

        // Decrement transactor-local active operations count.
        AbstractOperationCallback opCallback = getOperationCallback(opCtx);
        opCallback.decrementLocalNumActiveOperations();
        
        // If we need a flush, and this is the last completing operation, then schedule the flush.
        // Once the flush has completed notify all waiting operations.
        if (index.getFlushController().getFlushStatus(index) && numActiveOperations == 0) {
            index.getIOScheduler().scheduleOperation(accessor.createFlushOperation(FLUSHCALLBACK_INSTANCE));
        }
    }
    
    private AbstractOperationCallback getOperationCallback(ILSMIndexOperationContext opCtx) {
        IndexOperation op = opCtx.getOperation();
        if (op == IndexOperation.SEARCH || op == IndexOperation.DISKORDERSCAN) {
            return (AbstractOperationCallback) opCtx.getSearchOperationCallback();
        } else {
            return (AbstractOperationCallback) opCtx.getModificationCallback();
        }
    }
    
    public long getLastLSN() {
        return lastLsn;
    }
    
    public void setLastLSN(long lastLsn) {
        this.lastLsn = lastLsn;
    }
    
    private class FlushOperationCallback implements ILSMIOOperationCallback {
        @Override
        public void callback() {
            IndexOperationTracker.this.notifyAll();
        }
    }
}
