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

import edu.uci.ics.asterix.common.context.DatasetLifecycleManager.DatasetInfo;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import edu.uci.ics.asterix.common.transactions.AbstractOperationCallback;
import edu.uci.ics.asterix.common.transactions.ILogManager;
import edu.uci.ics.asterix.common.transactions.LogRecord;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexInternal;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMOperationType;

public class PrimaryIndexOperationTracker extends BaseOperationTracker {

    // Number of active operations on an ILSMIndex instance.
    private final AtomicInteger numActiveOperations;
    private final ILogManager logManager;
    private boolean flushOnExit = false;
    private boolean flushLogCreated = false;

    public PrimaryIndexOperationTracker(DatasetLifecycleManager datasetLifecycleManager, int datasetID,
            ILogManager logManager, DatasetInfo dsInfo) {
        super(datasetLifecycleManager, datasetID, dsInfo);
        this.logManager = logManager;
        this.numActiveOperations = new AtomicInteger();
    }

    @Override
    public void beforeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        if (opType == LSMOperationType.MODIFICATION || opType == LSMOperationType.FORCE_MODIFICATION) {
            incrementNumActiveOperations(modificationCallback);
        } else if (opType == LSMOperationType.FLUSH || opType == LSMOperationType.MERGE) {
            dsInfo.declareActiveIOOperation();
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
            if (numActiveOperations.get() == 0) {
                flushIfRequested();
            }
        } else if (opType == LSMOperationType.FLUSH || opType == LSMOperationType.MERGE) {
            dsInfo.undeclareActiveIOOperation();
        }
    }

    public void flushIfRequested() throws HyracksDataException {
        // If we need a flush, and this is the last completing operation, then schedule the flush,  
        // or if there is a flush scheduled by the checkpoint (flushOnExit), then schedule it

        boolean needsFlush = false;
        Set<ILSMIndex> indexes = dsInfo.getDatasetIndexes();

        if (!flushOnExit) {
            for (ILSMIndex lsmIndex : indexes) {
                if (((ILSMIndexInternal) lsmIndex).hasFlushRequestForCurrentMutableComponent()) {
                    needsFlush = true;
                    break;
                }
            }
        }

        if (needsFlush || flushOnExit) {
            LogRecord logRecord = new LogRecord();
            logRecord.formFlushLogRecord(datasetID, this);
            try {
                logManager.log(logRecord);
            } catch (ACIDException e) {
                throw new HyracksDataException("could not write flush log", e);
            }
            flushLogCreated = true;
        }
        
        if (flushOnExit) {
            //Make the current mutable components (if have been modified) UnWritable to stop coming modify operations from entering them
            for (ILSMIndex lsmIndex : indexes) {
                if (!((AbstractLSMIndex) lsmIndex).isCurrentMutableComponentEmpty()) {
                    ((AbstractLSMIndex) lsmIndex).makeCurrentMutableComponentUnWritable();
                }
            }
            flushOnExit = false;
        }
    }

    public void triggerScheduleFlush(LogRecord logRecord) throws HyracksDataException {
        // why synchronized ??
        synchronized (this) {
            for (ILSMIndex lsmIndex : dsInfo.getDatasetIndexes()) {

                //get resource
                ILSMIndexAccessor accessor = lsmIndex.createAccessor(NoOpOperationCallback.INSTANCE,
                        NoOpOperationCallback.INSTANCE);

                //update resource lsn
                AbstractLSMIOOperationCallback ioOpCallback = (AbstractLSMIOOperationCallback) lsmIndex
                        .getIOOperationCallback();
                ioOpCallback.updateLastLSN(logRecord.getLSN());

                //schedule flush after update
                accessor.scheduleFlush(lsmIndex.getIOOperationCallback());
            }
            
            flushLogCreated = false;
        }
    }

    @Override
    public void exclusiveJobCommitted() throws HyracksDataException {
        numActiveOperations.set(0);
        flushIfRequested();
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

    public boolean isFlushOnExit() {
        return flushOnExit;
    }

    public void setFlushOnExit(boolean flushOnExit) {
        this.flushOnExit = flushOnExit;
    }

    public boolean isFlushLogCreated() {
        return flushLogCreated;
    }

}