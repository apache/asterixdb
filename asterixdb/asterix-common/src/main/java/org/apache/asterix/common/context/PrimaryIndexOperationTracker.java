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

package org.apache.asterix.common.context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.common.transactions.AbstractOperationCallback;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IoOperationCompleteListener;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.FlushOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.util.annotations.NotThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@NotThreadSafe
public class PrimaryIndexOperationTracker extends BaseOperationTracker implements IoOperationCompleteListener {
    private static final Logger LOGGER = LogManager.getLogger();
    private final int partition;
    // Number of active operations on an ILSMIndex instance.
    private final AtomicInteger numActiveOperations;
    private final ILogManager logManager;
    private final ILSMComponentIdGenerator idGenerator;
    private boolean flushOnExit = false;
    private boolean flushLogCreated = false;
    private final Map<String, FlushOperation> scheduledFlushes = new HashMap<>();
    private long lastFlushTime = System.nanoTime();

    public PrimaryIndexOperationTracker(int datasetID, int partition, ILogManager logManager, DatasetInfo dsInfo,
            ILSMComponentIdGenerator idGenerator) {
        super(datasetID, dsInfo);
        this.partition = partition;
        this.logManager = logManager;
        this.numActiveOperations = new AtomicInteger();
        this.idGenerator = idGenerator;
    }

    @Override
    public void beforeOperation(ILSMIndex index, LSMOperationType opType, ISearchOperationCallback searchCallback,
            IModificationOperationCallback modificationCallback) throws HyracksDataException {
        super.beforeOperation(index, opType, searchCallback, modificationCallback);
        if (opType == LSMOperationType.MODIFICATION || opType == LSMOperationType.FORCE_MODIFICATION) {
            incrementNumActiveOperations(modificationCallback);
        }
    }

    @Override
    public synchronized void completeOperation(ILSMIndex index, LSMOperationType opType,
            ISearchOperationCallback searchCallback, IModificationOperationCallback modificationCallback)
            throws HyracksDataException {
        super.completeOperation(index, opType, searchCallback, modificationCallback);
        if (opType == LSMOperationType.MODIFICATION || opType == LSMOperationType.FORCE_MODIFICATION) {
            decrementNumActiveOperations(modificationCallback);
            flushIfNeeded();
        }
    }

    public synchronized void flushIfNeeded() throws HyracksDataException {
        if (canSafelyFlush()) {
            flushIfRequested();
        }
    }

    public void flushIfRequested() throws HyracksDataException {
        // If we need a flush, and this is the last completing operation, then schedule the flush,
        // or if there is a flush scheduled by the checkpoint (flushOnExit), then schedule it

        boolean needsFlush = false;
        Set<ILSMIndex> indexes = dsInfo.getDatasetPartitionOpenIndexes(partition);

        if (!flushOnExit) {
            for (ILSMIndex lsmIndex : indexes) {
                if (lsmIndex.hasFlushRequestForCurrentMutableComponent()) {
                    needsFlush = true;
                    break;
                }
            }
        }

        ILSMIndex primaryLsmIndex = null;
        if (needsFlush || flushOnExit) {
            flushOnExit = false;
            // make the current mutable components READABLE_UNWRITABLE to stop coming modify operations from entering
            // them until the current flush is scheduled.
            LSMComponentId primaryId = null;
            //Double check that the primary index has been modified

            synchronized (this) {
                if (numActiveOperations.get() > 0) {
                    throw new IllegalStateException(
                            "Can't request a flush on an index with active operations: " + numActiveOperations.get());
                }
                for (ILSMIndex lsmIndex : indexes) {
                    if (lsmIndex.isPrimaryIndex()) {
                        if (lsmIndex.isCurrentMutableComponentEmpty()) {
                            LOGGER.info("Primary index on dataset {} and partition {} is empty... skipping flush",
                                    dsInfo.getDatasetID(), partition);
                            return;
                        }
                        primaryLsmIndex = lsmIndex;
                        break;
                    }
                }
            }
            if (primaryLsmIndex == null) {
                throw new IllegalStateException(
                        "Primary index not found in dataset " + dsInfo.getDatasetID() + " and partition " + partition);
            }
            for (ILSMIndex lsmIndex : indexes) {
                ILSMOperationTracker opTracker = lsmIndex.getOperationTracker();
                synchronized (opTracker) {
                    ILSMMemoryComponent memComponent = lsmIndex.getCurrentMemoryComponent();
                    if (memComponent.getWriterCount() > 0) {
                        throw new IllegalStateException(
                                "Can't request a flush on a component with writers inside: Index:" + lsmIndex
                                        + " Component:" + memComponent);
                    }
                    if (memComponent.getState() == ComponentState.READABLE_WRITABLE && memComponent.isModified()) {
                        memComponent.setUnwritable();
                    }
                    if (lsmIndex.isPrimaryIndex()) {
                        primaryId = (LSMComponentId) memComponent.getId();
                    }
                }
            }
            if (primaryId == null) {
                throw new IllegalStateException("Primary index found in dataset " + dsInfo.getDatasetID()
                        + " and partition " + partition + " and is modified but its component id is null");
            }
            LogRecord logRecord = new LogRecord();
            if (dsInfo.isDurable()) {
                /*
                 * Generate a FLUSH log.
                 * Flush will be triggered when the log is written to disk by LogFlusher.
                 */
                TransactionUtil.formFlushLogRecord(logRecord, datasetID, partition, primaryId.getMinId(),
                        primaryId.getMaxId(), this);
                try {
                    logManager.log(logRecord);
                } catch (ACIDException e) {
                    throw new IllegalStateException("could not write flush log", e);
                }
                flushLogCreated = true;
            } else {
                //trigger flush for temporary indexes without generating a FLUSH log.
                triggerScheduleFlush(logRecord);
            }
        }
    }

    //This method is called sequentially by LogPage.notifyFlushTerminator in the sequence flushes were scheduled.
    public synchronized void triggerScheduleFlush(LogRecord logRecord) throws HyracksDataException {
        try {
            if (!canSafelyFlush()) {
                // if a force modification operation started before the flush is scheduled, this flush will fail
                // and a next attempt will be made when that operation completes. This is only expected for metadata
                // datasets since they always use force modification
                if (MetadataIndexImmutableProperties.isMetadataDataset(datasetID)) {
                    return;
                }
                throw new IllegalStateException("Operation started while index was pending scheduling a flush");
            }
            idGenerator.refresh();
            long flushLsn = logRecord.getLSN();
            if (flushLsn == 0) {
                LOGGER.warn("flushing an index with LSN 0. Flush log record: {}", logRecord::getLogRecordForDisplay);
            }
            ILSMComponentId nextComponentId = idGenerator.getId();
            Map<String, Object> flushMap = new HashMap<>();
            flushMap.put(LSMIOOperationCallback.KEY_FLUSH_LOG_LSN, flushLsn);
            flushMap.put(LSMIOOperationCallback.KEY_NEXT_COMPONENT_ID, nextComponentId);
            synchronized (scheduledFlushes) {
                for (ILSMIndex lsmIndex : dsInfo.getDatasetPartitionOpenIndexes(partition)) {
                    ILSMIndexAccessor accessor = lsmIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                    accessor.getOpContext().setParameters(flushMap);
                    ILSMIOOperation flush = accessor.scheduleFlush();
                    lastFlushTime = System.nanoTime();
                    scheduledFlushes.put(flush.getTarget().getRelativePath(), (FlushOperation) flush);
                    flush.addCompleteListener(this);
                }
            }
        } finally {
            flushLogCreated = false;
        }
    }

    @Override
    public void completed(ILSMIOOperation operation) {
        synchronized (scheduledFlushes) {
            scheduledFlushes.remove(operation.getTarget().getRelativePath());
        }
    }

    public List<FlushOperation> getScheduledFlushes() {
        synchronized (scheduledFlushes) {
            Collection<FlushOperation> scheduled = scheduledFlushes.values();
            List<FlushOperation> flushes = new ArrayList<FlushOperation>(scheduled.size());
            flushes.addAll(scheduled);
            return flushes;
        }
    }

    public int getNumActiveOperations() {
        return numActiveOperations.get();
    }

    private void incrementNumActiveOperations(IModificationOperationCallback modificationCallback) {
        //modificationCallback can be NoOpOperationCallback when redo/undo operations are executed.
        if (modificationCallback != NoOpOperationCallback.INSTANCE) {
            numActiveOperations.incrementAndGet();
            ((AbstractOperationCallback) modificationCallback).beforeOperation();
        }
    }

    private void decrementNumActiveOperations(IModificationOperationCallback modificationCallback) {
        //modificationCallback can be NoOpOperationCallback when redo/undo operations are executed.
        if (modificationCallback != NoOpOperationCallback.INSTANCE) {
            if (numActiveOperations.decrementAndGet() < 0) {
                throw new IllegalStateException("The number of active operations cannot be negative!");
            }
            ((AbstractOperationCallback) modificationCallback).afterOperation();
        }
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

    public int getPartition() {
        return partition;
    }

    public long getLastFlushTime() {
        return lastFlushTime;
    }

    @Override
    public String toString() {
        return "Dataset (" + datasetID + "), Partition (" + partition + ")";
    }

    private boolean canSafelyFlush() {
        return numActiveOperations.get() == 0;
    }
}
