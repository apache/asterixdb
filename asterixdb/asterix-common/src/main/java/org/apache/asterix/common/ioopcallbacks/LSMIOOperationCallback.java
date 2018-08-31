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

package org.apache.asterix.common.ioopcallbacks;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.impls.FlushOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.IndexComponentFileReference;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.util.ComponentUtils;
import org.apache.hyracks.storage.am.lsm.common.util.LSMComponentIdUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// A single LSMIOOperationCallback per LSM index used to perform actions around Flush and Merge operations
public class LSMIOOperationCallback implements ILSMIOOperationCallback {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final String KEY_FLUSH_LOG_LSN = "FlushLogLsn";
    public static final String KEY_NEXT_COMPONENT_ID = "NextComponentId";
    public static final String KEY_FLUSHED_COMPONENT_ID = "FlushedComponentId";
    private static final String KEY_FIRST_LSN = "FirstLsn";
    private static final MutableArrayValueReference KEY_METADATA_FLUSH_LOG_LSN =
            new MutableArrayValueReference(KEY_FLUSH_LOG_LSN.getBytes());
    public static final long INVALID_LSN = -1L;
    private final ArrayBackedValueStorage buffer = new ArrayBackedValueStorage(Long.BYTES);
    private final IIndexCheckpointManagerProvider indexCheckpointManagerProvider;
    protected final DatasetInfo dsInfo;
    protected final ILSMIndex lsmIndex;
    private long firstLsnForCurrentMemoryComponent = 0L;
    private long persistenceLsn = 0L;
    private int pendingFlushes = 0;
    private Deque<ILSMComponentId> componentIds = new ArrayDeque<>();

    public LSMIOOperationCallback(DatasetInfo dsInfo, ILSMIndex lsmIndex, ILSMComponentId componentId,
            IIndexCheckpointManagerProvider indexCheckpointManagerProvider) {
        this.dsInfo = dsInfo;
        this.lsmIndex = lsmIndex;
        this.indexCheckpointManagerProvider = indexCheckpointManagerProvider;
        componentIds.add(componentId);
    }

    @Override
    public void beforeOperation(ILSMIOOperation operation) throws HyracksDataException {
        // No Op
    }

    @Override
    public void afterOperation(ILSMIOOperation operation) throws HyracksDataException {
        if (operation.getStatus() == LSMIOOperationStatus.FAILURE) {
            return;
        }
        if (operation.getIOOpertionType() == LSMIOOperationType.LOAD) {
            Map<String, Object> map = operation.getParameters();
            putComponentIdIntoMetadata(operation.getNewComponent(), (LSMComponentId) map.get(KEY_FLUSHED_COMPONENT_ID));
        } else if (operation.getIOOpertionType() == LSMIOOperationType.FLUSH) {
            Map<String, Object> map = operation.getParameters();
            putLSNIntoMetadata(operation.getNewComponent(), (Long) map.get(KEY_FLUSH_LOG_LSN));
            putComponentIdIntoMetadata(operation.getNewComponent(),
                    ((FlushOperation) operation).getFlushingComponent().getId());
        } else if (operation.getIOOpertionType() == LSMIOOperationType.MERGE) {
            List<ILSMDiskComponent> mergedComponents = operation.getAccessor().getOpContext().getComponentsToBeMerged();
            putLSNIntoMetadata(operation.getNewComponent(), mergedComponents);
            putComponentIdIntoMetadata(operation.getNewComponent(), mergedComponents);
            LongPointable markerLsn =
                    LongPointable.FACTORY.createPointable(ComponentUtils.getLong(mergedComponents.get(0).getMetadata(),
                            ComponentUtils.MARKER_LSN_KEY, ComponentUtils.NOT_FOUND, buffer));
            operation.getNewComponent().getMetadata().put(ComponentUtils.MARKER_LSN_KEY, markerLsn);
        }
    }

    @Override
    public void afterFinalize(ILSMIOOperation operation) throws HyracksDataException {
        if (operation.getStatus() == LSMIOOperationStatus.FAILURE) {
            return;
        }
        if (operation.getIOOpertionType() != LSMIOOperationType.LOAD
                && operation.getAccessor().getOpContext().getOperation() == IndexOperation.DELETE_COMPONENTS) {
            deleteComponentsFromCheckpoint(operation);
        } else if (operation.getIOOpertionType() == LSMIOOperationType.FLUSH
                || operation.getIOOpertionType() == LSMIOOperationType.LOAD) {
            addComponentToCheckpoint(operation);
        }
    }

    private void addComponentToCheckpoint(ILSMIOOperation operation) throws HyracksDataException {
        // will always update the checkpoint file even if no new component was created
        FileReference target = operation.getTarget();
        Map<String, Object> map = operation.getParameters();
        final Long lsn =
                operation.getIOOpertionType() == LSMIOOperationType.FLUSH ? (Long) map.get(KEY_FLUSH_LOG_LSN) : 0L;
        final LSMComponentId id = (LSMComponentId) map.get(KEY_FLUSHED_COMPONENT_ID);
        final ResourceReference ref = ResourceReference.of(target.getAbsolutePath());
        final long componentSequence = IndexComponentFileReference.of(ref.getName()).getSequenceEnd();
        indexCheckpointManagerProvider.get(ref).flushed(componentSequence, lsn, id.getMaxId());
    }

    private void deleteComponentsFromCheckpoint(ILSMIOOperation operation) throws HyracksDataException {
        // component was deleted... if a flush, do nothing.. if a merge, must update the checkpoint file
        if (operation.getIOOpertionType() == LSMIOOperationType.MERGE) {
            // Get component id of the last disk component
            LSMComponentId mostRecentComponentId =
                    getMostRecentComponentId(operation.getAccessor().getOpContext().getComponentsToBeMerged());
            // Update the checkpoint file
            FileReference target = operation.getTarget();
            final ResourceReference ref = ResourceReference.of(target.getAbsolutePath());
            indexCheckpointManagerProvider.get(ref).setLastComponentId(mostRecentComponentId.getMaxId());
        } else if (operation.getIOOpertionType() != LSMIOOperationType.FLUSH) {
            throw new IllegalStateException("Unexpected IO operation: " + operation.getIOOpertionType());
        }
    }

    private LSMComponentId getMostRecentComponentId(Collection<ILSMDiskComponent> deletedComponents)
            throws HyracksDataException {
        // must sync on opTracker to ensure list of components doesn't change
        synchronized (lsmIndex.getOperationTracker()) {
            List<ILSMDiskComponent> diskComponents = lsmIndex.getDiskComponents();
            if (diskComponents.isEmpty()) {
                LOGGER.log(Level.INFO, "There are no disk components");
                return LSMComponentId.EMPTY_INDEX_LAST_COMPONENT_ID;
            }
            if (deletedComponents.contains(diskComponents.get(diskComponents.size() - 1))) {
                LOGGER.log(Level.INFO, "All disk components have been deleted");
                return LSMComponentId.EMPTY_INDEX_LAST_COMPONENT_ID;
            }
            int mostRecentComponentIndex = 0;
            for (int i = 0; i < diskComponents.size(); i++) {
                if (!deletedComponents.contains(diskComponents.get(i))) {
                    break;
                }
                mostRecentComponentIndex++;
            }
            ILSMDiskComponent mostRecentDiskComponent = diskComponents.get(mostRecentComponentIndex);
            return (LSMComponentId) mostRecentDiskComponent.getId();
        }
    }

    private void putLSNIntoMetadata(ILSMDiskComponent newComponent, List<ILSMDiskComponent> oldComponents)
            throws HyracksDataException {
        putLSNIntoMetadata(newComponent, getComponentLSN(oldComponents));
    }

    private void putLSNIntoMetadata(ILSMDiskComponent newComponent, long lsn) throws HyracksDataException {
        newComponent.getMetadata().put(KEY_METADATA_FLUSH_LOG_LSN, LongPointable.FACTORY.createPointable(lsn));
    }

    public static long getTreeIndexLSN(DiskComponentMetadata md) throws HyracksDataException {
        LongPointable pointable = new LongPointable();
        IMetadataPageManager metadataPageManager = md.getMetadataPageManager();
        metadataPageManager.get(metadataPageManager.createMetadataFrame(), KEY_METADATA_FLUSH_LOG_LSN, pointable);
        return pointable.getLength() == 0 ? INVALID_LSN : pointable.longValue();
    }

    private ILSMComponentId getMergedComponentId(List<? extends ILSMComponent> mergedComponents)
            throws HyracksDataException {
        if (mergedComponents.isEmpty()) {
            return null;
        }
        return LSMComponentIdUtils.union(mergedComponents.get(0).getId(),
                mergedComponents.get(mergedComponents.size() - 1).getId());
    }

    private void putComponentIdIntoMetadata(ILSMDiskComponent newComponent, List<ILSMDiskComponent> oldComponents)
            throws HyracksDataException {
        ILSMComponentId componentId = getMergedComponentId(oldComponents);
        putComponentIdIntoMetadata(newComponent, componentId);
    }

    private void putComponentIdIntoMetadata(ILSMDiskComponent newComponent, ILSMComponentId componentId)
            throws HyracksDataException {
        LSMComponentIdUtils.persist(componentId, newComponent.getMetadata());
    }

    public synchronized void setFirstLsnForCurrentMemoryComponent(long firstLsn) {
        this.firstLsnForCurrentMemoryComponent = firstLsn;
        if (pendingFlushes == 0) {
            this.persistenceLsn = firstLsn;
        }
    }

    public synchronized long getPersistenceLsn() {
        return persistenceLsn;
    }

    public long getComponentLSN(List<ILSMDiskComponent> diskComponents) throws HyracksDataException {
        if (diskComponents.isEmpty()) {
            throw new IllegalArgumentException("Can't get LSN from an empty list of disk components");
        }
        // Get max LSN from the diskComponents. Implies a merge IO operation or Recovery operation.
        long maxLSN = -1L;
        for (ILSMDiskComponent c : diskComponents) {
            DiskComponentMetadata md = c.getMetadata();
            maxLSN = Math.max(getTreeIndexLSN(md), maxLSN);
        }
        return maxLSN;
    }

    @Override
    public void recycled(ILSMMemoryComponent component) throws HyracksDataException {
        component.resetId(componentIds.poll(), false);
    }

    @Override
    public synchronized void scheduled(ILSMIOOperation operation) throws HyracksDataException {
        dsInfo.declareActiveIOOperation();
        if (operation.getIOOpertionType() == LSMIOOperationType.FLUSH) {
            pendingFlushes++;
            FlushOperation flush = (FlushOperation) operation;
            Map<String, Object> map = operation.getAccessor().getOpContext().getParameters();
            Long flushLsn = (Long) map.get(KEY_FLUSH_LOG_LSN);
            map.put(KEY_FIRST_LSN, firstLsnForCurrentMemoryComponent);
            map.put(KEY_FLUSHED_COMPONENT_ID, flush.getFlushingComponent().getId());
            componentIds.add((ILSMComponentId) map.get(KEY_NEXT_COMPONENT_ID));
            firstLsnForCurrentMemoryComponent = flushLsn; // Advance the first lsn for new component
        }
    }

    @Override
    public synchronized void completed(ILSMIOOperation operation) {
        if (operation.getIOOpertionType() == LSMIOOperationType.FLUSH) {
            pendingFlushes--;
            if (operation.getStatus() == LSMIOOperationStatus.SUCCESS) {
                Map<String, Object> map = operation.getAccessor().getOpContext().getParameters();
                persistenceLsn =
                        pendingFlushes == 0 ? firstLsnForCurrentMemoryComponent : (Long) map.get(KEY_FLUSH_LOG_LSN);
            }
        }
        dsInfo.undeclareActiveIOOperation();
    }

    public synchronized boolean hasPendingFlush() {
        return pendingFlushes > 0;
    }

    @Override
    public void allocated(ILSMMemoryComponent component) throws HyracksDataException {
        // no op
    }
}
