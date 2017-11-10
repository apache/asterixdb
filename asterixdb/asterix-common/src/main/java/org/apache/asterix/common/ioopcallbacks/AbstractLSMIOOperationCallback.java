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

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.util.ComponentUtils;
import org.apache.hyracks.storage.am.lsm.common.util.LSMComponentIdUtils;

// A single LSMIOOperationCallback per LSM index used to perform actions around Flush and Merge operations
public abstract class AbstractLSMIOOperationCallback implements ILSMIOOperationCallback {
    public static final MutableArrayValueReference LSN_KEY = new MutableArrayValueReference("LSN".getBytes());
    public static final long INVALID = -1L;

    protected final ILSMIndex lsmIndex;
    // First LSN per mutable component. TODO: move from hyracks to asterixdb
    protected final long[] firstLSNs;
    // A boolean array to keep track of flush operations
    protected final boolean[] flushRequested;
    // TODO: move from hyracks to asterixdb
    protected final long[] mutableLastLSNs;
    // Index of the currently flushing or next to be flushed component
    protected int readIndex;
    // Index of the currently being written to component
    protected int writeIndex;

    protected final ILSMComponentIdGenerator idGenerator;

    public AbstractLSMIOOperationCallback(ILSMIndex lsmIndex, ILSMComponentIdGenerator idGenerator) {
        this.lsmIndex = lsmIndex;
        this.idGenerator = idGenerator;
        int count = lsmIndex.getNumberOfAllMemoryComponents();
        mutableLastLSNs = new long[count];
        firstLSNs = new long[count];
        flushRequested = new boolean[count];
        readIndex = 0;
        writeIndex = 0;
    }

    @Override
    public void beforeOperation(LSMIOOperationType opType) throws HyracksDataException {
        if (opType == LSMIOOperationType.FLUSH) {
            /*
             * This method was called on the scheduleFlush operation.
             * We set the lastLSN to the last LSN for the index (the LSN for the flush log)
             * We mark the component flushing flag
             * We then move the write pointer to the next component and sets its first LSN to the flush log LSN
             */
            synchronized (this) {
                flushRequested[writeIndex] = true;
                writeIndex = (writeIndex + 1) % mutableLastLSNs.length;
                // Set the firstLSN of the next component unless it is being flushed
                if (writeIndex != readIndex) {
                    firstLSNs[writeIndex] = mutableLastLSNs[writeIndex];
                }
            }
        }
    }

    @Override
    public void afterFinalize(LSMIOOperationType opType, ILSMDiskComponent newComponent) {
        // The operation was complete and the next I/O operation for the LSM index didn't start yet
        if (opType == LSMIOOperationType.FLUSH && newComponent != null) {
            synchronized (this) {
                flushRequested[readIndex] = false;
                // if the component which just finished flushing is the component that will be modified next,
                // we set its first LSN to its previous LSN
                if (readIndex == writeIndex) {
                    firstLSNs[writeIndex] = mutableLastLSNs[writeIndex];
                }
                readIndex = (readIndex + 1) % mutableLastLSNs.length;
            }
        }
    }

    public void putLSNIntoMetadata(ILSMDiskComponent newComponent, List<ILSMComponent> oldComponents)
            throws HyracksDataException {
        newComponent.getMetadata().put(LSN_KEY, LongPointable.FACTORY.createPointable(getComponentLSN(oldComponents)));
    }

    public static long getTreeIndexLSN(DiskComponentMetadata md) throws HyracksDataException {
        LongPointable pointable = new LongPointable();
        IMetadataPageManager metadataPageManager = md.getMetadataPageManager();
        metadataPageManager.get(metadataPageManager.createMetadataFrame(), LSN_KEY, pointable);
        return pointable.getLength() == 0 ? INVALID : pointable.longValue();
    }

    private ILSMComponentId getMergedComponentId(List<ILSMComponent> mergedComponents) throws HyracksDataException {
        if (mergedComponents == null || mergedComponents.isEmpty()) {
            return null;
        }
        return LSMComponentIdUtils.union(mergedComponents.get(0).getId(),
                mergedComponents.get(mergedComponents.size() - 1).getId());

    }

    private void putComponentIdIntoMetadata(LSMIOOperationType opType, ILSMDiskComponent newComponent,
            List<ILSMComponent> oldComponents) throws HyracksDataException {
        // the id of flushed component is set when we copy the metadata of the memory component
        if (opType == LSMIOOperationType.MERGE) {
            ILSMComponentId componentId = getMergedComponentId(oldComponents);
            LSMComponentIdUtils.persist(componentId, newComponent.getMetadata());
        }
    }

    public synchronized void updateLastLSN(long lastLSN) {
        if (!flushRequested[writeIndex]) {
            //if the memory component pointed by writeIndex is being flushed, we should ignore this update call
            //since otherwise the original LSN is overwritten.
            //Moreover, since the memory component is already being flushed, the next scheduleFlush request must fail.
            //See https://issues.apache.org/jira/browse/ASTERIXDB-1917
            mutableLastLSNs[writeIndex] = lastLSN;
        }
    }

    public void setFirstLSN(long firstLSN) {
        // We make sure that this method is only called on an empty component so the first LSN is not set incorrectly
        firstLSNs[writeIndex] = firstLSN;
    }

    public synchronized long getFirstLSN() {
        // We make sure that this method is only called on a non-empty component so the returned LSN is meaningful
        // The firstLSN is always the lsn of the currently being flushed component or the next
        // to be flushed when no flush operation is on going
        return firstLSNs[readIndex];
    }

    public synchronized boolean hasPendingFlush() {

        for (int i = 0; i < flushRequested.length; i++) {
            if (flushRequested[i]) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void afterOperation(LSMIOOperationType opType, List<ILSMComponent> oldComponents,
            ILSMDiskComponent newComponent) throws HyracksDataException {
        //TODO: Copying Filters and all content of the metadata pages for flush operation should be done here
        if (newComponent != null) {
            putLSNIntoMetadata(newComponent, oldComponents);
            putComponentIdIntoMetadata(opType, newComponent, oldComponents);
            if (opType == LSMIOOperationType.MERGE) {
                // In case of merge, oldComponents are never null
                LongPointable markerLsn =
                        LongPointable.FACTORY.createPointable(ComponentUtils.getLong(oldComponents.get(0).getMetadata(),
                                ComponentUtils.MARKER_LSN_KEY, ComponentUtils.NOT_FOUND));
                newComponent.getMetadata().put(ComponentUtils.MARKER_LSN_KEY, markerLsn);
            }
        }
    }

    public long getComponentLSN(List<? extends ILSMComponent> diskComponents) throws HyracksDataException {
        if (diskComponents == null) {
            // Implies a flush IO operation. --> moves the flush pointer
            // Flush operation of an LSM index are executed sequentially.
            synchronized (this) {
                long lsn = mutableLastLSNs[readIndex];
                return lsn;
            }
        }
        // Get max LSN from the diskComponents. Implies a merge IO operation or Recovery operation.
        long maxLSN = -1L;
        for (ILSMComponent c : diskComponents) {
            DiskComponentMetadata md = ((ILSMDiskComponent) c).getMetadata();
            maxLSN = Math.max(getTreeIndexLSN(md), maxLSN);
        }
        return maxLSN;
    }

    @Override
    public void recycled(ILSMMemoryComponent component) throws HyracksDataException {
        component.resetId(idGenerator.getId());
    }

    @Override
    public void allocated(ILSMMemoryComponent component) throws HyracksDataException {
        component.resetId(idGenerator.getId());
    }

    /**
     * @param component
     * @param componentFilePath
     * @return The LSN byte offset in the LSM disk component if the index is valid,
     *         otherwise {@link IMetadataPageManager#INVALID_LSN_OFFSET}.
     * @throws HyracksDataException
     */
    public abstract long getComponentFileLSNOffset(ILSMDiskComponent component, String componentFilePath)
            throws HyracksDataException;

}
