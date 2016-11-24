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

package org.apache.hyracks.storage.am.lsm.common.impls;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationExecutionType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexInternal;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public abstract class AbstractLSMIndex implements ILSMIndexInternal {
    protected final ILSMHarness lsmHarness;
    protected final IIOManager ioManager;
    protected final ILSMIOOperationScheduler ioScheduler;
    protected final ILSMIOOperationCallback ioOpCallback;

    // In-memory components.
    protected final List<ILSMComponent> memoryComponents;
    protected final List<IVirtualBufferCache> virtualBufferCaches;
    protected AtomicInteger currentMutableComponentId;

    // On-disk components.
    protected final IBufferCache diskBufferCache;
    protected final ILSMIndexFileManager fileManager;
    protected final IFileMapProvider diskFileMapProvider;
    protected final List<ILSMComponent> diskComponents;
    protected final List<ILSMComponent> inactiveDiskComponents;
    protected final double bloomFilterFalsePositiveRate;
    protected final ILSMComponentFilterFrameFactory filterFrameFactory;
    protected final LSMComponentFilterManager filterManager;
    protected final int[] filterFields;
    protected final boolean durable;
    protected boolean isActivated;
    protected final AtomicBoolean[] flushRequests;
    protected boolean memoryComponentsAllocated = false;

    public AbstractLSMIndex(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            IBufferCache diskBufferCache,
            ILSMIndexFileManager fileManager, IFileMapProvider diskFileMapProvider, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, ILSMComponentFilterFrameFactory filterFrameFactory,
            LSMComponentFilterManager filterManager, int[] filterFields, boolean durable) {
        this.ioManager = ioManager;
        this.virtualBufferCaches = virtualBufferCaches;
        this.diskBufferCache = diskBufferCache;
        this.diskFileMapProvider = diskFileMapProvider;
        this.fileManager = fileManager;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.ioScheduler = ioScheduler;
        this.ioOpCallback = ioOpCallback;
        this.ioOpCallback.setNumOfMutableComponents(virtualBufferCaches.size());
        this.filterFrameFactory = filterFrameFactory;
        this.filterManager = filterManager;
        this.filterFields = filterFields;
        this.inactiveDiskComponents = new LinkedList<>();
        this.durable = durable;
        lsmHarness = new LSMHarness(this, mergePolicy, opTracker, diskBufferCache.isReplicationEnabled());
        isActivated = false;
        diskComponents = new ArrayList<>();
        memoryComponents = new ArrayList<>();
        currentMutableComponentId = new AtomicInteger();
        flushRequests = new AtomicBoolean[virtualBufferCaches.size()];
        for (int i = 0; i < virtualBufferCaches.size(); i++) {
            flushRequests[i] = new AtomicBoolean();
        }
    }

    // The constructor used by external indexes
    public AbstractLSMIndex(IIOManager ioManager, IBufferCache diskBufferCache, ILSMIndexFileManager fileManager,
            IFileMapProvider diskFileMapProvider, double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback,
            boolean durable) {
        this.ioManager = ioManager;
        this.diskBufferCache = diskBufferCache;
        this.diskFileMapProvider = diskFileMapProvider;
        this.fileManager = fileManager;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.ioScheduler = ioScheduler;
        this.ioOpCallback = ioOpCallback;
        this.durable = durable;
        lsmHarness = new ExternalIndexHarness(this, mergePolicy, opTracker, diskBufferCache.isReplicationEnabled());
        isActivated = false;
        diskComponents = new LinkedList<>();
        this.inactiveDiskComponents = new LinkedList<>();
        // Memory related objects are nulled
        this.virtualBufferCaches = null;
        memoryComponents = null;
        currentMutableComponentId = null;
        flushRequests = null;
        filterFrameFactory = null;
        filterManager = null;
        filterFields = null;
    }

    protected void markAsValidInternal(ITreeIndex treeIndex) throws HyracksDataException {
        int fileId = treeIndex.getFileId();
        IBufferCache bufferCache = treeIndex.getBufferCache();
        treeIndex.getMetaManager().close();
        // WARNING: flushing the metadata page should be done after releasing the write latch; otherwise, the page
        // won't be flushed to disk because it won't be dirty until the write latch has been released.
        // Force modified metadata page to disk.
        // If the index is not durable, then the flush is not necessary.
        if (durable) {
            bufferCache.force(fileId, true);
        }
    }

    protected void markAsValidInternal(IBufferCache bufferCache, BloomFilter filter) throws HyracksDataException {
        if (durable) {
            bufferCache.force(filter.getFileId(), true);
        }
    }

    @Override
    public void addComponent(ILSMComponent c) throws HyracksDataException {
        diskComponents.add(0, c);
    }

    @Override
    public void subsumeMergedComponents(ILSMComponent newComponent, List<ILSMComponent> mergedComponents)
            throws HyracksDataException {
        int swapIndex = diskComponents.indexOf(mergedComponents.get(0));
        diskComponents.removeAll(mergedComponents);
        diskComponents.add(swapIndex, newComponent);
    }

    @Override
    public void changeMutableComponent() {
        currentMutableComponentId.set((currentMutableComponentId.get() + 1) % memoryComponents.size());
        ((AbstractMemoryLSMComponent) memoryComponents.get(currentMutableComponentId.get())).setActive();
    }

    @Override
    public List<ILSMComponent> getImmutableComponents() {
        return diskComponents;
    }

    @Override
    public void changeFlushStatusForCurrentMutableCompoent(boolean needsFlush) {
        flushRequests[currentMutableComponentId.get()].set(needsFlush);
    }

    @Override
    public boolean hasFlushRequestForCurrentMutableComponent() {
        return flushRequests[currentMutableComponentId.get()].get();
    }

    @Override
    public ILSMOperationTracker getOperationTracker() {
        return lsmHarness.getOperationTracker();
    }

    @Override
    public ILSMIOOperationScheduler getIOScheduler() {
        return ioScheduler;
    }

    @Override
    public ILSMIOOperationCallback getIOOperationCallback() {
        return ioOpCallback;
    }

    @Override
    public IBufferCache getBufferCache() {
        return diskBufferCache;
    }

    public boolean isEmptyIndex() {
        boolean isModified = false;
        for (ILSMComponent c : memoryComponents) {
            AbstractMemoryLSMComponent mutableComponent = (AbstractMemoryLSMComponent) c;
            if (mutableComponent.isModified()) {
                isModified = true;
                break;
            }
        }
        return diskComponents.isEmpty() && !isModified;
    }

    @Override
    public String toString() {
        return "LSMIndex [" + fileManager.getBaseDir() + "]";
    }

    @Override
    public boolean hasMemoryComponents() {
        return true;
    }

    @Override
    public boolean isCurrentMutableComponentEmpty() throws HyracksDataException {
        //check if the current memory component has been modified
        return !((AbstractMemoryLSMComponent) memoryComponents.get(currentMutableComponentId.get())).isModified();
    }

    public void setCurrentMutableComponentState(ComponentState componentState) {
        ((AbstractMemoryLSMComponent) memoryComponents.get(currentMutableComponentId.get())).setState(componentState);
    }

    public ComponentState getCurrentMutableComponentState() {
        return ((AbstractMemoryLSMComponent) memoryComponents.get(currentMutableComponentId.get())).getState();
    }

    public int getCurrentMutableComponentWriterCount() {
        return ((AbstractMemoryLSMComponent) memoryComponents.get(currentMutableComponentId.get())).getWriterCount();
    }

    @Override
    public List<ILSMComponent> getInactiveDiskComponents() {
        return inactiveDiskComponents;
    }

    @Override
    public void addInactiveDiskComponent(ILSMComponent diskComponent) {
        inactiveDiskComponents.add(diskComponent);
    }

    public abstract Set<String> getLSMComponentPhysicalFiles(ILSMComponent newComponent);

    @Override
    public void scheduleReplication(ILSMIndexOperationContext ctx, List<ILSMComponent> lsmComponents, boolean bulkload,
            ReplicationOperation operation, LSMOperationType opType) throws HyracksDataException {
        //get set of files to be replicated for this component
        Set<String> componentFiles = new HashSet<>();

        //get set of files to be replicated for each component
        for (ILSMComponent lsmComponent : lsmComponents) {
            componentFiles.addAll(getLSMComponentPhysicalFiles(lsmComponent));
        }

        ReplicationExecutionType executionType;
        if (bulkload) {
            executionType = ReplicationExecutionType.SYNC;
        } else {
            executionType = ReplicationExecutionType.ASYNC;
        }

        //create replication job and submit it
        LSMIndexReplicationJob job = new LSMIndexReplicationJob(this, ctx, componentFiles, operation, executionType,
                opType);
        try {
            diskBufferCache.getIOReplicationManager().submitJob(job);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public abstract void allocateMemoryComponents() throws HyracksDataException;

    @Override
    public boolean isMemoryComponentsAllocated() {
        return memoryComponentsAllocated;
    }

    @Override
    public boolean isDurable() {
        return durable;
    }

    public ILSMComponent getCurrentMemoryComponent() {
        return memoryComponents.get(currentMutableComponentId.get());
    }
}
