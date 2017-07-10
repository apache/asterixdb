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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationExecutionType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.impls.AbstractSearchPredicate;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public abstract class AbstractLSMIndex implements ILSMIndex {
    protected final ILSMHarness lsmHarness;
    protected final IIOManager ioManager;
    protected final ILSMIOOperationScheduler ioScheduler;
    protected final ILSMIOOperationCallback ioOpCallback;

    // In-memory components.
    protected final List<ILSMMemoryComponent> memoryComponents;
    protected final List<IVirtualBufferCache> virtualBufferCaches;
    protected AtomicInteger currentMutableComponentId;
    // On-disk components.
    protected final IBufferCache diskBufferCache;
    protected final ILSMIndexFileManager fileManager;
    // components with lower indexes are newer than components with higher index
    protected final List<ILSMDiskComponent> diskComponents;
    protected final List<ILSMDiskComponent> inactiveDiskComponents;
    protected final double bloomFilterFalsePositiveRate;
    protected final IComponentFilterHelper filterHelper;
    protected final ILSMComponentFilterFrameFactory filterFrameFactory;
    protected final LSMComponentFilterManager filterManager;
    protected final int[] treeFields;
    protected final int[] filterFields;
    protected final boolean durable;
    protected boolean isActive;
    protected final AtomicBoolean[] flushRequests;
    protected boolean memoryComponentsAllocated = false;

    public AbstractLSMIndex(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            IBufferCache diskBufferCache, ILSMIndexFileManager fileManager, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, ILSMComponentFilterFrameFactory filterFrameFactory,
            LSMComponentFilterManager filterManager, int[] filterFields, boolean durable,
            IComponentFilterHelper filterHelper, int[] treeFields) {
        this.ioManager = ioManager;
        this.virtualBufferCaches = virtualBufferCaches;
        this.diskBufferCache = diskBufferCache;
        this.fileManager = fileManager;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.ioScheduler = ioScheduler;
        this.ioOpCallback = ioOpCallback;
        this.ioOpCallback.setNumOfMutableComponents(virtualBufferCaches.size());
        this.filterHelper = filterHelper;
        this.filterFrameFactory = filterFrameFactory;
        this.filterManager = filterManager;
        this.treeFields = treeFields;
        this.filterFields = filterFields;
        this.inactiveDiskComponents = new LinkedList<>();
        this.durable = durable;
        lsmHarness = new LSMHarness(this, mergePolicy, opTracker, diskBufferCache.isReplicationEnabled());
        isActive = false;
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
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback, boolean durable) {
        this.ioManager = ioManager;
        this.diskBufferCache = diskBufferCache;
        this.fileManager = fileManager;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.ioScheduler = ioScheduler;
        this.ioOpCallback = ioOpCallback;
        this.durable = durable;
        lsmHarness = new ExternalIndexHarness(this, mergePolicy, opTracker, diskBufferCache.isReplicationEnabled());
        isActive = false;
        diskComponents = new LinkedList<>();
        this.inactiveDiskComponents = new LinkedList<>();
        // Memory related objects are nulled
        this.virtualBufferCaches = null;
        memoryComponents = null;
        currentMutableComponentId = null;
        flushRequests = null;
        filterHelper = null;
        filterFrameFactory = null;
        filterManager = null;
        treeFields = null;
        filterFields = null;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        if (isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_CREATE_ACTIVE_INDEX);
        }
        fileManager.createDirs();
        diskComponents.clear();
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        if (isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_ACTIVATE_ACTIVE_INDEX);
        }
        loadDiskComponents();
        isActive = true;
    }

    protected void loadDiskComponents() throws HyracksDataException {
        diskComponents.clear();
        List<LSMComponentFileReferences> validFileReferences = fileManager.cleanupAndGetValidFiles();
        for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
            ILSMDiskComponent component = loadComponent(lsmComonentFileReference);
            diskComponents.add(component);
        }
    }

    @Override
    public final synchronized void deactivate() throws HyracksDataException {
        deactivate(true);
    }

    @Override
    public synchronized void deactivate(boolean flush) throws HyracksDataException {
        if (!isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_DEACTIVATE_INACTIVE_INDEX);
        }
        if (flush) {
            flushMemoryComponents();
        }
        deactivateDiskComponents();
        deactivateMemoryComponents();
        isActive = false;
    }

    // What if more than one memory component needs flushing??
    protected void flushMemoryComponents() throws HyracksDataException {
        BlockingIOOperationCallbackWrapper cb = new BlockingIOOperationCallbackWrapper(ioOpCallback);
        ILSMIndexAccessor accessor = createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        accessor.scheduleFlush(cb);
        try {
            cb.waitForIO();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        }
    }

    protected void deactivateDiskComponents() throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = diskComponents;
        for (ILSMDiskComponent c : immutableComponents) {
            deactivateDiskComponent(c);
        }
    }

    protected void deactivateMemoryComponents() throws HyracksDataException {
        if (memoryComponentsAllocated) {
            for (ILSMMemoryComponent c : memoryComponents) {
                deactivateMemoryComponent(c);
            }
            memoryComponentsAllocated = false;
        }
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_DESTROY_ACTIVE_INDEX);
        }
        destroyDiskComponents();
        fileManager.deleteDirs();
    }

    protected void destroyDiskComponents() throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = diskComponents;
        for (ILSMDiskComponent c : immutableComponents) {
            destroyDiskComponent(c);
        }
    }

    @Override
    public void clear() throws HyracksDataException {
        if (!isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_CLEAR_INACTIVE_INDEX);
        }
        clearMemoryComponents();
        clearDiskComponents();
    }

    private void clearDiskComponents() throws HyracksDataException {
        for (ILSMDiskComponent c : diskComponents) {
            clearDiskComponent(c);
        }
        diskComponents.clear();
    }

    protected void clearMemoryComponents() throws HyracksDataException {
        if (memoryComponentsAllocated) {
            for (ILSMMemoryComponent c : memoryComponents) {
                clearMemoryComponent(c);
            }
        }
    }

    @Override
    public void getOperationalComponents(ILSMIndexOperationContext ctx) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = diskComponents;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        int cmc = currentMutableComponentId.get();
        ctx.setCurrentMutableComponentId(cmc);
        operationalComponents.clear();
        switch (ctx.getOperation()) {
            case UPDATE:
            case PHYSICALDELETE:
            case FLUSH:
            case DELETE:
            case UPSERT:
                operationalComponents.add(memoryComponents.get(cmc));
                break;
            case INSERT:
                addOperationalMutableComponents(operationalComponents);
                operationalComponents.addAll(immutableComponents);
                break;
            case SEARCH:
                if (memoryComponentsAllocated) {
                    addOperationalMutableComponents(operationalComponents);
                }
                if (filterManager != null) {
                    for (ILSMComponent c : immutableComponents) {
                        if (c.getLSMComponentFilter().satisfy(
                                ((AbstractSearchPredicate) ctx.getSearchPredicate()).getMinFilterTuple(),
                                ((AbstractSearchPredicate) ctx.getSearchPredicate()).getMaxFilterTuple(),
                                ctx.getFilterCmp())) {
                            operationalComponents.add(c);
                        }
                    }
                } else {
                    operationalComponents.addAll(immutableComponents);
                }

                break;
            case MERGE:
                operationalComponents.addAll(ctx.getComponentsToBeMerged());
                break;
            case FULL_MERGE:
                operationalComponents.addAll(immutableComponents);
                break;
            case REPLICATE:
                operationalComponents.addAll(ctx.getComponentsToBeReplicated());
                break;
            case DISK_COMPONENT_SCAN:
                operationalComponents.addAll(immutableComponents);
                break;
            default:
                throw new UnsupportedOperationException("Operation " + ctx.getOperation() + " not supported.");
        }
    }

    @Override
    public void scanDiskComponents(ILSMIndexOperationContext ctx, IIndexCursor cursor) throws HyracksDataException {
        throw HyracksDataException.create(ErrorCode.DISK_COMPONENT_SCAN_NOT_ALLOWED_FOR_SECONDARY_INDEX);
    }

    @Override
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        ILSMMemoryComponent flushingComponent = (ILSMMemoryComponent) ctx.getComponentHolder().get(0);
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        AbstractLSMIndexOperationContext opCtx =
                createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        opCtx.setOperation(IndexOperation.FLUSH);
        opCtx.getComponentHolder().add(flushingComponent);
        ILSMIOOperation flushOp = createFlushOperation(opCtx, flushingComponent, componentFileRefs, callback);
        ioScheduler.scheduleOperation(flushOp);
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        // merge must create a different op ctx
        AbstractLSMIndexOperationContext opCtx =
                createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        opCtx.setOperation(IndexOperation.MERGE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        ILSMDiskComponent firstComponent = (ILSMDiskComponent) mergingComponents.get(0);
        ILSMDiskComponent lastComponent = (ILSMDiskComponent) mergingComponents.get(mergingComponents.size() - 1);
        LSMComponentFileReferences mergeFileRefs = getMergeFileReferences(firstComponent, lastComponent);
        ILSMIOOperation mergeOp = createMergeOperation(opCtx, mergingComponents, mergeFileRefs, callback);
        ioScheduler.scheduleOperation(mergeOp);
    }

    private void addOperationalMutableComponents(List<ILSMComponent> operationalComponents) {
        int cmc = currentMutableComponentId.get();
        int numMutableComponents = memoryComponents.size();
        for (int i = 0; i < numMutableComponents - 1; i++) {
            ILSMMemoryComponent c = memoryComponents.get((cmc + i + 1) % numMutableComponents);
            if (c.isReadable()) {
                // Make sure newest components are added first
                operationalComponents.add(0, c);
            }
        }
        // The current mutable component is always added
        operationalComponents.add(0, memoryComponents.get(cmc));
    }

    @Override
    public final IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws HyracksDataException {
        if (checkIfEmptyIndex && !isEmptyIndex()) {
            throw HyracksDataException.create(ErrorCode.LOAD_NON_EMPTY_INDEX);
        }
        return createBulkLoader(fillLevel, verifyInput, numElementsHint);
    }

    @Override
    public final synchronized void allocateMemoryComponents() throws HyracksDataException {
        if (!isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_ALLOCATE_MEMORY_FOR_INACTIVE_INDEX);
        }
        if (memoryComponentsAllocated) {
            return;
        }
        for (ILSMMemoryComponent c : memoryComponents) {
            allocateMemoryComponent(c);
        }
        memoryComponentsAllocated = true;
    }

    protected void markAsValidInternal(ITreeIndex treeIndex) throws HyracksDataException {
        int fileId = treeIndex.getFileId();
        IBufferCache bufferCache = treeIndex.getBufferCache();
        treeIndex.getPageManager().close();
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
    public void addDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        diskComponents.add(0, c);
    }

    @Override
    public void subsumeMergedComponents(ILSMDiskComponent newComponent, List<ILSMComponent> mergedComponents)
            throws HyracksDataException {
        int swapIndex = diskComponents.indexOf(mergedComponents.get(0));
        diskComponents.removeAll(mergedComponents);
        diskComponents.add(swapIndex, newComponent);
    }

    @Override
    public void changeMutableComponent() {
        currentMutableComponentId.set((currentMutableComponentId.get() + 1) % memoryComponents.size());
        memoryComponents.get(currentMutableComponentId.get()).activate();
    }

    @Override
    public List<ILSMDiskComponent> getImmutableComponents() {
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
            AbstractLSMMemoryComponent mutableComponent = (AbstractLSMMemoryComponent) c;
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
        return !memoryComponents.get(currentMutableComponentId.get()).isModified();
    }

    public void setCurrentMutableComponentState(ComponentState componentState) {
        memoryComponents.get(currentMutableComponentId.get()).setState(componentState);
    }

    public ComponentState getCurrentMutableComponentState() {
        return memoryComponents.get(currentMutableComponentId.get()).getState();
    }

    public int getCurrentMutableComponentWriterCount() {
        return memoryComponents.get(currentMutableComponentId.get()).getWriterCount();
    }

    @Override
    public List<ILSMDiskComponent> getInactiveDiskComponents() {
        return inactiveDiskComponents;
    }

    @Override
    public void addInactiveDiskComponent(ILSMDiskComponent diskComponent) {
        inactiveDiskComponents.add(diskComponent);
    }

    @Override
    public void scheduleReplication(ILSMIndexOperationContext ctx, List<ILSMDiskComponent> lsmComponents,
            boolean bulkload, ReplicationOperation operation, LSMOperationType opType) throws HyracksDataException {
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
        LSMIndexReplicationJob job =
                new LSMIndexReplicationJob(this, ctx, componentFiles, operation, executionType, opType);
        try {
            diskBufferCache.getIOReplicationManager().submitJob(job);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public boolean isMemoryComponentsAllocated() {
        return memoryComponentsAllocated;
    }

    @Override
    public boolean isDurable() {
        return durable;
    }

    @Override
    public ILSMMemoryComponent getCurrentMemoryComponent() {
        return memoryComponents.get(currentMutableComponentId.get());
    }

    @Override
    public int getCurrentMemoryComponentIndex() {
        return currentMutableComponentId.get();
    }

    @Override
    public List<ILSMMemoryComponent> getMemoryComponents() {
        return memoryComponents;
    }

    protected IBinaryComparatorFactory[] getFilterCmpFactories() {
        return filterHelper == null ? null : filterHelper.getFilterCmpFactories();
    }

    @Override
    public int getNumOfFilterFields() {
        return filterFields == null ? 0 : filterFields.length;
    }

    public double bloomFilterFalsePositiveRate() {
        return bloomFilterFalsePositiveRate;
    }

    @Override
    public void updateFilter(ILSMIndexOperationContext ctx, ITupleReference tuple) throws HyracksDataException {
        if (ctx.getFilterTuple() != null) {
            ctx.getFilterTuple().reset(tuple);
            memoryComponents.get(currentMutableComponentId.get()).getLSMComponentFilter().update(ctx.getFilterTuple(),
                    ctx.getFilterCmp());
        }
    }

    public int[] getFilterFields() {
        return filterFields;
    }

    public int[] getTreeFields() {
        return treeFields;
    }

    public LSMComponentFilterManager getFilterManager() {
        return filterManager;
    }

    public ILSMHarness getLsmHarness() {
        return lsmHarness;
    }

    @Override
    public final void validate() throws HyracksDataException {
        if (memoryComponentsAllocated) {
            for (ILSMMemoryComponent c : memoryComponents) {
                validateMemoryComponent(c);
            }
        }
        for (ILSMDiskComponent c : diskComponents) {
            validateDiskComponent(c);
        }
    }

    @Override
    public long getMemoryAllocationSize() {
        long size = 0;
        for (ILSMMemoryComponent c : memoryComponents) {
            size += getMemoryComponentSize(c);
        }
        return size;
    }

    public abstract Set<String> getLSMComponentPhysicalFiles(ILSMComponent newComponent);

    protected abstract void allocateMemoryComponent(ILSMMemoryComponent c) throws HyracksDataException;

    protected abstract IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint)
            throws HyracksDataException;

    protected abstract ILSMDiskComponent loadComponent(LSMComponentFileReferences refs) throws HyracksDataException;

    protected abstract void deactivateMemoryComponent(ILSMMemoryComponent c) throws HyracksDataException;

    protected abstract void deactivateDiskComponent(ILSMDiskComponent c) throws HyracksDataException;

    protected abstract void destroyDiskComponent(ILSMDiskComponent c) throws HyracksDataException;

    protected abstract void clearDiskComponent(ILSMDiskComponent c) throws HyracksDataException;

    protected abstract void clearMemoryComponent(ILSMMemoryComponent c) throws HyracksDataException;

    protected abstract void validateMemoryComponent(ILSMMemoryComponent c) throws HyracksDataException;

    protected abstract void validateDiskComponent(ILSMDiskComponent c) throws HyracksDataException;

    protected abstract long getMemoryComponentSize(ILSMMemoryComponent c);

    protected abstract LSMComponentFileReferences getMergeFileReferences(ILSMDiskComponent firstComponent,
            ILSMDiskComponent lastComponent) throws HyracksDataException;

    protected abstract AbstractLSMIndexOperationContext createOpContext(
            IModificationOperationCallback modificationCallback, ISearchOperationCallback searchCallback)
            throws HyracksDataException;

    protected abstract ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            ILSMMemoryComponent flushingComponent, LSMComponentFileReferences componentFileRefs,
            ILSMIOOperationCallback callback) throws HyracksDataException;

    protected abstract ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            List<ILSMComponent> mergingComponents, LSMComponentFileReferences mergeFileRefs,
            ILSMIOOperationCallback callback) throws HyracksDataException;

}
