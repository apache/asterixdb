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
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationExecutionType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.impls.AbstractSearchPredicate;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
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
import org.apache.hyracks.util.trace.ITracer;

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
    protected ITracer tracer;
    // Factory for creating on-disk index components during flush and merge.
    protected final ILSMDiskComponentFactory componentFactory;
    // Factory for creating on-disk index components during bulkload.
    protected final ILSMDiskComponentFactory bulkLoadComponentFactory;

    public AbstractLSMIndex(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            IBufferCache diskBufferCache, ILSMIndexFileManager fileManager, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory bulkLoadComponentFactory, ILSMComponentFilterFrameFactory filterFrameFactory,
            LSMComponentFilterManager filterManager, int[] filterFields, boolean durable,
            IComponentFilterHelper filterHelper, int[] treeFields, ITracer tracer) {
        this.ioManager = ioManager;
        this.virtualBufferCaches = virtualBufferCaches;
        this.diskBufferCache = diskBufferCache;
        this.fileManager = fileManager;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.ioScheduler = ioScheduler;
        this.ioOpCallback = ioOpCallbackFactory.createIoOpCallback(this);
        this.componentFactory = componentFactory;
        this.bulkLoadComponentFactory = bulkLoadComponentFactory;
        this.filterHelper = filterHelper;
        this.filterFrameFactory = filterFrameFactory;
        this.filterManager = filterManager;
        this.treeFields = treeFields;
        this.filterFields = filterFields;
        this.inactiveDiskComponents = new LinkedList<>();
        this.durable = durable;
        this.tracer = tracer;
        lsmHarness = new LSMHarness(this, mergePolicy, opTracker, diskBufferCache.isReplicationEnabled(), tracer);
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
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMDiskComponentFactory componentFactory, ILSMDiskComponentFactory bulkLoadComponentFactory,
            boolean durable) {
        this.ioManager = ioManager;
        this.diskBufferCache = diskBufferCache;
        this.fileManager = fileManager;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.ioScheduler = ioScheduler;
        this.ioOpCallback = ioOpCallbackFactory.createIoOpCallback(this);
        this.componentFactory = componentFactory;
        this.bulkLoadComponentFactory = bulkLoadComponentFactory;
        this.durable = durable;
        lsmHarness = new ExternalIndexHarness(this, mergePolicy, opTracker, diskBufferCache.isReplicationEnabled());
        isActive = false;
        diskComponents = new LinkedList<>();
        this.inactiveDiskComponents = new LinkedList<>();
        // Memory related objects are nulled
        virtualBufferCaches = null;
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

    private void loadDiskComponents() throws HyracksDataException {
        diskComponents.clear();
        List<LSMComponentFileReferences> validFileReferences = fileManager.cleanupAndGetValidFiles();
        for (LSMComponentFileReferences lsmComponentFileReferences : validFileReferences) {
            ILSMDiskComponent component =
                    createDiskComponent(componentFactory, lsmComponentFileReferences.getInsertIndexFileReference(),
                            lsmComponentFileReferences.getDeleteIndexFileReference(),
                            lsmComponentFileReferences.getBloomFilterFileReference(), false);
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
            flushMemoryComponent();
        }
        deactivateDiskComponents();
        deallocateMemoryComponents();
        isActive = false;
    }

    protected void flushMemoryComponent() throws HyracksDataException {
        BlockingIOOperationCallbackWrapper cb = new BlockingIOOperationCallbackWrapper(ioOpCallback);
        ILSMIndexAccessor accessor = createAccessor(NoOpIndexAccessParameters.INSTANCE);
        accessor.scheduleFlush(cb);
        try {
            cb.waitForIO();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        }
    }

    private void deactivateDiskComponents() throws HyracksDataException {
        for (ILSMDiskComponent c : diskComponents) {
            c.deactivateAndPurge();
        }
    }

    private void deallocateMemoryComponents() throws HyracksDataException {
        if (memoryComponentsAllocated) {
            for (ILSMMemoryComponent c : memoryComponents) {
                c.deallocate();
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

    private void destroyDiskComponents() throws HyracksDataException {
        for (ILSMDiskComponent c : diskComponents) {
            c.destroy();
        }
    }

    @Override
    public void clear() throws HyracksDataException {
        if (!isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_CLEAR_INACTIVE_INDEX);
        }
        resetMemoryComponents();
        deactivateAndDestroyDiskComponents();
    }

    private void deactivateAndDestroyDiskComponents() throws HyracksDataException {
        for (ILSMDiskComponent c : diskComponents) {
            c.deactivateAndDestroy();
        }
        diskComponents.clear();
    }

    private void resetMemoryComponents() throws HyracksDataException {
        if (memoryComponentsAllocated && memoryComponents != null) {
            for (ILSMMemoryComponent c : memoryComponents) {
                c.reset();
            }
        }
    }

    @Override
    public void purge() throws HyracksDataException {
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
            case DELETE_MEMORY_COMPONENT:
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
            case DELETE_DISK_COMPONENTS:
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
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        AbstractLSMIndexOperationContext opCtx =
                createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        opCtx.setOperation(ctx.getOperation());
        opCtx.getComponentHolder().addAll(ctx.getComponentHolder());
        ILSMIOOperation flushOp = createFlushOperation(opCtx, componentFileRefs, callback);
        ioScheduler.scheduleOperation(TracedIOOperation.wrap(flushOp, tracer));
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        // merge must create a different op ctx
        AbstractLSMIndexOperationContext opCtx =
                createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        opCtx.setOperation(ctx.getOperation());
        opCtx.getComponentHolder().addAll(mergingComponents);
        ILSMDiskComponent firstComponent = (ILSMDiskComponent) mergingComponents.get(0);
        ILSMDiskComponent lastComponent = (ILSMDiskComponent) mergingComponents.get(mergingComponents.size() - 1);
        LSMComponentFileReferences mergeFileRefs = getMergeFileReferences(firstComponent, lastComponent);
        ILSMIOOperation mergeOp = createMergeOperation(opCtx, mergeFileRefs, callback);
        ioScheduler.scheduleOperation(TracedIOOperation.wrap(mergeOp, tracer));
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

    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint)
            throws HyracksDataException {
        ioOpCallback.beforeOperation(LSMIOOperationType.LOAD);
        return new LSMIndexDiskComponentBulkLoader(this, fillLevel, verifyInput, numElementsHint);
    }

    @Override
    public ILSMDiskComponent createBulkLoadTarget() throws HyracksDataException {
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        return createDiskComponent(bulkLoadComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getDeleteIndexFileReference(), componentFileRefs.getBloomFilterFileReference(), true);
    }

    protected ILSMDiskComponent createDiskComponent(ILSMDiskComponentFactory factory, FileReference insertFileReference,
            FileReference deleteIndexFileReference, FileReference bloomFilterFileRef, boolean createComponent)
            throws HyracksDataException {
        ILSMDiskComponent component = factory.createComponent(this,
                new LSMComponentFileReferences(insertFileReference, deleteIndexFileReference, bloomFilterFileRef));
        component.activate(createComponent);
        return component;
    }

    @Override
    public final synchronized void allocateMemoryComponents() throws HyracksDataException {
        if (!isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_ALLOCATE_MEMORY_FOR_INACTIVE_INDEX);
        }
        if (memoryComponentsAllocated || memoryComponents == null) {
            return;
        }
        for (ILSMMemoryComponent c : memoryComponents) {
            c.allocate();
            ioOpCallback.allocated(c);
        }
        memoryComponentsAllocated = true;
    }

    @Override
    public void addDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        if (c != EmptyComponent.INSTANCE) {
            diskComponents.add(0, c);
        }
    }

    @Override
    public void subsumeMergedComponents(ILSMDiskComponent newComponent, List<ILSMComponent> mergedComponents)
            throws HyracksDataException {
        int swapIndex = diskComponents.indexOf(mergedComponents.get(0));
        diskComponents.removeAll(mergedComponents);
        if (newComponent != EmptyComponent.INSTANCE) {
            diskComponents.add(swapIndex, newComponent);
        }
    }

    @Override
    public void changeMutableComponent() {
        currentMutableComponentId.set((currentMutableComponentId.get() + 1) % memoryComponents.size());
        memoryComponents.get(currentMutableComponentId.get()).requestActivation();
    }

    @Override
    public List<ILSMDiskComponent> getDiskComponents() {
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
    public final String toString() {
        return "{\"class\" : \"" + getClass().getSimpleName() + "\", \"dir\" : \"" + fileManager.getBaseDir()
                + "\", \"memory\" : " + (memoryComponents == null ? 0 : memoryComponents.size()) + ", \"disk\" : "
                + diskComponents.size() + "}";
    }

    @Override
    public final int getNumberOfAllMemoryComponents() {
        return virtualBufferCaches == null ? 0 : virtualBufferCaches.size();
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
        for (ILSMDiskComponent lsmComponent : lsmComponents) {
            componentFiles.addAll(lsmComponent.getLSMComponentPhysicalFiles());
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
                c.validate();
            }
        }
        for (ILSMDiskComponent c : diskComponents) {
            c.validate();
        }
    }

    @Override
    public long getMemoryAllocationSize() {
        long size = 0;
        for (ILSMMemoryComponent c : memoryComponents) {
            size += c.getSize();
        }
        return size;
    }

    @Override
    public final ILSMDiskComponent flush(ILSMIOOperation operation) throws HyracksDataException {
        ILSMIndexAccessor accessor = operation.getAccessor();
        ILSMIndexOperationContext opCtx = accessor.getOpContext();
        return opCtx.getOperation() == IndexOperation.DELETE_MEMORY_COMPONENT ? EmptyComponent.INSTANCE
                : doFlush(operation);
    }

    @Override
    public final ILSMDiskComponent merge(ILSMIOOperation operation) throws HyracksDataException {
        ILSMIndexAccessor accessor = operation.getAccessor();
        ILSMIndexOperationContext opCtx = accessor.getOpContext();
        return opCtx.getOperation() == IndexOperation.DELETE_DISK_COMPONENTS ? EmptyComponent.INSTANCE
                : doMerge(operation);
    }

    protected abstract LSMComponentFileReferences getMergeFileReferences(ILSMDiskComponent firstComponent,
            ILSMDiskComponent lastComponent) throws HyracksDataException;

    protected abstract AbstractLSMIndexOperationContext createOpContext(
            IModificationOperationCallback modificationCallback, ISearchOperationCallback searchCallback)
            throws HyracksDataException;

    protected abstract ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences componentFileRefs, ILSMIOOperationCallback callback) throws HyracksDataException;

    protected abstract ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences mergeFileRefs, ILSMIOOperationCallback callback) throws HyracksDataException;

    protected abstract ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException;

    protected abstract ILSMDiskComponent doMerge(ILSMIOOperation operation) throws HyracksDataException;

}
