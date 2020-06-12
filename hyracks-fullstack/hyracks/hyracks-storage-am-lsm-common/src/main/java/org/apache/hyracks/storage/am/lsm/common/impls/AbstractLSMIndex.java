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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId.IdCompareResult;
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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractLSMIndex implements ILSMIndex {
    private static final Logger LOGGER = LogManager.getLogger();
    protected final ILSMHarness lsmHarness;
    protected final IIOManager ioManager;
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
    protected final List<ILSMMemoryComponent> inactiveMemoryComponents;
    protected final double bloomFilterFalsePositiveRate;
    protected final IComponentFilterHelper filterHelper;
    protected final ILSMComponentFilterFrameFactory filterFrameFactory;
    protected final LSMComponentFilterManager filterManager;
    protected final int[] treeFields;
    protected final int[] filterFields;
    protected final boolean durable;
    protected boolean isActive;
    protected volatile boolean isDeactivating = false;
    protected final AtomicBoolean[] flushRequests;
    protected volatile boolean memoryComponentsAllocated = false;
    protected ITracer tracer;
    // Factory for creating on-disk index components during flush and merge.
    protected final ILSMDiskComponentFactory componentFactory;
    // Factory for creating on-disk index components during bulkload.
    protected final ILSMDiskComponentFactory bulkLoadComponentFactory;
    protected final ILSMPageWriteCallbackFactory pageWriteCallbackFactory;
    private int numScheduledFlushes = 0;

    public AbstractLSMIndex(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            IBufferCache diskBufferCache, ILSMIndexFileManager fileManager, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            ILSMDiskComponentFactory componentFactory, ILSMDiskComponentFactory bulkLoadComponentFactory,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            int[] filterFields, boolean durable, IComponentFilterHelper filterHelper, int[] treeFields, ITracer tracer)
            throws HyracksDataException {
        this.ioManager = ioManager;
        this.virtualBufferCaches = virtualBufferCaches;
        this.diskBufferCache = diskBufferCache;
        this.fileManager = fileManager;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.ioOpCallback = ioOpCallbackFactory.createIoOpCallback(this);
        this.pageWriteCallbackFactory = pageWriteCallbackFactory;
        this.componentFactory = componentFactory;
        this.bulkLoadComponentFactory = bulkLoadComponentFactory;
        this.filterHelper = filterHelper;
        this.filterFrameFactory = filterFrameFactory;
        this.filterManager = filterManager;
        this.treeFields = treeFields;
        this.filterFields = filterFields;
        this.inactiveDiskComponents = new ArrayList<>();
        this.inactiveMemoryComponents = new ArrayList<>();
        this.durable = durable;
        this.tracer = tracer;
        lsmHarness = new LSMHarness(this, ioScheduler, mergePolicy, opTracker, diskBufferCache.isReplicationEnabled(),
                tracer);
        isActive = false;
        diskComponents = new ArrayList<>();
        memoryComponents = new ArrayList<>();
        currentMutableComponentId = new AtomicInteger(ioOpCallbackFactory.getCurrentMemoryComponentIndex());
        flushRequests = new AtomicBoolean[virtualBufferCaches.size()];
        for (int i = 0; i < virtualBufferCaches.size(); i++) {
            flushRequests[i] = new AtomicBoolean();
        }
    }

    // The constructor used by external indexes
    public AbstractLSMIndex(IIOManager ioManager, IBufferCache diskBufferCache, ILSMIndexFileManager fileManager,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory bulkLoadComponentFactory, boolean durable, ITracer tracer)
            throws HyracksDataException {
        this.ioManager = ioManager;
        this.diskBufferCache = diskBufferCache;
        this.fileManager = fileManager;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.ioOpCallback = ioOpCallbackFactory.createIoOpCallback(this);
        this.pageWriteCallbackFactory = pageWriteCallbackFactory;
        this.componentFactory = componentFactory;
        this.bulkLoadComponentFactory = bulkLoadComponentFactory;
        this.durable = durable;
        this.tracer = tracer;
        lsmHarness = new ExternalIndexHarness(this, ioScheduler, mergePolicy, opTracker,
                diskBufferCache.isReplicationEnabled());
        isActive = false;
        diskComponents = new ArrayList<>();
        this.inactiveDiskComponents = new ArrayList<>();
        this.inactiveMemoryComponents = new ArrayList<>();
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

    @SuppressWarnings({ "squid:S1181", "squid:S2142" })
    @Override
    public synchronized void deactivate(boolean flush) throws HyracksDataException {
        if (!isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_DEACTIVATE_INACTIVE_INDEX);
        }
        // The following member is used to prevent scheduling of new merges as memory components
        // get flushed. This now works only if the caller of deactivate waited for all IO
        // operations to complete. Otherwise, disk components can be evicted while background
        // merges are ongoing.
        isDeactivating = true;
        try {
            LOGGER.log(Level.INFO, "Deactivating the index: {}. STARTED", this);
            if (flush && memoryComponentsAllocated) {
                try {
                    createAccessor(NoOpIndexAccessParameters.INSTANCE).scheduleFlush().sync();
                } catch (InterruptedException e) {
                    throw HyracksDataException.create(e);
                }
                LOGGER.log(Level.INFO, "Deactivating the index: {}. Flushed", this);
            }
            LOGGER.log(Level.INFO, "Deactivating the disk components of: {}", this);
            deactivateDiskComponents();
            LOGGER.log(Level.INFO, "Deallocating memory components of: {}", this);
            deallocateMemoryComponents();
            isActive = false;
            LOGGER.log(Level.INFO, "Deactivating the index: {}. COMPLETED", this);
        } finally {
            isDeactivating = false;
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
    public synchronized void destroy() throws HyracksDataException {
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
    public synchronized void clear() throws HyracksDataException {
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
                c.cleanup();
                c.reset();
            }
        }
        numScheduledFlushes = 0;
        currentMutableComponentId.set(0);
    }

    @Override
    public void purge() throws HyracksDataException {
    }

    @Override
    public void getOperationalComponents(ILSMIndexOperationContext ctx) throws HyracksDataException {
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        int cmc = currentMutableComponentId.get();
        ctx.setCurrentMutableComponentId(cmc);
        operationalComponents.clear();
        switch (ctx.getOperation()) {
            case UPDATE:
            case PHYSICALDELETE:
            case DELETE_COMPONENTS:
            case DELETE:
            case UPSERT:
                operationalComponents.add(memoryComponents.get(cmc));
                break;
            case INSERT:
                addOperationalMemoryComponents(operationalComponents, true);
                operationalComponents.addAll(diskComponents);
                break;
            case SEARCH:
                if (memoryComponentsAllocated) {
                    addOperationalMemoryComponents(operationalComponents, false);
                }
                if (filterManager != null) {
                    for (int i = 0; i < diskComponents.size(); i++) {
                        ILSMComponent c = diskComponents.get(i);
                        if (c.getLSMComponentFilter().satisfy(
                                ((AbstractSearchPredicate) ctx.getSearchPredicate()).getMinFilterTuple(),
                                ((AbstractSearchPredicate) ctx.getSearchPredicate()).getMaxFilterTuple(),
                                ctx.getFilterCmp())) {
                            operationalComponents.add(c);
                        }
                    }
                } else {
                    operationalComponents.addAll(diskComponents);
                }

                break;
            case REPLICATE:
                operationalComponents.addAll(ctx.getComponentsToBeReplicated());
                break;
            case DISK_COMPONENT_SCAN:
                operationalComponents.addAll(diskComponents);
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
    public ILSMIOOperation createFlushOperation(ILSMIndexOperationContext ctx) throws HyracksDataException {
        ILSMMemoryComponent flushingComponent = getCurrentMemoryComponent();
        if (flushingComponent.getWriterCount() > 0) {
            throw new IllegalStateException(
                    "createFlushOperation is called on a component with writers: " + flushingComponent);
        }
        // take care of the flush cycling
        ILSMIOOperation flushOp =
                TracedIOOperation.wrap(createFlushOperation(createOpContext(NoOpIndexAccessParameters.INSTANCE),
                        fileManager.getRelFlushFileReference(), ioOpCallback), tracer);
        // Changing the flush status should *always* precede changing the mutable component.
        flushingComponent.schedule(LSMIOOperationType.FLUSH);
        numScheduledFlushes++;
        changeFlushStatusForCurrentMutableCompoent(false);
        changeMutableComponent();
        ILSMIndexAccessor accessor = flushOp.getAccessor();
        ILSMIndexOperationContext flushCtx = accessor.getOpContext();
        flushCtx.setOperation(ctx.getOperation()); // Could be component delete
        flushCtx.getComponentHolder().add(flushingComponent);
        flushCtx.setIoOperation(flushOp);
        propagateMap(ctx, flushCtx);
        ioOpCallback.scheduled(flushOp);
        return flushOp;
    }

    @Override
    public ILSMIOOperation createMergeOperation(ILSMIndexOperationContext ctx) throws HyracksDataException {
        List<ILSMDiskComponent> mergingComponents = ctx.getComponentsToBeMerged();
        // Merge operation can fail if another merge is already scheduled on those components
        // This should be guarded against by the merge policy but we still protect against here
        if (isDeactivating
                || (mergingComponents.size() < 2 && ctx.getOperation() != IndexOperation.DELETE_COMPONENTS)) {
            return NoOpIoOperation.INSTANCE;
        }
        for (int i = 0; i < mergingComponents.size(); i++) {
            if (mergingComponents.get(i).getState() == ComponentState.READABLE_MERGING) {
                return NoOpIoOperation.INSTANCE;
            }
        }
        // merge must create a different op ctx
        AbstractLSMIndexOperationContext mergeCtx = createOpContext(NoOpIndexAccessParameters.INSTANCE);
        mergeCtx.setOperation(ctx.getOperation());
        mergeCtx.getComponentHolder().addAll(mergingComponents);
        propagateMap(ctx, mergeCtx);
        mergingComponents.stream().forEach(mergeCtx.getComponentsToBeMerged()::add);
        ILSMDiskComponent lastComponent = mergingComponents.get(0);
        ILSMDiskComponent firstComponent = mergingComponents.get(mergingComponents.size() - 1);
        LSMComponentFileReferences mergeFileRefs = getMergeFileReferences(firstComponent, lastComponent);
        ILSMIOOperation mergeOp =
                TracedIOOperation.wrap(createMergeOperation(mergeCtx, mergeFileRefs, ioOpCallback), tracer);
        mergeCtx.setIoOperation(mergeOp);
        for (int i = 0; i < mergingComponents.size(); i++) {
            mergingComponents.get(i).schedule(LSMIOOperationType.MERGE);
        }
        ioOpCallback.scheduled(mergeOp);
        return mergeOp;
    }

    private static void propagateMap(ILSMIndexOperationContext src, ILSMIndexOperationContext destination) {
        Map<String, Object> map = src.getParameters();
        if (map != null && !map.isEmpty()) {
            destination.setParameters(new HashMap<>(map));
        }
    }

    private void addOperationalMemoryComponents(List<ILSMComponent> operationalComponents, boolean modification) {
        // add current memory component first if needed
        if (numScheduledFlushes < memoryComponents.size()) {
            ILSMMemoryComponent c = memoryComponents.get(currentMutableComponentId.get());
            // The current mutable component is added if modification or readable
            // This ensures that activation of new component only happens in case of modifications
            // and allow for controlling that without stopping search operations
            if (modification || c.isReadable()) {
                operationalComponents.add(c);
            }
        }
        if (modification && numScheduledFlushes >= memoryComponents.size()) {
            // will fail the enterComponent call and retry
            operationalComponents.add(memoryComponents.get(0));
            return;
        }
        addImmutableMemoryComponents(operationalComponents);
    }

    private void addImmutableMemoryComponents(List<ILSMComponent> operationalComponents) {
        int cmc = currentMutableComponentId.get();
        int numImmutableMemoryComponents = Integer.min(numScheduledFlushes, memoryComponents.size());
        int next = numScheduledFlushes < memoryComponents.size() ? cmc : getNextToBeFlushed();
        for (int i = 0; i < numImmutableMemoryComponents; i++) {
            next--;
            if (next < 0) {
                next = memoryComponents.size() - 1;
            }
            //newer components first
            ILSMMemoryComponent c = memoryComponents.get(next);
            if (c.isReadable()) {
                operationalComponents.add(c);
            }
        }
    }

    private ILSMMemoryComponent getOldestReadableMemoryComponent() {
        synchronized (getOperationTracker()) {
            int cmc = currentMutableComponentId.get();
            int numImmutableMemoryComponents = Integer.min(numScheduledFlushes, memoryComponents.size());
            int next = numScheduledFlushes < memoryComponents.size() ? cmc : getNextToBeFlushed();
            for (int i = 0; i < numImmutableMemoryComponents; i++) {
                next--;
                if (next < 0) {
                    next = memoryComponents.size() - 1;
                }
            }

            // start going forward
            for (int i = 0; i < numImmutableMemoryComponents; i++) {
                if (memoryComponents.get(next).isReadable()) {
                    return memoryComponents.get(next);
                }
                next++;
                if (next == memoryComponents.size()) {
                    next = 0;
                }
            }
            throw new IllegalStateException("Couldn't find any readable component");
        }
    }

    private int getNextToBeFlushed() {
        // we have:
        // 1. currentMemeoryComponent
        // 2. numMemoryComponents
        // 3. numScheduledFlushes
        int diff = numScheduledFlushes % memoryComponents.size();
        int cmc = currentMutableComponentId.get() - diff;
        return cmc < 0 ? memoryComponents.size() + cmc : cmc;
    }

    @Override
    public final IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, IPageWriteCallback callback) throws HyracksDataException {
        return createBulkLoader(fillLevel, verifyInput, numElementsHint, checkIfEmptyIndex, Collections.emptyMap());
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, Map<String, Object> parameters) throws HyracksDataException {
        if (checkIfEmptyIndex && !isEmptyIndex()) {
            throw HyracksDataException.create(ErrorCode.LOAD_NON_EMPTY_INDEX);
        }
        return createBulkLoader(fillFactor, verifyInput, numElementsHint, parameters);
    }

    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            Map<String, Object> parameters) throws HyracksDataException {
        AbstractLSMIndexOperationContext opCtx = createOpContext(NoOpIndexAccessParameters.INSTANCE);
        opCtx.setParameters(parameters);
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        LoadOperation loadOp = new LoadOperation(componentFileRefs, ioOpCallback, getIndexIdentifier(), parameters);
        loadOp.setNewComponent(createDiskComponent(bulkLoadComponentFactory,
                componentFileRefs.getInsertIndexFileReference(), componentFileRefs.getDeleteIndexFileReference(),
                componentFileRefs.getBloomFilterFileReference(), true));
        ioOpCallback.scheduled(loadOp);
        opCtx.setIoOperation(loadOp);
        return new LSMIndexDiskComponentBulkLoader(this, opCtx, fillLevel, verifyInput, numElementsHint);
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
    public synchronized void allocateMemoryComponents() throws HyracksDataException {
        if (!isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_ALLOCATE_MEMORY_FOR_INACTIVE_INDEX);
        }
        if (memoryComponentsAllocated || memoryComponents == null) {
            return;
        }
        int i = 0;
        boolean allocated = false;
        try {
            for (; i < memoryComponents.size(); i++) {
                allocated = false;
                ILSMMemoryComponent c = memoryComponents.get(i);
                c.allocate();
                allocated = true;
                ioOpCallback.allocated(c);
            }
        } finally {
            if (i < memoryComponents.size()) {
                // something went wrong
                if (allocated) {
                    ILSMMemoryComponent c = memoryComponents.get(i);
                    c.deallocate();
                }
                // deallocate all previous components
                for (int j = i - 1; j >= 0; j--) {
                    ILSMMemoryComponent c = memoryComponents.get(j);
                    c.deallocate();
                }
            }
        }
        memoryComponentsAllocated = true;
    }

    @Override
    public void addDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        if (c != EmptyComponent.INSTANCE) {
            diskComponents.add(0, c);
        }
        validateComponentIds();
    }

    @Override
    public void subsumeMergedComponents(ILSMDiskComponent newComponent, List<ILSMComponent> mergedComponents)
            throws HyracksDataException {
        int swapIndex = diskComponents.indexOf(mergedComponents.get(0));
        diskComponents.removeAll(mergedComponents);
        if (newComponent != EmptyComponent.INSTANCE) {
            diskComponents.add(swapIndex, newComponent);
        }
        validateComponentIds();
    }

    /**
     * A helper method to ensure disk components have proper Ids (non-decreasing)
     * We may get rid of this method once component Id is stablized
     *
     * @throws HyracksDataException
     */
    private void validateComponentIds() throws HyracksDataException {
        for (int i = 0; i < diskComponents.size() - 1; i++) {
            ILSMComponentId id1 = diskComponents.get(i).getId();
            ILSMComponentId id2 = diskComponents.get(i + 1).getId();
            IdCompareResult cmp = id1.compareTo(id2);
            if (cmp != IdCompareResult.UNKNOWN && cmp != IdCompareResult.GREATER_THAN) {
                throw new IllegalStateException(
                        "found non-decreasing component ids (" + id1 + " -> " + id2 + ") on index " + this);
            }
        }
    }

    @Override
    public void changeMutableComponent() {
        currentMutableComponentId.set((currentMutableComponentId.get() + 1) % memoryComponents.size());
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
                + "\", \"memory\" : " + (memoryComponents == null ? 0 : memoryComponents) + ", \"disk\" : "
                + diskComponents.size() + ", \"num-scheduled-flushes\":" + numScheduledFlushes
                + ", \"current-memory-component\":" + currentMutableComponentId.get() + "}";
    }

    @Override
    public final int getNumberOfAllMemoryComponents() {
        return virtualBufferCaches == null ? 0 : virtualBufferCaches.size();
    }

    @Override
    public boolean isCurrentMutableComponentEmpty() throws HyracksDataException {
        synchronized (getOperationTracker()) {
            ILSMMemoryComponent cmc = getCurrentMemoryComponent();
            ComponentState state = cmc.getState();
            return state == ComponentState.READABLE_UNWRITABLE_FLUSHING || state == ComponentState.INACTIVE
                    || state == ComponentState.UNREADABLE_UNWRITABLE || !cmc.isModified();
        }
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
    public List<ILSMMemoryComponent> getInactiveMemoryComponents() {
        return inactiveMemoryComponents;
    }

    @Override
    public void addInactiveMemoryComponent(ILSMMemoryComponent memoryComponent) {
        inactiveMemoryComponents.add(memoryComponent);
    }

    @Override
    public void scheduleReplication(ILSMIndexOperationContext ctx, List<ILSMDiskComponent> lsmComponents,
            ReplicationOperation operation, LSMOperationType opType) throws HyracksDataException {
        //get set of files to be replicated for this component
        Set<String> componentFiles = new HashSet<>();

        //get set of files to be replicated for each component
        for (ILSMDiskComponent lsmComponent : lsmComponents) {
            componentFiles.addAll(lsmComponent.getLSMComponentPhysicalFiles());
        }

        ReplicationExecutionType executionType;
        if (opType == LSMOperationType.LOAD) {
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
        if (ctx.getFilterTuple() != null && !ctx.isFilterSkipped()) {
            if (ctx.isRecovery()) {
                memoryComponents.get(currentMutableComponentId.get()).getLSMComponentFilter().update(tuple,
                        ctx.getFilterCmp(), ctx.getModificationCallback());
            } else {
                ctx.getFilterTuple().reset(tuple);
                memoryComponents.get(currentMutableComponentId.get()).getLSMComponentFilter()
                        .update(ctx.getFilterTuple(), ctx.getFilterCmp(), ctx.getModificationCallback());
            }
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

    @Override
    public ILSMHarness getHarness() {
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
    public void resetCurrentComponentIndex() {
        synchronized (lsmHarness.getOperationTracker()) {
            // validate no reader in any of the memory components and that all of them are INVALID
            for (ILSMMemoryComponent c : memoryComponents) {
                if (c.getReaderCount() > 0) {
                    throw new IllegalStateException(
                            "Attempt to reset current component index while readers are inside the components. " + c);
                }
                if (c.getState() != ComponentState.INACTIVE) {
                    throw new IllegalStateException(
                            "Attempt to reset current component index while a component is not INACTIVE. " + c);
                }
            }
            currentMutableComponentId.set(0);
            memoryComponents.get(0);
            try {
                memoryComponents.get(0).resetId(null, true);
            } catch (HyracksDataException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public final ILSMDiskComponent flush(ILSMIOOperation operation) throws HyracksDataException {
        ILSMIndexAccessor accessor = operation.getAccessor();
        ILSMIndexOperationContext opCtx = accessor.getOpContext();
        ILSMMemoryComponent memoryComponent = (ILSMMemoryComponent) opCtx.getComponentHolder().get(0);
        if (memoryComponent != getOldestReadableMemoryComponent()) {
            throw new IllegalStateException("An attempt to flush a memory component that is not the oldest");
        }
        if (!memoryComponent.isModified() || opCtx.getOperation() == IndexOperation.DELETE_COMPONENTS) {
            return EmptyComponent.INSTANCE;
        }
        if (LOGGER.isInfoEnabled()) {
            FlushOperation flushOp = (FlushOperation) operation;
            LOGGER.log(Level.INFO,
                    "Flushing component with id: " + flushOp.getFlushingComponent().getId() + " in the index " + this);
        }
        return doFlush(operation);
    }

    @Override
    public final ILSMDiskComponent merge(ILSMIOOperation operation) throws HyracksDataException {
        ILSMIndexAccessor accessor = operation.getAccessor();
        ILSMIndexOperationContext opCtx = accessor.getOpContext();
        return opCtx.getOperation() == IndexOperation.DELETE_COMPONENTS ? EmptyComponent.INSTANCE : doMerge(operation);
    }

    @Override
    public String getIndexIdentifier() {
        return fileManager.getBaseDir().getAbsolutePath();
    }

    //Called when a memory component is reset
    public void memoryComponentsReset() {
        numScheduledFlushes = Integer.max(0, numScheduledFlushes - 1);
    }

    protected abstract LSMComponentFileReferences getMergeFileReferences(ILSMDiskComponent firstComponent,
            ILSMDiskComponent lastComponent) throws HyracksDataException;

    protected abstract AbstractLSMIndexOperationContext createOpContext(IIndexAccessParameters iap)
            throws HyracksDataException;

    protected abstract ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences componentFileRefs, ILSMIOOperationCallback callback) throws HyracksDataException;

    protected abstract ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences mergeFileRefs, ILSMIOOperationCallback callback) throws HyracksDataException;

    protected abstract ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException;

    protected abstract ILSMDiskComponent doMerge(ILSMIOOperation operation) throws HyracksDataException;

    public Optional<Long> getLatestDiskComponentSequence() {
        if (diskComponents.isEmpty()) {
            return Optional.empty();
        }
        final ILSMDiskComponent latestDiskComponent = diskComponents.get(0);
        final Set<String> diskComponentPhysicalFiles = latestDiskComponent.getLSMComponentPhysicalFiles();
        final String fileName = diskComponentPhysicalFiles.stream().findAny()
                .orElseThrow(() -> new IllegalStateException("Disk component without any physical files"));
        return Optional
                .of(IndexComponentFileReference.of(Paths.get(fileName).getFileName().toString()).getSequenceEnd());
    }

    public ILSMPageWriteCallbackFactory getPageWriteCallbackFactory() {
        return pageWriteCallbackFactory;
    }

}
