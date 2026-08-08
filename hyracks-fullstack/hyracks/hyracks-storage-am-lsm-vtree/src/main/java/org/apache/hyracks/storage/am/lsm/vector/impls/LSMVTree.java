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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor.ICursorFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMVTreeComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LoadOperation;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleBuilderFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunctionFactory;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.impls.VTreeFlushLoader;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchPredicate;
import org.apache.hyracks.storage.am.vector.utils.CrossPollinationConfig;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * LSM Vector Clustering Tree. Uses {@link VTree} as both in-memory components and disk components.
 * The static structure (root + interior + leaf navigation tree) is built once at CREATE INDEX time
 * and stored in its own {@link LSMVTreeDiskComponent}; memory components navigate via a shared
 * accessor onto that static-structure component, while each flushed/bulk-loaded disk component
 * carries its own copy of the navigation pages. The per-cluster metadata ("directory") pages and
 * the data pages are always component-local.
 */
public class LSMVTree extends AbstractLSMIndex implements ITreeIndex {
    private static final Logger LOGGER = LogManager.getLogger();

    private static final ICursorFactory cursorFactory = LSMVTreeSearchCursor::new;

    protected final ITreeIndexFrameFactory interiorFrameFactory;
    protected final ITreeIndexFrameFactory leafFrameFactory;
    protected final ITreeIndexFrameFactory metadataFrameFactory;
    protected final ITreeIndexFrameFactory insertDataFrameFactory;
    protected final ITreeIndexFrameFactory deleteDataFrameFactory;

    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final int vectorDimensions;
    protected final IVTreeBinaryAccessorFactory vectorAccessorFactory;

    // Data tuple format depends on quantization (see VTreeDataTupleBuilder):
    //   non-quantized: [distance, centroidId, primary_keys..., include_fields...]   (pkStartField=2)
    //   quantized:     [distance, centroidId, quantized_distance, quantized_embedding,
    //                   primary_keys..., include_fields...]                          (pkStartField=4)
    // Quantization is enforced at index creation in this build, so the quantized form is the default.
    protected final int numPrimaryKeyFields;
    protected final int numIncludeFields;
    protected final IVTreeDataTupleBuilderFactory dataTupleBuilderFactory;

    // Raw quantization params for lazy quantizer creation at query time (null = non-quantized path)
    protected final VTreeQuantizationParams quantizationParams;
    protected final IVTreeDistanceFunctionFactory distanceFunctionFactory;
    protected final CrossPollinationConfig crossPollination;

    // volatile: written under synchronized setStaticStructure/activation but read unsynchronized on the
    // search/flush path; the volatile publishes the reference safely to those readers.
    protected volatile LSMVTreeDiskComponent staticStructure;

    public LSMVTree(NCConfig storageConfig, IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            ITreeIndexFrameFactory metadataFrameFactory, ITreeIndexFrameFactory insertDataFrameFactory,
            ITreeIndexFrameFactory deleteDataFrameFactory, IBufferCache diskBufferCache,
            ILSMIndexFileManager fileManager, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory bulkLoadComponentFactory, IComponentFilterHelper filterHelper,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            double bloomFilterFalsePositiveRate, IBinaryComparatorFactory[] cmpFactories, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            int vectorDimensions, int[] vectorFields, int[] filterFields, boolean durable, boolean atomic,
            IVTreeBinaryAccessorFactory vectorAccessorFactory, int numPrimaryKeyFields, int numIncludeFields,
            IVTreeDataTupleBuilderFactory dataTupleBuilderFactory, VTreeQuantizationParams quantizationParams,
            IVTreeDistanceFunctionFactory distanceFunctionFactory, CrossPollinationConfig crossPollination)
            throws HyracksDataException {

        super(storageConfig, ioManager, virtualBufferCaches, diskBufferCache, fileManager, bloomFilterFalsePositiveRate,
                mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, componentFactory,
                bulkLoadComponentFactory, filterFrameFactory, filterManager, filterFields, durable, filterHelper,
                vectorFields, ITracer.NONE, atomic);
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.metadataFrameFactory = metadataFrameFactory;
        this.insertDataFrameFactory = insertDataFrameFactory;
        this.deleteDataFrameFactory = deleteDataFrameFactory;
        this.cmpFactories = cmpFactories;
        this.vectorDimensions = vectorDimensions;
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.numPrimaryKeyFields = numPrimaryKeyFields;
        this.numIncludeFields = numIncludeFields;
        this.dataTupleBuilderFactory = dataTupleBuilderFactory;
        this.quantizationParams = quantizationParams;
        this.distanceFunctionFactory = distanceFunctionFactory;
        this.crossPollination = crossPollination != null ? crossPollination : CrossPollinationConfig.LEGACY;

        int i = 0;
        for (IVirtualBufferCache virtualBufferCache : virtualBufferCaches) {
            String baseDirPath = fileManager.getBaseDir() + "_virtual_" + i;
            FileReference virtualFileRef = ioManager.resolve(baseDirPath);
            // Memory components use insertDataFrameFactory for normal inserts
            VTree vTree = new VTree(virtualBufferCache, new VirtualFreePageManager(virtualBufferCache),
                    interiorFrameFactory, leafFrameFactory, metadataFrameFactory, insertDataFrameFactory, cmpFactories,
                    1, vectorDimensions, virtualFileRef, vectorAccessorFactory, dataTupleBuilderFactory,
                    quantizationParams, this.distanceFunctionFactory, this.crossPollination);
            LSMVTreeMemoryComponent mutableComponent = new LSMVTreeMemoryComponent(this, vTree, virtualBufferCache,
                    filterHelper == null ? null : filterHelper.createFilter());
            memoryComponents.add(mutableComponent);
            i++;
        }
    }

    public void setStaticStructure(LSMVTreeDiskComponent staticStructure) throws HyracksDataException {
        this.staticStructure = staticStructure;
        staticStructure.setInitialized();
        if (memoryComponentsAllocated) {
            initializeMemoryComponentsStaticStructure();
        }
    }

    @Override
    public synchronized void allocateMemoryComponents() throws HyracksDataException {
        super.allocateMemoryComponents();
        initializeMemoryComponentsStaticStructure();
    }

    private void initializeMemoryComponentsStaticStructure() throws HyracksDataException {
        if (staticStructure == null) {
            return; // Not yet loaded (during initial index creation)
        }
        for (ILSMMemoryComponent memComponent : memoryComponents) {
            reinitializeMemoryComponent((LSMVTreeMemoryComponent) memComponent);
        }
    }

    /**
     * Re-initialize a single memory component's static structure directory pages.
     * Called during initial setup and after a memory component is recycled post-flush.
     */
    void reinitializeMemoryComponent(LSMVTreeMemoryComponent memComponent) throws HyracksDataException {
        if (staticStructure == null) {
            return;
        }
        VTree vTree = memComponent.getIndex();
        if (!vTree.isInitialized()) {
            VTree.VTreeAccessor staticAccessor =
                    (VTree.VTreeAccessor) staticStructure.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
            try {
                vTree.setStaticStructure(staticAccessor);
            } finally {
                // setStaticStructure only reads from the accessor; destroy it so its op-context (frames +
                // any pinned static metadata page) is not leaked on every memory-component recycle.
                staticAccessor.destroy();
            }
        }
    }

    protected ILSMDiskComponent createStaticStructure(ILSMDiskComponentFactory factory,
            FileReference staticStructureFileReference, boolean createComponent) throws HyracksDataException {
        ILSMDiskComponent component =
                factory.createComponent(this, new LSMComponentFileReferences(staticStructureFileReference, null, null));
        try {
            ((LSMVTreeDiskComponent) component).setStaticStructure(true);
            component.activate(createComponent);
        } catch (HyracksDataException e) {
            component.returnPages();
            throw e;
        }
        return component;
    }

    @Override
    public void addBulkLoadedDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        LSMVTreeDiskComponent vTreeDiskComponent = (LSMVTreeDiskComponent) c;
        if (vTreeDiskComponent.isStaticStructure()) {
            setStaticStructure(vTreeDiskComponent);
            return;
        }
        vTreeDiskComponent.setInitialized();
        diskComponents.addFirst(c);
        validateComponentIds();
    }

    public LSMVTreeDiskComponent getStaticStructure() {
        if (staticStructure == null) {
            throw new IllegalStateException("Static structure must be built before loading records");
        }
        return staticStructure;
    }

    /**
     * Creates a bulk loader for either static-structure creation or data loading, depending on
     * whether structure-building parameters are present.
     */
    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            Map<String, Object> parameters) throws HyracksDataException {
        if (parameters == null) {
            // Both paths dereference parameters (data load also writes the static-structure component into
            // it); fail with a clear message rather than NPEing below.
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "createBulkLoader requires a non-null parameters map (static-structure build params, or "
                            + "a map to receive the static-structure component for data load)");
        }
        AbstractLSMIndexOperationContext opCtx = createOpContext(NoOpIndexAccessParameters.INSTANCE);
        boolean isStaticStructureLoad = LSMVTreeDiskComponent.isStaticStructureLoad(parameters);
        if (!isStaticStructureLoad) {
            // For data loading, ensure static structure is already built
            parameters.put(LSMVTreeDiskComponent.PARAM_STATIC_STRUCTURE_COMPONENT, getStaticStructure());
        }
        opCtx.setParameters(parameters);
        LSMVTreeComponentFileReferences componentFileRefs =
                (LSMVTreeComponentFileReferences) fileManager.getRelFlushFileReference();
        LoadOperation loadOp = new LoadOperation(componentFileRefs, ioOpCallback, getIndexIdentifier(), parameters);

        ILSMDiskComponent component;
        if (isStaticStructureLoad) {
            component = createStaticStructure(bulkLoadComponentFactory,
                    componentFileRefs.getStaticStructureFileReference(), true);
        } else {
            component = createDiskComponent(bulkLoadComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                    null, null, true);
        }

        loadOp.setNewComponent(component);
        ioOpCallback.scheduled(loadOp);
        opCtx.setIoOperation(loadOp);
        return new LSMIndexDiskComponentBulkLoader(storageConfig, this, opCtx, fillFactor, verifyInput,
                numElementsHint);
    }

    @Override
    public boolean isPrimaryIndex() {
        // VTree is always a secondary index (there is no vector primary index), matching
        // LSMRTree/LSMInvertedIndex.
        return false;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    public int getVectorDimensions() {
        return vectorDimensions;
    }

    /**
     * Whether this index stores data tuples in the quantized layout (pkStartField=4). Production
     * quantized indexes carry non-null {@code quantizationParams}; test fixtures may select the
     * quantized layout purely through the data-tuple-creator factory.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    public boolean isQuantized() {
        return quantizationParams != null || dataTupleBuilderFactory.isQuantized();
    }

    public int getNumPrimaryKeyFields() {
        return numPrimaryKeyFields;
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
        LSMVTreeOpContext ctx = (LSMVTreeOpContext) ictx;

        ITupleReference indexTuple;
        if (ctx.getIndexTuple() != null) {
            ctx.getIndexTuple().reset(tuple);
            indexTuple = ctx.getIndexTuple();
        } else {
            indexTuple = tuple;
        }

        switch (ctx.getOperation()) {
            case PHYSICALDELETE:
                ctx.getCurrentMutableVTreeAccessor().delete(indexTuple);
                break;
            case INSERT:
                insert(indexTuple, ctx);
                break;
            case DELETE:
                delete(indexTuple, ctx);
                break;
            default:
                ctx.getCurrentMutableVTreeAccessor().upsert(indexTuple);
                break;
        }
        updateFilter(ctx, tuple);
    }

    private void insert(ITupleReference tuple, LSMVTreeOpContext ctx) throws HyracksDataException {
        ctx.getCurrentMutableVTreeAccessor().insert(tuple);
    }

    private void delete(ITupleReference tuple, LSMVTreeOpContext ctx) throws HyracksDataException {
        ctx.getCurrentMutableVTreeAccessor().delete(tuple);
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException {
        LSMVTreeOpContext ctx = (LSMVTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        ctx.getSearchInitialState().reset(pred, operationalComponents);
        cursor.open(ctx.getSearchInitialState(), pred);
    }

    @Override
    public void scanDiskComponents(ILSMIndexOperationContext ictx, IIndexCursor cursor) {
        // Vector clustering trees don't support disk-component scanning the way BTrees do.
        throw new UnsupportedOperationException("Disk component scanning not supported for vector clustering trees");
    }

    @Override
    public ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException {
        LSMVTreeFlushOperation flushOp = (LSMVTreeFlushOperation) operation;
        LSMVTreeMemoryComponent flushingComponent = (LSMVTreeMemoryComponent) flushOp.getFlushingComponent();
        VTree memTree = flushingComponent.getIndex();

        ILSMDiskComponent component;
        VTreeFlushLoader flushLoader = null;
        // Transient accessors used only for this flush; each holds an op-context (frames + possibly a
        // pinned static metadata page) and MUST be destroyed, else every flush leaks one.
        VTree.VTreeAccessor vTreeAccessor = null;
        VTree.VTreeAccessor staticAccessor = null;

        try {
            component = createDiskComponent(componentFactory, flushOp.getTarget(), null, null, true);
            VTree diskTree = ((LSMVTreeDiskComponent) component).getIndex();

            // Create flush loader (new signature: callback, diskTree, sourceMemoryTree)
            IPageWriteCallback callback = pageWriteCallbackFactory.createPageWriteCallback();
            flushLoader = new VTreeFlushLoader(callback, diskTree, memTree);
            callback.initialize(flushLoader);

            // Copy ALL VBC pages (identity mapping: VBC page N -> disk page N)
            vTreeAccessor = (VTree.VTreeAccessor) memTree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            ITreeIndexMetadataFrame componentMetaFrame = vTreeAccessor.getOpContext().getMetaFrame();
            int maxPageId = memTree.getPageManager().getMaxPageId(componentMetaFrame);

            for (int pageId = 0; pageId <= maxPageId; pageId++) {
                ICachedPage sourcePage = vTreeAccessor.getCachedPage(pageId);
                try {
                    flushLoader.copyPage(sourcePage);
                } finally {
                    // Release the pin even if copyPage throws (I/O, compression, disk-full); otherwise a
                    // failed flush permanently leaks a buffer-cache pin and progressively starves the cache.
                    vTreeAccessor.releasePage(sourcePage);
                }
            }

            // Append static structure pages at end of file. Route through getStaticStructure() so a flush
            // scheduled before the static structure is built fails with a clear message, not an NPE.
            staticAccessor = (VTree.VTreeAccessor) getStaticStructure().getIndex()
                    .createAccessor(NoOpIndexAccessParameters.INSTANCE);
            int rootPageId = flushLoader.copyStaticStructure(staticAccessor);

            // Finalize with correct metadata
            flushLoader.end(memTree.getNumLeafCentroidMem(), memTree.getFirstLeafCentroidIdMem(), rootPageId);

        } catch (Throwable e) {
            try {
                if (flushLoader != null) {
                    flushLoader.abort();
                }
            } catch (Throwable th) {
                e.addSuppressed(th);
            }
            throw e;
        } finally {
            // Best-effort: destroy both transient accessors without masking a primary failure.
            Throwable failure = CleanupUtils.destroy(null, vTreeAccessor, staticAccessor);
            if (failure != null) {
                LOGGER.warn("Failed to destroy transient flush accessors", failure);
            }
        }
        return component;
    }

    @Override
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    public ILSMDiskComponent doMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMVTreeMergeOperation mergeOp = (LSMVTreeMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        ILSMDiskComponent mergedComponent;
        ILSMDiskComponentBulkLoader componentBulkLoader = null;

        try {
            try {
                // Full-scan predicate: epsilon=0 disables level-wise selection. The cursor was
                // created in full-scan mode by createMergeOperation(), so it uses
                // SequentialClusterSelectionStrategy to iterate clusters in order.
                VTreeSearchPredicate mergePred = new VTreeSearchPredicate();
                mergePred.setEpsilon(0.0);
                // The merge cursor (LSMVTreeSearchCursor) derives pkStartField from the index's
                // isQuantized flag, so its reconciliation key is <distance (field 0), PK...> — quantized
                // layouts correctly skip the quantized_distance/quantized_embedding fields. (Getting this
                // wrong would pull field 2, whose write semantics differ between bulk load and DML, into
                // the key and break matter/antimatter cancellation during COMPACT.)

                // Cursor was already created in full-scan mode by createMergeOperation()
                search(mergeOp.getAccessor().getOpContext(), cursor, mergePred);

                try {
                    mergedComponent = createDiskComponent(componentFactory, mergeOp.getTarget(), null, null, true);

                    // Static structure reference is required by VTreeBulkLoader
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put(LSMVTreeDiskComponent.PARAM_STATIC_STRUCTURE_COMPONENT, getStaticStructure());
                    mergeOp.getAccessor().getOpContext().setParameters(parameters);

                    IPageWriteCallback pageWriteCallback = pageWriteCallbackFactory.createPageWriteCallback();
                    // numElementsHint = 0 (unknown): the VTree merge loader does not consume it, and the
                    // former value (sum of component byte sizes) was the wrong unit — a tuple count, not bytes.
                    componentBulkLoader = mergedComponent.createBulkLoader(storageConfig, operation, 1.0f, false, 0L,
                            false, false, false, pageWriteCallback);

                    // Cursor delivers tuples cluster-by-cluster, already antimatter-reconciled
                    while (cursor.hasNext()) {
                        cursor.next();
                        ITupleReference frameTuple = cursor.getTuple();
                        componentBulkLoader.add(frameTuple);
                    }
                } finally {
                    cursor.close();
                }
            } finally {
                cursor.destroy();
            }

            componentBulkLoader.end();
        } catch (Throwable e) { // NOSONAR.. As per the contract, we should either abort or end
            try {
                if (componentBulkLoader != null) {
                    componentBulkLoader.abort();
                }
            } catch (Throwable th) { // NOSONAR Don't lose the root failure
                e.addSuppressed(th);
            }
            throw e;
        }

        return mergedComponent;
    }

    @Override
    protected ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences componentFileRefs, ILSMIOOperationCallback callback) {
        return new LSMVTreeFlushOperation(createAccessor(opCtx), componentFileRefs.getInsertIndexFileReference(),
                callback, getIndexIdentifier());
    }

    @Override
    public LSMVTreeOpContext createOpContext(IIndexAccessParameters iap) {
        return new LSMVTreeOpContext(this, memoryComponents, getTreeFields(), getFilterFields(),
                getFilterCmpFactories(), (IExtendedModificationOperationCallback) iap.getModificationCallback(),
                iap.getSearchOperationCallback(), tracer, iap);
    }

    @Override
    public ILSMIndexAccessor createAccessor(IIndexAccessParameters iap) {
        return createAccessor(createOpContext(iap));
    }

    public ILSMIndexAccessor createAccessor(AbstractLSMIndexOperationContext opCtx) {
        return new LSMVTreeIndexAccessor(getHarness(), opCtx, getCursorFactory(), this);
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        LSMVTreeMemoryComponent mutableComponent =
                (LSMVTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getInteriorFrameFactory();
    }

    public ITreeIndexFrameFactory getMetadataFrameFactory() {
        return metadataFrameFactory;
    }

    public ITreeIndexFrameFactory getInsertDataFrameFactory() {
        return insertDataFrameFactory;
    }

    public ITreeIndexFrameFactory getDeleteDataFrameFactory() {
        return deleteDataFrameFactory;
    }

    public IBinaryComparatorFactory[] getCmpFactories() {
        return cmpFactories;
    }

    @Override
    public int getFieldCount() {
        LSMVTreeMemoryComponent mutableComponent =
                (LSMVTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getFieldCount();
    }

    @Override
    public int getFileId() {
        LSMVTreeMemoryComponent mutableComponent =
                (LSMVTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getFileId();
    }

    @Override
    public IPageManager getPageManager() {
        LSMVTreeMemoryComponent mutableComponent =
                (LSMVTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getPageManager();
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        LSMVTreeMemoryComponent mutableComponent =
                (LSMVTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getLeafFrameFactory();
    }

    @Override
    public int getRootPageId() {
        LSMVTreeMemoryComponent mutableComponent =
                (LSMVTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getRootPageId();
    }

    @Override
    protected LSMComponentFileReferences getMergeFileReferences(ILSMDiskComponent firstComponent,
            ILSMDiskComponent lastComponent) throws HyracksDataException {
        // Extract file names from components (e.g., "0_0" from file "0_0_vct")
        LSMVTreeDiskComponent first = (LSMVTreeDiskComponent) firstComponent;
        LSMVTreeDiskComponent last = (LSMVTreeDiskComponent) lastComponent;

        String firstName = first.getIndex().getFileReference().getFile().getName();
        String lastName = last.getIndex().getFileReference().getFile().getName();

        return fileManager.getRelMergeFileReference(firstName, lastName);
    }

    @Override
    protected ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences mergeFileRefs, ILSMIOOperationCallback callback) {

        // If we're merging the oldest disk component (i.e. there's nothing older that could still
        // need anti-matter tuples for reconciliation), antimatter tuples can be discarded during the
        // merge; otherwise they must be preserved.
        List<ILSMComponent> mergingComponents = opCtx.getComponentHolder();
        boolean returnDeletedTuples = true;
        if (!diskComponents.isEmpty() && !mergingComponents.isEmpty()
                && mergingComponents.getLast() == diskComponents.getLast()) {
            returnDeletedTuples = false;
        }

        // Create cursor with full-scan mode enabled for merge
        IIndexCursorStats stats = NoOpIndexCursorStats.INSTANCE;
        ILSMIndexAccessor accessor = createAccessor(opCtx);

        // Create LSMVTreeSearchCursor in full-scan mode for merge operations
        // fullScanMode=true enables sequential cluster iteration (0→1→2→...)
        // returnDeletedTuples=true ensures antimatter tuples are visible for reconciliation
        IIndexCursor cursor = new LSMVTreeSearchCursor(opCtx, returnDeletedTuples, true, stats);

        return new LSMVTreeMergeOperation(accessor, cursor, stats, mergeFileRefs.getInsertIndexFileReference(),
                callback, getIndexIdentifier());
    }

    protected ICursorFactory getCursorFactory() {
        return cursorFactory;
    }

    /**
     * Creates the top-K search cursor: all search work happens in {@code open()}, results
     * are collected into a quantized-distance-keyed top-K window, and the cursor drains that
     * window on subsequent {@code hasNext()/next()/getTuple()} calls. This is the canonical
     * search cursor for the VTree index; the streaming {@link LSMVTreeSearchCursor} is reserved
     * for component merges.
     */
    public IIndexCursor createTopKSearchCursor(ILSMIndexOperationContext opCtx) {
        return new LSMVTreeTopKSearchCursor(opCtx);
    }

    @Override
    public synchronized void deactivate(boolean flush) throws HyracksDataException {
        // Flush must happen before static structure cleanup because doFlush() needs staticStructure
        super.deactivate(flush);
        // Clean up static structure component after parent deactivation (including flush)
        if (staticStructure != null) {
            try {
                staticStructure.deactivateAndPurge();
            } catch (Exception e) {
                // Best-effort cleanup; do not fail deactivation if static structure cleanup fails. Log at
                // WARN with the throwable — a failed purge leaks a file handle / buffer-cache file slot,
                // which is worth surfacing rather than hiding at TRACE.
                LOGGER.warn("Failed to deactivate static structure component", e);
            }
            staticStructure = null;
        }
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        // Clean up static structure component before parent destruction
        if (staticStructure != null) {
            try {
                staticStructure.destroy();
            } catch (Exception e) {
                // Best-effort cleanup; do not fail destruction if static structure cleanup fails. Log at
                // WARN with the throwable — a failed destroy leaves an orphaned static-structure file.
                LOGGER.warn("Failed to destroy static structure component", e);
            }
            staticStructure = null;
        }
        super.destroy();
    }

    private void loadStaticStructure() throws HyracksDataException {
        LSMVTreeFileManager vTreeFileManager = (LSMVTreeFileManager) fileManager;
        LSMVTreeComponentFileReferences ssFileRef = vTreeFileManager.getStaticStructureFileReference();
        if (ssFileRef == null) {
            return;
        }
        ILSMDiskComponent ssComponent =
                createStaticStructure(componentFactory, ssFileRef.getStaticStructureFileReference(), false);
        setStaticStructure((LSMVTreeDiskComponent) ssComponent);
    }

    /**
     * Load the data components exactly as the base class does, then attach the shared static structure —
     * which is required to navigate any of them.
     * <p>
     * The base implementation is sufficient for the data components: this index's file manager leaves the
     * bloom-filter slot empty (a VTree component has no bloom filter) and carries the shared static structure
     * in {@link LSMVTreeComponentFileReferences}' own slot, which {@link #loadStaticStructure()} reads.
     */
    @Override
    protected void loadDiskComponents() throws HyracksDataException {
        super.loadDiskComponents();
        loadStaticStructure();
    }
}
