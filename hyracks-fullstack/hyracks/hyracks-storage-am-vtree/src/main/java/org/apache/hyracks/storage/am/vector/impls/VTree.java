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

package org.apache.hyracks.storage.am.vector.impls;

import static org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider.NEW;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.common.impls.TreeIndexDiskOrderScanCursor;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleBuilderFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunctionFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizerFactory;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.am.vector.frames.VTreeDataFrame;
import org.apache.hyracks.storage.am.vector.frames.VTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.tuples.VTreeTupleUtils;
import org.apache.hyracks.storage.am.vector.utils.CrossPollinationConfig;
import org.apache.hyracks.storage.am.vector.utils.RngAcceptanceFilter;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.storage.am.vector.utils.VTreeMetadataKeys;
import org.apache.hyracks.storage.am.vector.utils.VTreeMetadataTupleAccessor;
import org.apache.hyracks.storage.am.vector.utils.VTreeNavigationUtils;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.ISketchSampler;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Vector Clustering Tree implementation for multi-level k-means vector index.
 *
 * This tree supports hierarchical vector clustering with specialized frame types:
 * - Interior frames: Store cluster
 * centroids and child page pointers
 * - Leaf frames: Store cluster centroids and metadata page pointers
 * - Metadata frames: Store max distances and data page pointers
 * - Data frames: Store distances, primary keys
 */
public class VTree extends AbstractTreeIndex {

    private static final Logger LOGGER = LogManager.getLogger();

    private final int vectorDimensions;
    private final ITreeIndexFrameFactory metadataFrameFactory;
    private final ITreeIndexFrameFactory dataFrameFactory;
    private final IVTreeBinaryAccessorFactory vectorAccessorFactory;
    private final IVTreeDataTupleBuilderFactory dataTupleBuilderFactory;
    // Raw quantization params: {minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount}
    // null = non-quantized index.
    private final VTreeQuantizationParams quantizationParams;
    private final IVTreeDistanceFunctionFactory distanceFunctionFactory;
    // Distance function for this index's metric, used by the write/clustering path; search builds
    // per-query functions from the factory.
    private final IVTreeDistanceFunction distanceFunction;
    // Cross-pollination placement parameters (M=1 = legacy single-closest). Threaded from the index
    // WITH clause so incremental insert/delete replicate into the same leaf clusters as bulk-load.
    private final CrossPollinationConfig crossPollination;

    // Static-structure navigation state (memory components only). Threading contract: this group is
    // written exactly once by setStaticStructure() (idempotent via the `initialized` guard) while a
    // memory component is being allocated or recycled — i.e. before that component is published for
    // operations — and is read-only thereafter until resetInitialization() clears it for the next
    // recycle. Publication to the threads that later run searches/inserts on the component is provided
    // by the LSM harness's operation-tracker happens-before (a thread must enter the component through
    // the tracker before touching it), the same mechanism that publishes all other in-memory component
    // state (e.g. BTree memory frames). These fields are therefore intentionally NOT volatile: adding
    // volatility would tax the per-navigation read of staticBufferCache without adding a guarantee the
    // harness does not already give. Do not read this group outside that established happens-before.
    private boolean initialized = false;

    // For memory components: reference to static structure for navigation
    private IBufferCache staticBufferCache;
    private int staticFileId;
    private int staticRootPage;

    // Centroid-to-directory-page mapping (memory components only)
    private int[] centroidDirPageMap; // centroidIndex -> VBC directory page ID
    private int firstLeafCentroidIdMem;
    private int numLeafCentroidMem;

    public VTree(IBufferCache bufferCache, IPageManager freePageManager, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, ITreeIndexFrameFactory metadataFrameFactory,
            ITreeIndexFrameFactory dataFrameFactory, IBinaryComparatorFactory[] cmpFactories, int fieldCount,
            int vectorDimensions, FileReference file, IVTreeBinaryAccessorFactory vectorAccessorFactory,
            IVTreeDataTupleBuilderFactory dataTupleBuilderFactory, VTreeQuantizationParams quantizationParams,
            IVTreeDistanceFunctionFactory distanceFunctionFactory, CrossPollinationConfig crossPollination)
            throws HyracksDataException {
        super(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories, fieldCount, file);
        this.vectorDimensions = vectorDimensions;
        this.metadataFrameFactory = metadataFrameFactory;
        this.dataFrameFactory = dataFrameFactory;
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.dataTupleBuilderFactory = dataTupleBuilderFactory;
        this.quantizationParams = quantizationParams;
        this.distanceFunctionFactory = distanceFunctionFactory;
        this.distanceFunction = distanceFunctionFactory.createDistanceFunction();
        this.crossPollination = crossPollination != null ? crossPollination : CrossPollinationConfig.LEGACY;
    }

    /**
     * Get the data frame factory for creating data frames.
     *
     * @return the data frame factory
     */
    public ITreeIndexFrameFactory getDataFrameFactory() {
        return dataFrameFactory;
    }

    /**
     * Get the metadata frame factory for creating metadata frames.
     *
     * @return the metadata frame factory
     */
    public ITreeIndexFrameFactory getMetadataFrameFactory() {
        return metadataFrameFactory;
    }

    @Override
    public ITreeIndexAccessor createAccessor(IIndexAccessParameters iap) {
        return new VTreeAccessor(this, iap);
    }

    @Override
    public int getNumOfFilterFields() {
        return 0;
    }

    public IIndexBulkLoader createComponentBulkLoader(IPageWriteCallback callback, ITreeIndexAccessor staticAccessor,
            ISketchSampler sampler) throws HyracksDataException {
        return new VTreeBulkLoader(callback, this, staticAccessor, sampler, null);
    }

    /**
     * Variant that writes data frames through the supplied frame factory instead of this tree's
     * default. Used by merges to install a polarity-preserving (copy) tuple writer; matter/delete-marker
     * semantics are defined in the LSM layer (see hyracks-storage-am-lsm-vtree).
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    public IIndexBulkLoader createComponentBulkLoader(IPageWriteCallback callback, ITreeIndexAccessor staticAccessor,
            ISketchSampler sampler, ITreeIndexFrameFactory dataFrameFactoryOverride) throws HyracksDataException {
        return new VTreeBulkLoader(callback, this, staticAccessor, sampler, dataFrameFactoryOverride);
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, ISketchSampler sampler, IPageWriteCallback callback)
            throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a bulk loader for building the static hierarchical clustering structure.
     * This is called during index creation to build the multi-level centroid tree.
     *
     * @param numLevels Number of levels in the hierarchy
     * @param clustersPerLevel List of cluster counts per level
     * @param centroidsPerCluster List of centroids per cluster per level
     * @param maxEntriesPerPage Maximum entries per page
     * @param callback Page write callback
     * @return IIndexBulkLoader for building static structure
     */
    public IIndexBulkLoader createStaticStructureBulkLoader(int numLevels, List<Integer> clustersPerLevel,
            List<List<Integer>> centroidsPerCluster, int maxEntriesPerPage, IPageWriteCallback callback)
            throws HyracksDataException {
        return new VTreeStaticStructureBuilder(callback, this, numLevels, clustersPerLevel, centroidsPerCluster,
                maxEntriesPerPage);
    }

    /**
     * Insert a vector into the clustering tree. The vector tuple contains: <vector_embedding, primary_key,
     * [additional_fields]>
     */
    private void insertVector(ITupleReference tuple, VTreeOpContext ctx) throws HyracksDataException {
        double[] vector = VTreeTupleUtils.extractVectorFromTuple(tuple, 0, vectorAccessorFactory);
        if (vector == null) {
            // A tuple with no extractable vector is corrupt/unexpected; ILLEGAL_STATE carries the message.
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Failed to extract vector from tuple");
        }

        // Cross-pollination: write one replica per accepted cluster (M=1 = single closest cluster).
        for (ClusterSearchResult clusterResult : findReplicaClusters(vector)) {
            try (ClusterAccessResult accessResult = prepareClusterAccess(clusterResult, ctx)) {
                // Distance is to THIS cluster's centroid so each replica's stored key is self-consistent.
                double distance = distanceFunction.apply(vector, clusterResult.centroid);
                insertIntoDataPages(accessResult.metadataPageId(), vector, distance, clusterResult.centroidId, tuple,
                        ctx);
            }
        }
    }

    /**
     * Insert vector data into data pages via metadata pages. This method traverses through all linked metadata pages to
     * find the appropriate data page.
     */
    private void insertIntoDataPages(long metadataPageId, double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, VTreeOpContext ctx) throws HyracksDataException {

        // Traverse through all linked directory (metadata) pages to find the appropriate data page.
        // Guard against a corrupted next-page chain that loops back on itself by tracking the page
        // ids already visited; a repeat is a genuine cycle, not just a long-but-valid chain.
        //
        // Concurrency: this is a forward-only walk that write-latches one directory page at a time and
        // releases it before pinning the next (it does not hold the whole chain). That is safe because
        // the directory chain is globally max_distance-ascending and only ever grows by in-place split
        // (VTreeMetadataFrame keeps entries sorted; handleMetadataPageOverflow moves the upper entries to
        // a new page linked *after* the current one — pages are never removed or reordered). Each page is
        // read under its own latch, so the walker sees a consistent snapshot per page. Since this walker
        // advanced past page N only because the record's distance exceeded every entry on N, a concurrent
        // split of N (which can only relocate entries <= N's max to a new page inserted between N and its
        // old successor) cannot hold this record's band — so following N's already-read successor pointer
        // never skips the correct page. The walker therefore always converges on the right band by moving
        // forward. tryPhysicalDelete relies on the same invariant.
        long currentMetadataPageId = metadataPageId;
        Set<Long> visitedMetadataPageIds = new HashSet<>();

        while (currentMetadataPageId != -1) {
            if (!visitedMetadataPageIds.add(currentMetadataPageId)) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "Cycle detected in directory page chain starting at page " + metadataPageId
                                + " (revisited page " + currentMetadataPageId + ")");
            }
            ICachedPage metadataPage =
                    bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) currentMetadataPageId));
            ctx.setMetadataPageId(currentMetadataPageId);
            boolean latched = false;
            try {
                metadataPage.acquireWriteLatch();
                latched = true;
                ctx.getMetadataFrame().setPage(metadataPage);

                // Determine if this is the last directory page in the chain
                int nextMetadataPageId = ctx.getMetadataFrame().getNextPage();
                boolean isLastInChain = (nextMetadataPageId == VTreeDataTupleAccessor.NO_NEXT_PAGE);

                // Try to find appropriate data page based on distance.
                // Only uses catch-all (last data page) on the last directory page;
                // otherwise returns -1 so we traverse to the next directory page.
                long targetDataPageId = findDataPageInMetadataPage(ctx.getMetadataFrame(), distance, isLastInChain);

                if (targetDataPageId != -1) {
                    // Found appropriate data page - try to insert
                    boolean inserted =
                            tryInsertIntoDataPage(targetDataPageId, vector, distance, centroidId, originalTuple, ctx);

                    if (inserted) {
                        return; // Successfully inserted
                    }

                    // If insert failed due to space, we need to handle overflow
                    handleDataPageOverflow(currentMetadataPageId, vector, distance, centroidId, originalTuple, ctx);
                    return;
                }

                // No match on this directory page
                if (isLastInChain) {
                    // Last page in chain - create new data page
                    handleDataPageOverflow(currentMetadataPageId, vector, distance, centroidId, originalTuple, ctx);
                    return;
                }

                // Traverse to next directory page
                currentMetadataPageId = nextMetadataPageId;

            } finally {
                if (latched) {
                    metadataPage.releaseWriteLatch(true);
                }
                bufferCache.unpin(metadataPage);
            }
        }
    }

    /**
     * Find the appropriate data page in a specific metadata page based on distance. This searches for a data page that
     * can accommodate the given distance.
     *
     * Returns last data page as catch-all when distance > all max_distance values.
     * This is needed for BOTH insertion and deletion:
     * - Matter insertion: Vectors with distance > all max values go into last page (catch-all)
     * - Delete-marker tuple insertion: Same as matter insertion - uses last page as catch-all
     * - Physical deletion: To find those vectors, we must check the last page (same catch-all)
     *
     * The last page dynamically expands and metadata max_distance is updated automatically
     * via updateMetadataMaxDistanceIfNeeded() in the insertion path.
     *
     * @param metadataFrame The metadata frame to search
     * @param distance The distance to search for
     * @return Data page ID, or -1 if metadata is empty
     */
    private long findDataPageInMetadataPage(IVTreeMetadataFrame metadataFrame, double distance, boolean isLastInChain)
            throws HyracksDataException {

        int tupleCount = metadataFrame.getTupleCount();

        // Entries are kept sorted by max_distance ascending (VTreeMetadataFrame invariant, maintained
        // by findInsertPosition on every metadata insert). Binary-search the first entry whose
        // max_distance >= distance (i.e. the first data page whose range covers this distance) instead
        // of an O(n) scan. This matches the previous linear "first distance <= maxDistance" result.
        int lo = 0;
        int hi = tupleCount; // half-open [lo, hi)
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (metadataFrame.getMaxDistance(mid) >= distance) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if (lo < tupleCount) {
            return metadataFrame.getDataPagePointer(lo);
        }

        // Only use catch-all (last data page) on the last directory page in the chain.
        // For non-last pages, return -1 so the caller traverses to the next directory page.
        if (isLastInChain && tupleCount > 0) {
            return metadataFrame.getDataPagePointer(tupleCount - 1);
        }

        return -1; // No match on this page (or empty)

    }

    /**
     * Try to insert into a specific data page. Returns true if successful, false if page is full.
     */
    private boolean tryInsertIntoDataPage(long dataPageId, double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, VTreeOpContext ctx) throws HyracksDataException {

        ICachedPage dataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) dataPageId));

        boolean latched = false;
        try {
            dataPage.acquireWriteLatch();
            latched = true;
            ctx.getDataFrame().setPage(dataPage);

            // Create data tuple: <distance, centroidId, vector, PK>
            // Pass context so buildDataTuple can check operation type and encode a delete-polarity tuple
            // if DELETE (the encoding is decided by the caller-supplied frame's tuple writer)
            ITupleReference dataTuple =
                    ctx.getDataTupleBuilder().buildDataTuple(vector, distance, centroidId, originalTuple);

            // Check if there's space for the tuple
            FrameOpSpaceStatus spaceStatus = ctx.getDataFrame().hasSpaceInsert(dataTuple);

            switch (spaceStatus) {
                case SUFFICIENT_CONTIGUOUS_SPACE:
                    insertSortedIntoDataPage(dataTuple, distance, dataPageId, originalTuple, ctx);
                    return true;
                case SUFFICIENT_SPACE:
                    // Fix bug-vtree-delete-frame-corruption: reclaimable space exists but is fragmented
                    // (FREE_SPACE_OFFSET has been pushed past the slot region by prior inserts whose
                    // deletes only updated TOTAL_FREE_SPACE_OFFSET). Compact first to reset
                    // FREE_SPACE_OFFSET to a safe high-water mark, then insert. Matches the canonical
                    // BTreeNSMLeafFrame pattern (BTree.java:309-315).
                    ctx.getDataFrame().compact();
                    insertSortedIntoDataPage(dataTuple, distance, dataPageId, originalTuple, ctx);
                    return true;
                case INSUFFICIENT_SPACE:
                    // Handle overflow by splitting the data page (split recomputes the insertion index
                    // in whichever half the tuple lands, so no position needs to be passed in).
                    splitDataPageMaintainOrder(ctx.getMetadataPageId(), dataPageId, dataTuple, ctx);
                    return true;

                default:
                    throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Unexpected FrameOpSpaceStatus "
                            + spaceStatus + " from VTreeDataFrame.hasSpaceInsert on data page " + dataPageId);
            }

        } finally {
            if (latched) {
                dataPage.releaseWriteLatch(true);
            }
            bufferCache.unpin(dataPage);
        }
    }

    /**
     * Insert a data tuple into the currently-latched data frame at its distance-sorted position, fire the
     * modification callback, bump the page LSN, and grow the catch-all page's metadata max_distance if this
     * distance is a new maximum. Shared by the contiguous-space and post-compaction insert paths in
     * {@link #tryInsertIntoDataPage}.
     */
    private void insertSortedIntoDataPage(ITupleReference dataTuple, double distance, long dataPageId,
            ITupleReference originalTuple, VTreeOpContext ctx) throws HyracksDataException {
        int insertIndex = ((VTreeDataFrame) ctx.getDataFrame()).findInsertPosition(distance);
        ctx.getDataFrame().insert(dataTuple, insertIndex);
        ctx.getModificationCallback().found(null, originalTuple);
        ctx.getDataFrame().setPageLsn(ctx.getDataFrame().getPageLsn() + 1);
        updateMetadataMaxDistanceIfNeeded(ctx.getMetadataPageId(), dataPageId, distance, ctx);
    }

    /**
     * Split data page while maintaining distance-based ordering.
     */
    private void splitDataPageMaintainOrder(long metadataPageId, long dataPageId, ITupleReference newTuple,
            VTreeOpContext ctx) throws HyracksDataException {

        // Create new data page for split
        int newDataPageId = freePageManager.takePage(ctx.getMetaFrame());
        ICachedPage newDataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), newDataPageId), NEW);

        boolean latched = false;
        try {
            newDataPage.acquireWriteLatch();
            latched = true;
            VTreeDataFrame newFrame = (VTreeDataFrame) ctx.getDataFrameFactory().createFrame();
            newFrame.setPage(newDataPage);
            newFrame.initBuffer((byte) 0);

            // Use the frame's split method (following BTree pattern)
            ctx.getDataFrame().split(newFrame, newTuple);

            // Update page links (maintain linked list structure)
            int originalNextPage = ctx.getDataFrame().getNextPage();
            ctx.getDataFrame().setNextPage(newDataPageId);
            newFrame.setNextPage(originalNextPage);

            // Bump both pages past the source page's prior LSN. Page LSNs are not currently
            // consulted for recovery on these LSM-component pages (no readers in this codebase),
            // but keeping the value monotonic mirrors the increment pattern used elsewhere in
            // this class and avoids the non-monotonicity of System.currentTimeMillis().
            long currentLsn = ctx.getDataFrame().getPageLsn() + 1;
            ctx.getDataFrame().setPageLsn(currentLsn);
            newFrame.setPageLsn(currentLsn);

            // Update metadata to reflect the split
            updateMetadataAfterDataSplit(metadataPageId, dataPageId, newDataPageId, ctx);

        } finally {
            if (latched) {
                newDataPage.releaseWriteLatch(true);
            }
            bufferCache.unpin(newDataPage);
        }
    }

    /**
     * Update metadata page after data page split.
     * Updates BOTH original page's maxDistance and adds new page's entry.
     */
    private void updateMetadataAfterDataSplit(long targetMetadataPageId, long originalDataPageId, int newDataPageId,
            VTreeOpContext ctx) throws HyracksDataException {
        if (targetMetadataPageId == -1) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "updateMetadataAfterDataSplit called without a metadata page id (originalDataPageId="
                            + originalDataPageId + ")");
        }

        // Read each page's post-split max distance (original shrank, new page is the spilled-off tail).
        double originalPageMaxDistance = readMaxDistanceInDataPage(originalDataPageId, ctx);
        double newPageMaxDistance = readMaxDistanceInDataPage(newDataPageId, ctx);

        // Update ORIGINAL page's maxDistance in metadata (decreased after split)
        forceUpdateMetadataMaxDistance(targetMetadataPageId, originalDataPageId, originalPageMaxDistance, ctx);

        // Add NEW page's metadata entry
        updateMetadataWithNewDataPage(targetMetadataPageId, newDataPageId, newPageMaxDistance, ctx);
    }

    /**
     * Read the maximum distance-to-centroid stored in a data page. Data-page tuples are kept sorted by
     * distance ascending, so the last tuple carries the page's max; an empty page reports {@code 0.0}.
     * Pins and read-latches the page for the duration.
     */
    private double readMaxDistanceInDataPage(long dataPageId, VTreeOpContext ctx) throws HyracksDataException {
        ICachedPage dataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) dataPageId));
        boolean latched = false;
        try {
            dataPage.acquireReadLatch();
            latched = true;
            IVTreeDataFrame dataFrame = (IVTreeDataFrame) ctx.getDataFrameFactory().createFrame();
            dataFrame.setPage(dataPage);
            int tupleCount = dataFrame.getTupleCount();
            return tupleCount > 0 ? dataFrame.getDistanceToCentroid(tupleCount - 1) : 0.0;
        } finally {
            if (latched) {
                dataPage.releaseReadLatch();
            }
            bufferCache.unpin(dataPage);
        }
    }

    /**
     * In-place deletion with three-scenario handling (follows LSMBTree pattern):
     * 1. Tuple not found → Insert a logical-delete marker tuple (delegates to insertIntoDataPages)
     * 2. Tuple found (matter only) → Physical DELETE
     * 3. Tuple found (after a delete marker was replaced by matter during reinsertion) → Physical DELETE
     *
     * Note on layering: VTree never reads or sets deletion polarity itself — the injected frame
     * factory's tuple writer decides how a delete-polarity tuple is encoded, and matter/delete-marker
     * semantics and reconciliation are defined in the LSM layer (see hyracks-storage-am-lsm-vtree).
     */
    private void deleteVector(ITupleReference tuple, VTreeOpContext ctx) throws HyracksDataException {

        // Extract vector and primary key (binary format - no type assumption)
        double[] vector = VTreeTupleUtils.extractVectorFromTuple(tuple, 0, vectorAccessorFactory);
        byte[] primaryKey = VTreeTupleUtils.extractPrimaryKeyFromTuple(tuple);

        // Cross-pollination: the record was replicated into every accepted cluster at insert/bulk-load
        // time, so a delete must reconcile in EVERY one of them — otherwise the non-cancelled replicas
        // resurface in ANN search. findReplicaClusters() recomputes the identical centroid set (same
        // eps/M/rng on the immutable static structure), so each replica is matched in its own cluster.
        for (ClusterSearchResult clusterResult : findReplicaClusters(vector)) {
            try (ClusterAccessResult accessResult = prepareClusterAccess(clusterResult, ctx)) {
                double distance = distanceFunction.apply(vector, clusterResult.centroid);

                // Try to find and physically delete tuple from data pages (Scenarios 2 & 3)
                boolean foundAndDeleted =
                        tryPhysicalDelete(accessResult.metadataPageId(), distance, primaryKey, tuple, ctx);

                if (!foundAndDeleted) {
                    // Scenario 1: Tuple not found in this cluster → Insert a delete-marker tuple via
                    // insertIntoDataPages, which handles empty metadata pages, page splits, and metadata
                    // max-distance updates.
                    insertIntoDataPages(accessResult.metadataPageId(), vector, distance, clusterResult.centroidId,
                            tuple, ctx);
                }
            }
        }
    }

    /**
     * Try to physically delete a tuple from data pages. Searches through metadata
     * pages to find the tuple and delete it. Returns true if found and deleted,
     * false if not found (caller should insert a delete-marker tuple).
     *
     * Uses binary comparison for primary key matching - no type assumption.
     */
    private boolean tryPhysicalDelete(long metadataPageId, double distance, byte[] primaryKey,
            ITupleReference originalTuple, VTreeOpContext ctx) throws HyracksDataException {

        // Traverse through all linked directory (metadata) pages
        long currentMetadataPageId = metadataPageId;

        while (currentMetadataPageId != -1) {
            ICachedPage metadataPage =
                    bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) currentMetadataPageId));

            boolean metadataLatched = false;
            try {
                metadataPage.acquireReadLatch();
                metadataLatched = true;
                ctx.getMetadataFrame().setPage(metadataPage);

                // Determine if this is the last directory page in the chain
                int nextMetadataPageId = ctx.getMetadataFrame().getNextPage();
                boolean isLastInChain = (nextMetadataPageId == VTreeDataTupleAccessor.NO_NEXT_PAGE);

                // Find appropriate data page based on distance
                long targetDataPageId = findDataPageInMetadataPage(ctx.getMetadataFrame(), distance, isLastInChain);

                if (targetDataPageId != -1) {
                    // Try physical deletion in this data page
                    ICachedPage dataPage =
                            bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) targetDataPageId));

                    boolean dataLatched = false;
                    try {
                        dataPage.acquireWriteLatch();
                        dataLatched = true;
                        ctx.getDataFrame().setPage(dataPage);

                        // Search for tuple by distance + PK (uses binary comparison internally)
                        int pkFieldIndex = VTreeDataTupleAccessor.getPkStartField(quantizationParams != null);
                        int tupleIndex = ((VTreeDataFrame) ctx.getDataFrame())
                                .findTupleByDistanceAndPrimaryKey(distance, primaryKey, pkFieldIndex);

                        if (tupleIndex >= 0) {
                            // Found: findTupleByDistanceAndPrimaryKey only returns an index whose PK equals
                            // primaryKey (binary compare), and the data page stays write-latched here, so the
                            // match cannot change under us — no re-check is needed. Physically delete it.
                            ctx.getDataFrame().delete(originalTuple, tupleIndex);
                            return true;
                        }

                        // Not found in this data page
                    } finally {
                        if (dataLatched) {
                            dataPage.releaseWriteLatch(true);
                        }
                        bufferCache.unpin(dataPage);
                    }
                }

                // No match or not found - check next directory page
                if (isLastInChain) {
                    break; // End of chain
                }
                currentMetadataPageId = nextMetadataPageId;

            } finally {
                if (metadataLatched) {
                    metadataPage.releaseReadLatch();
                }
                bufferCache.unpin(metadataPage);
            }
        }

        return false; // Not found in any data page
    }

    private void handleDataPageOverflow(long metadataPageId, double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, VTreeOpContext ctx) throws HyracksDataException {
        // Use the frame factories and page manager to handle overflow
        IVTreeDataFrame dataFrame = (IVTreeDataFrame) ctx.getDataFrameFactory().createFrame();
        IPageManager pageManager = ctx.getFreePageManager();

        // Create a new data page for overflow
        int newDataPageId = pageManager.takePage(ctx.getMetaFrame());
        ICachedPage newPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), newDataPageId), NEW);

        boolean latched = false;
        try {
            newPage.acquireWriteLatch();
            latched = true;
            // Initialize the new data frame
            dataFrame.setPage(newPage);
            dataFrame.initBuffer((byte) 0);

            // Create data tuple for the new vector
            ITupleReference dataTuple =
                    ctx.getDataTupleBuilder().buildDataTuple(vector, distance, centroidId, originalTuple);

            // Insert the tuple into the new page
            dataFrame.insert(dataTuple, 0);

            // Update metadata page to include the new data page
            updateMetadataWithNewDataPage(metadataPageId, newDataPageId, distance, ctx);

        } finally {
            if (latched) {
                newPage.releaseWriteLatch(true);
            }
            bufferCache.unpin(newPage);
        }
    }

    /**
     * Update metadata maxDistance if the new distance exceeds the current maxDistance.
     * This is needed when inserting into the last data page with a distance greater than
     * the current maxDistance - the last page acts as a catch-all that dynamically expands.
     */
    private void updateMetadataMaxDistanceIfNeeded(long metadataPageId, long dataPageId, double newDistance,
            VTreeOpContext ctx) throws HyracksDataException {

        VTreeMetadataFrame metadataFrame = requireLatchedMetadataFrame(metadataPageId, ctx);

        // Find the metadata entry for this data page
        int tupleCount = metadataFrame.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            long pagePtr = metadataFrame.getDataPagePointer(i);

            if (pagePtr == dataPageId) {
                double currentMaxDistance = metadataFrame.getMaxDistance(i);

                // Only update if new distance is larger
                if (newDistance > currentMaxDistance) {
                    metadataFrame.updateMaxDistance(i, newDistance);
                }
                break;
            }
        }
    }

    /**
     * Force update metadata maxDistance to a specific value (regardless of increase/decrease).
     * This is needed after data page splits where the original page's maxDistance decreases.
     */
    private void forceUpdateMetadataMaxDistance(long metadataPageId, long dataPageId, double newMaxDistance,
            VTreeOpContext ctx) throws HyracksDataException {

        VTreeMetadataFrame metadataFrame = requireLatchedMetadataFrame(metadataPageId, ctx);

        // Find the metadata entry for this data page
        int tupleCount = metadataFrame.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            long pagePtr = metadataFrame.getDataPagePointer(i);

            if (pagePtr == dataPageId) {
                metadataFrame.updateMaxDistance(i, newMaxDistance);
                break;
            }
        }
    }

    /**
     * Update metadata page to include a new data page. Handles metadata page overflow by splitting when necessary.
     */
    private void updateMetadataWithNewDataPage(long metadataPageId, int newDataPageId, double maxDistance,
            VTreeOpContext ctx) throws HyracksDataException {

        VTreeMetadataFrame metadataFrame = requireLatchedMetadataFrame(metadataPageId, ctx);

        // Create metadata tuple for new data page
        ITupleReference metadataTuple = VTreeMetadataTupleAccessor.createMetadataTuple(maxDistance, newDataPageId);
        ITreeIndexTupleWriter metadataFrameTupleWriter = metadataFrame.getTupleWriter();
        int slotSize = metadataFrame.getSlotSize();

        // Check if there's space for the new metadata entry
        // Check if directory page has space
        int spaceNeeded = metadataFrameTupleWriter.bytesRequired(metadataTuple) + slotSize;
        int spaceAvailable = metadataFrame.getTotalFreeSpace();

        if (spaceNeeded > spaceAvailable) {
            // Insufficient space - need to split metadata page
            handleMetadataPageOverflow(metadataPageId, metadataTuple, ctx);
        } else {
            // Insert the new data-page entry in its sorted position by max_distance, preserving the
            // directory's max_distance-ascending invariant (see VTreeMetadataFrame javadoc) that
            // findDataPageInMetadataPage() relies on for correct distance-based routing. Appending at
            // getTupleCount() here corrupted that invariant after a non-last data-page split (the new
            // page's max_distance falls between existing entries), which mis-routed subsequent inserts,
            // produced overlapping data-page distance ranges, and broke the sorted-stream precondition
            // of the search-side merge and the matter/delete-marker reconciliation done in the LSM layer.
            int insertPos = metadataFrame.findInsertPosition(maxDistance);
            metadataFrame.insert(metadataTuple, insertPos);
        }
    }

    /**
     * Return the shared metadata frame that the caller already holds pinned and write-latched for
     * {@code metadataPageId}.
     * <p>
     * All metadata-mutation helpers ({@link #updateMetadataMaxDistanceIfNeeded},
     * {@link #forceUpdateMetadataMaxDistance}, {@link #updateMetadataWithNewDataPage}) run only from
     * inside {@link #insertIntoDataPages}, which pins the current directory page, write-latches it, sets
     * {@code ctx.getMetadataFrame()} to it, and releases the latch (marking the page dirty) in its own
     * {@code finally}. Operating on that already-latched frame here — instead of re-pinning and
     * re-latching the same page — removes redundant buffer-cache I/O and the former reliance on latch
     * reentrancy ({@code ReentrantReadWriteLock}) that a non-reentrant latch would have turned into a
     * self-deadlock. {@link #handleMetadataPageOverflow} already assumes this same shared-frame contract.
     */
    private VTreeMetadataFrame requireLatchedMetadataFrame(long metadataPageId, VTreeOpContext ctx) {
        assert metadataPageId == ctx.getMetadataPageId() : "metadata mutation on page " + metadataPageId
                + " but the write-latched page is " + ctx.getMetadataPageId();
        return (VTreeMetadataFrame) ctx.getMetadataFrame();
    }

    /**
     * Handle metadata page overflow by splitting the page and distributing tuples.
     */
    private void handleMetadataPageOverflow(long metadataPageId, ITupleReference newTuple, VTreeOpContext ctx)
            throws HyracksDataException {

        // Allocate a new metadata page
        int newMetadataPageId = freePageManager.takePage(ctx.getMetaFrame());
        ICachedPage newMetadataPage =
                bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), newMetadataPageId), NEW);

        boolean latched = false;
        try {
            newMetadataPage.acquireWriteLatch();
            latched = true;

            // Create new metadata frame for the split page
            IVTreeMetadataFrame rightFrame = (IVTreeMetadataFrame) metadataFrameFactory.createFrame();
            rightFrame.setPage(newMetadataPage);
            rightFrame.initBuffer((byte) 0);

            // Split the current metadata page using the correct method from VTreeMetadataFrame
            ((VTreeMetadataFrame) ctx.getMetadataFrame()).split(rightFrame, newTuple);

            // Update the next page pointer in the original metadata page
            ctx.getMetadataFrame().setNextPage(newMetadataPageId);

            // Initialize the next page pointer in the new metadata page to end-of-chain.
            rightFrame.setNextPage(VTreeDataTupleAccessor.NO_NEXT_PAGE);

        } finally {
            if (latched) {
                newMetadataPage.releaseWriteLatch(true);
            }
            bufferCache.unpin(newMetadataPage);
        }
    }

    @Override
    public void validate() throws HyracksDataException {
        // Validation logic specific to vector clustering tree
    }

    /**
     * Find the closest cluster starting from root and traversing down to leaf level. Handles overflow pages for both
     * interior and leaf frames.
     */
    public ClusterSearchResult findClosestClusterFromRoot(double[] queryVector, IVTreeDistanceFunction distanceFunction)
            throws HyracksDataException {
        return findClosestClusterFromRoot(queryVector, distanceFunction, null, null);
    }

    /**
     * Find the closest cluster starting from root and traversing down to leaf level,
     * optionally computing a quantized distance for the best result.
     *
     * @param queryVector Query vector for navigation (full precision)
     * @param distanceFunction Distance function for centroid comparison
     * @param quantizedQueryVector Quantized form of queryVector (nullable — pass null to skip quantized distance)
     * @param quantizer Quantizer for dequantizing leaf centroid bytes (nullable — pass null to skip)
     * @return ClusterSearchResult with optional quantizedDistance
     */
    public ClusterSearchResult findClosestClusterFromRoot(double[] queryVector, IVTreeDistanceFunction distanceFunction,
            double[] quantizedQueryVector, IVTreeQuantizer quantizer) throws HyracksDataException {

        // For memory components: navigate via static structure reference
        IBufferCache navBC = (staticBufferCache != null) ? staticBufferCache : bufferCache;
        int navFileId = (staticBufferCache != null) ? staticFileId : getFileId();
        int navRoot = (staticBufferCache != null) ? staticRootPage : rootPage;

        LOGGER.log(Level.TRACE, "Starting findClosestClusterFromRoot with navRoot={}, isMemoryComponent={}", navRoot,
                staticBufferCache != null);

        ClusterSearchResult result =
                VTreeNavigationUtils.findClosestCentroid(navBC, navFileId, navRoot, getInteriorFrameFactory(),
                        getLeafFrameFactory(), queryVector, distanceFunction, quantizedQueryVector, quantizer);

        // For memory components: replace directoryPageId with VBC mapping
        if (centroidDirPageMap != null) {
            int centroidIndex = result.centroidId - firstLeafCentroidIdMem;
            if (centroidIndex >= 0 && centroidIndex < centroidDirPageMap.length) {
                result = ClusterSearchResult.create(result.leafPageId, result.clusterIndex, result.centroid,
                        result.distance, result.centroidId, centroidDirPageMap[centroidIndex],
                        result.quantizedDistance);
            }
        }

        return result;
    }

    public int getVectorDimensions() {
        return vectorDimensions;
    }

    /**
     * Get the buffer cache to use for tree navigation.
     * For memory components, this returns the static structure's buffer cache.
     * For disk components, this returns the component's own buffer cache.
     */
    public IBufferCache getNavigationBufferCache() {
        return (staticBufferCache != null) ? staticBufferCache : bufferCache;
    }

    /**
     * Get the file ID to use for tree navigation.
     * For memory components, this returns the static structure's file ID.
     * For disk components, this returns the component's own file ID.
     */
    public int getNavigationFileId() {
        return (staticBufferCache != null) ? staticFileId : getFileId();
    }

    /**
     * Get the root page ID to use for tree navigation.
     * For memory components, this returns the static structure's root page.
     * For disk components, this returns the component's own root page.
     */
    public int getNavigationRootPageId() {
        return (staticBufferCache != null) ? staticRootPage : rootPage;
    }

    /**
     * Find close centroids via level-wise traversal with global sort (delegates to
     * VTreeNavigationUtils.findCloseCentroidsLevelWiseGlobalSort). Handles overflow pages for both
     * interior and leaf frames.
     */
    public List<ClusterSearchResult> findCloseCentroidsLevelWiseGlobalSortFromRoot(double[] queryVector,
            IVTreeDistanceFunction distanceFunction, double ep) throws HyracksDataException {

        // For memory components: navigate via static structure reference
        IBufferCache navBC = (staticBufferCache != null) ? staticBufferCache : bufferCache;
        int navFileId = (staticBufferCache != null) ? staticFileId : getFileId();
        int navRoot = (staticBufferCache != null) ? staticRootPage : rootPage;

        LOGGER.log(Level.TRACE, "Starting findCloseCentroidsLevelWiseFromRoot with navRoot={}", navRoot);

        List<ClusterSearchResult> results = VTreeNavigationUtils.findCloseCentroidsLevelWiseGlobalSort(navBC, navFileId,
                navRoot, getInteriorFrameFactory(), getLeafFrameFactory(), queryVector, distanceFunction, ep);

        // For memory components: replace directoryPageId with VBC mapping
        if (centroidDirPageMap != null) {
            for (int r = 0; r < results.size(); r++) {
                ClusterSearchResult result = results.get(r);
                int centroidIndex = result.centroidId - firstLeafCentroidIdMem;
                if (centroidIndex >= 0 && centroidIndex < centroidDirPageMap.length) {
                    results.set(r,
                            ClusterSearchResult.create(result.leafPageId, result.clusterIndex, result.centroid,
                                    result.distance, result.centroidId, centroidDirPageMap[centroidIndex],
                                    result.quantizedDistance));
                }
            }
        }

        return results;
    }

    /**
     * Get the raw quantization parameters, or null if no quantization is configured.
     * Format: {minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount}
     */
    public VTreeQuantizationParams getQuantizationParams() {
        return quantizationParams;
    }

    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Reset initialization state so that static structure directory pages
     * can be re-created after a memory component flush/recycle.
     */
    public void resetInitialization() {
        initialized = false;
        centroidDirPageMap = null;
    }

    /**
     * Initialize this memory component's static-structure navigation state (see the field-group comment
     * above for the full threading contract). Called by the LSM layer during memory-component allocation
     * or post-flush recycle, before the component is published for operations; {@code synchronized} plus
     * the {@code initialized} guard make it a safe, idempotent single write. Reads of the fields it sets
     * rely on the LSM harness's operation-tracker happens-before for visibility, so callers must not read
     * them concurrently with this method outside that ordering.
     */
    public synchronized void setStaticStructure(VTreeAccessor staticAccessor) throws HyracksDataException {
        if (initialized) {
            return; // Already initialized, skip
        }

        VTree staticStructure = staticAccessor.getIndex();
        ITreeIndexMetadataFrame metaFrame = staticAccessor.getOpContext().getMetaFrame();

        // Store references to static structure for read-only navigation
        this.staticBufferCache = staticStructure.getBufferCache();
        this.staticFileId = staticStructure.getFileId();
        this.staticRootPage = staticStructure.rootPage;

        // Pin the static structure's metadata page onto metaFrame before reading
        // (getMaxPageId internally calls metaFrame.setPage() which initializes the frame's buffer)
        staticStructure.getPageManager().getMaxPageId(metaFrame);

        // Read metadata from static structure
        LongPointable value1 = LongPointable.FACTORY.createPointable();
        LongPointable value2 = LongPointable.FACTORY.createPointable();
        metaFrame.get(VTreeMetadataKeys.NUM_LEAF_CENTROIDS, value1);
        metaFrame.get(VTreeMetadataKeys.FIRST_LEAF_CENTROID_ID, value2);
        this.numLeafCentroidMem = value1.intValue();
        this.firstLeafCentroidIdMem = value2.intValue();

        // Create empty directory pages in VBC (using takePage() directly)
        ITreeIndexMetadataFrame vbcMetaFrame = freePageManager.createMetadataFrame();
        ITreeIndexFrame directoryFrame = metadataFrameFactory.createFrame();
        centroidDirPageMap = new int[numLeafCentroidMem];

        for (int i = 0; i < numLeafCentroidMem; i++) {
            int dirPageId = freePageManager.takePage(vbcMetaFrame);
            centroidDirPageMap[i] = dirPageId;

            ICachedPage targetPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), dirPageId), NEW);
            try {
                // No latch is taken here: the page is freshly allocated (NEW) and not yet visible to
                // other threads, so a failing setPage/initBuffer only needs the pin unwound.
                directoryFrame.setPage(targetPage);
                directoryFrame.initBuffer((byte) 0);
            } finally {
                bufferCache.unpin(targetPage);
            }

            LOGGER.log(Level.TRACE, "Created directory page {} for leaf centroid {}", dirPageId, i);
        }

        initialized = true;
    }

    public void setRootPageId(int rootPageId) {
        rootPage = rootPageId;
    }

    public int[] getCentroidDirPageMap() {
        return centroidDirPageMap;
    }

    public int getFirstLeafCentroidIdMem() {
        return firstLeafCentroidIdMem;
    }

    public int getNumLeafCentroidMem() {
        return numLeafCentroidMem;
    }

    /**
     * Result of {@link #prepareClusterAccess}: carries the cluster lookup, the (still
     * write-latched) leaf page, and the metadata page id. Implements {@link AutoCloseable} so
     * callers can release the latch and unpin via try-with-resources. {@code leafPage} is
     * {@code null} for memory components — in that case {@code close()} is a no-op.
     */
    public record ClusterAccessResult(ClusterSearchResult clusterResult, ICachedPage leafPage, long metadataPageId,
            IBufferCache bufferCache) implements AutoCloseable {

    @Override
    public void close() {
        if (leafPage != null) {
            leafPage.releaseWriteLatch(true);
            bufferCache.unpin(leafPage);
        }
    }

    }

    /**
     * Resolve the leaf clusters a record must be written to / deleted from.
     * <p>
     * For ALL {@code M} (including the legacy {@code M == 1}, where the top-1 is returned) this reproduces
     * the bulk-load placement exactly: the eps-filtered, RNG-thinned top-{@code M} accepted centroids
     * ({@link #findCloseCentroidsLevelWiseGlobalSortFromRoot} + {@link RngAcceptanceFilter#accept}). Because
     * the static structure (centroid set) is immutable and the eps/M/rng parameters are fixed for the index,
     * this resolves to the same set at bulk-load, insert AND delete time — the property delete reconciliation
     * relies on to cancel every replica. (Using greedy {@code findClosestClusterFromRoot} for {@code M == 1}
     * broke this: it diverged from bulk-load's level-wise choice and leaked deletes — see the method body.)
     */
    private List<ClusterSearchResult> findReplicaClusters(double[] vector) throws HyracksDataException {
        // Route via the SAME level-wise global-sort navigation + RNG thinning that BULK-LOAD uses, for ALL
        // M — including the legacy M==1 case, where RngAcceptanceFilter returns the single globally-closest
        // centroid. The old M==1 shortcut to greedy findClosestClusterFromRoot picked a *locally* closest
        // leaf that, for boundary records, differed from bulk-load's globally-closest choice; the delete then
        // wrote its antimatter into a different cluster (and a different field-0 distance) than the bulk-loaded
        // matter, so search-time reconciliation could never cancel it and the deleted record leaked in ANN
        // results (~66% of bulk-loaded deletes at some k-means seeds). One routing function across bulk-load,
        // insert and delete guarantees matter and antimatter always agree on cluster and distance-to-centroid.
        List<ClusterSearchResult> candidates =
                findCloseCentroidsLevelWiseGlobalSortFromRoot(vector, distanceFunction, crossPollination.epsilon());
        List<ClusterSearchResult> accepted = RngAcceptanceFilter.accept(candidates, distanceFunction,
                crossPollination.rngFactor(), crossPollination.m(), null);
        if (accepted.isEmpty()) {
            // Defensive: navigation found nothing within eps — never silently drop the record; fall back to
            // the single closest cluster.
            return Collections.singletonList(requireCluster(findClosestClusterFromRoot(vector, distanceFunction)));
        }
        return accepted;
    }

    private static ClusterSearchResult requireCluster(ClusterSearchResult clusterResult) throws HyracksDataException {
        if (clusterResult == null) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "No cluster found for vector");
        }
        return clusterResult;
    }

    /**
     * Prepare data-page access for a single (already resolved) cluster: pin/latch the leaf page when needed
     * and resolve the metadata page id. Used by insert, delete, and update once per replica cluster.
     * <ul>
     *   <li>Memory components ({@code centroidDirPageMap != null}): the metadata page is taken straight from
     *       the VBC centroid→directory mapping carried on {@code clusterResult}; no leaf page is pinned.</li>
     *   <li>Disk components: the leaf page is write-latched and the metadata pointer read from its frame.</li>
     * </ul>
     */
    private ClusterAccessResult prepareClusterAccess(ClusterSearchResult clusterResult, VTreeOpContext ctx)
            throws HyracksDataException {
        if (centroidDirPageMap != null) {
            return new ClusterAccessResult(clusterResult, null, clusterResult.directoryPageId, bufferCache);
        }

        ICachedPage leafPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), clusterResult.leafPageId));
        try {
            leafPage.acquireWriteLatch();
            ctx.getLeafFrame().setPage(leafPage);
            long metadataPageId = ctx.getLeafFrame().getMetadataPagePointer(clusterResult.clusterIndex);
            return new ClusterAccessResult(clusterResult, leafPage, metadataPageId, bufferCache);
        } catch (Exception e) {
            try {
                leafPage.releaseWriteLatch(false);
            } finally {
                bufferCache.unpin(leafPage);
            }
            throw e;
        }
    }

    public class VTreeAccessor implements ITreeIndexAccessor {

        private VTree tree;
        private VTreeOpContext ctx;
        private IIndexAccessParameters iap;
        private boolean destroyed = false;
        // The IAP map and the tree's quantization params are fixed for the accessor's lifetime, so these are
        // resolved once at construction and read as fixed fields during search. queryDistanceFunctionFactory
        // is the optional query-time factory (null → fall back to the index's own); binaryAccessorFactory
        // decodes the query tuple; quantizerFactory / injectedQuantizer are the production and test quantizer
        // seams. Only the quantizer instance is built per search (it depends on the predicate's distance metric).
        private final IVTreeDistanceFunctionFactory queryDistanceFunctionFactory;
        private final IVTreeBinaryAccessorFactory binaryAccessorFactory;
        private final IVTreeQuantizerFactory quantizerFactory;
        private final IVTreeQuantizer injectedQuantizer;
        private final VTreeQuantizationParams quantizationParams;

        public VTreeAccessor(VTree tree, IIndexAccessParameters iap) {
            this.tree = tree;
            this.iap = iap;
            this.ctx = new VTreeOpContext(this, tree.interiorFrameFactory, tree.leafFrameFactory,
                    tree.metadataFrameFactory, tree.dataFrameFactory, tree.freePageManager, tree.cmpFactories,
                    tree.vectorDimensions, iap.getModificationCallback(), iap.getSearchOperationCallback(),
                    tree.dataTupleBuilderFactory, tree.quantizationParams);
            this.queryDistanceFunctionFactory =
                    (IVTreeDistanceFunctionFactory) iap.getParameters().get(IVTreeDistanceFunctionFactory.IAP_KEY);
            this.binaryAccessorFactory =
                    (IVTreeBinaryAccessorFactory) iap.getParameters().get(IVTreeBinaryAccessorFactory.IAP_KEY);
            this.quantizerFactory = (IVTreeQuantizerFactory) iap.getParameters().get(IVTreeQuantizerFactory.IAP_KEY);
            this.injectedQuantizer = (IVTreeQuantizer) iap.getParameters().get(IVTreeQuantizer.IAP_KEY);
            this.quantizationParams = tree.getQuantizationParams();
        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException {
            ctx.setOperation(IndexOperation.INSERT);
            insertVector(tuple, ctx);
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException {
            ctx.setOperation(IndexOperation.UPDATE);
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException {
            ctx.setOperation(IndexOperation.DELETE);
            // Use in-place deletion instead of always inserting a delete-marker tuple
            tree.deleteVector(tuple, ctx);
        }

        @Override
        public void upsert(ITupleReference tuple) throws HyracksDataException {
            throw new UnsupportedOperationException();
        }

        @Override
        public IIndexCursor createSearchCursor(boolean exclusive) throws HyracksDataException {
            return createSearchCursor(exclusive, false);
        }

        /**
         * Create a search cursor with explicit full-scan mode control.
         *
         * @param exclusive Whether to create an exclusive cursor
         * @param fullScanMode true for full-scan (merge) mode, false for query mode
         * @return Configured search cursor
         */
        public IIndexCursor createSearchCursor(boolean exclusive, boolean fullScanMode) throws HyracksDataException {
            VTreeSearchCursor cursor = new VTreeSearchCursor();
            configureCursor(cursor);
            cursor.setFullScanMode(fullScanMode);
            return cursor;
        }

        private void configureCursor(VTreeSearchCursor cursor) {
            if (tree.staticBufferCache != null) {
                cursor.setBufferCache(tree.staticBufferCache);
                cursor.setFileId(tree.staticFileId);
                cursor.setRootPageId(tree.staticRootPage);
                cursor.setDataBufferCache(tree.bufferCache, tree.getFileId());
                cursor.setCentroidDirPageMap(tree.centroidDirPageMap, tree.firstLeafCentroidIdMem);
            } else {
                cursor.setBufferCache(tree.bufferCache);
                cursor.setFileId(tree.getFileId());
                cursor.setRootPageId(tree.rootPage);
                cursor.setDataBufferCache(tree.bufferCache, tree.getFileId());
            }
            cursor.setFrameFactories(tree.interiorFrameFactory, tree.leafFrameFactory, tree.metadataFrameFactory,
                    tree.dataFrameFactory);
        }

        public VTree getIndex() {
            return tree;
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
            ctx.setOperation(IndexOperation.SEARCH);

            VTreeSearchCursor vectorCursor = (VTreeSearchCursor) cursor;
            configureCursor(vectorCursor);

            // Extract query vector and distance metric from predicate using the accessor factory
            // The predicate holds the tuple reference (updated per-tuple in resetSearchPredicate)
            double[] queryVector = extractQueryVector(searchPred);
            IVTreeDistanceFunction distanceFunction = resolveDistanceFunctionFactory().createDistanceFunction();

            VTreeCursorInitialState initialState = buildInitialState(queryVector, distanceFunction);
            resolveAndSetQuantizer(initialState, queryVector);

            // Open the cursor - it will perform centroid finding and position on data pages
            vectorCursor.open(initialState, searchPred);
        }

        /** Decode the query vector from the predicate's tuple via the IAP accessor factory (null if none). */
        private double[] extractQueryVector(ISearchPredicate searchPred) throws HyracksDataException {
            if (searchPred instanceof VTreeSearchPredicate vectorPred && vectorPred.getQueryTuple() != null) {
                // binaryAccessorFactory was resolved from the IAP at accessor construction.
                return VTreeTupleUtils.extractVectorFromTuple(vectorPred.getQueryTuple(),
                        vectorPred.getQueryFieldIndex(), binaryAccessorFactory);
            }
            return null;
        }

        /**
         * Prefer a query-time factory from IAP (see
         * VTreeSearchOperatorNodePushable#addAdditionalIndexAccessorParams), else the factory the index
         * was built with.
         */
        private IVTreeDistanceFunctionFactory resolveDistanceFunctionFactory() {
            // queryDistanceFunctionFactory was resolved from the IAP at accessor construction.
            return queryDistanceFunctionFactory != null ? queryDistanceFunctionFactory : tree.distanceFunctionFactory;
        }

        /** Build the cursor initial state: root page (static for memory components), query vector, metric fn. */
        private VTreeCursorInitialState buildInitialState(double[] queryVector,
                IVTreeDistanceFunction distanceFunction) {
            VTreeCursorInitialState initialState = new VTreeCursorInitialState(ctx.getAccessor());
            // For memory components, use staticRootPage (the static structure's root);
            // for disk components, use the tree's own rootPage
            initialState.setRootPageId(tree.staticBufferCache != null ? tree.staticRootPage : tree.rootPage);
            if (queryVector != null) {
                initialState.setQueryVector(queryVector);
            }
            initialState.setDistanceFunction(distanceFunction);
            return initialState;
        }

        /**
         * Resolve the query-time quantizer (if any) and publish its quantized query vector onto
         * {@code initialState}. Production path: an {@link IVTreeQuantizerFactory} supplied via IAP
         * by VTreeSearchOperatorNodePushable builds a quantizer from the float[6] quantization
         * params persisted on the tree. Test fallback: a pre-built {@link IVTreeQuantizer} injected
         * directly under {@link IVTreeQuantizer#IAP_KEY} (e.g. NoOpVectorQuantizer.INSTANCE in
         * VectorTreeTestUtils). Leaves {@code initialState} unquantized when neither applies.
         */
        private void resolveAndSetQuantizer(VTreeCursorInitialState initialState, double[] queryVector)
                throws HyracksDataException {
            // quantizationParams / quantizerFactory / injectedQuantizer were resolved from the tree and IAP at
            // accessor construction; the quantizer's distance metric is baked into the factory.
            if (quantizationParams != null && queryVector != null && quantizerFactory != null) {
                IVTreeQuantizer quantizer = quantizerFactory.createQuantizer(tree.vectorDimensions, quantizationParams);
                initialState.setQuantizedQueryVector(quantizer.quantize(queryVector));
                initialState.setQuantizer(quantizer);
            }

            // Fallback: a pre-built IVTreeQuantizer injected directly under IVTreeQuantizer.IAP_KEY.
            if (initialState.getQuantizer() == null && queryVector != null && injectedQuantizer != null) {
                initialState.setQuantizedQueryVector(injectedQuantizer.quantize(queryVector));
                initialState.setQuantizer(injectedQuantizer);
            }
        }

        /**
         * Find the closest leaf centroid for a given query vector.
         * This method delegates to the tree's findClosestClusterFromRoot implementation.
         *
         * @param queryVector The query vector to find the closest centroid for
         * @param distanceFunction The distance function to use for centroid finding
         * @return ClusterSearchResult containing information about the closest leaf centroid
         * @throws HyracksDataException if any error occurs during the search
         */
        public ClusterSearchResult findClosestLeafCentroid(double[] queryVector,
                IVTreeDistanceFunction distanceFunction) throws HyracksDataException {
            return findClosestLeafCentroid(queryVector, distanceFunction, null, null);
        }

        /**
         * Find the closest leaf centroid, optionally computing quantized distance.
         *
         * @param queryVector The query vector to find the closest centroid for
         * @param distanceFunction The distance function to use for centroid finding
         * @param quantizedQueryVector Quantized form of queryVector (nullable)
         * @param quantizer Quantizer for dequantizing leaf centroid bytes (nullable)
         * @return ClusterSearchResult with optional quantizedDistance
         * @throws HyracksDataException if any error occurs during the search
         */
        public ClusterSearchResult findClosestLeafCentroid(double[] queryVector,
                IVTreeDistanceFunction distanceFunction, double[] quantizedQueryVector, IVTreeQuantizer quantizer)
                throws HyracksDataException {
            validateQueryVector(queryVector);
            return tree.findClosestClusterFromRoot(queryVector, distanceFunction, quantizedQueryVector, quantizer);
        }

        /** Reject a query on a destroyed accessor, a null query vector, or a dimension mismatch. */
        private void validateQueryVector(double[] queryVector) throws HyracksDataException {
            if (destroyed) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Accessor has been destroyed");
            }
            if (queryVector == null) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Query vector cannot be null");
            }
            if (queryVector.length != tree.vectorDimensions) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Query vector dimension ("
                        + queryVector.length + ") does not match tree dimension (" + tree.vectorDimensions + ")");
            }
        }

        @Override
        public void destroy() throws HyracksDataException {
            if (destroyed) {
                return;
            }
            destroyed = true;
            ctx.destroy();
        }

        @Override
        public ITreeIndexCursor createDiskOrderScanCursor() {
            return new TreeIndexDiskOrderScanCursor(leafFrameFactory.createFrame());
        }

        @Override
        public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException {
            ctx.setOperation(IndexOperation.DISKORDERSCAN);
            throw new UnsupportedOperationException("Disk order scan not yet implemented");
        }

        public VTreeOpContext getOpContext() {
            return ctx;
        }

        public ICachedPage getCachedPage(int pageId) throws HyracksDataException {
            return bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId));
        }

        public void releasePage(ICachedPage page) {
            bufferCache.unpin(page);
        }

        public List<ClusterSearchResult> findCloseCentroidsLevelWiseGlobalSort(double[] queryVector,
                IVTreeDistanceFunction hyracksDistanceFunction, double epi) throws HyracksDataException {
            validateQueryVector(queryVector);
            return tree.findCloseCentroidsLevelWiseGlobalSortFromRoot(queryVector, hyracksDistanceFunction, epi);
        }
    }
}
