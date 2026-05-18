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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.storage.am.vector.utils.VTreeLeafNeighborList;
import org.apache.hyracks.storage.am.vector.utils.VTreeMetadataKeys;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.ISketchSampler;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.PageWriteFailureCallback;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VTreeBulkLoader extends PageWriteFailureCallback implements IIndexBulkLoader {
    private static final Logger LOGGER = LogManager.getLogger();

    private final IBufferCache bufferCache;
    private final IPageManager freePageManager;
    private final ITreeIndexMetadataFrame metaFrame;
    private final AbstractTreeIndex treeIndex;
    private final int fileId;
    private final int slotSize;
    private final IFIFOPageWriter pageWriter;
    private final ICompressedPageWriter compressedPageWriter;

    // Accessor to the source static structure, which is immutable and stays open for the
    // lifetime of this load. Its pages are re-read one at a time in end() rather than being
    // snapshotted up front, so the loader holds O(1) pages at any moment.
    private final VTree.VTreeAccessor staticAccessor;
    private final int numStaticPages;
    // Root page id within the static structure's page id space. After the static
    // pages are copied into this data component starting at staticBasePageId, the
    // root of the copied structure sits at (staticBasePageId + staticStructureRootPage).
    private final int staticStructureRootPage;

    private final int firstLeafCentroidId;
    private final int numLeafCentroid;

    // Per-cluster directory page tracking: clusterIndex -> first dir page ID
    private final int[] clusterFirstDirPageId;

    // Current cluster state
    private int currentLeafClusterIndex;
    private int currentCentroidId;

    // Current data page (only one in memory at a time — written immediately when full)
    private ICachedPage currentDataPage;
    private int currentDataPageId;
    private final ITreeIndexFrame currentDataFrame;
    private final ITreeIndexTupleWriter dataFrameTupleWriter;
    private int entriesInCurrentDataPage;

    // Directory pages for current cluster. Confiscated with INVALID_DPID and kept in memory
    // until the cluster is finalized, at which point they receive real page IDs, get chained
    // via nextPage pointers, and are written to disk. Typically only 1 page per cluster in
    // production (one 32KB directory page holds ~2000 entries).
    private final ITreeIndexFrame currentDirectoryFrame;
    private final ITreeIndexTupleWriter directoryFrameTupleWriter;
    private final List<ICachedPage> pendingDirectoryPages = new ArrayList<>();
    private ICachedPage currentDirectoryPage;
    private final ISketchSampler sampler;

    public VTreeBulkLoader(IPageWriteCallback callback, VTree vectorTree, ITreeIndexAccessor staticAccessor,
            ISketchSampler sampler) throws HyracksDataException {
        this(callback, vectorTree, staticAccessor, sampler, null);
    }

    /**
     * @param dataFrameFactoryOverride data-frame factory to write with instead of the tree's
     *            default, or {@code null} for the default. Merges pass a polarity-preserving
     *            (copy) factory so preserved delete-marker tuples are not re-encoded as matter;
     *            marker semantics are defined in the LSM layer (see hyracks-storage-am-lsm-vtree).
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED)
    public VTreeBulkLoader(IPageWriteCallback callback, VTree vectorTree, ITreeIndexAccessor staticAccessor,
            ISketchSampler sampler, ITreeIndexFrameFactory dataFrameFactoryOverride) throws HyracksDataException {

        this.sampler = sampler;
        this.bufferCache = vectorTree.getBufferCache();
        this.freePageManager = vectorTree.getPageManager();
        this.fileId = vectorTree.getFileId();
        this.treeIndex = vectorTree;
        this.metaFrame = freePageManager.createMetadataFrame();

        // Initialize frames
        this.currentDirectoryFrame = vectorTree.getMetadataFrameFactory().createFrame();
        this.currentDataFrame = dataFrameFactoryOverride != null ? dataFrameFactoryOverride.createFrame()
                : vectorTree.getDataFrameFactory().createFrame();
        this.dataFrameTupleWriter = currentDataFrame.getTupleWriter();
        this.directoryFrameTupleWriter = currentDirectoryFrame.getTupleWriter();
        this.slotSize = currentDataFrame.getSlotSize();
        this.currentLeafClusterIndex = -1;
        this.currentCentroidId = -1;

        this.pageWriter = bufferCache.createFIFOWriter(callback, this, DefaultBufferCacheWriteContext.INSTANCE);
        this.compressedPageWriter = bufferCache.getCompressedPageWriter(fileId);

        this.staticAccessor = (VTree.VTreeAccessor) staticAccessor;
        VTree vtree = this.staticAccessor.getIndex();
        ITreeIndexMetadataFrame staticMetaFrame = this.staticAccessor.getOpContext().getMetaFrame();
        int maxPageId = vtree.getPageManager().getMaxPageId(staticMetaFrame);

        LongPointable value1 = LongPointable.FACTORY.createPointable();
        LongPointable value2 = LongPointable.FACTORY.createPointable();
        staticMetaFrame.get(VTreeMetadataKeys.NUM_LEAF_CENTROIDS, value1);
        staticMetaFrame.get(VTreeMetadataKeys.FIRST_LEAF_CENTROID_ID, value2);
        this.numLeafCentroid = value1.intValue();
        this.firstLeafCentroidId = value2.intValue();

        // Only static-structure METADATA is read here. The page contents themselves are copied
        // one page at a time in end() — the source component is immutable and remains open for
        // the whole load, so no up-front snapshot is needed.
        numStaticPages = maxPageId + 1;
        // VTreeStaticStructureBuilder writes the root at the highest page id (bottom-up).
        // Capture it now so we can translate when the static pages are copied into this
        // data component with an offset.
        this.staticStructureRootPage = vtree.getRootPageId();

        // Initialize per-cluster directory page tracking
        clusterFirstDirPageId = new int[numLeafCentroid];
        for (int i = 0; i < numLeafCentroid; i++) {
            clusterFirstDirPageId[i] = VTreeDataTupleAccessor.UNASSIGNED_DIR_PAGE;
        }

        LOGGER.log(Level.TRACE,
                "VTreeBulkLoader initialized: numLeafCentroid={}, firstLeafCentroidId={}, numStaticPages={}",
                numLeafCentroid, firstLeafCentroidId, numStaticPages);
    }

    /**
     * Create a directory page confiscated with INVALID_DPID.
     * Directory pages are kept in memory until the cluster is finalized,
     * at which point they receive real page IDs.
     */
    private void createDirectoryPage() throws HyracksDataException {
        currentDirectoryPage = bufferCache.confiscatePage(IBufferCache.INVALID_DPID);
        currentDirectoryFrame.setPage(currentDirectoryPage);
        currentDirectoryFrame.initBuffer((byte) 0);

        LOGGER.log(Level.TRACE, "Created directory page (in-memory) for cluster {}", currentLeafClusterIndex);
    }

    private int extractCentroidId(ITupleReference tuple) {
        // Centroid id lives at field index 1 in both the non-quantized and quantized data-tuple
        // layouts (NQ_CENTROID_ID_FIELD == Q_CENTROID_ID_FIELD).
        final int cidField = VTreeDataTupleAccessor.NQ_CENTROID_ID_FIELD;
        return IntegerPointable.getInteger(tuple.getFieldData(cidField), tuple.getFieldStart(cidField));
    }

    @Override
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    public void add(ITupleReference tuple) throws HyracksDataException {
        sampler.addTuple(tuple);
        int tupleCentroidId = extractCentroidId(tuple);
        if (currentCentroidId == -1) {
            // First tuple being added - initialize for first cluster
            LOGGER.log(Level.TRACE, "Starting bulk load with first centroid cluster: {}", tupleCentroidId);
            currentCentroidId = tupleCentroidId;
            currentLeafClusterIndex = tupleCentroidId - firstLeafCentroidId;
            createDirectoryPage();
            createNewDataPage();
        } else if (currentCentroidId != tupleCentroidId) {
            // Moved to a new centroid cluster
            LOGGER.log(Level.TRACE, "Switching from centroid {} to centroid {}", currentCentroidId, tupleCentroidId);
            currentCentroidId = tupleCentroidId;
            int targetClusterIndex = tupleCentroidId - firstLeafCentroidId;
            loadToNextLeafCluster(targetClusterIndex);
        }
        try {
            int spaceNeeded = dataFrameTupleWriter.bytesRequired(tuple) + slotSize;
            int spaceAvailable = currentDataFrame.getTotalFreeSpace();

            // A tuple larger than a fresh empty data page's usable space (pageSize - pageHeaderSize -
            // slotSize) can never fit; reject it here instead of looping/overrunning at insertSorted().
            int maxUsableTupleBytes = bufferCache.getPageSize() - currentDataFrame.getPageHeaderSize() - slotSize;
            int tupleBytes = dataFrameTupleWriter.bytesRequired(tuple);
            if (tupleBytes > maxUsableTupleBytes) {
                throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.RECORD_IS_TOO_LARGE,
                        tupleBytes, maxUsableTupleBytes);
            }

            if (spaceNeeded > spaceAvailable) {
                // The current data page cannot hold this tuple, so flush it and start a fresh one.
                // (The empty-page case is unreachable here: the oversized-tuple guard above already
                // rejected any tuple that would not fit in a fresh empty page, so an empty current
                // page always has spaceAvailable >= spaceNeeded and never enters this branch. The
                // former "if tupleCount==0 returnPage(currentDataPage)" sub-branch returned the page
                // and then fell through to insertSorted() on it — a dead use-after-return.)
                // Data page full - write it to disk immediately
                finishCurrentDataPage(false);
            }
            ((IVTreeDataFrame) currentDataFrame).insertSorted(tuple);
            entriesInCurrentDataPage++;

            LOGGER.log(Level.TRACE, "Added tuple to leaf cluster {}, data page entries: {}", currentLeafClusterIndex,
                    entriesInCurrentDataPage);
        } catch (HyracksDataException | RuntimeException e) {
            logDataPageState(tuple, e);
            handleException();
            throw e;
        }
    }

    /**
     * Switch to a specific leaf cluster. Finishes the current data page,
     * finalizes the current cluster's directory pages, then starts the new cluster.
     */
    public void loadToNextLeafCluster(int targetClusterIndex) throws HyracksDataException {
        if (targetClusterIndex < 0 || targetClusterIndex >= numLeafCentroid) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Target cluster index out of bounds: " + targetClusterIndex + " (valid range: 0-"
                            + (numLeafCentroid - 1) + ")");
        }

        if (currentLeafClusterIndex == targetClusterIndex) {
            return;
        }

        // Finish current data page if it has data
        if (currentDataPage != null && entriesInCurrentDataPage > 0) {
            finishCurrentDataPage(true);
        }

        // Finalize directory pages for current cluster (assign IDs, chain, write)
        finalizeClusterDirectory();

        // Start new cluster
        currentLeafClusterIndex = targetClusterIndex;
        createDirectoryPage();
        createNewDataPage();

        LOGGER.log(Level.TRACE, "Moved to leaf cluster {} (centroid ID: {})", currentLeafClusterIndex,
                firstLeafCentroidId + currentLeafClusterIndex);
    }

    /**
     * Create a new data page with a real page ID.
     * Data pages get real IDs immediately so they can be written to disk right away.
     */
    private void createNewDataPage() throws HyracksDataException {
        currentDataPageId = freePageManager.takePage(metaFrame);
        long dpid = BufferedFileHandle.getDiskPageId(fileId, currentDataPageId);
        currentDataPage = bufferCache.confiscatePage(dpid);
        currentDataFrame.setPage(currentDataPage);
        currentDataFrame.initBuffer((byte) 0);
        entriesInCurrentDataPage = 0;

        LOGGER.log(Level.TRACE, "Created new data page {} for leaf cluster {}", currentDataPageId,
                currentLeafClusterIndex);
    }

    /**
     * Finish the current data page: set the next-page pointer, write to disk immediately,
     * and add a directory entry for it.
     *
     * @param lastPage true if this is the last data page for the current cluster
     */
    private void finishCurrentDataPage(boolean lastPage) throws HyracksDataException {
        int tupleCount = currentDataFrame.getTupleCount();
        if (tupleCount == 0) {
            return;
        }

        double maxDistance = ((IVTreeDataFrame) currentDataFrame).getDistanceToCentroid(tupleCount - 1);
        int writtenDataPageId = currentDataPageId;

        if (lastPage) {
            // Last data page in cluster - no next page
            ((IVTreeDataFrame) currentDataFrame).setNextPage(VTreeDataTupleAccessor.NO_NEXT_PAGE);

            write(currentDataPage);
            currentDataPage = null;
            entriesInCurrentDataPage = 0;
        } else {
            // Allocate next data page ID and set forward pointer before writing
            int nextDataPageId = freePageManager.takePage(metaFrame);
            ((IVTreeDataFrame) currentDataFrame).setNextPage(nextDataPageId);

            // Write current data page to disk immediately
            write(currentDataPage);

            // Create new data page with the pre-allocated ID
            currentDataPageId = nextDataPageId;
            long dpid = BufferedFileHandle.getDiskPageId(fileId, currentDataPageId);
            currentDataPage = bufferCache.confiscatePage(dpid);
            currentDataFrame.setPage(currentDataPage);
            currentDataFrame.initBuffer((byte) 0);
            entriesInCurrentDataPage = 0;

            LOGGER.log(Level.TRACE, "Created new data page {} for leaf cluster {}", currentDataPageId,
                    currentLeafClusterIndex);
        }

        // Add directory entry for the written data page
        addDirectoryEntry(maxDistance, writtenDataPageId);
    }

    /**
     * Add a directory entry <maxDistance, dataPageId> to the current directory page.
     * If the directory page is full, move it to the pending list and create a new overflow.
     */
    private void addDirectoryEntry(double maxDistance, int dataPageId) throws HyracksDataException {
        try {
            ITupleReference directoryEntry =
                    TupleUtils.createTuple(new ISerializerDeserializer[] { DoubleSerializerDeserializer.INSTANCE,
                            IntegerSerializerDeserializer.INSTANCE }, maxDistance, dataPageId);

            // Check if directory page has space
            int spaceNeeded = directoryFrameTupleWriter.bytesRequired(directoryEntry) + slotSize;
            int spaceAvailable = currentDirectoryFrame.getTotalFreeSpace();

            if (spaceNeeded > spaceAvailable) {
                // Directory page full - keep in pending list and create overflow
                pendingDirectoryPages.add(currentDirectoryPage);
                createDirectoryPage();

                LOGGER.log(Level.TRACE, "Directory page full for cluster {}, created overflow",
                        currentLeafClusterIndex);
            }

            ((IVTreeFrame) currentDirectoryFrame).insertSorted(directoryEntry);

            LOGGER.log(Level.TRACE, "Added directory entry for data page {} (maxDist={}) to directory, cluster {}",
                    dataPageId, maxDistance, currentLeafClusterIndex);

        } catch (HyracksDataException e) {
            throw e;
        } catch (Exception e) {
            throw new HyracksDataException("Failed to create directory entry", e);
        }
    }

    /**
     * Finalize directory pages for the current cluster:
     * 1. Assign real sequential page IDs to all pending directory pages
     * 2. Set nextPage chain (dir0 -> dir1 -> ... -> -1)
     * 3. Write all directory pages in ascending ID order
     * 4. Record clusterFirstDirPageId for leaf frame pointer assignment
     *
     * Since directory page IDs are allocated after all data pages have been written,
     * the overall write order is: data pages (lower IDs) then directory pages (higher IDs),
     * which naturally maintains strict FIFO ordering.
     */
    private void finalizeClusterDirectory() throws HyracksDataException {
        // Add current directory page to the pending list
        if (currentDirectoryPage != null) {
            pendingDirectoryPages.add(currentDirectoryPage);
            currentDirectoryPage = null;
        }

        if (pendingDirectoryPages.isEmpty()) {
            return;
        }

        // Allocate real page IDs for all directory pages
        int numDirPages = pendingDirectoryPages.size();
        int[] dirPageIds = new int[numDirPages];
        for (int i = 0; i < numDirPages; i++) {
            dirPageIds[i] = freePageManager.takePage(metaFrame);
        }

        // Set disk page IDs, nextPage chain, and write
        for (int i = 0; i < numDirPages; i++) {
            ICachedPage dirPage = pendingDirectoryPages.get(i);

            // Assign real disk page ID
            dirPage.setDiskPageId(BufferedFileHandle.getDiskPageId(fileId, dirPageIds[i]));

            // Set nextPage chain
            currentDirectoryFrame.setPage(dirPage);
            if (i < numDirPages - 1) {
                ((IVTreeMetadataFrame) currentDirectoryFrame).setNextPage(dirPageIds[i + 1]);
            } else {
                ((IVTreeMetadataFrame) currentDirectoryFrame).setNextPage(VTreeDataTupleAccessor.NO_NEXT_PAGE);

            }

            write(dirPage);
        }

        // Record first directory page ID for this cluster
        clusterFirstDirPageId[currentLeafClusterIndex] = dirPageIds[0];
        pendingDirectoryPages.clear();

        LOGGER.log(Level.TRACE, "Finalized directory for cluster {}: {} pages, first dir page = {}",
                currentLeafClusterIndex, numDirPages, dirPageIds[0]);
    }

    private void logDataPageState(ITupleReference tuple, Exception e) {
        try {
            if (currentDataFrame != null) {
                int tupleSize = currentDataFrame.getBytesRequiredToWriteTuple(tuple);
                int spaceNeeded = dataFrameTupleWriter.bytesRequired(tuple) + slotSize;
                int spaceUsed = currentDataFrame.getBuffer().capacity() - currentDataFrame.getTotalFreeSpace();

                LOGGER.log(Level.TRACE,
                        "Data page state - tupleSize: {}, spaceNeeded: {}, spaceUsed: {}, entriesInCurrentDataPage: {}",
                        tupleSize, spaceNeeded, spaceUsed, entriesInCurrentDataPage);
            }
        } catch (Throwable t) {
            e.addSuppressed(t);
        }
    }

    private void handleException() {
        compressedPageWriter.abort();
        // Return pending directory pages (confiscated with INVALID_DPID or real IDs)
        for (ICachedPage page : pendingDirectoryPages) {
            if (page != null && page.confiscated()) {
                bufferCache.returnPage(page, false);
            }
        }
        pendingDirectoryPages.clear();
        if (currentDirectoryPage != null && currentDirectoryPage.confiscated()) {
            bufferCache.returnPage(currentDirectoryPage, false);
            currentDirectoryPage = null;
        }
        if (currentDataPage != null && currentDataPage.confiscated()) {
            bufferCache.returnPage(currentDataPage, false);
            currentDataPage = null;
        }
        freePageManager.returnAllPages();
    }

    @Override
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED)
    public void end() throws HyracksDataException {
        // Finish last cluster's remaining data page
        if (currentDataPage != null && entriesInCurrentDataPage > 0) {
            finishCurrentDataPage(true);
        }

        // Finalize last cluster's directory pages
        finalizeClusterDirectory();

        // --- Copy static pages to end of file ---
        // Destination page ids are deterministic: sequential from staticBasePageId, in source
        // page-id order. This lets both passes below run with a bounded number of pages in
        // memory (one source pin plus at most one confiscated destination page at a time).
        int staticBasePageId = freePageManager.takePage(metaFrame);
        // Allocate remaining S-1 pages
        for (int i = 1; i < numStaticPages; i++) {
            freePageManager.takePage(metaFrame);
        }

        // Create frames for pointer adjustment
        IVTreeInteriorFrame intFrame = (IVTreeInteriorFrame) treeIndex.getInteriorFrameFactory().createFrame();
        IVTreeLeafFrame lfFrame = (IVTreeLeafFrame) treeIndex.getLeafFrameFactory().createFrame();

        // PASS 1: walk the source leaf pages one at a time to map each leaf centroid id to its
        // final placement in this data component: {pageId, slot}. This O(K)-entry map is what
        // allows graph-neighbor entries to be resolved during the single copy pass below without
        // holding every copied page in memory at once.
        Map<Integer, int[]> cidToFinalLoc = new HashMap<>();
        for (int i = 0; i < numStaticPages; i++) {
            ICachedPage sourcePage = staticAccessor.getCachedPage(i);
            try {
                lfFrame.setPage(sourcePage);
                if (lfFrame.getLevel() == 0) {
                    int leafTupleCount = lfFrame.getTupleCount();
                    for (int t = 0; t < leafTupleCount; t++) {
                        cidToFinalLoc.put(lfFrame.getCentroidId(t), new int[] { staticBasePageId + i, t });
                    }
                }
            } finally {
                staticAccessor.releasePage(sourcePage);
            }
        }

        // PASS 2: copy each source page into one confiscated destination page, patch its
        // pointers, and write it out before moving to the next page. A failure mid-copy would
        // otherwise leak the in-flight confiscated destination page and leave a half-written
        // component; track it so the catch can return it and route through handleException()
        // (which also returns any pending directory pages and all taken pages), matching add().
        ICachedPage inFlightPage = null;
        try {
            for (int i = 0; i < numStaticPages; i++) {
                int newPageId = staticBasePageId + i;
                long dpid = BufferedFileHandle.getDiskPageId(fileId, newPageId);
                ICachedPage page = bufferCache.confiscatePage(dpid);
                inFlightPage = page;

                // Copy content directly from the pinned source page (released before patching)
                ICachedPage sourcePage = staticAccessor.getCachedPage(i);
                try {
                    System.arraycopy(sourcePage.getBuffer().array(), 0, page.getBuffer().array(), 0,
                            sourcePage.getBuffer().capacity());
                } finally {
                    staticAccessor.releasePage(sourcePage);
                }

                // Determine page type via level field and adjust pointers
                intFrame.setPage(page);
                byte level = intFrame.getLevel();
                int tupleCount = intFrame.getTupleCount();

                if (level > 0) {
                    // Interior page: offset child pointers by staticBasePageId
                    for (int t = 0; t < tupleCount; t++) {
                        int oldChildId = intFrame.getChildPageId(t);
                        intFrame.setChildPageId(t, oldChildId + staticBasePageId);
                    }
                    // Offset next-page (overflow) pointer if present
                    if (intFrame.getOverflowFlagBit()) {
                        intFrame.setNextPage(intFrame.getNextPage() + staticBasePageId);
                    }
                } else {
                    // Leaf page: set metadata pointers to actual dir page IDs
                    // Use centroidId from each tuple to compute correct cluster index,
                    // because page-ID order does NOT match centroid BFS order when
                    // overflow pages exist (overflow pages have higher IDs than
                    // subsequent clusters' main pages).
                    lfFrame.setPage(page);
                    int leafTupleCount = lfFrame.getTupleCount();
                    for (int t = 0; t < leafTupleCount; t++) {
                        int centroidId = lfFrame.getCentroidId(t);
                        int clusterIndex = centroidId - firstLeafCentroidId;
                        if (clusterIndex >= 0 && clusterIndex < numLeafCentroid) {
                            lfFrame.setMetadataPagePointer(t, clusterFirstDirPageId[clusterIndex]);
                        }
                    }
                    // Offset next-leaf pointer (overflow or sibling chain)
                    int oldNextLeaf = lfFrame.getNextLeaf();
                    if (oldNextLeaf >= 0) {
                        lfFrame.setNextLeaf(oldNextLeaf + staticBasePageId);
                    }
                    // Translate graph-neighbor pointers into this component's page space. The static
                    // structure carries resolved pointers (resolved by the builder in the scaffold's
                    // own page space), so each is offset by staticBasePageId — the same shift applied
                    // to child/next-leaf pointers above. Any still-provisional entry (e.g. from an
                    // older scaffold) is resolved from the pass-1 directory instead.
                    resolveLeafNeighborPointers(lfFrame, cidToFinalLoc, staticBasePageId);
                }

                write(page);
                inFlightPage = null;
            }
        } catch (HyracksDataException | RuntimeException e) {
            if (inFlightPage != null && inFlightPage.confiscated()) {
                bufferCache.returnPage(inFlightPage, false);
            }
            handleException();
            throw e;
        }

        // Set root page and metadata. In bottom-up static structures the root sits at
        // the highest page id; staticStructureRootPage captures that offset within the
        // static page id space, and we translate it into this data component's space
        // by adding staticBasePageId.
        int rootPageId = staticBasePageId + staticStructureRootPage;
        ((VTree) treeIndex).setRootPageId(rootPageId);
        freePageManager.setRootPageId(rootPageId);

        metaFrame.put(VTreeMetadataKeys.NUM_LEAF_CENTROIDS, LongPointable.FACTORY.createPointable(numLeafCentroid));
        metaFrame.put(VTreeMetadataKeys.FIRST_LEAF_CENTROID_ID,
                LongPointable.FACTORY.createPointable(firstLeafCentroidId));

        if (hasFailed()) {
            throw HyracksDataException.create(getFailure());
        }
    }

    private void write(ICachedPage cPage) throws HyracksDataException {
        compressedPageWriter.prepareWrite(cPage);
        pageWriter.write(cPage);
    }

    /**
     * Resolve the given leaf page's graph-neighbor lists from provisional form (neighbor centroid ids)
     * to resolved physical pointers ({pageId, slot} in this data component), using the directory built
     * in pass 1 of {@link #end()}. The leaf frame is already set on the destination page. Quantized
     * leaf tuples carry the neighbor list at field index 3 (a length-prefixed byte array of
     * fixed-width entries); leaf layouts without a neighbor field are skipped, as are entries whose
     * neighbor centroid id is not in the directory (dangling).
     * <p>
     * The page is rewritten in place (same byte width, so slots do not move) before it is written
     * exactly once by the caller — no page is re-uploaded, honoring the cloud append-only rule.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED)
    private void resolveLeafNeighborPointers(IVTreeLeafFrame lfFrame, Map<Integer, int[]> cidToFinalLoc,
            int staticBasePageId) throws HyracksDataException {
        VTreeLeafNeighborList.forEachLeafNeighborEntry(lfFrame, (neighborList, start, e) -> {
            if (VTreeLeafNeighborList.isResolved(neighborList, start, e)) {
                // Scaffold already resolved this to (pageId, slot) in its own 0-based page space;
                // shift the page id into this component's space (slot copied byte-for-byte).
                int pageId = VTreeLeafNeighborList.readPageId(neighborList, start, e);
                int slot = VTreeLeafNeighborList.readSlot(neighborList, start, e);
                VTreeLeafNeighborList.writeResolved(neighborList, start, e, pageId + staticBasePageId, slot);
            } else {
                // Fallback for a still-provisional entry: resolve from this copy's directory,
                // which already maps to component-space (page, slot).
                int neighborCid = VTreeLeafNeighborList.readCentroidId(neighborList, start, e);
                int[] loc = cidToFinalLoc.get(neighborCid);
                if (loc != null) {
                    VTreeLeafNeighborList.writeResolved(neighborList, start, e, loc[0], loc[1]);
                }
            }
        });
    }

    @Override
    public void abort() throws HyracksDataException {
        LOGGER.log(Level.TRACE, "VTreeBulkLoader aborted");
        handleException();
    }

    @Override
    public void force() throws HyracksDataException {
        bufferCache.force(fileId, false);
    }
}
