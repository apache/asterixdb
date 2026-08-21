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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.am.vector.api.VTreeStaticTupleConstants;
import org.apache.hyracks.storage.am.vector.utils.VTreeLeafNeighborList;
import org.apache.hyracks.storage.am.vector.utils.VTreeMetadataKeys;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.PageWriteFailureCallback;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.buffercache.context.write.LocalOnlyWriteContext;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Builds the static structure of a VTree (interior + leaf pages) in bottom-up,
 * append-only fashion. Tuples must arrive leaf-first (level numLevels-1), one
 * level at a time, ending with the root level (level 0). Within each level,
 * clusters arrive in ascending order; within each cluster, centroids arrive in
 * insertion order.
 *
 * Page IDs are assigned via {@link IPageManager#takePage(ITreeIndexMetadataFrame)}
 * as pages are filled, so the root (allocated last) ends up at the highest page
 * ID. Each page is written to disk as soon as its forward pointers (overflow
 * chain within a cluster, sibling chain across leaf clusters, child pointers
 * for interior tuples) are known, so the builder keeps at most one page
 * confiscated at a time.
 *
 * Centroid IDs follow the BFS-from-root convention (root = 0..N_root-1, leaves
 * at highest IDs), independent of arrival order. The builder simply records
 * whatever centroid id is in each tuple and writes it back unchanged.
 *
 * Leaf tuples carry a sentinel metadata-page pointer of -1; {@link VTreeBulkLoader}
 * overwrites the pointer for each leaf tuple based on its centroid id when the
 * data component is loaded.
 */
public class VTreeStaticStructureBuilder extends PageWriteFailureCallback implements IIndexBulkLoader {

    private static final Logger LOGGER = LogManager.getLogger();

    /** Sentinel metadata-page pointer written into leaf tuples; overwritten by VTreeBulkLoader. */
    private static final int LEAF_METADATA_PTR_SENTINEL = -1;

    // Storage infrastructure
    private final IBufferCache bufferCache;
    private final IPageManager freePageManager;
    private final ITreeIndexMetadataFrame metaFrame;
    private final int fileId;
    private final int slotSize;
    private final IFIFOPageWriter pageWriter;
    // Leaf pages are published local-only during placement (their neighbor lists are still provisional)
    // and uploaded later, once, by the leaf resolution pass — see resolveAndUploadLeafNeighbors().
    private final IFIFOPageWriter leafPlacementWriter;
    private final ICompressedPageWriter compressedPageWriter;

    // Maps each leaf centroid id to its placement in this static structure: {pageId, slot}. Built during
    // leaf placement; used by the resolution pass to turn provisional neighbor entries into pointers.
    private final Map<Integer, int[]> leafDirectory = new HashMap<>();
    // Leaf page ids in ascending order (the order leaves must be re-uploaded in, for the cloud writer).
    private final TreeSet<Integer> leafPageIds = new TreeSet<>();
    // Whether the leaf resolution/upload pass has already run. Guards against a second pass re-uploading
    // pages the append-only cloud writer has already seen (it requires monotonically increasing offsets).
    private boolean leafNeighborsResolved;

    // Structure shape
    private final int numLevels;
    private final List<Integer> clustersPerLevel;
    private final List<List<Integer>> centroidsPerCluster;
    private final int maxEntriesPerPage;

    // Frames
    private final IVTreeInteriorFrame interiorFrame;
    private final IVTreeLeafFrame leafFrame;
    // Reused per-entry scratch: createEntryTuple builds one entry at a time (single-threaded build) and
    // insertSorted copies it into the page before the next entry, so one builder/reference is reused rather
    // than allocated per centroid. The builder is rebuilt only when the entry field count changes (once, at
    // the leaf->interior level boundary), since its offset array must be sized exactly to the fields written.
    private ArrayTupleBuilder entryTupleBuilder;
    private int entryTupleFieldCount = -1;
    private final ArrayTupleReference entryTupleRef = new ArrayTupleReference();

    // Build state — bottom-up traversal: leaf first, root last
    private int currentLevel;
    private int currentClusterInLevel;
    private int currentCentroidInCluster;
    private ICachedPage currentPage;
    private ITreeIndexFrame currentFrame;
    private int currentPageId;

    // firstPageIdOfCluster[L][C] = page id of the first page of cluster C at level L.
    // Filled in by startNewClusterPage() as we visit each cluster. Used by interior
    // tuples (at level L) to look up child page ids in level L+1, which by virtue of
    // bottom-up ordering has already been fully written.
    private final int[][] firstPageIdOfCluster;

    // For first_leaf_centroid_id metadata
    private final int[] totalCentroidsUpToLevel;

    public VTreeStaticStructureBuilder(IPageWriteCallback callback, VTree vectorTree, int numLevels,
            List<Integer> clustersPerLevel, List<List<Integer>> centroidsPerCluster, int maxEntriesPerPage)
            throws HyracksDataException {

        this.bufferCache = vectorTree.getBufferCache();
        this.freePageManager = vectorTree.getPageManager();
        this.fileId = vectorTree.getFileId();
        this.metaFrame = freePageManager.createMetadataFrame();

        this.interiorFrame = (IVTreeInteriorFrame) vectorTree.getInteriorFrameFactory().createFrame();
        this.leafFrame = (IVTreeLeafFrame) vectorTree.getLeafFrameFactory().createFrame();
        this.slotSize = ((ITreeIndexFrame) leafFrame).getSlotSize();

        this.pageWriter = bufferCache.createFIFOWriter(callback, this, DefaultBufferCacheWriteContext.INSTANCE);
        this.leafPlacementWriter = bufferCache.createFIFOWriter(callback, this, LocalOnlyWriteContext.INSTANCE);
        this.compressedPageWriter = bufferCache.getCompressedPageWriter(fileId);

        this.numLevels = numLevels;
        this.clustersPerLevel = new ArrayList<>(clustersPerLevel);
        this.centroidsPerCluster = new ArrayList<>();
        for (List<Integer> levelCentroids : centroidsPerCluster) {
            this.centroidsPerCluster.add(new ArrayList<>(levelCentroids));
        }
        this.maxEntriesPerPage = maxEntriesPerPage;

        // Bottom-up: start at leaf level
        this.currentLevel = numLevels - 1;
        this.currentClusterInLevel = 0;
        this.currentCentroidInCluster = 0;

        // Allocate the firstPageIdOfCluster grid (all -1 until populated)
        this.firstPageIdOfCluster = new int[numLevels][];
        for (int L = 0; L < numLevels; L++) {
            this.firstPageIdOfCluster[L] = new int[this.clustersPerLevel.get(L)];
            Arrays.fill(this.firstPageIdOfCluster[L], -1);
        }

        // Compute cumulative centroid counts (used for first_leaf_centroid_id metadata)
        this.totalCentroidsUpToLevel = new int[numLevels + 1];
        totalCentroidsUpToLevel[0] = 0;
        for (int L = 0; L < numLevels; L++) {
            int centroidsInLevel = 0;
            for (int c : this.centroidsPerCluster.get(L)) {
                centroidsInLevel += c;
            }
            totalCentroidsUpToLevel[L + 1] = totalCentroidsUpToLevel[L] + centroidsInLevel;
        }

        // Open the first page (cluster 0, leaf level) so the very first add() has a target.
        startNewClusterPage();

        LOGGER.log(Level.TRACE,
                "VTreeStaticStructureBuilder (bottom-up) initialized: numLevels={}, maxEntriesPerPage={}", numLevels,
                maxEntriesPerPage);
        printStructureInfo();
    }

    @Override
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    public void add(ITupleReference tuple) throws HyracksDataException {
        int childPageId = determineChildPageId();
        ITupleReference entryTuple = createEntryTuple(tuple, childPageId);

        int spaceNeeded = currentFrame.getBytesRequiredToWriteTuple(entryTuple) + slotSize;
        int spaceAvailable = currentFrame.getTotalFreeSpace();

        // An entry tuple larger than a fresh empty page's usable space (pageSize - pageHeaderSize -
        // slotSize) can never fit; reject it here instead of looping/overrunning at insertSorted().
        int maxUsableTupleBytes = bufferCache.getPageSize() - currentFrame.getPageHeaderSize() - slotSize;
        int entryTupleBytes = currentFrame.getBytesRequiredToWriteTuple(entryTuple) - slotSize;
        if (entryTupleBytes > maxUsableTupleBytes) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.RECORD_IS_TOO_LARGE,
                    entryTupleBytes, maxUsableTupleBytes);
        }

        if (spaceNeeded > spaceAvailable) {
            createOverflowPage();
        }

        ((IVTreeFrame) currentFrame).insertSorted(entryTuple);
        advancePosition();
    }

    /**
     * Compute the child pointer to write for the next tuple.
     * <p>
     * Leaf tuples carry a sentinel — {@link VTreeBulkLoader} overwrites with the real
     * directory page pointer when loading data. Interior tuples look up the first
     * page of their target child cluster in level+1, which (by bottom-up order) has
     * already been written and recorded in {@link #firstPageIdOfCluster}.
     */
    private int determineChildPageId() {
        if (currentLevel == numLevels - 1) {
            return LEAF_METADATA_PTR_SENTINEL;
        }
        int childClusterIndex = computeChildClusterIndex();
        int childPageId = firstPageIdOfCluster[currentLevel + 1][childClusterIndex];
        if (childPageId < 0) {
            // TODO(vector-errors): uncoded IllegalStateException -> reaches the user as "Internal error".
            throw new IllegalStateException(
                    "Child page id not yet recorded for level=" + (currentLevel + 1) + ", cluster=" + childClusterIndex
                            + ". Input must be in bottom-up order (leaf level first, root last).");
        }
        return childPageId;
    }

    /**
     * The Nth centroid emitted in level L points to the Nth cluster in level L+1.
     * Returns N based on the current build position.
     */
    private int computeChildClusterIndex() {
        int centroidsProcessedInCurrentLevel = 0;
        for (int c = 0; c < currentClusterInLevel; c++) {
            centroidsProcessedInCurrentLevel += centroidsPerCluster.get(currentLevel).get(c);
        }
        centroidsProcessedInCurrentLevel += currentCentroidInCluster;
        return centroidsProcessedInCurrentLevel;
    }

    /**
     * Create the stored entry tuple from an input centroid tuple by copying every input field through
     * verbatim and appending {@code childPageId} as the LAST field — the navigation child pointer for
     * interior tuples, the metadata-page pointer for leaves. Keeping it last means
     * {@code getMetadataPagePointer()} resolves it via {@code getFieldCount() - 1} regardless of how many
     * optional fields the leaf carried.
     * <p>
     * The copy is schema-agnostic: interior tuples and every leaf variant take the same path, since
     * appending a field never depends on the existing field types. Passing the raw field bytes through
     * (rather than deserializing to {@code double[]}/{@code byte[]} and re-serializing) avoids a per-centroid
     * round trip; the field layout this preserves is documented in {@link VTreeStaticTupleConstants}.
     * Interior vs leaf is still driven by {@link #currentLevel} in {@code confiscateAndInitFrame}, which
     * selects the target frame; this method does not need to distinguish them.
     */
    private ITupleReference createEntryTuple(ITupleReference tuple, int childPageId) throws HyracksDataException {
        int inputFieldCount = tuple.getFieldCount();

        if (LOGGER.isTraceEnabled()) {
            int centroidId =
                    IntegerPointable.getInteger(tuple.getFieldData(VTreeStaticTupleConstants.CENTROID_ID_FIELD),
                            tuple.getFieldStart(VTreeStaticTupleConstants.CENTROID_ID_FIELD));
            LOGGER.log(Level.TRACE, "Adding centroid {} at level={}, cluster={}, position={}, inputFields={}",
                    centroidId, currentLevel, currentClusterInLevel, currentCentroidInCluster, inputFieldCount);
        }

        try {
            // Rebuild the reused builder only when the entry field count changes (constant within a level, so
            // this happens once at the leaf->interior boundary). ArrayTupleReference reads its field count from
            // the offsets-array length, so the builder must be sized exactly to the fields written.
            int fieldCount = inputFieldCount + 1;
            if (entryTupleFieldCount != fieldCount) {
                entryTupleBuilder = new ArrayTupleBuilder(fieldCount);
                entryTupleFieldCount = fieldCount;
            }
            entryTupleBuilder.reset();
            for (int i = 0; i < inputFieldCount; i++) {
                entryTupleBuilder.addField(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            }
            entryTupleBuilder.getDataOutput().writeInt(childPageId);
            entryTupleBuilder.addFieldEndOffset();

            entryTupleRef.reset(entryTupleBuilder.getFieldEndOffsets(), entryTupleBuilder.getByteArray());
            return entryTupleRef;
        } catch (Exception e) {
            LOGGER.log(Level.TRACE, "Error creating entry tuple: {}", e.getMessage());
            throw HyracksDataException.create(ErrorCode.VECTOR_INDEX_BUILD_FAILED, e,
                    "Failed to create an entry tuple");
        }
    }

    /**
     * Allocate a fresh page id and confiscate it as the first page of the current
     * cluster at the current level. Records the page id in firstPageIdOfCluster so
     * the parent level can later resolve its child pointers.
     */
    private void startNewClusterPage() throws HyracksDataException {
        int pageId = freePageManager.takePage(metaFrame);
        firstPageIdOfCluster[currentLevel][currentClusterInLevel] = pageId;
        confiscateAndInitFrame(pageId);
    }

    /**
     * Allocate an overflow page within the current cluster, set the current page's
     * next-page pointer to it (overflow flag = true), flush the current page, then
     * continue building in the new page.
     */
    private void createOverflowPage() throws HyracksDataException {
        int overflowPageId = freePageManager.takePage(metaFrame);
        if (currentLevel == numLevels - 1) {
            leafFrame.setNextLeaf(overflowPageId);
            leafFrame.setOverflowFlagBit(true);
        } else {
            interiorFrame.setNextPage(overflowPageId);
            interiorFrame.setOverflowFlagBit(true);
        }
        writePage(currentPage);
        confiscateAndInitFrame(overflowPageId);
        LOGGER.log(Level.TRACE, "Created overflow page {} for level {}, cluster {}", overflowPageId, currentLevel,
                currentClusterInLevel);
    }

    /**
     * Confiscate a page with the given id and initialize the appropriate frame.
     */
    private void confiscateAndInitFrame(int pageId) throws HyracksDataException {
        long dpid = BufferedFileHandle.getDiskPageId(fileId, pageId);
        currentPage = bufferCache.confiscatePage(dpid);
        if (currentLevel == numLevels - 1) {
            currentFrame = leafFrame;
            leafFrame.setPage(currentPage);
            leafFrame.initBuffer((byte) 0);
            leafFrame.setLevel((byte) 0);
        } else {
            currentFrame = interiorFrame;
            interiorFrame.setPage(currentPage);
            interiorFrame.initBuffer((byte) 0);
            interiorFrame.setLevel((byte) 1);
        }
        currentPageId = pageId;
        LOGGER.log(Level.TRACE, "Opened page {} for level {}", pageId, currentLevel);
    }

    private void writePage(ICachedPage page) throws HyracksDataException {
        if (currentLevel == numLevels - 1) {
            // Leaf page: its neighbor lists are still provisional, so record each centroid's placement
            // and publish the page LOCAL-ONLY. resolveAndUploadLeafNeighbors() later re-reads, resolves,
            // and uploads it exactly once.
            recordLeafPositions();
            compressedPageWriter.prepareWrite(page);
            leafPlacementWriter.write(page);
        } else {
            compressedPageWriter.prepareWrite(page);
            pageWriter.write(page);
        }
    }

    /** Record {@code centroidId -> (currentPageId, slot)} for every tuple on the leaf page being written. */
    private void recordLeafPositions() throws HyracksDataException {
        int tupleCount = ((ITreeIndexFrame) leafFrame).getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            leafDirectory.put(leafFrame.getCentroidId(i), new int[] { currentPageId, i });
        }
        leafPageIds.add(currentPageId);
    }

    /**
     * Leaf resolution pass. Re-reads each leaf page (from local storage, where placement published it) into a
     * <em>confiscated</em> buffer, rewrites its provisional neighbor entries to physical pointers
     * {@code (pageId, slot)} using the directory, and uploads the page exactly once through the same FIFO
     * writer used for interior pages (local + cloud). Pages are processed in ascending page-id order, so the
     * append-only cloud writer sees monotonically increasing offsets (and all leaf uploads precede any
     * interior/root upload, exactly as in the single-pass build).
     * <p>
     * The page is confiscated (not pinned) for the whole re-read/resolve/upload, so it is invisible to the
     * cleaner sweep and to eviction — the build thread is the sole writer of each leaf, which the append-only
     * cloud writer requires. This matches how interior pages are written; going through the FIFO writer also
     * runs the compression prepare step (unlike the previous pin/flush path, which assumed no compression).
     */
    private void resolveAndUploadLeafNeighbors() throws HyracksDataException {
        if (leafNeighborsResolved) {
            return;
        }
        leafNeighborsResolved = true;
        for (int leafPageId : leafPageIds) {
            long dpid = BufferedFileHandle.getDiskPageId(fileId, leafPageId);
            ICachedPage page = bufferCache.confiscateAndLoad(dpid);
            try {
                resolveLeafPageNeighbors(page);
                compressedPageWriter.prepareWrite(page);
                pageWriter.write(page);
            } finally {
                // pageWriter.write returns the page (clearing its confiscated flag). If resolve/prepare
                // threw before it ran, the page is still confiscated, so we return it here (cf. abort()).
                if (page.confiscated()) {
                    bufferCache.returnPage(page, false);
                }
            }
        }
    }

    /** Rewrite provisional neighbor entries on a single leaf page to resolved {@code (pageId, slot)} pointers. */
    private void resolveLeafPageNeighbors(ICachedPage page) throws HyracksDataException {
        leafFrame.setPage(page);
        VTreeLeafNeighborList.forEachLeafNeighborEntry(leafFrame, (neighborList, start, e) -> {
            if (VTreeLeafNeighborList.isResolved(neighborList, start, e)) {
                return;
            }
            int neighborCid = VTreeLeafNeighborList.readCentroidId(neighborList, start, e);
            int[] loc = leafDirectory.get(neighborCid);
            if (loc != null) {
                VTreeLeafNeighborList.writeResolved(neighborList, start, e, loc[0], loc[1]);
            }
        });
    }

    /**
     * Advance counters; if a cluster or level boundary is crossed, handle the
     * transition (chain pages, flush, open a new page for the next position).
     */
    private void advancePosition() throws HyracksDataException {
        currentCentroidInCluster++;

        if (currentCentroidInCluster >= centroidsPerCluster.get(currentLevel).get(currentClusterInLevel)) {
            currentCentroidInCluster = 0;
            currentClusterInLevel++;

            if (currentClusterInLevel >= clustersPerLevel.get(currentLevel)) {
                transitionToNextLevel();
            } else {
                transitionToNextCluster();
            }
        }
    }

    /**
     * Move to the next cluster within the current level. For leaf level we set
     * the just-finished page's nextLeaf to the new cluster's first page id (with
     * overflow=false, since this is a sibling link not an intra-cluster chain).
     * For interior levels each cluster is independent, so we simply flush the
     * old page and start fresh.
     */
    private void transitionToNextCluster() throws HyracksDataException {
        if (currentLevel == numLevels - 1) {
            int nextClusterPageId = freePageManager.takePage(metaFrame);
            firstPageIdOfCluster[currentLevel][currentClusterInLevel] = nextClusterPageId;
            leafFrame.setNextLeaf(nextClusterPageId);
            leafFrame.setOverflowFlagBit(false);
            writePage(currentPage);
            confiscateAndInitFrame(nextClusterPageId);
            LOGGER.log(Level.TRACE, "Leaf cluster transition: chained to page {} (cluster {})", nextClusterPageId,
                    currentClusterInLevel);
        } else {
            writePage(currentPage);
            startNewClusterPage();
            LOGGER.log(Level.TRACE, "Interior cluster transition at level {} to cluster {}", currentLevel,
                    currentClusterInLevel);
        }
    }

    /**
     * Move from the current level to the next level up. Flushes the last page of
     * the current level (no successor pointer to set) and opens the first page of
     * the next level if any. After level 0 (root) is done, currentPage is cleared.
     */
    private void transitionToNextLevel() throws HyracksDataException {
        boolean finishingLeafLevel = (currentLevel == numLevels - 1);
        writePage(currentPage);
        if (finishingLeafLevel) {
            // Every leaf page is now placed locally and the directory is complete; resolve neighbor
            // pointers in place and upload the leaf pages once, before any higher-level page is written.
            resolveAndUploadLeafNeighbors();
        }
        currentLevel--;
        currentClusterInLevel = 0;
        currentCentroidInCluster = 0;
        if (currentLevel >= 0) {
            startNewClusterPage();
            LOGGER.log(Level.TRACE, "Moving up to level {}", currentLevel);
        } else {
            // All levels written; building is complete.
            currentPage = null;
            currentFrame = null;
        }
    }

    @Override
    public void end() throws HyracksDataException {
        // Defensive: if the structure spec didn't drain currentPage via advancePosition
        // (e.g. caller called end() with a partially-built page), flush it now.
        if (currentPage != null) {
            LOGGER.log(Level.TRACE, "end(): flushing unfinalized page {}", currentPageId);
            boolean flushedLeafPage = currentLevel == numLevels - 1;
            writePage(currentPage);
            currentPage = null;
            if (flushedLeafPage) {
                // writePage() published that page LOCAL-ONLY (leaf placement). transitionToNextLevel() —
                // the only other caller of the resolution pass — never ran for the leaf level in this case
                // (a single-level structure, or end() on a partially-built structure), so without this the
                // page would stay local and never be uploaded in cloud deployments. The pass is
                // idempotent-guarded, so this cannot double-upload leaves that were already resolved.
                resolveAndUploadLeafNeighbors();
            }
        }

        if (hasFailed()) {
            throw HyracksDataException.create(getFailure());
        }

        // Root cluster's first page is the entry point. In bottom-up build this is
        // also the page with the highest id since the root level is processed last.
        int rootPageId = firstPageIdOfCluster[0][0];
        if (rootPageId < 0) {
            // TODO(vector-errors): uncoded IllegalStateException -> reaches the user as "Internal error".
            throw HyracksDataException
                    .create(new IllegalStateException("Root page id was never recorded; static structure incomplete."));
        }

        metaFrame.put(VTreeMetadataKeys.NUM_LEAF_CENTROIDS,
                LongPointable.FACTORY.createPointable(getNumLeafCentroids()));
        metaFrame.put(VTreeMetadataKeys.FIRST_LEAF_CENTROID_ID,
                LongPointable.FACTORY.createPointable(totalCentroidsUpToLevel[numLevels - 1]));

        freePageManager.setRootPageId(rootPageId);

        LOGGER.log(Level.TRACE, "VTreeStaticStructureBuilder (bottom-up) done; rootPageId={}", rootPageId);
    }

    @Override
    public void abort() throws HyracksDataException {
        LOGGER.log(Level.TRACE, "VTreeStaticStructureBuilder aborted");
        compressedPageWriter.abort();
        if (currentPage != null && currentPage.confiscated()) {
            bufferCache.returnPage(currentPage, false);
        }
        currentPage = null;
        freePageManager.returnAllPages();
    }

    @Override
    public void force() throws HyracksDataException {
        bufferCache.force(fileId, false);
    }

    private int getNumLeafCentroids() {
        return totalCentroidsUpToLevel[numLevels] - totalCentroidsUpToLevel[numLevels - 1];
    }

    private void printStructureInfo() {
        if (!LOGGER.isTraceEnabled()) {
            return; // skip the StringBuilder work entirely when trace is off
        }
        LOGGER.log(Level.TRACE, "Structure configuration (bottom-up):");
        for (int level = 0; level < numLevels; level++) {
            StringBuilder sb = new StringBuilder();
            sb.append("Level ").append(level).append(": ").append(clustersPerLevel.get(level))
                    .append(" clusters, centroids=[");
            List<Integer> levelCentroids = centroidsPerCluster.get(level);
            for (int cluster = 0; cluster < levelCentroids.size(); cluster++) {
                sb.append(levelCentroids.get(cluster));
                if (cluster < levelCentroids.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append("]");
            LOGGER.log(Level.TRACE, sb.toString());
        }
    }
}
