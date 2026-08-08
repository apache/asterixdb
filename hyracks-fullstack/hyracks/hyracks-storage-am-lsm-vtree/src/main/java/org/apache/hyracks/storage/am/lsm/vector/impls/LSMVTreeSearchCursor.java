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

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.tuples.ReferenceFrameTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.lsm.vector.utils.LSMVTreeUtils;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.impls.VTree.VTreeAccessor;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchPredicate;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * LSM search cursor for Vector Clustering Tree.
 *
 * This cursor coordinates searches across multiple LSM components (memory and disk)
 * by delegating to VTreeSearchCursor for each component and merging results.
 *
 * Supports two modes:
 *
 * 1. Query Mode (fullScanMode=false):
 *    - Used for ANN (Approximate Nearest Neighbor) queries
 *    - Level-wise + DFS cluster selection (nprobe logic)
 *    - All components advance to the SAME cluster (synchronized exploration)
 *    - Early termination based on nprobe and K parameters
 *    - INCLUDE field filtering support
 *
 * 2. Full Scan Mode (fullScanMode=true):
 *    - Used for merge operations where we need ALL data
 *    - Sequential cluster iteration (0 → 1 → 2 → ...)
 *    - All components advance through clusters TOGETHER in lock-step
 *    - No early termination (must process all data)
 *    - Returns antimatter tuples for proper reconciliation
 *
 * Following the pattern of LSMBTreeRangeSearchCursor:
 * - Extends LSMIndexSearchCursor for priority queue and component switching infrastructure
 * - Creates VTreeAccessor for each component
 * - Handles component state changes (memory → disk transitions)
 * - Uses priority queue to merge results sorted by <distance, primary_key> (query) or <primary_key> (merge)
 * - Filters antimatter tuples and handles matter/antimatter cancellation
 */
public class LSMVTreeSearchCursor extends LSMIndexSearchCursor {
    private static final Logger LOGGER = LogManager.getLogger();

    // Accessor array for each component's VTree
    private VTreeAccessor[] vTreeAccessors;

    // Track component types to detect memory → disk transitions
    protected boolean[] isMemoryComponent;

    // Store search predicate for component switching
    private ISearchPredicate searchPredicate;

    // Track K (target limit) for cluster advancement decisions
    private int K;

    // Track nprobe (minimum clusters to probe before checking K)
    private int nprobe;

    // Cluster selection strategy (encapsulates level-wise + DFS logic)
    private IClusterSelectionStrategy clusterStrategy;

    // Track cluster exhaustion state for synchronized advancement
    private boolean[] clusterExhausted; // Whether each component exhausted its current cluster
    private boolean stopAdvancing; // Flag to stop advancing after K reached

    // Progress counters that feed cluster-advancement / early-termination decisions. These are load-bearing,
    // not diagnostics: both are maintained in doNext() (one tuple produced == one doNext() call), never in
    // doGetTuple(), which a consumer may call any number of times per tuple.
    private int tuplesProduced; // Tuples handed to the consumer so far (K progress)
    private int validTuplesFromCurrentCluster; // Tuples produced from the current cluster (empty-cluster nprobe)

    // Debug counters to track reconciliation (trace logging only — see doClose())
    private int totalTuplesPopped; // Total tuples popped from priority queue (including cancelled)
    private int antimatterTuplesDetected; // Antimatter tuples detected
    private int cancellationsMade; // Matter tuples cancelled by antimatter
    private int getTupleCallCount; // Track how many times doGetTuple() is called
    private int tuplesFilteredOut; // Tuples that failed INCLUDE field filter

    // Tuple filter for INCLUDE field predicates (e.g., year > 2000)
    // When set, only tuples passing this filter are returned and counted toward K
    private ITupleFilter tupleFilter;

    // Wrapper to convert ITupleReference to IFrameTupleReference for filter evaluation
    private ReferenceFrameTupleReference referenceFilterTuple;

    // Full-scan mode flag (for merge operations)
    // true = merge mode (sequential cluster iteration, no early termination)
    // false = query mode (level-wise + DFS, nprobe/K-based early termination)
    private final boolean fullScanMode;

    // Field index where primary keys start in the data tuple
    // Non-quantized format: 2 (distance, centroidId, PK...)
    // Quantized format: 4 (distance, centroidId, quantized_distance, quantized_embedding, PK...)
    private int pkStartField;

    // Number of primary key fields (from the index). The reconciliation/ordering key is exactly
    // <distance (field 0), PK fields> — secondary fields between them (centroidId, and for
    // quantized layouts quantized_distance/quantized_embedding) AND trailing INCLUDE fields are
    // excluded, since they may legitimately differ between matter/antimatter twins.
    private int numPrimaryKeyFields;

    public LSMVTreeSearchCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, false, false, NoOpIndexCursorStats.INSTANCE);
    }

    public LSMVTreeSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples, boolean fullScanMode,
            IIndexCursorStats stats) {
        super(opCtx, returnDeletedTuples, stats);
        this.fullScanMode = fullScanMode;
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // Get LSM-specific initial state
        LSMVTreeCursorInitialState lsmInitialState = (LSMVTreeCursorInitialState) initialState;

        // Save search predicate for component switching
        this.searchPredicate = searchPred;

        // Extract K from search predicate for cluster advancement decisions
        this.K = extractK(searchPred);

        // Field layout depends only on whether the index is quantized; derive pkStartField from that
        // single source (VTreeDataTupleAccessor) rather than a predicate value set at a distance.
        this.pkStartField = new VTreeDataTupleAccessor(((LSMVTree) opCtx.getIndex()).isQuantized()).pkStartField();

        // The comparison key ends after the primary keys (INCLUDE fields are not part of it)
        this.numPrimaryKeyFields = ((LSMVTree) opCtx.getIndex()).getNumPrimaryKeyFields();

        // Extract minProbeFraction and epsilon from search predicate
        double minProbeFraction = extractMinProbeFraction(searchPred);
        double epsilon = extractEpsilon(searchPred);
        this.nprobe = 1; // Will be computed by NprobeClusterSelectionStrategy from minProbeFraction

        // Create cluster selection strategy based on mode
        if (fullScanMode) {
            // Merge mode: sequential cluster iteration, no early termination
            this.clusterStrategy = new SequentialClusterSelectionStrategy();
        } else {
            // Query mode: level-wise + DFS, minProbeFraction/K-based early termination
            this.clusterStrategy = new NprobeClusterSelectionStrategy(minProbeFraction, epsilon);
        }

        // Extract tuple filter from search predicate for INCLUDE field predicates
        this.tupleFilter = extractTupleFilter(searchPred);
        if (this.tupleFilter != null) {
            this.referenceFilterTuple = new ReferenceFrameTupleReference();
        }

        // Initialize debug counters
        this.totalTuplesPopped = 0;
        this.antimatterTuplesDetected = 0;
        this.cancellationsMade = 0;
        this.getTupleCallCount = 0;
        this.tuplesFilteredOut = 0;
        this.tuplesProduced = 0;
        this.validTuplesFromCurrentCluster = 0;

        // Set up comparator and operational components
        cmp = lsmInitialState.getOriginalKeyComparator();
        LSMVTreeUtils.validateKeyComparators(cmp, pkStartField, numPrimaryKeyFields);
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();

        // For vector index, we don't need mutable component special handling initially
        includeMutableComponent = false;

        int numVTrees = operationalComponents.size();

        // Initialize cluster tracking arrays for synchronized advancement
        clusterExhausted = new boolean[numVTrees];
        Arrays.fill(clusterExhausted, false);
        stopAdvancing = false;

        // Initialize or resize accessor/cursor arrays
        if (rangeCursors == null) {
            // First open: create arrays
            rangeCursors = new IIndexCursor[numVTrees];
            vTreeAccessors = new VTreeAccessor[numVTrees];
            isMemoryComponent = new boolean[numVTrees];
        } else if (rangeCursors.length != numVTrees) {
            // Component count changed (due to flush/merge): destroy and recreate
            Throwable failure = CleanupUtils.destroy(null, vTreeAccessors);
            vTreeAccessors = null;
            failure = CleanupUtils.destroy(failure, rangeCursors);
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
            rangeCursors = new IIndexCursor[numVTrees];
            vTreeAccessors = new VTreeAccessor[numVTrees];
            isMemoryComponent = new boolean[numVTrees];
        }

        // Create accessors and cursors for each component
        for (int i = 0; i < numVTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            LSMComponentType type = component.getType();

            // Track if this is a memory component
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
            }

            // Check if we need to destroy incompatible accessor/cursor
            if (vTreeAccessors[i] == null || destroyIncompatible(component, i)) {
                vTreeAccessors[i] = createAccessor(component, i);
                rangeCursors[i] = createCursor(type, vTreeAccessors[i]);
            } else {
                // Re-use existing cursor
                rangeCursors[i].close();
            }

            isMemoryComponent[i] = type == LSMComponentType.MEMORY;
        }

        // Open all cursors with the search predicate
        IndexCursorUtils.open(vTreeAccessors, rangeCursors, searchPred);

        LOGGER.trace("doOpen: numComponents={}, K={}, nprobe={}, pkStartField={}", numVTrees, K, nprobe, pkStartField);

        // Initialize strategy and set up DFS fallback
        if (numVTrees > 0) {
            VTreeSearchCursor firstCursor = (VTreeSearchCursor) rangeCursors[0];
            double[] queryVector = firstCursor.getQueryVector();

            // Initialize strategy with first component's tree
            ILSMComponent firstComponent = operationalComponents.get(0);
            VTree vTree = (VTree) firstComponent.getIndex();
            clusterStrategy.initialize(vTree, queryVector, firstCursor.getDistanceFunction(), K);

            // Set first cursor for DFS fallback
            clusterStrategy.setFirstCursorForDFS(firstCursor);

            // Pass shared visited set from strategy to all VTreeSearchCursors
            Set<Integer> visitedSet = clusterStrategy.getVisitedCentroidIds();
            for (int i = 0; i < numVTrees; i++) {
                if (rangeCursors[i] instanceof VTreeSearchCursor) {
                    ((VTreeSearchCursor) rangeCursors[i]).setSharedVisitedSet(visitedSet);
                }
            }

            // DFS in each cursor may pick a DIFFERENT closest cluster than level-wise (level-wise
            // does a global sort across ALL centroids, DFS is greedy). Re-open ALL cursors to
            // level-wise[0] so every component agrees on the first cluster.
            ClusterSearchResult firstCluster = clusterStrategy.getFirstCluster();
            if (firstCluster != null) {
                ClusterSearchResult dfsCluster = firstCursor.getCurrentClusterResult();
                if (dfsCluster != null && dfsCluster.centroidId != firstCluster.centroidId) {
                    LOGGER.trace(
                            "DFS found cid={} but level-wise[0] is cid={} - re-opening all cursors to level-wise[0]",
                            dfsCluster.centroidId, firstCluster.centroidId);

                    for (int i = 0; i < numVTrees; i++) {
                        if (rangeCursors[i] instanceof VTreeSearchCursor) {
                            VTreeSearchCursor vcCursor = (VTreeSearchCursor) rangeCursors[i];
                            // Reset cluster counter since we're replacing the initial DFS cluster
                            // with level-wise[0], not advancing to a second cluster
                            vcCursor.resetClustersProbed();
                            vcCursor.openClusterByResult(firstCluster);
                        }
                    }

                    visitedSet.remove(dfsCluster.centroidId);
                }

                LOGGER.trace("Computed {} level-wise clusters, first cluster cid={} marked visited",
                        clusterStrategy.getLevelWiseClusterCount(), firstCluster.centroidId);
            } else {
                // No level-wise clusters - mark first cluster from DFS as visited
                ClusterSearchResult dfsFallbackCluster = firstCursor.getCurrentClusterResult();
                if (dfsFallbackCluster != null) {
                    visitedSet.add(dfsFallbackCluster.centroidId);
                    LOGGER.trace("Level-wise disabled, marked first DFS cluster as visited: cid={}",
                            dfsFallbackCluster.centroidId);
                }
            }
        }

        // Initialize priority queue for merging results from all components
        try {
            setPriorityQueueComparator();
            initPriorityQueue();
        } catch (Throwable th) { // NOSONAR Must catch all
            IndexCursorUtils.close(rangeCursors, th);
            throw HyracksDataException.create(th);
        }
    }

    @Override
    public void initPriorityQueue() throws HyracksDataException {
        // Initialize priority queue and populate with first element from each cursor
        int pqInitSize = (rangeCursors.length > 0) ? rangeCursors.length : 1;
        if (outputPriorityQueue == null) {
            outputPriorityQueue = new PriorityQueue<>(pqInitSize, pqCmp);
            pqes = new PriorityQueueElement[pqInitSize];
            for (int i = 0; i < pqInitSize; i++) {
                pqes[i] = new PriorityQueueElement(i);
            }
        } else {
            outputPriorityQueue.clear();
            if (pqInitSize != pqes.length) {
                pqes = new PriorityQueueElement[pqInitSize];
                for (int i = 0; i < pqInitSize; i++) {
                    pqes[i] = new PriorityQueueElement(i);
                }
            }
        }

        // Populate priority queue with first element from each cursor
        // If a cursor has no data (empty cluster), mark it as exhausted
        for (int i = 0; i < rangeCursors.length; i++) {
            if (rangeCursors[i].hasNext()) {
                rangeCursors[i].next();
                ITupleReference tuple = rangeCursors[i].getTuple();
                pqes[i].reset(tuple);
                outputPriorityQueue.offer(pqes[i]);
            } else {
                // Cursor has no data in initial cluster - mark as exhausted
                clusterExhausted[i] = true;
                LOGGER.trace("Component {} has empty initial cluster (marked exhausted)", i);
            }
        }

        // If ALL components started with empty clusters, advance all to the next cluster.
        boolean allInitiallyExhausted = true;
        for (int i = 0; i < clusterExhausted.length; i++) {
            if (!clusterExhausted[i]) {
                allInitiallyExhausted = false;
                break;
            }
        }

        if (allInitiallyExhausted) {
            LOGGER.trace("All components have empty initial cluster, advancing to next");
            advanceAllComponentsToNextCluster();
        }
    }

    @Override
    protected void pushIntoQueueFromCursorAndReplaceThisElement(PriorityQueueElement e) throws HyracksDataException {
        // Get next tuple from this cursor and add to priority queue
        int cursorIndex = e.getCursorIndex();
        if (rangeCursors[cursorIndex].hasNext()) {
            rangeCursors[cursorIndex].next();
            e.reset(rangeCursors[cursorIndex].getTuple());
            outputPriorityQueue.offer(e);
        }
    }

    /**
     * Check if accessor/cursor needs to be destroyed due to component type change.
     * This happens when a memory component is replaced with a disk component.
     */
    private boolean destroyIncompatible(ILSMComponent component, int index) throws HyracksDataException {
        // XOR: if component type changed (memory → disk or disk → memory)
        if (component.getType() == LSMComponentType.MEMORY ^ isMemoryComponent[index]) {
            Throwable failure = CleanupUtils.destroy(null, vTreeAccessors[index]);
            vTreeAccessors[index] = null;
            failure = CleanupUtils.destroy(failure, rangeCursors[index]);
            rangeCursors[index] = null;
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
            return true;
        }
        return false;
    }

    /**
     * Create accessor for a VTree component.
     */
    protected VTreeAccessor createAccessor(ILSMComponent component, int index) throws HyracksDataException {
        VTree vTree = (VTree) component.getIndex();
        // Get iap from operation context instead of using cursor's default iap
        LSMVTreeOpContext vcOpCtx = (LSMVTreeOpContext) opCtx;
        return (VTreeAccessor) vTree.createAccessor(vcOpCtx.getIndexAccessParameters());
    }

    /**
     * Create cursor for a VTree component.
     * Passes fullScanMode to enable sequential cluster iteration for merge operations.
     */
    protected IIndexCursor createCursor(LSMComponentType type, VTreeAccessor accessor) throws HyracksDataException {
        return accessor.createSearchCursor(false, fullScanMode);
    }

    @Override
    public void doClose() throws HyracksDataException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                    "Search summary: mode={}, K={}, nprobe={}, levelWiseClusters={}, levelWiseComplete={},"
                            + " minClustersProbed={}, tuplesProcessed={}, antimatter={}, cancellations={},"
                            + " filteredOut={}, tuplesProduced={}, getTupleCalls={}",
                    fullScanMode ? "MERGE" : "QUERY", K, nprobe,
                    clusterStrategy != null ? clusterStrategy.getLevelWiseClusterCount() : 0,
                    clusterStrategy != null && clusterStrategy.isLevelWisePhaseComplete(), getMinClustersProbed(),
                    totalTuplesPopped, antimatterTuplesDetected, cancellationsMade, tuplesFilteredOut, tuplesProduced,
                    getTupleCallCount);
        }
        super.doClose();
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        hasNextCallCount++;
        checkPriorityQueue();
        // Use priority queue - check if there's a valid element at the top
        return !outputPriorityQueue.isEmpty();
    }

    @Override
    public void doNext() throws HyracksDataException {
        // Pop element from priority queue and mark for replacement
        outputElement = outputPriorityQueue.poll();
        needPushElementIntoQueue = true;

        // Track total tuples popped (including those that may be cancelled)
        totalTuplesPopped++;

        // Count the tuple as produced HERE, not in doGetTuple(): getTuple() is not contractually called
        // exactly once per tuple, so counting there would inflate K-progress for any consumer that reads the
        // same tuple twice and trigger early termination sooner — silently reducing recall. Both counters
        // below feed cluster-advancement decisions (see pushIntoQueueAndAdvanceClusterIfNeeded).
        if (outputElement != null) {
            tuplesProduced++;
            validTuplesFromCurrentCluster++;
        }
    }

    @Override
    protected void checkPriorityQueue() throws HyracksDataException {
        // Periodically check if memory components need to be swapped with disk components
        checkAndSwitchMemoryComponentsIfNeeded();

        // Process queue until we find a valid output tuple or queue is exhausted
        while (!outputPriorityQueue.isEmpty() || needPushElementIntoQueue) {
            if (outputPriorityQueue.isEmpty()) {
                // Queue empty but pending element exists - refill it
                refillQueueFromPendingElement();
                continue;
            }

            PriorityQueueElement checkElement = outputPriorityQueue.peek();

            if (outputElement == null) {
                // No pending element - process top of queue
                if (processTopElement(checkElement)) {
                    break; // Found valid output tuple
                }
            } else {
                // Have pending element - check for antimatter cancellation
                processWithPendingElement(checkElement);
            }
        }
    }

    /**
     * Check if memory components should be swapped with disk components.
     */
    private void checkAndSwitchMemoryComponentsIfNeeded() throws HyracksDataException {
        if (hasNextCallCount >= SWITCH_COMPONENT_CYCLE) {
            replaceMemoryComponentWithDiskComponentIfNeeded();
            hasNextCallCount = 0;
        }
    }

    /**
     * Process the top element of the priority queue when no pending element exists.
     *
     * @param checkElement the element at top of queue
     * @return true if this is a valid output tuple, false if processing should continue
     */
    private boolean processTopElement(PriorityQueueElement checkElement) throws HyracksDataException {
        // Check if top element is antimatter
        if (isDeleted(checkElement) && !returnDeletedTuples) {
            // Antimatter tuple - hold for cancellation check with next tuple
            outputElement = outputPriorityQueue.poll();
            needPushElementIntoQueue = true;
            antimatterTuplesDetected++;
            return false; // Continue processing
        }

        // Matter tuple - apply INCLUDE field filter if configured
        if (!passesTupleFilter(checkElement)) {
            // Tuple fails filter - skip without counting toward K
            PriorityQueueElement skippedElement = outputPriorityQueue.poll();
            pushIntoQueueAndAdvanceClusterIfNeeded(skippedElement);
            return false; // Continue processing
        }

        // Valid output tuple - passed antimatter reconciliation and filter
        return true;
    }

    /**
     * Check if tuple passes the INCLUDE field filter.
     * Filter is applied AFTER antimatter reconciliation because:
     * 1. Antimatter from newer components comes first in queue (lower cursorIndex)
     * 2. By the time we reach here, any matching antimatter has cancelled this tuple
     * 3. Tuples that fail filter should NOT be counted toward K
     *
     * @param element the element to check
     * @return true if tuple passes filter (or no filter configured), false otherwise
     */
    private boolean passesTupleFilter(PriorityQueueElement element) throws HyracksDataException {
        if (tupleFilter == null) {
            return true;
        }

        ITupleReference tuple = element.getTuple();
        referenceFilterTuple.reset(tuple);

        if (tupleFilter.accept(referenceFilterTuple)) {
            return true;
        }

        // Tuple fails filter
        tuplesFilteredOut++;
        return false;
    }

    /**
     * Process queue element when we have a pending element (potential antimatter cancellation).
     *
     * @param checkElement the element at top of queue
     */
    private void processWithPendingElement(PriorityQueueElement checkElement) throws HyracksDataException {
        if (compare(cmp, outputElement.getTuple(), checkElement.getTuple()) == 0) {
            // Same <distance, PK> as the reference (a held antimatter, or the just-output matter). This
            // head is an older, superseded version of that key. Discard ONLY this head and pull the next
            // tuple from ITS cursor, keeping outputElement as the running reference and letting
            // checkPriorityQueue() keep looping so EVERY older same-key version drains — not just one.
            //
            // This mirrors LSMIndexSearchCursor's reconciliation. Draining only a single matter/antimatter
            // pair (and resetting the reference) would leak the 3rd+ version of the same key: e.g.
            // insert -> delete -> re-insert of the same vector across >=3 components would resurface the
            // deleted row and duplicate the key.
            //
            // Safe w.r.t. cluster advancing: the queue holds at most one element per component and the
            // reference's own component is held out (not advanced) during the drain, so its
            // clusterExhausted[] entry stays false and advanceAllComponentsToNextCluster() cannot fire
            // here (which would otherwise reset the reused pqes[] element backing outputElement).
            cancellationsMade++;
            PriorityQueueElement duplicate = outputPriorityQueue.poll();
            pushIntoQueueAndAdvanceClusterIfNeeded(duplicate);
        } else {
            // Different key - refill pending element's cursor
            refillPendingElementCursor();
        }
    }

    /**
     * Refill the pending element's cursor and reset state.
     */
    private void refillPendingElementCursor() throws HyracksDataException {
        if (needPushElementIntoQueue) {
            pushIntoQueueAndAdvanceClusterIfNeeded(outputElement);
            needPushElementIntoQueue = false;
        }
        outputElement = null;
    }

    /**
     * Refill queue from pending element when queue is empty.
     */
    private void refillQueueFromPendingElement() throws HyracksDataException {
        pushIntoQueueAndAdvanceClusterIfNeeded(outputElement);
        needPushElementIntoQueue = false;
        outputElement = null;
    }

    /**
     * Replace memory components with disk components if they were flushed.
     * This is called periodically to handle concurrent flushes during search.
     */
    private void replaceMemoryComponentWithDiskComponentIfNeeded() throws HyracksDataException {
        int replaceFrom = findFirstComponentToReplace();
        if (replaceFrom < 0) {
            // No switch needed
            return;
        }

        // Ask LSM harness to replace memory components with their flushed disk versions
        opCtx.getIndex().getHarness().replaceMemoryComponentsWithDiskComponents(getOpCtx(), replaceFrom);

        // Redo searches on the new disk components
        for (int i = replaceFrom; i < switchRequest.length && i < operationalComponents.size(); i++) {
            if (switchRequest[i]) {
                ILSMComponent component = operationalComponents.get(i);

                // Check if first component is now disk (no more mutable component)
                if (i == 0 && component.getType() != LSMComponentType.MEMORY) {
                    includeMutableComponent = false;
                }

                // If we had an active element from this component, restart search from that point
                if (switchedElements[i] != null) {
                    rangeCursors[i].close();
                    // Use the op-context's real access parameters (with the vector accessor factory + task
                    // context), NOT the cursor's NoOp default iap — otherwise the reopened disk cursor
                    // searches with degraded/missing config and can drop that component's results.
                    vTreeAccessors[i] = createAccessor(component, i);
                    vTreeAccessors[i].search(rangeCursors[i], searchPredicate);

                    // Try to position cursor at the same element
                    if (rangeCursors[i].hasNext()) {
                        rangeCursors[i].next();
                        switchedElements[i].reset(rangeCursors[i].getTuple());
                    }
                }
            }
            switchRequest[i] = false;
            switchedElements[i] = null;
            // Any failed switch makes further switches pointless
            switchPossible = switchPossible && operationalComponents.get(i).getType() == LSMComponentType.DISK;
        }
    }

    /**
     * Find the first component that needs to be replaced (has been flushed).
     * Returns the index of the first component to replace, or -1 if no replacement needed.
     */
    private int findFirstComponentToReplace() throws HyracksDataException {
        int replaceFrom = -1;

        if (!switchPossible) {
            return replaceFrom;
        }

        for (int i = 0; i < operationalComponents.size(); i++) {
            ILSMComponent component = operationalComponents.get(i);

            if (component.getType() == LSMComponentType.DISK) {
                if (i == 0) {
                    // First component is already disk, no more switching possible
                    switchPossible = false;
                }
                break;
            } else if (component.getState() == ILSMComponent.ComponentState.UNREADABLE_UNWRITABLE) {
                // Component was flushed while cursor is active - mark for replacement
                if (replaceFrom < 0) {
                    replaceFrom = i;
                }

                // Find the element from this cursor (if any)
                PriorityQueueElement element = findElementInQueue(i);

                // Mark this cursor for switching
                rangeCursors[i].close();
                switchRequest[i] = true;
                switchedElements[i] = element;
            }
        }

        return replaceFrom;
    }

    /**
     * Find an element in the priority queue or output element from a specific cursor.
     */
    private PriorityQueueElement findElementInQueue(int cursorIndex) {
        // Check if output element is from this cursor
        if (outputElement != null && outputElement.getCursorIndex() == cursorIndex) {
            return outputElement;
        }

        // Search in priority queue
        for (PriorityQueueElement element : outputPriorityQueue) {
            if (element.getCursorIndex() == cursorIndex) {
                return element;
            }
        }

        return null;
    }

    @Override
    protected void setPriorityQueueComparator() {
        // For vector index: sort by distance (field 0), then primary key (field pkStartField+)
        // Skip secondary fields between distance and PKs (centroidId, and optionally quantized fields)
        if (pqCmp == null || pqCmp.getMultiComparator() != cmp) {
            pqCmp = new VectorPriorityQueueComparator(cmp);
        }
    }

    /**
     * Custom priority queue comparator for vector index tuples.
     * Compares field 0 (distance) then fields from pkStartField onward (PKs + includes),
     * skipping secondary fields (centroidId, and optionally quantized_distance/quantized_embedding).
     */
    private class VectorPriorityQueueComparator extends PriorityQueueComparator {

        public VectorPriorityQueueComparator(MultiComparator cmp) {
            super(cmp);
        }

        @Override
        public int compare(PriorityQueueElement elementA, PriorityQueueElement elementB) {
            ITupleReference tupleA = elementA.getTuple();
            ITupleReference tupleB = elementB.getTuple();

            int result;
            try {
                // Merge ordering shares the exact <distance, PK> key with antimatter cancellation.
                result = compareKey(cmp, tupleA, tupleB);
            } catch (Throwable e) {
                throw new IllegalArgumentException(e);
            }
            if (result != 0) {
                return result;
            }

            // Tiebreaker: prefer tuples from earlier components
            if (elementA.getCursorIndex() > elementB.getCursorIndex()) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    @Override
    public ITupleReference doGetTuple() {
        getTupleCallCount++;
        return outputElement != null ? outputElement.getTuple() : null;
    }

    @Override
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    protected int compare(MultiComparator cmp, ITupleReference tupleA, ITupleReference tupleB)
            throws HyracksDataException {
        // Antimatter cancellation uses the same <distance, PK> key as the merge ordering.
        return compareKey(cmp, tupleA, tupleB);
    }

    /**
     * The <distance, PK> key shared by antimatter cancellation
     * ({@link #compare(MultiComparator, ITupleReference, ITupleReference)}) and merge ordering
     * ({@link VectorPriorityQueueComparator#compare}). Both must define the exact same key, otherwise a
     * delete marker and its live twin could sort apart and fail to cancel.
     *
     * <p>Tuple format (non-quantized): {@code [distance, centroidId, PKs..., includes...]};
     * quantized: {@code [distance, centroidId, quantized_distance, quantized_embedding, PKs..., includes...]}.
     * The key is exactly {@code <distance (field 0), PK fields>}: secondary fields (centroidId, and for
     * quantized layouts quantized_distance/quantized_embedding) and trailing INCLUDE fields are excluded —
     * they may differ between matter/antimatter twins.
     */
    private int compareKey(MultiComparator cmp, ITupleReference tupleA, ITupleReference tupleB)
            throws HyracksDataException {
        // Compare field 0 (distance) using ADM-aware comparator 0
        int result = cmp.getComparators()[0].compare(tupleA.getFieldData(0), tupleA.getFieldStart(0),
                tupleA.getFieldLength(0), tupleB.getFieldData(0), tupleB.getFieldStart(0), tupleB.getFieldLength(0));

        if (result != 0) {
            return result;
        }

        // Compare the PK fields starting at pkStartField
        int numRemainingFields = Math.min(cmp.getComparators().length - pkStartField, numPrimaryKeyFields);
        for (int i = 0; i < numRemainingFields; i++) {
            int fieldIdx = pkStartField + i;
            int cmpIdx = pkStartField + i;

            // Check if field exists in tuple before comparing
            if (fieldIdx >= tupleA.getFieldCount() || fieldIdx >= tupleB.getFieldCount()) {
                break;
            }

            result = cmp.getComparators()[cmpIdx].compare(tupleA.getFieldData(fieldIdx), tupleA.getFieldStart(fieldIdx),
                    tupleA.getFieldLength(fieldIdx), tupleB.getFieldData(fieldIdx), tupleB.getFieldStart(fieldIdx),
                    tupleB.getFieldLength(fieldIdx));
            if (result != 0) {
                return result;
            }
        }

        return 0;
    }

    @Override
    protected boolean isDeleted(PriorityQueueElement element) throws HyracksDataException {
        // Check if tuple has antimatter bit set (indicates deleted record).
        // During merge with full-scan mode, tuples may be rebuilt as ArrayTupleReference
        // which doesn't have antimatter bit - treat those as matter tuples.
        ITupleReference tuple = element.getTuple();
        return tuple instanceof ILSMTreeTupleReference && ((ILSMTreeTupleReference) tuple).isAntimatter();
    }

    /**
     * Extract K value from search predicate for cluster advancement decisions.
     */
    private int extractK(ISearchPredicate searchPred) {
        return ((VTreeSearchPredicate) searchPred).getK();
    }

    /**
     * Extract minProbeFraction from search predicate (fraction of leaf clusters to probe).
     */
    private double extractMinProbeFraction(ISearchPredicate searchPred) {
        return ((VTreeSearchPredicate) searchPred).getMinProbeFraction();
    }

    /**
     * Extract epsilon value from search predicate (distance threshold for level-wise).
     */
    private double extractEpsilon(ISearchPredicate searchPred) {
        return ((VTreeSearchPredicate) searchPred).getEpsilon();
    }

    /**
     * Get the minimum number of clusters probed across all VTreeSearchCursors.
     * This ensures synchronized nprobe checking across all LSM components.
     */
    private int getMinClustersProbed() {
        int minProbed = Integer.MAX_VALUE;

        for (int i = 0; i < rangeCursors.length; i++) {
            VTreeSearchCursor vcCursor = (VTreeSearchCursor) rangeCursors[i];
            int probed = vcCursor.getClustersProbed();
            if (probed < minProbed) {
                minProbed = probed;
            }
        }

        // If no VTreeSearchCursors found, return 0
        return minProbed == Integer.MAX_VALUE ? 0 : minProbed;
    }

    /**
     * Extract tuple filter from search predicate for INCLUDE field predicates.
     * When set, only tuples passing this filter are returned and counted toward K.
     */
    private ITupleFilter extractTupleFilter(ISearchPredicate searchPred) {
        if (searchPred instanceof VTreeSearchPredicate) {
            return ((VTreeSearchPredicate) searchPred).getTupleFilter();
        }
        return null;
    }

    /**
     * Push next element from component cursor into queue.
     * If cursor's current cluster is exhausted, mark it as exhausted.
     * When ALL components' clusters are exhausted, decide whether to advance ALL to next cluster.
     *
     * @param e the priority queue element to refill
     * @throws HyracksDataException if an error occurs
     */
    private void pushIntoQueueAndAdvanceClusterIfNeeded(PriorityQueueElement e) throws HyracksDataException {
        int cursorIndex = e.getCursorIndex();
        IIndexCursor cursor = rangeCursors[cursorIndex];

        if (cursor.hasNext()) {
            // Current cluster/page has more data
            cursor.next();
            ITupleReference tuple = cursor.getTuple();
            e.reset(tuple);
            outputPriorityQueue.offer(e);

            return;
        }

        // Current cluster exhausted for THIS component
        clusterExhausted[cursorIndex] = true;

        // Check if ALL components have exhausted their current cluster
        boolean allExhausted = true;
        for (int i = 0; i < clusterExhausted.length; i++) {
            if (!clusterExhausted[i]) {
                allExhausted = false;
                break;
            }
        }

        if (!allExhausted) {
            // Some components still have data in their current cluster
            // Don't advance yet - wait for all to exhaust
            return;
        }

        // ALL components exhausted their current cluster
        // Decision: Should we advance ALL components to next cluster?
        if (stopAdvancing) {
            return;
        }

        // Calculate minimum clusters explored across all components
        int minClustersExplored = getMinClustersProbed();
        int resultsCollected = tuplesProduced;

        if (clusterStrategy.shouldStopAdvancing(minClustersExplored, resultsCollected)) {
            // We have enough results AND probed enough clusters - stop advancing
            stopAdvancing = true;
            LOGGER.trace("Early termination: minClustersExplored={}, returned={}", minClustersExplored,
                    resultsCollected);
            return;
        }

        // Not enough results or haven't probed enough clusters yet - advance ALL components
        // Exclude empty clusters from nprobe (query mode only)
        if (!fullScanMode && validTuplesFromCurrentCluster == 0) {
            for (int i = 0; i < rangeCursors.length; i++) {
                if (rangeCursors[i] instanceof VTreeSearchCursor) {
                    ((VTreeSearchCursor) rangeCursors[i]).decrementClustersProbed();
                }
            }
        }
        validTuplesFromCurrentCluster = 0;
        advanceAllComponentsToNextCluster();
    }

    /**
     * Advance ALL component cursors to the SAME next cluster (determined by the strategy in query
     * mode, or by the first cursor's own iterator in full-scan/merge mode). Synchronized
     * advancement is required so nprobe accounting is consistent across components.
     * <p>
     * Iterates instead of recursing to tolerate long runs of consecutive empty clusters without
     * blowing the stack.
     */
    private void advanceAllComponentsToNextCluster() throws HyracksDataException {
        while (true) {
            // Reset exhaustion flags for new cluster
            Arrays.fill(clusterExhausted, false);

            if (fullScanMode) {
                // Full-scan mode (merge): each cursor advances independently using its own
                // allDirectoryPageIds. Directory page IDs are component-local and cannot be
                // shared across components (bulk-loaded vs flushed components have different layouts).
                boolean anyAdvanced = false;
                for (int i = 0; i < rangeCursors.length; i++) {
                    if (!(rangeCursors[i] instanceof VTreeSearchCursor)) {
                        clusterExhausted[i] = true;
                        continue;
                    }
                    VTreeSearchCursor vcCursor = (VTreeSearchCursor) rangeCursors[i];
                    boolean advanced = vcCursor.advanceToNextCluster();
                    if (!advanced) {
                        clusterExhausted[i] = true;
                        continue;
                    }
                    anyAdvanced = true;
                    if (vcCursor.hasNext()) {
                        vcCursor.next();
                        pqes[i].reset(vcCursor.getTuple());
                        outputPriorityQueue.offer(pqes[i]);
                    } else {
                        clusterExhausted[i] = true;
                    }
                }
                if (!anyAdvanced) {
                    // All cursors exhausted all clusters
                    return;
                }
                if (!shouldSkipToNextCluster()) {
                    return;
                }
                continue; // All empty, try next cluster
            }

            // Query mode: strategy determines the cluster ALL components should advance to
            ClusterSearchResult nextCluster = clusterStrategy.getNextCluster();

            if (nextCluster == null) {
                LOGGER.trace("No more clusters available globally");
                Arrays.fill(clusterExhausted, true);
                return;
            }

            LOGGER.trace("Global cluster selected: cid={}, distance={}, dirPage={}", nextCluster.centroidId,
                    nextCluster.distance, nextCluster.directoryPageId);

            // Tell ALL components to open this SAME cluster
            for (int i = 0; i < rangeCursors.length; i++) {
                advanceComponentToCluster(i, nextCluster);
            }

            // Check if all components found empty cluster
            // If so, continue loop to try next cluster (instead of recursion)
            if (!shouldSkipToNextCluster()) {
                return; // At least one component has data, or we should stop advancing
            }
            // All components empty and should continue - loop to next cluster
        }
    }

    /**
     * Advance a single component to a specific cluster using ClusterSearchResult.
     * Uses O(1) directoryPageId access when available.
     */
    private void advanceComponentToCluster(int componentIndex, ClusterSearchResult cluster)
            throws HyracksDataException {
        IIndexCursor cursor = rangeCursors[componentIndex];

        if (!(cursor instanceof VTreeSearchCursor)) {
            clusterExhausted[componentIndex] = true;
            return;
        }

        VTreeSearchCursor vcCursor = (VTreeSearchCursor) cursor;

        // O(1) access via directoryPageId
        boolean hasData = vcCursor.openClusterByResult(cluster);

        if (hasData && vcCursor.hasNext()) {
            vcCursor.next();
            ITupleReference tuple = vcCursor.getTuple();
            PriorityQueueElement pqe = pqes[componentIndex];
            pqe.reset(tuple);
            outputPriorityQueue.offer(pqe);
        } else {
            clusterExhausted[componentIndex] = true;
            LOGGER.trace("advCluster: comp={} cluster={} is empty", componentIndex, cluster.centroidId);
        }
    }

    /**
     * Check if we should skip to next cluster (all components empty) or stop.
     * Returns true if we should continue to next cluster, false otherwise.
     */
    private boolean shouldSkipToNextCluster() {
        // Check if ALL components found the cluster empty
        boolean allExhausted = true;
        for (int i = 0; i < clusterExhausted.length; i++) {
            if (!clusterExhausted[i]) {
                allExhausted = false;
                break;
            }
        }

        if (!allExhausted) {
            // At least one component has data - don't skip
            return false;
        }

        // All components exhausted this cluster - check if we should continue
        if (stopAdvancing) {
            return false;
        }

        // Check if any component has more clusters available
        boolean hasMoreClusters;
        if (fullScanMode) {
            // In fullScanMode, check cursors directly (strategy is not used for advancement)
            hasMoreClusters = false;
            for (IIndexCursor cursor : rangeCursors) {
                if (cursor instanceof VTreeSearchCursor && ((VTreeSearchCursor) cursor).hasMoreClusters()) {
                    hasMoreClusters = true;
                    break;
                }
            }
        } else {
            hasMoreClusters = clusterStrategy.hasMoreClusters();
        }
        if (!hasMoreClusters) {
            return false;
        }

        return true; // Continue to next cluster
    }
}
