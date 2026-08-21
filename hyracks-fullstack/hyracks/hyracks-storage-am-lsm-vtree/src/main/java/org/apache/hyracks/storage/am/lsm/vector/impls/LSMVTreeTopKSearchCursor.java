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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.tuples.ReferenceFrameTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.vector.utils.LSMVTreeUtils;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessor;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.impls.VTree.VTreeAccessor;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchPredicate;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Blocked top-K cursor for vector ANN search.
 * <p>
 * Lifecycle: all per-component cursor work — priority-queue merge across LSM components, antimatter
 * reconciliation, INCLUDE-field filtering, approximate distance computation against the quantized
 * embedding — happens inside {@link #open}. Surviving candidates are collected into a
 * {@link SpillableTopKBuffer} (in-memory MaxHeap keyed by approximate distance, with optional
 * disk spill on memory pressure). {@link #hasNext()}/{@link #next()}/{@link #getTuple()} then
 * drain that buffer in approximate-distance ascending order.
 * <p>
 * This cursor is for quantized indexes: the tuple format is
 * {@code [distance, centroidId, quantized_distance, quantized_embedding, PKs..., includes...]}
 * (pkStartField=4). Reranking against the unquantized vector is the caller's responsibility.
 */
public class LSMVTreeTopKSearchCursor extends EnforcedIndexCursor implements IVectorSearchCursor {

    /**
     * Index-access-parameters key — set {@code Boolean.TRUE} under this key to route search to
     * this cursor (the quantized top-K window cursor used by production ANN queries). When absent
     * or false, {@link LSMVTreeIndexAccessor#createSearchCursor(boolean)} returns the streaming
     * {@link LSMVTreeSearchCursor} — also the cursor used by component merges and by test
     * fixtures that verify inserts/deletes through full-scan iteration.
     */
    public static final String IAP_KEY = "USE_TOPK_SEARCH";

    private static final Logger LOGGER = LogManager.getLogger();

    // Operation context
    private ILSMIndexOperationContext opCtx;
    private List<ILSMComponent> operationalComponents;

    // Per-component accessors and cursors (same as LSMVTreeSearchCursor)
    private VTreeAccessor[] vTreeAccessors;
    private IIndexCursor[] rangeCursors;
    private int numComponents;

    // Priority queue for merging results from multiple components
    private PriorityQueue<PriorityQueueElement> outputPriorityQueue;
    private PriorityQueueElement[] pqes;
    private MultiComparator cmp;

    // Antimatter reconciliation: stable COPIES of matters that survived reconciliation of the current
    // equal-distance group, buffered for emission one at a time. See getNextValidTuple.
    private final ArrayDeque<ITupleReference> readyMatters = new ArrayDeque<>();
    // Number of primary-key fields (INCLUDE fields excluded) — the reconciliation key.
    private int numPrimaryKeyFields;

    // Spillable top-K buffer: frame-backed in-memory heap with disk spill on budget exceeded.
    // Encapsulates MaxHeap + VariableDeletableTupleMemoryManager + RunFileWriter spill.
    private SpillableTopKBuffer topKBuffer;
    private SpillableTopKDrainIterator drainIterator;

    // Search parameters
    private int K;
    /** Number of candidates to collect for reranking: {@code K * max(1, kMultiplier)} from the predicate. */
    private int candidateLimit;
    private double epsilon;
    private double[] queryVector;
    private IVTreeDistanceFunction distanceFunction;

    // Vector accessor for extracting vectors from tuples
    private IVTreeBinaryAccessor vectorAccessor;

    // Quantization state (propagated from first search cursor)
    private double[] quantizedQueryVector;
    private IVTreeQuantizer quantizer;

    // Data-tuple layout authority. This cursor is quantized-only (see class javadoc), so the layout
    // is the quantized one.
    private final VTreeDataTupleAccessor dataAccessor = new VTreeDataTupleAccessor(true);

    // Cluster selection strategy (nprobe + DFS fallback)
    private IClusterSelectionStrategy clusterStrategy;

    // First component's search cursor (for query vector/distance function extraction and DFS)
    private VTreeSearchCursor firstSearchCursor;

    // Cluster tracking (synchronized advancement like LSMVTreeSearchCursor)
    private boolean[] clusterExhausted;
    private boolean stopAdvancing;
    private int clustersExplored;

    // Tuple filter for INCLUDE field predicates (e.g., year > 2000)
    private ITupleFilter tupleFilter;
    private ReferenceFrameTupleReference referenceFilterTuple;

    // Field index where primary keys start in the data tuple
    private int pkStartField;

    // Statistics
    private int totalTuplesProcessed;
    private int nextCallCount;
    private int antimatterCancellations;
    private int tuplesFilteredOut;
    private int validTuplesFromCurrentCluster; // Valid tuples from current cluster (for empty-cluster nprobe)

    public LSMVTreeTopKSearchCursor(ILSMIndexOperationContext opCtx) {
        this.opCtx = opCtx;
    }

    @Override
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        this.totalTuplesProcessed = 0;
        this.nextCallCount = 0;
        this.antimatterCancellations = 0;
        this.tuplesFilteredOut = 0;
        this.validTuplesFromCurrentCluster = 0;
        this.readyMatters.clear();

        // Get initial state
        LSMVTreeCursorInitialState lsmInitialState = (LSMVTreeCursorInitialState) initialState;
        this.cmp = lsmInitialState.getOriginalKeyComparator();
        this.operationalComponents = lsmInitialState.getOperationalComponents();
        this.numComponents = operationalComponents.size();

        // Extract search parameters from predicate
        VTreeSearchPredicate vectorPred = (VTreeSearchPredicate) searchPred;
        this.K = vectorPred.getK();
        int mult = vectorPred.getKMultiplier();
        this.candidateLimit = K * Math.max(1, mult); // Send K*kMultiplier to PK for reranking
        this.epsilon = vectorPred.getEpsilon();
        this.pkStartField = dataAccessor.pkStartField();
        this.numPrimaryKeyFields = ((LSMVTree) opCtx.getIndex()).getNumPrimaryKeyFields();

        // This cursor is quantized-only: dataAccessor is fixed to the quantized layout, so it reads field 3
        // as the quantized embedding and locates the PKs at pkStartField = 4. It is selected purely by the
        // USE_TOPK_SEARCH access parameter, which is independent of the index's quantization — so check the
        // assumption instead of inheriting it. On a non-quantized index field 3 is a PK/INCLUDE field and the
        // whole read would be silent garbage.
        if (!((LSMVTree) opCtx.getIndex()).isQuantized()) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "LSMVTreeTopKSearchCursor requires a quantized VTree index (USE_TOPK_SEARCH was requested for a "
                            + "non-quantized index)");
        }
        LSMVTreeUtils.validateKeyComparators(cmp, pkStartField, numPrimaryKeyFields);

        // Extract tuple filter from search predicate for INCLUDE field predicates
        this.tupleFilter = vectorPred.getTupleFilter();
        if (this.tupleFilter != null) {
            this.referenceFilterTuple = new ReferenceFrameTupleReference();
        }

        // Get index access parameters
        IIndexAccessParameters iap = ((LSMVTreeOpContext) opCtx).getIndexAccessParameters();

        // Initialize vector accessor from factory in parameters
        IVTreeBinaryAccessorFactory vectorAccessorFactory =
                (IVTreeBinaryAccessorFactory) iap.getParameters().get(IVTreeBinaryAccessorFactory.IAP_KEY);
        if (vectorAccessorFactory != null) {
            this.vectorAccessor = vectorAccessorFactory.createAccessor();
        }

        // Create cluster selection strategy (minProbeFraction → nprobe + DFS fallback)
        this.clusterStrategy = new NprobeClusterSelectionStrategy(vectorPred.getMinProbeFraction(), epsilon);

        // Create spillable top-K buffer (follows inverted index pattern: pass ctx via IAP)
        IHyracksTaskContext ctx = (IHyracksTaskContext) iap.getParameters().get(HyracksConstants.HYRACKS_TASK_CONTEXT);
        this.topKBuffer = new SpillableTopKBuffer(candidateLimit, ctx);

        // Initialize cluster tracking arrays
        clusterExhausted = new boolean[numComponents];
        Arrays.fill(clusterExhausted, false);
        stopAdvancing = false;
        clustersExplored = 0;

        // Create accessors and cursors for each component (same as LSMVTreeSearchCursor)
        vTreeAccessors = new VTreeAccessor[numComponents];
        rangeCursors = new IIndexCursor[numComponents];

        try {
            openComponentsAndSearch(searchPred, iap);
        } catch (Throwable th) { // NOSONAR must catch all: release per-component cursors/accessors + the
                                     // spill buffer here, since a failed open() leaves the caller no handle to close.
            Throwable failure = CleanupUtils.destroy(th, vTreeAccessors);
            failure = CleanupUtils.destroy(failure, rangeCursors);
            if (topKBuffer != null) {
                try {
                    topKBuffer.close();
                } catch (Throwable t) { // NOSONAR
                    failure.addSuppressed(t);
                }
                topKBuffer = null;
            }
            throw HyracksDataException.create(failure);
        }
    }

    /**
     * Body of {@link #open}: create and open a search cursor per component, initialize the cluster
     * selection strategy, and run the blocked search. Extracted so {@code open()} can wrap it in one
     * try/catch that releases all of open()'s resources on any failure.
     */
    private void openComponentsAndSearch(ISearchPredicate searchPred, IIndexAccessParameters iap)
            throws HyracksDataException {
        for (int i = 0; i < numComponents; i++) {
            ILSMComponent component = operationalComponents.get(i);
            VTree vTree = (VTree) component.getIndex();
            vTreeAccessors[i] = (VTreeAccessor) vTree.createAccessor(iap);
            rangeCursors[i] = vTreeAccessors[i].createSearchCursor(false);
        }

        // Open all cursors with the search predicate
        IndexCursorUtils.open(vTreeAccessors, rangeCursors, searchPred);

        // Initialize strategy and set up DFS fallback (same as LSMVTreeSearchCursor)
        if (numComponents > 0) {
            this.firstSearchCursor = (VTreeSearchCursor) rangeCursors[0];
            this.queryVector = firstSearchCursor.getQueryVector();
            this.distanceFunction = firstSearchCursor.getDistanceFunction();

            // Extract quantized state from first cursor (null = non-quantized path)
            this.quantizedQueryVector = firstSearchCursor.getQuantizedQueryVector();
            this.quantizer = firstSearchCursor.getQuantizer();

            if (this.queryVector == null) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "A query vector is required for the vector index blocked search");
            }

            // computeApproximateDistance() dequantizes every candidate with these two, and they arrive
            // independently of the USE_TOPK_SEARCH flag that selected this cursor (from a quantizer factory
            // or instance in the index access parameters). Fail here rather than NPE per candidate.
            if (this.quantizer == null || this.quantizedQueryVector == null) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "LSMVTreeTopKSearchCursor requires a quantizer and a quantized query vector; none was supplied "
                                + "through the index access parameters");
            }

            // Initialize strategy with first component's tree (candidateLimit so we collect 2*K for reranking)
            ILSMComponent firstComponent = operationalComponents.get(0);
            VTree vTree = (VTree) firstComponent.getIndex();
            clusterStrategy.initialize(vTree, queryVector, distanceFunction, candidateLimit);

            // Set first cursor for DFS fallback
            clusterStrategy.setFirstCursorForDFS(firstSearchCursor);

            // Pass shared visited set from strategy to all cursors
            Set<Integer> visitedSet = clusterStrategy.getVisitedCentroidIds();
            for (int i = 0; i < numComponents; i++) {
                if (rangeCursors[i] instanceof VTreeSearchCursor) {
                    ((VTreeSearchCursor) rangeCursors[i]).setSharedVisitedSet(visitedSet);
                }
            }

            // Re-open all cursors to the first level-wise cluster for consistency
            ClusterSearchResult firstCluster = clusterStrategy.getFirstCluster();
            if (firstCluster != null) {
                ClusterSearchResult dfsCluster = firstSearchCursor.getCurrentClusterResult();
                if (dfsCluster != null && dfsCluster.centroidId != firstCluster.centroidId) {
                    LOGGER.trace("DFS found cid={} but level-wise[0] is cid={} - re-opening", dfsCluster.centroidId,
                            firstCluster.centroidId);
                    // The greedy-DFS seed (dfsCluster) was consumed from every component cursor's DFS
                    // iterator and marked visited by initializeClusterIterator. We are displacing it here
                    // for the globally-closest level-wise[0]. Defer it to the strategy so it is probed once
                    // after the level-wise phase — otherwise its cluster is never scanned and records routed
                    // there (cross_pollination_m=1: no replica) silently disappear from results. All
                    // components share one static structure, so every cursor's greedy seed is this centroid.
                    clusterStrategy.deferSeedCluster(dfsCluster);
                    for (int i = 0; i < numComponents; i++) {
                        if (rangeCursors[i] instanceof VTreeSearchCursor) {
                            VTreeSearchCursor vcCursor = (VTreeSearchCursor) rangeCursors[i];
                            vcCursor.resetClustersProbed();
                            vcCursor.openClusterByResult(firstCluster);
                        }
                    }
                }
                LOGGER.trace("Initialized with K={}, epsilon={}, level-wise clusters={}", K, epsilon,
                        clusterStrategy.getLevelWiseClusterCount());
            }
        }

        // Initialize priority queue for merging results from all components
        initPriorityQueue();

        // Perform the blocked search: drain all clusters and collect results
        performBlockedSearch();

        // Prepare drain: sort entries by dqx ascending for output (with merge if spilled)
        this.drainIterator = topKBuffer.drain();

        LOGGER.trace("Search complete: topK={}, processed={}, filtered={}, cancellations={}, clusters={}",
                topKBuffer.getNumEntries(), totalTuplesProcessed, tuplesFilteredOut, antimatterCancellations,
                clustersExplored);
    }

    /**
     * Initialize priority queue and populate with first element from each cursor.
     */
    private void initPriorityQueue() throws HyracksDataException {
        int pqInitSize = Math.max(numComponents, 1);
        outputPriorityQueue = new PriorityQueue<>(pqInitSize, new NaivePriorityQueueComparator());
        pqes = new PriorityQueueElement[pqInitSize];
        for (int i = 0; i < pqInitSize; i++) {
            pqes[i] = new PriorityQueueElement(i);
        }

        // Populate priority queue with first element from each cursor
        for (int i = 0; i < numComponents; i++) {
            if (rangeCursors[i].hasNext()) {
                rangeCursors[i].next();
                pqes[i].reset(rangeCursors[i].getTuple());
                outputPriorityQueue.offer(pqes[i]);
            } else {
                clusterExhausted[i] = true;
            }
        }

        clustersExplored = 1; // First cluster opened

        // If all components started empty, advance to next cluster
        if (allComponentsExhausted()) {
            advanceAllComponentsToNextCluster();
        }
    }

    /**
     * Perform the blocked search: drain the priority queue, apply antimatter reconciliation
     * and filtering, compute distances, and collect results into topKWindow.
     *
     * This continues until we've probed enough clusters and have enough results,
     * or all clusters are exhausted.
     */
    private void performBlockedSearch() throws HyracksDataException {
        while (true) {
            // Process current cluster's data via priority queue (getNextValidTuple returns null once the
            // queue AND the reconciled-matter buffer are both drained).
            ITupleReference validTuple;
            while ((validTuple = getNextValidTuple()) != null) {
                // Apply INCLUDE field filter
                if (passesTupleFilter(validTuple)) {
                    validTuplesFromCurrentCluster++;
                    // Compute approximate distance using quantized embedding
                    double dqx = computeApproximateDistance(validTuple);
                    topKBuffer.insert(validTuple, dqx);
                }
                totalTuplesProcessed++;
            }

            // Current cluster(s) exhausted - check if we should advance
            if (stopAdvancing) {
                break;
            }

            // Check strategy for stop condition
            int minClustersExplored = getMinClustersProbed();
            if (clusterStrategy.shouldStopAdvancing(minClustersExplored, topKBuffer.getNumEntries())) {
                stopAdvancing = true;
                LOGGER.trace("Early termination: clusters={}, topK={}", minClustersExplored,
                        topKBuffer.getNumEntries());
                break;
            }

            // Try to advance to next cluster
            if (!clusterStrategy.hasMoreClusters()) {
                LOGGER.trace("No more clusters available");
                break;
            }

            // Exclude empty clusters from nprobe
            if (validTuplesFromCurrentCluster == 0) {
                for (int i = 0; i < rangeCursors.length; i++) {
                    if (rangeCursors[i] instanceof VTreeSearchCursor) {
                        ((VTreeSearchCursor) rangeCursors[i]).decrementClustersProbed();
                    }
                }
            }
            validTuplesFromCurrentCluster = 0;
            advanceAllComponentsToNextCluster();
        }
    }

    /**
     * Get next valid tuple with antimatter reconciliation.
     * Follows the checkPriorityQueue() pattern from LSMVTreeSearchCursor.
     *
     * @return next valid matter tuple, or null if queue exhausted
     */
    private ITupleReference getNextValidTuple() throws HyracksDataException {
        while (true) {
            if (!readyMatters.isEmpty()) {
                return readyMatters.poll();
            }
            if (outputPriorityQueue.isEmpty()) {
                return null;
            }

            // Drain the entire run of elements that share field-0 (distance-to-centroid) across ALL
            // component streams, then reconcile by primary key. This is required because the merge order
            // is by distance, NOT by PK: a per-component cursor iterates same-distance tuples in insertion
            // order, so a delete marker and its live twin can be separated by an unrelated same-distance
            // record. Reconciling the whole group by PK — instead of only cancelling against the adjacent
            // queue element — cancels the twin wherever it lands in the run, so a deleted record cannot
            // resurface (see LSMVTreeAntimatterCollisionQuantizedTest).
            //
            // Each element's tuple points into its component cursor's frame and is invalidated when that
            // cursor advances, so capture a stable COPY before pushIntoQueueAndAdvanceClusterIfNeeded.
            PriorityQueueElement first = outputPriorityQueue.poll();
            ITupleReference groupKey = TupleUtils.copyTuple(first.tuple);
            List<ReconcileEntry> group = new ArrayList<>();
            group.add(new ReconcileEntry(groupKey, first.componentId, isAntimatter(first.tuple)));
            pushIntoQueueAndAdvanceClusterIfNeeded(first);
            while (!outputPriorityQueue.isEmpty() && compareField0(outputPriorityQueue.peek().tuple, groupKey) == 0) {
                PriorityQueueElement e = outputPriorityQueue.poll();
                group.add(new ReconcileEntry(TupleUtils.copyTuple(e.tuple), e.componentId, isAntimatter(e.tuple)));
                pushIntoQueueAndAdvanceClusterIfNeeded(e);
            }
            reconcileGroupByPrimaryKey(group);
        }
    }

    /**
     * Reconcile one equal-distance group by primary key using LSM newest-wins semantics: for each PK, the
     * entry from the newest component (lowest componentId) determines presence — if it is matter it is
     * emitted once, if it is a delete marker the record is suppressed and no older matter for that PK
     * survives. Surviving matters are appended to {@link #readyMatters}.
     */
    private void reconcileGroupByPrimaryKey(List<ReconcileEntry> group) throws HyracksDataException {
        for (ReconcileEntry e : group) {
            boolean newestForPk = true;
            for (ReconcileEntry other : group) {
                if (other != e && other.componentId < e.componentId && samePrimaryKey(e.tuple, other.tuple)) {
                    newestForPk = false;
                    break;
                }
            }
            if (!newestForPk) {
                continue; // shadowed by a newer tuple for the same PK
            }
            if (e.antimatter) {
                antimatterCancellations++; // newest is a delete marker → record is absent
            } else {
                readyMatters.add(e.tuple);
            }
        }
    }

    /** A stable copy of one queue element, captured before its cursor advances (see getNextValidTuple). */
    private static final class ReconcileEntry {
        private final ITupleReference tuple;
        private final int componentId;
        private final boolean antimatter;

        private ReconcileEntry(ITupleReference tuple, int componentId, boolean antimatter) {
            this.tuple = tuple;
            this.componentId = componentId;
            this.antimatter = antimatter;
        }
    }

    /** Compare field 0 (distance-to-centroid), the priority-queue ordering key. */
    private int compareField0(ITupleReference a, ITupleReference b) throws HyracksDataException {
        return cmp.getComparators()[0].compare(a.getFieldData(0), a.getFieldStart(0), a.getFieldLength(0),
                b.getFieldData(0), b.getFieldStart(0), b.getFieldLength(0));
    }

    /**
     * True iff two tuples carry the same primary key. Only the {@code numPrimaryKeyFields} PK fields are
     * compared — trailing INCLUDE fields are excluded, since a delete marker and its live twin may differ
     * in INCLUDE values and must still reconcile.
     */
    private boolean samePrimaryKey(ITupleReference a, ITupleReference b) throws HyracksDataException {
        int numPkFields = Math.min(cmp.getComparators().length - pkStartField, numPrimaryKeyFields);
        for (int i = 0; i < numPkFields; i++) {
            int fieldIdx = pkStartField + i;
            if (fieldIdx >= a.getFieldCount() || fieldIdx >= b.getFieldCount()) {
                return false;
            }
            if (cmp.getComparators()[fieldIdx].compare(a.getFieldData(fieldIdx), a.getFieldStart(fieldIdx),
                    a.getFieldLength(fieldIdx), b.getFieldData(fieldIdx), b.getFieldStart(fieldIdx),
                    b.getFieldLength(fieldIdx)) != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Push next element from component cursor into queue.
     * If cursor's current cluster is exhausted, mark it as exhausted.
     * When ALL components' clusters are exhausted, the loop in performBlockedSearch handles advancement.
     */
    private void pushIntoQueueAndAdvanceClusterIfNeeded(PriorityQueueElement e) throws HyracksDataException {
        int cursorIndex = e.componentId;
        IIndexCursor cursor = rangeCursors[cursorIndex];

        if (cursor.hasNext()) {
            cursor.next();
            e.reset(cursor.getTuple());
            outputPriorityQueue.offer(e);
            return;
        }

        // Current cluster exhausted for this component
        clusterExhausted[cursorIndex] = true;
    }

    /**
     * Advance ALL component cursors to the next cluster.
     * Uses iterative loop to handle consecutive empty clusters.
     */
    private void advanceAllComponentsToNextCluster() throws HyracksDataException {
        while (true) {
            Arrays.fill(clusterExhausted, false);

            ClusterSearchResult nextCluster = clusterStrategy.getNextCluster();
            if (nextCluster == null) {
                LOGGER.trace("No more clusters available globally");
                Arrays.fill(clusterExhausted, true);
                stopAdvancing = true;
                return;
            }

            LOGGER.trace("Advancing to cluster cid={}, distance={}, dirPage={}", nextCluster.centroidId,
                    nextCluster.distance, nextCluster.directoryPageId);

            // Open all components to this cluster
            for (int i = 0; i < numComponents; i++) {
                advanceComponentToCluster(i, nextCluster);
            }
            clustersExplored++;

            // Check if all components found empty cluster - try next
            if (!allComponentsExhausted()) {
                return; // At least one component has data
            }

            // All empty - check if should continue
            if (!clusterStrategy.hasMoreClusters()) {
                stopAdvancing = true;
                return;
            }
            // Loop to try next cluster
        }
    }

    /**
     * Advance a single component to a specific cluster.
     */
    private void advanceComponentToCluster(int componentIndex, ClusterSearchResult cluster)
            throws HyracksDataException {
        IIndexCursor cursor = rangeCursors[componentIndex];

        if (!(cursor instanceof VTreeSearchCursor)) {
            clusterExhausted[componentIndex] = true;
            return;
        }

        VTreeSearchCursor vcCursor = (VTreeSearchCursor) cursor;
        boolean hasData = vcCursor.openClusterByResult(cluster);

        if (hasData && vcCursor.hasNext()) {
            vcCursor.next();
            pqes[componentIndex].reset(vcCursor.getTuple());
            outputPriorityQueue.offer(pqes[componentIndex]);
        } else {
            clusterExhausted[componentIndex] = true;
        }
    }

    /**
     * Check if all components have exhausted their current cluster.
     */
    private boolean allComponentsExhausted() {
        for (boolean exhausted : clusterExhausted) {
            if (!exhausted) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get the minimum number of clusters probed across all VTreeSearchCursors.
     */
    private int getMinClustersProbed() {
        int minProbed = Integer.MAX_VALUE;
        for (int i = 0; i < rangeCursors.length; i++) {
            if (rangeCursors[i] instanceof VTreeSearchCursor) {
                int probed = ((VTreeSearchCursor) rangeCursors[i]).getClustersProbed();
                if (probed < minProbed) {
                    minProbed = probed;
                }
            }
        }
        return minProbed == Integer.MAX_VALUE ? 0 : minProbed;
    }

    /**
     * Check if tuple is antimatter.
     */
    private boolean isAntimatter(ITupleReference tuple) {
        if (tuple instanceof ILSMTreeTupleReference) {
            return ((ILSMTreeTupleReference) tuple).isAntimatter();
        }
        return false;
    }

    /**
     * Check if tuple passes the INCLUDE field filter.
     * Applied AFTER antimatter reconciliation. Tuples that fail filter don't enter topKWindow.
     */
    private boolean passesTupleFilter(ITupleReference tuple) throws HyracksDataException {
        if (tupleFilter == null) {
            return true;
        }
        referenceFilterTuple.reset(tuple);
        if (tupleFilter.accept(referenceFilterTuple)) {
            return true;
        }
        tuplesFilteredOut++;
        return false;
    }

    /**
     * Compute approximate distance D(q, x) using quantized embedding.
     *
     * This cursor is dedicated for quantized vector indexes.
     * Quantized data tuple format (pkStartField=4):
     *   Field 0: distance_to_centroid, Field 1: centroidId,
     *   Field 2: quantized_distance, Field 3: quantized_embedding, Field 4+: PKs
     *
     * Dequantizes the stored embedding bytes (field 3) and computes distance
     * against the quantized query vector.
     */
    private double computeApproximateDistance(ITupleReference tuple) throws HyracksDataException {
        // Quantized embedding content bytes (field 3, ByteArrayPointable prefix stripped) → dequantize.
        byte[] qBytes = dataAccessor.getQuantizedEmbedding(tuple);
        double[] dequantized = quantizer.dequantize(qBytes);
        return distanceFunction.apply(quantizedQueryVector, dequantized);
    }

    // ==================== IIndexCursor Interface (EnforcedIndexCursor template methods) ====================

    @Override
    protected boolean doHasNext() throws HyracksDataException {
        return drainIterator != null && drainIterator.hasNext();
    }

    @Override
    protected void doNext() throws HyracksDataException {
        if (!doHasNext()) {
            // TODO(vector-errors): uncoded IllegalStateException -> reaches the user as "Internal error".
            throw HyracksDataException.create(new IllegalStateException("No more tuples"));
        }
        drainIterator.next();
        nextCallCount++;
    }

    @Override
    protected ITupleReference doGetTuple() {
        if (drainIterator == null) {
            return null;
        }
        return drainIterator.getTuple();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Delegates to {@link SpillableTopKDrainIterator#getCurrentDqx()}, which returns the
     * {@code D(q,x)} computed from the quantized embedding during the blocked-search phase.
     * Valid between {@link #next()} and the next {@link #next()} / {@link #close()}; returns
     * {@link Double#NaN} otherwise.
     */
    @Override
    public double getCurrentDistance() {
        return drainIterator != null ? drainIterator.getCurrentDqx() : Double.NaN;
    }

    @Override
    protected void doClose() throws HyracksDataException {
        // EnforcedIndexCursor only routes here on an OPENED -> CLOSED transition, so no isOpen guard is
        // needed. doClose() ends the current search (the cursor can be re-opened); the null-guards below
        // still cover a failure early in doOpen() that left these unallocated.
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                    "Search summary: K={}, epsilon={}, clustersExplored={}, tuplesProcessed={},"
                            + " antimatterCancellations={}, filteredOut={}, resultsReturned={}",
                    K, epsilon, clustersExplored, totalTuplesProcessed, antimatterCancellations, tuplesFilteredOut,
                    nextCallCount);
        }

        if (rangeCursors != null) {
            for (int i = 0; i < rangeCursors.length; i++) {
                if (rangeCursors[i] != null) {
                    rangeCursors[i].close();
                }
            }
        }
        if (drainIterator != null) {
            drainIterator.close();
            drainIterator = null;
        }
        if (topKBuffer != null) {
            topKBuffer.close();
            topKBuffer = null;
        }
    }

    @Override
    protected void doDestroy() throws HyracksDataException {
        // The enforced contract guarantees doClose() already ran (destroy requires the CLOSED state), which
        // ended the current search. doDestroy() reclaims the per-component accessors + cursors for good,
        // matching LSMIndexSearchCursor.doDestroy.
        Throwable failure = CleanupUtils.destroy(null, vTreeAccessors);
        failure = CleanupUtils.destroy(failure, rangeCursors);
        vTreeAccessors = null;
        rangeCursors = null;
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }

    // ==================== Inner Classes ====================

    /**
     * Priority queue element holding tuple and component info (for multi-component merging).
     */
    private static class PriorityQueueElement {
        int componentId;
        ITupleReference tuple;

        PriorityQueueElement(int componentId) {
            this.componentId = componentId;
        }

        void reset(ITupleReference tuple) {
            this.tuple = tuple;
        }
    }

    /**
     * Priority queue comparator for merging results from multiple components.
     * Compares by distance (field 0), then PK fields, then component ID.
     */
    private class NaivePriorityQueueComparator implements Comparator<PriorityQueueElement> {
        @Override
        public int compare(PriorityQueueElement a, PriorityQueueElement b) {
            ITupleReference tupleA = a.tuple;
            ITupleReference tupleB = b.tuple;

            try {
                int result = cmp.getComparators()[0].compare(tupleA.getFieldData(0), tupleA.getFieldStart(0),
                        tupleA.getFieldLength(0), tupleB.getFieldData(0), tupleB.getFieldStart(0),
                        tupleB.getFieldLength(0));
                if (result != 0) {
                    return result;
                }

                // Cap at the PK fields; trailing INCLUDE fields are excluded from the ordering key so a
                // delete marker and its live twin (which may differ in INCLUDE values) order together.
                int numRemainingFields = Math.min(cmp.getComparators().length - pkStartField, numPrimaryKeyFields);
                for (int i = 0; i < numRemainingFields; i++) {
                    int fieldIdx = pkStartField + i;
                    if (fieldIdx >= tupleA.getFieldCount() || fieldIdx >= tupleB.getFieldCount()) {
                        break;
                    }
                    result = cmp.getComparators()[pkStartField + i].compare(tupleA.getFieldData(fieldIdx),
                            tupleA.getFieldStart(fieldIdx), tupleA.getFieldLength(fieldIdx),
                            tupleB.getFieldData(fieldIdx), tupleB.getFieldStart(fieldIdx),
                            tupleB.getFieldLength(fieldIdx));
                    if (result != 0) {
                        return result;
                    }
                }
            } catch (Throwable e) {
                // Matches the LSMIndexSearchCursor / LSMRTree / LSMBTree comparator idiom: Comparator.compare
                // cannot throw a checked exception. The cause is a coded HyracksDataException and
                // ExceptionUtils.unwrap() follows it, so the error code survives to the user.
                throw new IllegalArgumentException(e);
            }

            return Integer.compare(a.componentId, b.componentId);
        }
    }
}
