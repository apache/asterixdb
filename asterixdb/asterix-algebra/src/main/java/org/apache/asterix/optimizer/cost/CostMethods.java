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

package org.apache.asterix.optimizer.cost;

import java.util.Map;

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.optimizer.rules.cbo.AbstractPlanNode;
import org.apache.asterix.optimizer.rules.cbo.JoinNode;
import org.apache.asterix.optimizer.rules.cbo.JoinPlanNode;
import org.apache.asterix.optimizer.rules.cbo.ScanPlanNode;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;

public class CostMethods implements ICostMethods {

    // Measured against secondary-index plans, a scan that a limit stops early runs several times slower than
    // the rows it reads suggest, so it is costed as reading this many times further. Without this weight it
    // undercuts an index plan on selective predicates with large limits, where the index plan runs faster.
    private static final double LIMITED_SCAN_WEIGHT = 8.0;

    protected IOptimizationContext optCtx;
    protected PhysicalOptimizationConfig physOptConfig;
    protected long blockSize;
    protected long DOP;
    protected double maxMemorySizeForJoin;
    protected double maxMemorySizeForGroup;
    protected double maxMemorySizeForSort;

    public CostMethods(IOptimizationContext context) {
        setContext(context);
    }

    public void setContext(IOptimizationContext context) {
        optCtx = context;
        physOptConfig = context.getPhysicalOptimizationConfig();
        blockSize = getBufferCachePageSize();
        DOP = getDOP();
        maxMemorySizeForJoin = getMaxMemorySizeForJoin();
        maxMemorySizeForGroup = getMaxMemorySizeForGroup();
        maxMemorySizeForSort = getMaxMemorySizeForSort();
    }

    private long getBufferCacheSize() {
        MetadataProvider metadataProvider = (MetadataProvider) optCtx.getMetadataProvider();
        return metadataProvider.getStorageProperties().getBufferCacheSize();
    }

    public long getBufferCachePageSize() {
        MetadataProvider metadataProvider = (MetadataProvider) optCtx.getMetadataProvider();
        return metadataProvider.getStorageProperties().getBufferCachePageSize();
    }

    public long getDOP() {
        return optCtx.getComputationNodeDomain().cardinality();
    }

    public double getMaxMemorySizeForJoin() {
        return physOptConfig.getMaxFramesForJoin() * physOptConfig.getFrameSize();
    }

    public double getMaxMemorySizeForGroup() {
        return physOptConfig.getMaxFramesForGroupBy() * physOptConfig.getFrameSize();
    }

    public double getMaxMemorySizeForSort() {
        return physOptConfig.getMaxFramesExternalSort() * physOptConfig.getFrameSize();
    }

    /**
     * Rows an operator reads before a limit pushed into it is reached, across all partitions. Each
     * partition stops once it has produced {@code limit} rows, so the cluster reads until
     * {@code DOP * limit} rows qualify.
     *
     * @param jn the dataset being read
     * @param readCard rows the operator reads when there is no limit
     * @param readSel selectivity of the predicates already applied to the rows being read
     */
    private double limitedReadCard(JoinNode jn, double readCard, double readSel) {
        int limit = jn.getLimitVal();
        double origCard = jn.getOrigCardinality();
        double outputCard = jn.getCardinality();
        if (limit <= 0 || origCard <= 0 || outputCard <= 0) {
            return readCard;
        }
        double outputSel = outputCard / origCard; // all the predicates on this dataset
        return Math.min(readCard, DOP * limit * readSel / outputSel);
    }

    public Cost costFullScan(JoinNode jn) {
        double inputCard = jn.getOrigCardinality();
        double outputCard = jn.getCardinality();
        double documentSize = jn.getAvgDocSize(); //the entire row will come out
        double inputSize = jn.getInputSize(); // This is the size coming out of the disk
        if (!jn.getColumnar()) {
            inputSize = documentSize;
        }
        double outputSize = jn.getOutputSize(); // this is what leaves after the scan; only join and result vars are included here

        // A limit pushed into the scan stops it early, on each partition, but never makes it dearer than a full scan.
        double scannedCard = Math.min(inputCard, LIMITED_SCAN_WEIGHT * limitedReadCard(jn, inputCard, 1.0));
        double producedCard = scannedCard < inputCard ? scannedCard * outputCard / inputCard : outputCard;

        double docsProcessed = scannedCard / DOP * docSizeFactor(inputSize);
        double docsProduced = producedCard / DOP * docSizeFactor(outputSize);

        // Since we do not have exchanges at the logical plan level, docsSent for the exchange above the
        // scan will be computed by the logical operator above the scan which will determine the properties
        // of the exchange operator above the scan in the final physical plan.
        double docsSent = 0;
        double ioSeq = scannedCard / DOP * inputSize / blockSize;
        double ioRand = 0;

        return new Cost(docsProcessed, docsProduced, docsSent, 0, ioSeq, ioRand);
    }

    public Cost costIndexScan(JoinNode jn, Index index, double indexSel) {
        double outputSize = jn.getOutputSize();

        double scannedCard = jn.getOrigCardinality() * indexSel;
        if (index.isPrimaryIndex()) {
            // A limit is pushed into a primary-index search only; a secondary-index search returns every match.
            scannedCard = limitedReadCard(jn, scannedCard, indexSel);
        }

        double docsProcessed = scannedCard / DOP * docSizeFactor(outputSize);
        double docsProduced = scannedCard / DOP * docSizeFactor(outputSize);
        double docsSent = 0;
        double ioSeq = scannedCard / DOP * outputSize / blockSize;
        double ioRand = 0;
        return new Cost(docsProcessed, docsProduced, docsSent, 0, ioSeq, ioRand);
    }

    public Cost costIndexDataScan(JoinNode jn, double indexSel) {
        double inputSize = jn.getInputSize(); // original size of the dataset
        double outputSize = jn.getOutputSize();

        double sortedCard = jn.getOrigCardinality() * indexSel; // every RID the index search finds is sorted
        double fetchedCard = limitedReadCard(jn, sortedCard, indexSel); // the fetch stops at the limit

        return costPrimaryFetch(sortedCard, fetchedCard, jn.getUnnestFactor(), inputSize, outputSize);
    }

    /**
     * Cost of an index-only plan once its secondary index has been searched: the primary keys the search returns
     * are sorted and their duplicates removed, and no record is fetched. A limit does not shorten this work,
     * since the sort is blocking and sees every primary key the search returns.
     *
     * @param jn the dataset being read
     * @param indexSel selectivity of the index search
     */
    public Cost costIndexOnlyDistinct(JoinNode jn, double indexSel) {
        double outputSize = jn.getOutputSize();
        double sortedCardPerPartition = jn.getOrigCardinality() * indexSel / DOP;

        double docsProcessed = sortedCardPerPartition * docSizeFactor(outputSize);
        docsProcessed += costSort(sortedCardPerPartition, outputSize);
        double docsProduced = sortedCardPerPartition / jn.getUnnestFactor() * docSizeFactor(outputSize);

        return new Cost(docsProcessed, docsProduced, 0, 0, 0, 0);
    }

    /**
     * Fetch of rows from the primary index, including the sort of their primary keys that precedes it.
     *
     * @param sortedCard rows whose primary keys are sorted ahead of the fetch
     * @param fetchedCard rows the fetch reads; fewer than {@code sortedCard} when a limit stops it early
     * @param unnestFactor duplicate RIDs the distinct operator removes; 1.0 for regular indexes
     * @param inputSize original size of the dataset
     * @param outputSize width of the variables leaving the fetch
     */
    private Cost costPrimaryFetch(double sortedCard, double fetchedCard, double unnestFactor, double inputSize,
            double outputSize) {
        // Duplicates RIDs will be removed by the distinct operator in case of array indexes.
        // We account for that with the unnestFactor, which is 1.0 for regular indexes.
        double docsProcessed = fetchedCard / DOP / unnestFactor * docSizeFactor(outputSize);
        double docsProduced = fetchedCard / DOP * docSizeFactor(outputSize);
        double docsSent = 0;

        // Since we do not have exchanges at the logical plan level, docsSent for the exchange above the
        // scan will be computed by the logical operator above the scan which will determine the properties
        // of the exchange operator above the scan in the final physical plan.
        double ioSeq = 0;
        double ioRand = 0;

        // Need to sort the RIDs before the data access. This happens in the sort operator
        // between the index scan and the data scan operator, so we cost it here. The sort is blocking,
        // so it sees every RID whatever limit stops the fetch.
        docsProcessed += costSort(sortedCard / DOP / unnestFactor * docSizeFactor(outputSize), inputSize);

        return new Cost(docsProcessed, docsProduced, docsSent, 0, ioSeq, ioRand);
    }

    /**
     * Cost of answering a similarity search from a vector index: the descent through the centroid
     * hierarchy and a distance evaluation per entry in every probed cluster.
     * <p>
     * @param geometry how much of the index one storage partition's search touches
     * @param fetchedCard candidates reranked; zero when the index answers the query on its own
     * @param inputSize original size of the dataset
     * @param outputSize width of the variables leaving the search
     */
    public Cost costVectorIndexScan(VectorIndexGeometry geometry, double fetchedCard, double inputSize,
            double outputSize) {
        double sizeFactor = docSizeFactor(outputSize);

        // Routing evaluations down the hierarchy, then one distance evaluation per entry in every
        // probed cluster.
        double docsProcessed = (geometry.getTreeCentroids() + geometry.getScannedEntries()) / DOP * sizeFactor;
        double docsProduced = fetchedCard / DOP * sizeFactor;

        Cost searchCost = new Cost(docsProcessed, docsProduced, 0, 0, geometry.getDataPages() / DOP, 0);

        return (Cost) searchCost.costAdd(costPrimaryFetch(fetchedCard, fetchedCard, 1.0, inputSize, outputSize));
    }

    public Cost costHashJoin(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        JoinNode rightJn = jn.getRightJn();

        double probeCard = leftJn.getCardinality();
        double probeSize = leftJn.getOutputSize();
        double buildCard = rightJn.getCardinality();
        double buildSize = rightJn.getOutputSize();
        double joinCard = jn.getCardinality();
        double joinSize = jn.getOutputSize();

        double probeCardPerPartition = probeCard / DOP;
        double buildCardPerPartition = buildCard / DOP;
        double joinCardPerPartition = joinCard / DOP;

        double probeSizeFactor = docSizeFactor(probeSize);
        double buildSizeFactor = docSizeFactor(buildSize);
        double joinSizeFactor = docSizeFactor(joinSize);

        // Regular (not broadcast) hash join.
        double docsProcessed = probeCardPerPartition * probeSizeFactor + buildCardPerPartition * buildSizeFactor;

        double overFlowCost =
                computeHashJoinOverflowCost(probeCardPerPartition, probeSize, buildCardPerPartition, buildSize);

        double docsProduced = joinCardPerPartition * joinSizeFactor;

        // Since we do not have exchanges at the logical plan level, docsSent for the exchange above the
        // join will be computed by the logical operator above the join which will determine the properties
        // of the exchange operator above the join in the final physical plan.
        double docsSent = 0;

        return new Cost(docsProcessed, docsProduced, docsSent, overFlowCost, 0, 0);
    }

    // This will be used later in the final physical plan to assign costs to the exchange and
    // subtract it from the cost assigned to the hash join.
    public Cost computeHJProbeExchangeCost(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        double probeCard = leftJn.getCardinality();
        double probeSize = leftJn.getOutputSize();
        double probeCardPerPartition = probeCard / DOP;
        double probeSizeFactor = docSizeFactor(probeSize);
        boolean probePartitioned = false;
        double docsSent = 0;
        if (!probePartitioned) {
            docsSent += probeCardPerPartition * probeSizeFactor;
        }
        return new Cost(0, 0, docsSent, 0, 0, 0);
    }

    // This will be used later in the final physical plan to assign costs to the exchange and
    // subtract it from the cost assigned to the hash join.
    public Cost computeHJBuildExchangeCost(JoinNode jn) {
        JoinNode rightJn = jn.getRightJn();
        double buildCard = rightJn.getCardinality();
        double buildSize = rightJn.getOutputSize();
        double buildCardPerPartition = buildCard / DOP;
        double buildSizeFactor = docSizeFactor(buildSize);
        boolean buildPartitioned = false;
        double docsSent = 0;
        if (!buildPartitioned) {
            docsSent += buildCardPerPartition * buildSizeFactor;
        }
        return new Cost(0, 0, docsSent, 0, 0, 0);
    }

    public Cost costBroadcastHashJoin(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        JoinNode rightJn = jn.getRightJn();

        double probeCard = leftJn.getCardinality();
        double probeSize = leftJn.getOutputSize();
        double buildCard = rightJn.getCardinality();
        double buildSize = rightJn.getOutputSize();
        double joinCard = jn.getCardinality();
        double joinSize = jn.getOutputSize();

        double probeCardPerPartition = probeCard / DOP;
        double buildCardPerPartition = buildCard; // The build side is broadcast
        double joinCardPerPartition = joinCard / DOP;

        double probeSizeFactor = docSizeFactor(probeSize);
        double buildSizeFactor = docSizeFactor(buildSize);
        double joinSizeFactor = docSizeFactor(joinSize);

        // Broadcast hash join.
        double docsProcessed = probeCardPerPartition * probeSizeFactor + buildCardPerPartition * buildSizeFactor;
        double overFlowCost =
                computeHashJoinOverflowCost(probeCardPerPartition, probeSize, buildCardPerPartition, buildSize);

        double docsProduced = joinCardPerPartition * joinSizeFactor;
        // Since we do not have exchanges at the logical plan level, docsSent for the exchange above the
        // join will be computed by the logical operator above the join which will determine the properties
        // of the exchange operator above the join in the final physical plan.
        double docsSent = 0;

        return new Cost(docsProcessed, docsProduced, docsSent, overFlowCost, 0, 0);
    }

    // This will be used later in the final physical plan to assign costs to the exchange and
    // subtract it from the cost assigned to the broadcast hash join.
    public Cost computeBHJBuildExchangeCost(JoinNode jn) {
        JoinNode rightJn = jn.getRightJn();
        double buildCard = rightJn.getCardinality();
        double buildSize = rightJn.getOutputSize();
        double buildCardPerPartition = buildCard;
        double buildSizeFactor = docSizeFactor(buildSize);
        double docsSent = buildCardPerPartition * buildSizeFactor;
        return new Cost(0, 0, docsSent, 0, 0, 0);
    }

    // This routine is the weakest. May need to revisit this multiple times.
    public Cost costIndexNLJoin(JoinNode jn, Index index) {
        JoinNode leftJn = jn.getLeftJn();
        JoinNode rightJn = jn.getRightJn();

        double outerCard = leftJn.getCardinality();
        double outerSize = leftJn.getOutputSize();
        double innerCard = rightJn.getCardinality();
        double innerSize = rightJn.getOutputSize();
        double joinCard = jn.getCardinality();
        double joinSize = jn.getOutputSize();

        double outerCardPerPartition = outerCard; // outer side is broadcast to all nodes
        double innerCardPerPartition = innerCard / DOP;
        double joinCardPerPartition = joinCard / DOP;

        double outerSizeFactor = docSizeFactor(outerSize);
        double innerSizeFactor = docSizeFactor(innerSize);
        double joinSizeFactor = docSizeFactor(joinSize);

        double origRightCard = rightJn.getOrigCardinality();
        double tuplesTobeSortedPerInstance = joinCardPerPartition * origRightCard / innerCard;
        // innerCard/origRightCard is the selectivity for the right side.

        // The probes from the outer side are processed by the nested join
        // and sent down to the inner side. The result join tuples flow back
        // up to the nested join.

        double docsProcessed = outerCardPerPartition * outerSizeFactor;

        double docsProduced = joinCardPerPartition * joinSizeFactor;

        // Since we do not have exchanges at the logical plan level, docsSent for the exchange above the
        // join will be computed by the logical operator above the join which will determine the properties
        // of the exchange operator above the join in the final physical plan.
        double docsSent = 0;
        double ioSeq = joinCardPerPartition * innerSize / blockSize;
        if (!index.isPrimaryIndex()) {
            docsProcessed += costSort(tuplesTobeSortedPerInstance, joinSize);
            docsProcessed += 5 * outerCardPerPartition * outerSizeFactor; // empirical evidence
        }
        // we will assume that the outerCard is always sorted.
        // This need not be the same if the outercard is already sorted. But hard to tell this at the LO level.
        if (outerCard > 1 && outerSideIsNotSorted(jn)) {
            docsProcessed += costSort(4 * outerCard, outerSize); // had to increase this cost by 4x from empirical evidence
        }

        return new Cost(docsProcessed, docsProduced, docsSent, 0, ioSeq, 0);
    }

    private boolean outerSideIsNotSorted(JoinNode jn) {

        JoinNode leftJn = jn.getLeftJn();
        AbstractPlanNode pn = leftJn.getCheapestPlanNode();
        if (pn instanceof JoinPlanNode) {
            return true; // we cant tell here if the outer side is sorted or not.
        }
        Index index = ((ScanPlanNode) pn).getSoleAccessIndex();

        if (index == null) {
            return true;
        }

        if (index.isPrimaryIndex()) {
            return false;
        }

        return true;
    }

    // This will be used later in the final physical plan to assign costs to the exchange and
    // subtract it from the cost assigned to the index NL join.
    public Cost computeNLJOuterExchangeCost(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        double outerCard = leftJn.getCardinality();
        double outerSize = leftJn.getOutputSize();
        double outerCardPerPartition = outerCard;
        double outerSizeFactor = docSizeFactor(outerSize);
        double docsSent = outerCardPerPartition * outerSizeFactor;
        return new Cost(0, 0, docsSent, 0, 0, 0);
    }

    public Cost costCartesianProductJoin(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        JoinNode rightJn = jn.getRightJn();

        double leftCard = leftJn.getCardinality();
        double leftSize = leftJn.getOutputSize();
        double rightCard = rightJn.getCardinality();
        double rightSize = rightJn.getOutputSize();
        double joinCard = jn.getCardinality();
        double joinSize = jn.getOutputSize();

        double leftCardPerPartition = leftCard / DOP;
        double rightCardPerPartition = rightCard; // the right side is broadcast
        double joinCardPerPartition = joinCard / DOP;

        double leftSizeFactor = docSizeFactor(leftSize);
        double rightSizeFactor = docSizeFactor(rightSize);
        double joinSizeFactor = docSizeFactor(joinSize);

        double docsProcessed = Math.max(leftCardPerPartition, Cost.MIN_CARD) * leftSizeFactor
                * Math.max(rightCardPerPartition, Cost.MIN_CARD) * rightSizeFactor;
        double docsProduced = joinCardPerPartition * joinSizeFactor;
        // Since we do not have exchanges at the logical plan level, docsSent for the exchange above the
        // join will be computed by the logical operator above the join which will determine the properties
        // of the exchange operator above the join in the final physical plan.
        double docsSent = 0;

        return new Cost(docsProcessed, docsProduced, docsSent, 0, 0, 0);
    }

    // This will be used later in the final physical plan to assign costs to the exchange and
    // subtract it from the cost assigned to the cartesian product join.
    public Cost computeCPRightExchangeCost(JoinNode jn) {
        return computeBHJBuildExchangeCost(jn);
    }

    public Cost costHashGroupBy(GroupByOperator groupByOperator) {
        double inputCard, inputCardPerPartition;
        double outputCard, outputCardPerPartition;
        double inputSize = 1.0; // for now
        double outputSize = 1.0; // for now

        Pair<Double, Double> cards = getOpCards(groupByOperator);
        inputCard = cards.getLeft();
        outputCard = cards.getRight();

        if (groupByOperator.isGlobal()) {
            inputCardPerPartition = outputCard * DOP;
        } else {
            inputCardPerPartition = inputCard / DOP;
        }
        outputCardPerPartition = outputCard;

        double docsProcessed = inputCardPerPartition * docSizeFactor(inputSize);
        double docsProduced = outputCardPerPartition * docSizeFactor(outputSize);
        double docsSent = 0.0;
        double overFlowCost =
                computeHashGroupByOverflowCost(inputCardPerPartition, inputSize, outputCardPerPartition, outputSize);

        return new Cost(docsProcessed, docsProduced, docsSent, overFlowCost, 0, 0);
    }

    public Cost costSortGroupBy(GroupByOperator groupByOperator) {
        double inputCard, inputCardPerPartition;
        double outputCard, outputCardPerPartition;
        double inputSize = 1.0; // for now
        double outputSize = 1.0; // for now

        Pair<Double, Double> cards = getOpCards(groupByOperator);
        inputCard = cards.getLeft();
        outputCard = cards.getRight();

        if (groupByOperator.isGlobal()) {
            inputCardPerPartition = outputCard * DOP;
        } else {
            inputCardPerPartition = inputCard / DOP;
        }
        outputCardPerPartition = outputCard;

        double docsProcessed = inputCardPerPartition * docSizeFactor(inputSize);
        docsProcessed += costSort(inputCardPerPartition, inputSize);
        double docsProduced = outputCardPerPartition * docSizeFactor(outputSize);
        double docsSent = 0.0;
        double overFlowCost = computeSortOverflowCost(inputCardPerPartition, inputSize);

        return new Cost(docsProcessed, docsProduced, docsSent, overFlowCost, 0, 0);
    }

    public Cost costDistinct(DistinctOperator distinctOp) {
        double inputCard, inputCardPerPartition;
        double outputCard, outputCardPerPartition;
        double inputSize = 1.0; // for now
        double outputSize = 1.0; // for now

        Pair<Double, Double> cards = getOpCards(distinctOp);
        inputCard = cards.getLeft();
        outputCard = cards.getRight();

        inputCardPerPartition = inputCard / DOP;
        outputCardPerPartition = outputCard / DOP;

        double docsProcessed = inputCardPerPartition * docSizeFactor(inputSize);
        docsProcessed += costSort(inputCardPerPartition, inputSize);
        double docsProduced = outputCardPerPartition * docSizeFactor(outputSize);
        double docsSent = 0.0;
        double overFlowCost = computeSortOverflowCost(inputCardPerPartition, inputSize);

        return new Cost(docsProcessed, docsProduced, docsSent, overFlowCost, 0, 0);
    }

    public Cost costOrderBy(OrderOperator orderOp) {
        double inputCard, inputCardPerPartition;
        double outputCard, outputCardPerPartition;
        double inputSize = 1.0; // for now
        double outputSize = 1.0; // for now

        Pair<Double, Double> cards = getOpCards(orderOp);
        inputCard = cards.getLeft();
        outputCard = cards.getRight();

        inputCardPerPartition = inputCard / DOP;
        outputCardPerPartition = outputCard / DOP;

        double docsProcessed = inputCardPerPartition * docSizeFactor(inputSize);
        docsProcessed += costSort(inputCardPerPartition, inputSize);
        double docsProduced = outputCardPerPartition * docSizeFactor(outputSize);
        double docsSent = 0.0;
        double overFlowCost = computeSortOverflowCost(inputCardPerPartition, inputSize);

        return new Cost(docsProcessed, docsProduced, docsSent, overFlowCost, 0, 0);
    }

    private double docSizeFactor(double size) {
        return 1.0;
        //return Math.sqrt(size);
    }

    private double computeHashJoinOverflowCost(double probeCard, double probeSize, double buildCard, double buildSize) {
        double memoryUsed = buildCard * buildSize;
        double probeSizeFactor = docSizeFactor(probeSize);
        double buildSizeFactor = docSizeFactor(buildSize);

        if (memoryUsed <= maxMemorySizeForJoin) {
            return 0;
        }

        // memoryUsed > maxMemorySize
        double fractionOverflow = 1.0 - maxMemorySizeForJoin / memoryUsed;

        // The factor of 2 comes from having to write overflow tuples to disk and
        // read back the overflow tuples from disk.
        double buildOverFlow = 2.0 * fractionOverflow * buildCard * buildSizeFactor;

        // The factor of 2 comes from having to write overflow tuples to disk and
        // read back the overflow tuples from disk.
        double probeOverFlow = 2.0 * fractionOverflow * probeCard * probeSizeFactor;

        return (buildOverFlow + probeOverFlow);
    }

    private double computeHashGroupByOverflowCost(double inputCard, double inputSize, double outputCard,
            double outputSize) {
        double memoryUsed = outputCard * outputSize;
        double inputSizeFactor = docSizeFactor(inputSize);

        if (memoryUsed <= maxMemorySizeForGroup) {
            return 0;
        }

        // memoryUsed > maxMemorySize
        double fractionOverflow = 1.0 - maxMemorySizeForGroup / memoryUsed;

        // The factor of 2 comes from having to write overflow tuples to disk and
        // read back the overflow tuples from disk.
        double overFlow = 2.0 * fractionOverflow * inputCard * inputSizeFactor;

        return overFlow;
    }

    private double computeSortOverflowCost(double inputCard, double inputSize) {
        double memoryUsed = inputCard * inputSize;
        double inputSizeFactor = docSizeFactor(inputSize);

        if (memoryUsed <= maxMemorySizeForSort) {
            return 0;
        }

        // memoryUsed > maxMemorySize
        double fractionOverflow = 1.0 - maxMemorySizeForSort / memoryUsed;

        // The factor of 2 comes from having to write overflow tuples to disk and
        // read back the overflow tuples from disk.
        double overFlow = 2.0 * fractionOverflow * inputCard * inputSizeFactor;

        return overFlow;
    }

    protected Pair<Double, Double> getOpCards(ILogicalOperator op) {
        MutablePair<Double, Double> cardCost = new MutablePair<>(0.0, 0.0);

        for (Map.Entry<String, Object> anno : op.getAnnotations().entrySet()) {
            if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_INPUT_CARDINALITY)) {
                cardCost.setLeft((Double) anno.getValue());
            } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_CARDINALITY)) {
                cardCost.setRight((Double) anno.getValue());
            }
        }
        return cardCost;
    }

    public double costSort(double inputCard, double inputSize) {
        double docsProcessed = 0;
        if (inputCard > 1) {
            docsProcessed = inputCard * Math.log(inputCard) / Math.log(2); // log to the base 2
            // want to avoid -ve costs as log can return -ve values.
            docsProcessed = Math.max(docsProcessed, 0);
        }
        docsProcessed *= docSizeFactor(inputSize);

        return docsProcessed;
    }
}
