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
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;

public class CostMethods implements ICostMethods {

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

    public Cost costFullScan(JoinNode jn) {
        int limit = jn.getLimitVal();
        double factor = 1.0;
        double inputCard = jn.getOrigCardinality();
        double outputCard = jn.getCardinality();
        double documentSize = jn.getAvgDocSize(); //the entire row will come out
        double inputSize = jn.getInputSize(); // This is the size coming out of the disk
        if (!jn.getColumnar()) {
            inputSize = documentSize;
        }
        double outputSize = jn.getOutputSize(); // this is what leaves after the scan; only join and result vars are included here

        if (limit > 0 && limit < outputCard) { // for single tables only
            factor = limit / outputCard;
            outputCard = limit;
        }
        double docsProcessed = factor * inputCard / DOP * docSizeFactor(inputSize);
        double docsProduced = outputCard / DOP * docSizeFactor(outputSize);

        // Since we do not have exchanges at the logical plan level, docsSent for the exchange above the
        // scan will be computed by the logical operator above the scan which will determine the properties
        // of the exchange operator above the scan in the final physical plan.
        double docsSent = 0;
        double ioSeq = factor * inputCard / DOP * inputSize / blockSize;
        double ioRand = 0;

        return new Cost(docsProcessed, docsProduced, docsSent, 0, ioSeq, ioRand);
    }

    public Cost costIndexScan(JoinNode jn, double indexSel) {
        int limit = jn.getLimitVal();
        double inputCard = jn.getOrigCardinality();
        double outputCard = jn.getCardinality();
        double outputSize = jn.getOutputSize();

        if (limit > 0 && limit < outputCard) {
            indexSel = limit / inputCard;
        }

        double docsProcessed = inputCard * indexSel / DOP * docSizeFactor(outputSize);
        double docsProduced = inputCard * indexSel / DOP * docSizeFactor(outputSize);
        double docsSent = 0;
        double ioSeq = inputCard * indexSel / DOP * outputSize / blockSize;
        double ioRand = 0;
        return new Cost(docsProcessed, docsProduced, docsSent, 0, ioSeq, ioRand);
    }

    public Cost costIndexDataScan(JoinNode jn, double indexSel) {
        int limit = jn.getLimitVal(); // stored earlier
        double inputCard = jn.getOrigCardinality();
        double outputCard = jn.getCardinality();
        double inputSize = jn.getInputSize(); // original size of the dataset
        double outputSize = jn.getOutputSize();

        if (limit > 0 && limit < outputCard) {
            indexSel = limit / inputCard;
        }

        // Duplicates RIDs will be removed by the distinct operator in case of array indexes.
        // We account for that with the unnestFactor, which is 1.0 for regular indexes.
        double docsProcessed = inputCard * indexSel / DOP / jn.getUnnestFactor() * docSizeFactor(outputSize);
        double docsProduced = inputCard * indexSel / DOP * docSizeFactor(outputSize);
        double docsSent = 0;

        // Since we do not have exchanges at the logical plan level, docsSent for the exchange above the
        // scan will be computed by the logical operator above the scan which will determine the properties
        // of the exchange operator above the scan in the final physical plan.
        double ioSeq = 0;
        double ioRand = 0;

        // Need to sort the RIDs before the data access. This happens in the sort operator
        // between the index scan and the data scan operator, so we cost it here.
        docsProcessed += costSort(docsProcessed, inputSize);

        return new Cost(docsProcessed, docsProduced, docsSent, 0, ioSeq, ioRand);
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
        inputCard = cards.getFirst();
        outputCard = cards.getSecond();

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
        inputCard = cards.getFirst();
        outputCard = cards.getSecond();

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
        inputCard = cards.getFirst();
        outputCard = cards.getSecond();

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
        inputCard = cards.getFirst();
        outputCard = cards.getSecond();

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
        Pair<Double, Double> cardCost = new Pair<>(0.0, 0.0);

        for (Map.Entry<String, Object> anno : op.getAnnotations().entrySet()) {
            if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_INPUT_CARDINALITY)) {
                cardCost.setFirst((Double) anno.getValue());
            } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_CARDINALITY)) {
                cardCost.setSecond((Double) anno.getValue());
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
