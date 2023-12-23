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
import org.apache.asterix.optimizer.rules.cbo.JoinNode;
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
    protected static double selectivityForSecondaryIndexSelection = 0.1;
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

    // These cost methods are very simple and rudimentary for now.
    // These can be improved by asterixdb developers as needed.
    public Cost costFullScan(JoinNode jn) {
        return new Cost(jn.getOrigCardinality());
    }

    public Cost costIndexScan(JoinNode jn, double indexSel) {
        return new Cost(indexSel * jn.getOrigCardinality());
    }

    public Cost costIndexDataScan(JoinNode jn, double indexSel) {
        if (indexSel < selectivityForSecondaryIndexSelection) {
            return new Cost(indexSel * jn.getOrigCardinality());
        }

        // If index selectivity is not very selective, make index scan more expensive than full scan.
        return new Cost(jn.getOrigCardinality());
    }

    public Cost costHashJoin(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        JoinNode rightJn = jn.getRightJn();
        return new Cost(leftJn.getCardinality() + rightJn.getCardinality());
    }

    public Cost computeHJProbeExchangeCost(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        return new Cost(leftJn.getCardinality());
    }

    public Cost computeHJBuildExchangeCost(JoinNode jn) {
        JoinNode rightJn = jn.getRightJn();
        return new Cost(rightJn.getCardinality());
    }

    public Cost costBroadcastHashJoin(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        JoinNode rightJn = jn.getRightJn();
        return new Cost(leftJn.getCardinality() + DOP * rightJn.getCardinality());
    }

    public Cost computeBHJBuildExchangeCost(JoinNode jn) {
        JoinNode rightJn = jn.getRightJn();
        return new Cost(DOP * rightJn.getCardinality());
    }

    public Cost costIndexNLJoin(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        JoinNode rightJn = jn.getRightJn();
        double origRightCard = rightJn.getOrigCardinality();
        double innerCard = rightJn.getCardinality();
        double joinCard = jn.getCardinality();

        return new Cost(4 * leftJn.getCardinality() + joinCard * origRightCard / innerCard);
    }

    public Cost computeNLJOuterExchangeCost(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        return new Cost(DOP * leftJn.getCardinality());
    }

    public Cost costCartesianProductJoin(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        JoinNode rightJn = jn.getRightJn();
        return new Cost(leftJn.getCardinality() * rightJn.getCardinality());
    }

    public Cost computeCPRightExchangeCost(JoinNode jn) {
        JoinNode rightJn = jn.getRightJn();
        return new Cost(DOP * rightJn.getCardinality());
    }

    public Cost costHashGroupBy(GroupByOperator groupByOperator) {
        Pair<Double, Double> cards = getOpCards(groupByOperator);
        double inputCard = cards.getFirst();
        return new Cost(inputCard);
    }

    public Cost costSortGroupBy(GroupByOperator groupByOperator) {
        Pair<Double, Double> cards = getOpCards(groupByOperator);
        double inputCard = cards.getFirst();
        return new Cost(costSort(inputCard));
    }

    public Cost costDistinct(DistinctOperator distinctOperator) {
        Pair<Double, Double> cards = getOpCards(distinctOperator);
        double inputCard = cards.getFirst();
        return new Cost(costSort(inputCard));
    }

    public Cost costOrderBy(OrderOperator orderOp) {
        Pair<Double, Double> cards = getOpCards(orderOp);
        double inputCard = cards.getFirst();
        return new Cost(costSort(inputCard));
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

    protected double costSort(double inputCard) {
        return (inputCard <= 1 ? 0 : inputCard * Math.log(inputCard) / Math.log(2)); // log to the base 2
    }
}
