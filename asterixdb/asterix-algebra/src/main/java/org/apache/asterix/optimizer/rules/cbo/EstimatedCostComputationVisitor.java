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
package org.apache.asterix.optimizer.rules.cbo;

import java.util.Map;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SwitchOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * A visitor that annotates an operator with its estimated cardinality and estimated cost.
 */
public class EstimatedCostComputationVisitor
        implements ILogicalOperatorVisitor<EstimatedCostComputationVisitor.CardSizeCost, Double> {

    public EstimatedCostComputationVisitor() {
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitAggregateOperator(AggregateOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitRunningAggregateOperator(RunningAggregateOperator op,
            Double arg) throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op,
            Double arg) throws AlgebricksException {
        // Empty tuple source operator sends an empty tuple to downstream operators.
        return new EstimatedCostComputationVisitor.CardSizeCost(0.0, 0.0, 0.0);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitGroupByOperator(GroupByOperator op, Double arg)
            throws AlgebricksException {

        return annotateGroupByDistinct(op, arg);
    }

    private EstimatedCostComputationVisitor.CardSizeCost annotateGroupByDistinct(ILogicalOperator op, Double arg)
            throws AlgebricksException {
        double groupByDistinctCost = 0.0;
        EstimatedCostComputationVisitor.CardSizeCost cardSizeCost =
                op.getInputs().getFirst().getValue().accept(this, arg);

        for (Map.Entry<String, Object> anno : op.getAnnotations().entrySet()) {
            if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_CARDINALITY)) {
                cardSizeCost.setCard((Double) anno.getValue());
            }
            if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_COST_LOCAL)) {
                groupByDistinctCost = (double) anno.getValue();
            }
        }

        double totalCost = cardSizeCost.getCost() + groupByDistinctCost;
        cardSizeCost.setCost(totalCost);
        annotateOp(op, cardSizeCost);

        return cardSizeCost;
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitLimitOperator(LimitOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitInnerJoinOperator(InnerJoinOperator op, Double arg)
            throws AlgebricksException {
        return visitInnerJoin(op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Double arg)
            throws AlgebricksException {
        return visitLeftOuterUnnest(op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitNestedTupleSourceOperator(NestedTupleSourceOperator op,
            Double arg) throws AlgebricksException {
        EstimatedCostComputationVisitor.CardSizeCost cardSizeCost = annotate(this, op, arg);
        return op.getDataSourceReference().getValue().getOperatorTag() == LogicalOperatorTag.SUBPLAN ? cardSizeCost
                : new EstimatedCostComputationVisitor.CardSizeCost(0.0, 0.0, 0.0);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitOrderOperator(OrderOperator op, Double arg)
            throws AlgebricksException {
        double orderCost = 0.0;
        EstimatedCostComputationVisitor.CardSizeCost cardSizeCost =
                op.getInputs().getFirst().getValue().accept(this, arg);

        for (Map.Entry<String, Object> anno : op.getAnnotations().entrySet()) {
            if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_COST_LOCAL)) {
                orderCost = (double) anno.getValue();
            }
        }

        double totalCost = cardSizeCost.getCost() + orderCost;
        op.getAnnotations().put(OperatorAnnotations.OP_COST_TOTAL, (double) Math.round(totalCost * 100) / 100);
        cardSizeCost.setCost(totalCost);
        annotateOp(op, cardSizeCost);

        return cardSizeCost;
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitAssignOperator(AssignOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitWindowOperator(WindowOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitSelectOperator(SelectOperator op, Double arg)
            throws AlgebricksException {
        EstimatedCostComputationVisitor.CardSizeCost cardSizeCost =
                op.getInputs().getFirst().getValue().accept(this, arg);
        for (Map.Entry<String, Object> anno : op.getAnnotations().entrySet()) {
            if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_CARDINALITY)) {
                cardSizeCost.setCard((Double) anno.getValue());
            } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_COST_TOTAL)) {
                cardSizeCost.setCost((Double) anno.getValue());
            }
        }

        annotateOp(op, cardSizeCost);
        return cardSizeCost;
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitDelegateOperator(DelegateOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitProjectOperator(ProjectOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitReplicateOperator(ReplicateOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitSplitOperator(SplitOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitSwitchOperator(SwitchOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitMaterializeOperator(MaterializeOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitScriptOperator(ScriptOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitSubplanOperator(SubplanOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitSinkOperator(SinkOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitUnionOperator(UnionAllOperator op, Double arg)
            throws AlgebricksException {
        // Needs more work.
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitUnnestOperator(UnnestOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op,
            Double arg) throws AlgebricksException {
        return visitLeftOuterUnnest(op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitUnnestMapOperator(UnnestMapOperator op, Double arg)
            throws AlgebricksException {
        EstimatedCostComputationVisitor.CardSizeCost cardSizeCost =
                new EstimatedCostComputationVisitor.CardSizeCost(0.0, 0.0, 0.0);

        for (Map.Entry<String, Object> anno : op.getAnnotations().entrySet()) {
            if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_CARDINALITY)) {
                cardSizeCost.setCard((Double) anno.getValue());
            } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_DOCSIZE)) {
                cardSizeCost.setSize((Double) anno.getValue());
            } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_COST_TOTAL)) {
                cardSizeCost.setCost((Double) anno.getValue());
            }
        }

        annotateOp(op, cardSizeCost);
        return cardSizeCost;
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op,
            Double arg) throws AlgebricksException {
        return visitLeftOuterUnnest(op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitDataScanOperator(DataSourceScanOperator op, Double arg)
            throws AlgebricksException {
        EstimatedCostComputationVisitor.CardSizeCost cardSizeCost =
                new EstimatedCostComputationVisitor.CardSizeCost(0.0, 0.0, 0.0);

        for (Map.Entry<String, Object> anno : op.getAnnotations().entrySet()) {
            if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_CARDINALITY)) {
                if (op.getSelectCondition() != null) {
                    cardSizeCost.setCard((Double) anno.getValue());
                }
            } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_INPUT_CARDINALITY)) {
                if (op.getSelectCondition() == null) {
                    cardSizeCost.setCard((Double) anno.getValue());
                }
            } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_DOCSIZE)) {
                cardSizeCost.setSize((Double) anno.getValue());
            } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_COST_TOTAL)) {
                cardSizeCost.setCost((Double) anno.getValue());
            }
        }

        annotateOp(op, cardSizeCost);
        return cardSizeCost;
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitDistinctOperator(DistinctOperator op, Double arg)
            throws AlgebricksException {

        return annotateGroupByDistinct(op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitExchangeOperator(ExchangeOperator op, Double arg)
            throws AlgebricksException {
        double exchCost = 0;
        if (arg != null) {
            exchCost = arg;
        }

        EstimatedCostComputationVisitor.CardSizeCost cardSizeCost =
                op.getInputs().getFirst().getValue().accept(this, arg);
        if (exchCost != 0) {
            op.getAnnotations().put(OperatorAnnotations.OP_COST_LOCAL, (double) Math.round(exchCost * 100) / 100);
            op.getAnnotations().put(OperatorAnnotations.OP_COST_TOTAL,
                    (double) Math.round((exchCost + cardSizeCost.getCost()) * 100) / 100);
        } else {
            op.getAnnotations().put(OperatorAnnotations.OP_COST_LOCAL, 0.0);
            op.getAnnotations().put(OperatorAnnotations.OP_COST_TOTAL,
                    (double) Math.round(cardSizeCost.getCost() * 100) / 100);
        }
        op.getAnnotations().put(OperatorAnnotations.OP_OUTPUT_CARDINALITY,
                (double) Math.round(cardSizeCost.getCard() * 100) / 100);
        op.getAnnotations().put(OperatorAnnotations.OP_OUTPUT_DOCSIZE,
                (double) Math.round(cardSizeCost.getSize() * 100) / 100);

        annotateOp(op, cardSizeCost);
        return cardSizeCost;
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitWriteOperator(WriteOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitDistributeResultOperator(DistributeResultOperator op,
            Double arg) throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op,
            Double arg) throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitIndexInsertDeleteUpsertOperator(
            IndexInsertDeleteUpsertOperator op, Double arg) throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitTokenizeOperator(TokenizeOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitForwardOperator(ForwardOperator op, Double arg)
            throws AlgebricksException {
        return annotate(this, op, arg);
    }

    @Override
    public EstimatedCostComputationVisitor.CardSizeCost visitIntersectOperator(IntersectOperator op, Double arg)
            throws AlgebricksException {
        // Needs more work.
        return annotate(this, op, arg);
    }

    private void annotateOp(ILogicalOperator op, EstimatedCostComputationVisitor.CardSizeCost cardSizeCost) {
        op.getAnnotations().put(OperatorAnnotations.OP_OUTPUT_CARDINALITY,
                (double) Math.round(cardSizeCost.getCard() * 100) / 100);
        op.getAnnotations().put(OperatorAnnotations.OP_OUTPUT_DOCSIZE,
                (double) Math.round(cardSizeCost.getSize() * 100) / 100);
        op.getAnnotations().put(OperatorAnnotations.OP_COST_TOTAL,
                (double) Math.round(cardSizeCost.getCost() * 100) / 100);
        op.getAnnotations().put(OperatorAnnotations.OP_COST_LOCAL, 0.0);
    }

    private EstimatedCostComputationVisitor.CardSizeCost annotate(EstimatedCostComputationVisitor visitor,
            ILogicalOperator op, Double arg) throws AlgebricksException {
        if (op.getInputs().isEmpty()) {
            return new EstimatedCostComputationVisitor.CardSizeCost(0.0, 0.0, 0.0);
        }
        EstimatedCostComputationVisitor.CardSizeCost cardSizeCost =
                op.getInputs().getFirst().getValue().accept(visitor, arg);
        annotateOp(op, cardSizeCost);

        return cardSizeCost;
    }

    // Visits an operator that has the left outer semantics, e.g.,
    // left outer join, left outer unnest, left outer unnest map.
    private EstimatedCostComputationVisitor.CardSizeCost visitLeftOuterUnnest(ILogicalOperator operator, Double arg)
            throws AlgebricksException {
        // Needs more work.
        return operator.getInputs().getFirst().getValue().accept(this, arg);
    }

    // Visits an inner join operator.
    private EstimatedCostComputationVisitor.CardSizeCost visitInnerJoin(InnerJoinOperator joinOperator, Double arg)
            throws AlgebricksException {
        EstimatedCostComputationVisitor.CardSizeCost cardSizeCost =
                new EstimatedCostComputationVisitor.CardSizeCost(0.0, 0.0, 0.0);

        ILogicalOperator left = joinOperator.getInputs().getFirst().getValue();
        ILogicalOperator right = joinOperator.getInputs().get(1).getValue();
        double leftExchangeCost = 0;
        double rightExchangeCost = 0;

        for (Map.Entry<String, Object> anno : joinOperator.getAnnotations().entrySet()) {
            if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_CARDINALITY)) {
                cardSizeCost.setCard((Double) anno.getValue());
            } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_OUTPUT_DOCSIZE)) {
                cardSizeCost.setSize((Double) anno.getValue());
            } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_COST_TOTAL)) {
                cardSizeCost.setCost((Double) anno.getValue());
            } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_LEFT_EXCHANGE_COST)) {
                leftExchangeCost = (double) anno.getValue();
            } else if (anno.getValue() != null && anno.getKey().equals(OperatorAnnotations.OP_RIGHT_EXCHANGE_COST)) {
                rightExchangeCost = (double) anno.getValue();
            }
        }

        // Visit the left and right branches.
        left.accept(this, leftExchangeCost);
        right.accept(this, rightExchangeCost);

        return cardSizeCost;
    }

    public static class CardSizeCost {

        public double card;
        public double size;
        public double cost;

        public CardSizeCost(double card, double size, double cost) {
            this.card = card;
            this.size = size;
            this.cost = cost;
        }

        public double getCard() {
            return card;
        }

        public double getSize() {
            return size;
        }

        public double getCost() {
            return cost;
        }

        public void setCard(double card) {
            this.card = card;
        }

        public void setSize(double size) {
            this.size = size;
        }

        public void setCost(double cost) {
            this.cost = cost;
        }
    }
}