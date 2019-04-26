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
package org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * A visitor that provides the basic, static inference of tuple cardinalities of an
 * operator's output. There are only three cases:
 * <p/>
 * 1. ZERO_OR_ONE: the cardinality is either zero or one.
 * <p/>
 * 2. ONE: the cardinality is exactly one in any case;
 * <p/>
 * 3. ZERO_OR_ONE_GROUP: if we group output tuples of the operator by any variable in <code>keyVariables</code>, it will
 * result in zero or one group;
 * <p/>
 * 4. ONE_GROUP: if we group output tuples of the operator by any variable in <code>keyVariables</code>, it will
 * result in exact one group;
 * <p/>
 * 5. the cardinality is some unknown value.
 */
public class CardinalityInferenceVisitor implements ILogicalOperatorVisitor<Long, Void> {
    private static final long ZERO_OR_ONE = 0L;
    private static final long ONE = 1L;
    private static final long ZERO_OR_ONE_GROUP = 2L;
    private static final long ONE_GROUP = 3L;
    private static final long UNKNOWN = 1000L;

    private final Set<LogicalVariable> keyVariables = new HashSet<>();

    @Override
    public Long visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
        return ONE;
    }

    @Override
    public Long visitRunningAggregateOperator(RunningAggregateOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg) throws AlgebricksException {
        // Empty tuple source operator sends an empty tuple to downstream operators.
        return ONE;
    }

    @Override
    public Long visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        if (op.isGroupAll()) {
            return ONE;
        }
        ILogicalOperator inputOp = op.getInputs().get(0).getValue();
        long inputCardinality = inputOp.accept(this, arg);
        List<LogicalVariable> gbyVar = op.getGroupByVarList();
        if (inputCardinality == ONE_GROUP && keyVariables.containsAll(gbyVar)) {
            keyVariables.clear();
            return ONE;
        }
        if (inputCardinality == ZERO_OR_ONE_GROUP && keyVariables.containsAll(gbyVar)) {
            keyVariables.clear();
            return ZERO_OR_ONE;
        }
        // ZERO_OR_ONE, ONE, ZERO_OR_ONE_GROUP, ONE_GROUP, OR UNKNOWN
        return inputCardinality;
    }

    @Override
    public Long visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        return adjustCardinalityForTupleReductionOperator(op.getInputs().get(0).getValue().accept(this, arg));
    }

    @Override
    public Long visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
        // Inner join can have 0 to M * N tuples, where M is the cardinality of the left input branch
        // and N is the cardinality of the right input branch.
        // We only provide inference for the case the JOIN condition is TRUE.
        return visitInnerJoin(op, arg);
    }

    @Override
    public Long visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
        return visitLeftOuterUnnest(op, arg);
    }

    @Override
    public Long visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg) throws AlgebricksException {
        return ONE;
    }

    @Override
    public Long visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
        return adjustCardinalityForTupleReductionOperator(op.getInputs().get(0).getValue().accept(this, arg));
    }

    @Override
    public Long visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
        keyVariables.retainAll(op.getVariables()); // Only returns live variables.
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitSinkOperator(SinkOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg) throws AlgebricksException {
        return visitLeftOuterUnnest(op, arg);
    }

    @Override
    public Long visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg) throws AlgebricksException {
        return visitLeftOuterUnnest(op, arg);
    }

    @Override
    public Long visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitWriteOperator(WriteOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitDistributeResultOperator(DistributeResultOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitWriteResultOperator(WriteResultOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void arg)
            throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
        return UNKNOWN;
    }

    @Override
    public Long visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
        return op.getInputs().get(0).getValue().accept(this, arg);
    }

    @Override
    public Long visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
        long cardinality = UNKNOWN;
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            long inputCardinality = inputOpRef.getValue().accept(this, arg);
            if (inputCardinality <= ONE) {
                return ZERO_OR_ONE;
            }
            if (inputCardinality == ZERO_OR_ONE_GROUP || inputCardinality == ONE_GROUP) {
                cardinality = ZERO_OR_ONE_GROUP;
            }
        }
        return cardinality;
    }

    // Visits an operator that has the left outer semantics, e.g.,
    // left outer join, left outer unnest, left outer unnest map.
    private long visitLeftOuterUnnest(ILogicalOperator operator, Void arg) throws AlgebricksException {
        ILogicalOperator left = operator.getInputs().get(0).getValue();
        long leftCardinality = left.accept(this, arg);
        if (leftCardinality == ONE) {
            keyVariables.clear();
            VariableUtilities.getLiveVariables(left, keyVariables);
            return ONE_GROUP;
        }
        if (leftCardinality == ZERO_OR_ONE) {
            keyVariables.clear();
            VariableUtilities.getLiveVariables(left, keyVariables);
            return ZERO_OR_ONE_GROUP;
        }
        // ZERO_OR_ONE_GROUP, ONE_GROUP (maintained from the left branch) or UNKNOWN.
        return leftCardinality;
    }

    // Visits an inner join operator, particularly, deals with the case the join is a cartesian product.
    private long visitInnerJoin(InnerJoinOperator joinOperator, Void arg) throws AlgebricksException {
        ILogicalExpression conditionExpr = joinOperator.getCondition().getValue();
        if (!conditionExpr.equals(ConstantExpression.TRUE)) {
            // Currently we are not able to estimate for more general join conditions.
            return UNKNOWN;
        }
        Set<LogicalVariable> newKeyVars = new HashSet<>();

        // Visits the left branch and adds left key variables (if the left branch delivers one group).
        ILogicalOperator left = joinOperator.getInputs().get(0).getValue();
        long leftCardinality = left.accept(this, arg);
        newKeyVars.addAll(keyVariables);

        // Visits the right branch and adds right key variables (if the right branch delivers one group).
        ILogicalOperator right = joinOperator.getInputs().get(1).getValue();
        long rightCardinality = right.accept(this, arg);
        newKeyVars.addAll(keyVariables);

        // If any branch has carinality zero or one, the result will have cardinality zero or one.
        if (leftCardinality == ZERO_OR_ONE && rightCardinality == ZERO_OR_ONE) {
            return ZERO_OR_ONE;
        }

        // If both branches has cardinality one, the result for sure has cardinality one.
        if (leftCardinality == ONE && rightCardinality == ONE) {
            return ONE;
        }

        keyVariables.clear();
        // If one branch has cardinality zero_or_one, the result for sure has cardinality one.
        if (leftCardinality == ZERO_OR_ONE || rightCardinality == ZERO_OR_ONE) {
            VariableUtilities.getLiveVariables(leftCardinality == ONE ? left : right, keyVariables);
            return ZERO_OR_ONE_GROUP;
        }

        // If one branch has cardinality one, the result has one group.
        if (leftCardinality == ONE || rightCardinality == ONE) {
            VariableUtilities.getLiveVariables(leftCardinality == ONE ? left : right, keyVariables);
            return ONE_GROUP;
        }

        // If one branch has zero or one group, the result has one zero or one group.
        if (leftCardinality == ONE_GROUP || rightCardinality == ONE_GROUP || leftCardinality == ZERO_OR_ONE_GROUP
                || rightCardinality == ZERO_OR_ONE_GROUP) {
            keyVariables.addAll(newKeyVars);
            return Math.min(leftCardinality, rightCardinality);
        }
        return UNKNOWN;
    }

    // For operators including SELECT and LIMIT.
    private long adjustCardinalityForTupleReductionOperator(long inputCardinality) {
        if (inputCardinality == ONE) {
            return ZERO_OR_ONE;
        }
        if (inputCardinality == ONE_GROUP) {
            return ZERO_OR_ONE_GROUP;
        }
        return inputCardinality;
    }

}
