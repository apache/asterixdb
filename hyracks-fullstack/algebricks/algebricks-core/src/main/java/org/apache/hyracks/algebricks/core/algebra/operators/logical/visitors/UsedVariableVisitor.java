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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
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
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HashPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HashPartitionMergeExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RangePartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RangePartitionMergeExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SortMergeExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class UsedVariableVisitor implements ILogicalOperatorVisitor<Void, Void> {

    private final Collection<LogicalVariable> usedVariables;

    public UsedVariableVisitor(Collection<LogicalVariable> usedVariables) {
        this.usedVariables = usedVariables;
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, Void arg) {
        for (Mutable<ILogicalExpression> exprRef : op.getExpressions()) {
            exprRef.getValue().getUsedVariables(usedVariables);
        }
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, Void arg) {
        for (Mutable<ILogicalExpression> exprRef : op.getExpressions()) {
            exprRef.getValue().getUsedVariables(usedVariables);
        }
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Void arg) {
        if (op.getAdditionalFilteringExpressions() != null) {
            for (Mutable<ILogicalExpression> e : op.getAdditionalFilteringExpressions()) {
                e.getValue().getUsedVariables(usedVariables);
            }
        }
        if (op.getSelectCondition() != null) {
            Set<LogicalVariable> usedVariablesBySelect = new HashSet<>();
            op.getSelectCondition().getValue().getUsedVariables(usedVariablesBySelect);
            usedVariablesBySelect.removeAll(op.getVariables());
            usedVariables.addAll(usedVariablesBySelect);
        }
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, Void arg) {
        for (Mutable<ILogicalExpression> eRef : op.getExpressions()) {
            eRef.getValue().getUsedVariables(usedVariables);
        }
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg) {
        // does not use any variable
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        // Used variables depend on the physical operator.
        if (op.getPhysicalOperator() != null) {
            IPhysicalOperator physOp = op.getPhysicalOperator();
            switch (physOp.getOperatorTag()) {
                case BROADCAST_EXCHANGE:
                case ONE_TO_ONE_EXCHANGE:
                case RANDOM_MERGE_EXCHANGE:
                case SEQUENTIAL_MERGE_EXCHANGE:
                    // No variables used.
                    break;
                case HASH_PARTITION_EXCHANGE:
                    HashPartitionExchangePOperator hashPartitionPOp = (HashPartitionExchangePOperator) physOp;
                    usedVariables.addAll(hashPartitionPOp.getHashFields());
                    break;
                case HASH_PARTITION_MERGE_EXCHANGE:
                    HashPartitionMergeExchangePOperator hashMergePOp = (HashPartitionMergeExchangePOperator) physOp;
                    usedVariables.addAll(hashMergePOp.getPartitionFields());
                    for (OrderColumn orderCol : hashMergePOp.getOrderColumns()) {
                        usedVariables.add(orderCol.getColumn());
                    }
                    break;
                case SORT_MERGE_EXCHANGE:
                    SortMergeExchangePOperator sortMergePOp = (SortMergeExchangePOperator) physOp;
                    for (OrderColumn orderCol : sortMergePOp.getSortColumns()) {
                        usedVariables.add(orderCol.getColumn());
                    }
                    break;
                case RANGE_PARTITION_EXCHANGE:
                    RangePartitionExchangePOperator rangePartitionPOp = (RangePartitionExchangePOperator) physOp;
                    for (OrderColumn partCol : rangePartitionPOp.getPartitioningFields()) {
                        usedVariables.add(partCol.getColumn());
                    }
                    break;
                case RANGE_PARTITION_MERGE_EXCHANGE:
                    RangePartitionMergeExchangePOperator rangeMergePOp = (RangePartitionMergeExchangePOperator) physOp;
                    for (OrderColumn partCol : rangeMergePOp.getPartitioningFields()) {
                        usedVariables.add(partCol.getColumn());
                    }
                    break;
                case RANDOM_PARTITION_EXCHANGE:
                    break;
                default:
                    throw new AlgebricksException("Unhandled physical operator tag '" + physOp.getOperatorTag() + "'.");
            }
        }
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        visitNestedPlans(op);
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> g : op.getGroupByList()) {
            g.second.getValue().getUsedVariables(usedVariables);
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> g : op.getDecorList()) {
            g.second.getValue().getUsedVariables(usedVariables);
        }
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, Void arg) {
        op.getCondition().getValue().getUsedVariables(usedVariables);
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) {
        op.getCondition().getValue().getUsedVariables(usedVariables);
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, Void arg) {
        op.getMaxObjects().getValue().getUsedVariables(usedVariables);
        ILogicalExpression offsetExpr = op.getOffset().getValue();
        if (offsetExpr != null) {
            offsetExpr.getUsedVariables(usedVariables);
        }
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg) {
        // does not use any variable
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, Void arg) {
        for (Pair<IOrder, Mutable<ILogicalExpression>> oe : op.getOrderExpressions()) {
            oe.second.getValue().getUsedVariables(usedVariables);
        }
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, Void arg) {
        List<LogicalVariable> parameterVariables = op.getVariables();
        for (LogicalVariable v : parameterVariables) {
            if (!usedVariables.contains(v)) {
                usedVariables.add(v);
            }
        }
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, Void arg) {
        for (Mutable<ILogicalExpression> exprRef : op.getExpressions()) {
            exprRef.getValue().getUsedVariables(usedVariables);
        }
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, Void arg) {
        List<LogicalVariable> parameterVariables = op.getInputVariables();
        for (LogicalVariable v : parameterVariables) {
            if (!usedVariables.contains(v)) {
                usedVariables.add(v);
            }
        }
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, Void arg) {
        op.getCondition().getValue().getUsedVariables(usedVariables);
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        visitNestedPlans(op);
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, Void arg) {
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> m : op.getVariableMappings()) {
            if (!usedVariables.contains(m.first)) {
                usedVariables.add(m.first);
            }
            if (!usedVariables.contains(m.second)) {
                usedVariables.add(m.second);
            }
        }
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
        boolean hasExtraVars = op.hasExtraVariables();
        for (int i = 0; i < op.getNumInput(); i++) {
            for (LogicalVariable var : op.getInputCompareVariables(i)) {
                if (!usedVariables.contains(var)) {
                    usedVariables.add(var);
                }
            }
            if (hasExtraVars) {
                for (LogicalVariable var : op.getInputExtraVariables(i)) {
                    if (!usedVariables.contains(var)) {
                        usedVariables.add(var);
                    }
                }
            }
        }
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, Void arg) {
        getUsedVarsFromExprAndFilterExpr(op);
        if (op.getSelectCondition() != null) {
            Set<LogicalVariable> usedVariablesBySelect = new HashSet<>();
            op.getSelectCondition().getValue().getUsedVariables(usedVariablesBySelect);
            usedVariablesBySelect.removeAll(op.getVariables());
            usedVariables.addAll(usedVariablesBySelect);
        }
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg) throws AlgebricksException {
        getUsedVarsFromExprAndFilterExpr(op);
        return null;
    }

    private void getUsedVarsFromExprAndFilterExpr(AbstractUnnestMapOperator op) {
        op.getExpressionRef().getValue().getUsedVariables(usedVariables);
        if (op.getAdditionalFilteringExpressions() != null) {
            for (Mutable<ILogicalExpression> e : op.getAdditionalFilteringExpressions()) {
                e.getValue().getUsedVariables(usedVariables);
            }
        }
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, Void arg) {
        op.getExpressionRef().getValue().getUsedVariables(usedVariables);
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, Void arg) {
        for (Mutable<ILogicalExpression> expr : op.getExpressions()) {
            expr.getValue().getUsedVariables(usedVariables);
        }
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, Void arg) {
        for (Mutable<ILogicalExpression> expr : op.getExpressions()) {
            expr.getValue().getUsedVariables(usedVariables);
        }
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, Void arg) {
        op.getPayloadExpression().getValue().getUsedVariables(usedVariables);
        for (Mutable<ILogicalExpression> e : op.getKeyExpressions()) {
            e.getValue().getUsedVariables(usedVariables);
        }
        if (op.getAdditionalFilteringExpressions() != null) {
            for (Mutable<ILogicalExpression> e : op.getAdditionalFilteringExpressions()) {
                e.getValue().getUsedVariables(usedVariables);
            }
        }
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void arg) {
        // 1. The record variable
        op.getPayloadExpression().getValue().getUsedVariables(usedVariables);

        // 2. The primary key variables
        for (Mutable<ILogicalExpression> e : op.getPrimaryKeyExpressions()) {
            e.getValue().getUsedVariables(usedVariables);
        }
        // 3. The filters variables
        if (op.getAdditionalFilteringExpressions() != null) {
            for (Mutable<ILogicalExpression> e : op.getAdditionalFilteringExpressions()) {
                e.getValue().getUsedVariables(usedVariables);
            }
        }
        // 4. The Other variables (Not key, Not payload, and Not Filter)
        if (op.getAdditionalNonFilteringExpressions() != null) {
            for (Mutable<ILogicalExpression> e : op.getAdditionalNonFilteringExpressions()) {
                e.getValue().getUsedVariables(usedVariables);
            }
        }
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void arg) {
        for (Mutable<ILogicalExpression> e : op.getPrimaryKeyExpressions()) {
            e.getValue().getUsedVariables(usedVariables);
        }
        for (Mutable<ILogicalExpression> e : op.getSecondaryKeyExpressions()) {
            e.getValue().getUsedVariables(usedVariables);
        }
        if (op.getFilterExpression() != null) {
            op.getFilterExpression().getValue().getUsedVariables(usedVariables);
        }
        if (op.getAdditionalFilteringExpressions() != null) {
            for (Mutable<ILogicalExpression> e : op.getAdditionalFilteringExpressions()) {
                e.getValue().getUsedVariables(usedVariables);
            }
        }
        if (op.getPrevAdditionalFilteringExpression() != null) {
            op.getPrevAdditionalFilteringExpression().getValue().getUsedVariables(usedVariables);
        }
        if (op.getPrevSecondaryKeyExprs() != null) {
            for (Mutable<ILogicalExpression> e : op.getPrevSecondaryKeyExprs()) {
                e.getValue().getUsedVariables(usedVariables);
            }
        }
        if (op.getUpsertIndicatorExpr() != null) {
            op.getUpsertIndicatorExpr().getValue().getUsedVariables(usedVariables);
        }
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, Void arg) {
        for (Mutable<ILogicalExpression> e : op.getPrimaryKeyExpressions()) {
            e.getValue().getUsedVariables(usedVariables);
        }
        for (Mutable<ILogicalExpression> e : op.getSecondaryKeyExpressions()) {
            e.getValue().getUsedVariables(usedVariables);
        }
        return null;
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
        op.getSideDataExpression().getValue().getUsedVariables(usedVariables);
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, Void arg) {
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
        op.getBranchingExpression().getValue().getUsedVariables(usedVariables);
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
        op.getDelegate().getUsedVariables(usedVariables);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg) throws AlgebricksException {
        op.getExpressionRef().getValue().getUsedVariables(usedVariables);
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
        visitNestedPlans(op);
        for (Mutable<ILogicalExpression> exprRef : op.getPartitionExpressions()) {
            exprRef.getValue().getUsedVariables(usedVariables);
        }
        for (Pair<IOrder, Mutable<ILogicalExpression>> p : op.getOrderExpressions()) {
            p.second.getValue().getUsedVariables(usedVariables);
        }
        for (Pair<IOrder, Mutable<ILogicalExpression>> p : op.getFrameValueExpressions()) {
            p.second.getValue().getUsedVariables(usedVariables);
        }
        for (Mutable<ILogicalExpression> exprRef : op.getFrameStartExpressions()) {
            exprRef.getValue().getUsedVariables(usedVariables);
        }
        for (Mutable<ILogicalExpression> exprRef : op.getFrameStartValidationExpressions()) {
            exprRef.getValue().getUsedVariables(usedVariables);
        }
        for (Mutable<ILogicalExpression> exprRef : op.getFrameEndExpressions()) {
            exprRef.getValue().getUsedVariables(usedVariables);
        }
        for (Mutable<ILogicalExpression> exprRef : op.getFrameEndValidationExpressions()) {
            exprRef.getValue().getUsedVariables(usedVariables);
        }
        for (Mutable<ILogicalExpression> exprRef : op.getFrameExcludeExpressions()) {
            exprRef.getValue().getUsedVariables(usedVariables);
        }
        ILogicalExpression frameExcludeUnaryExpr = op.getFrameExcludeUnaryExpression().getValue();
        if (frameExcludeUnaryExpr != null) {
            frameExcludeUnaryExpr.getUsedVariables(usedVariables);
        }
        ILogicalExpression frameOffsetExpr = op.getFrameOffsetExpression().getValue();
        if (frameOffsetExpr != null) {
            frameOffsetExpr.getUsedVariables(usedVariables);
        }
        for (Mutable<ILogicalExpression> exprRef : op.getExpressions()) {
            exprRef.getValue().getUsedVariables(usedVariables);
        }
        return null;
    }

    private void visitNestedPlans(AbstractOperatorWithNestedPlans op) throws AlgebricksException {
        for (ILogicalPlan p : op.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                VariableUtilities.getUsedVariablesInDescendantsAndSelf(r.getValue(), usedVariables);
            }
        }
    }
}
