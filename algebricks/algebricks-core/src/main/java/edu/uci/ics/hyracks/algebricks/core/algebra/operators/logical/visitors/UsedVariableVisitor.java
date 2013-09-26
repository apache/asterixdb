/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.PartitioningSplitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.HashPartitionExchangePOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.HashPartitionMergeExchangePOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.RangePartitionPOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.SortMergeExchangePOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.OrderColumn;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class UsedVariableVisitor implements ILogicalOperatorVisitor<Void, Void> {

    private Collection<LogicalVariable> usedVariables;

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
        // does not use any variable
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
                case RANDOM_MERGE_EXCHANGE: {
                    // No variables used.
                    break;
                }
                case HASH_PARTITION_EXCHANGE: {
                    HashPartitionExchangePOperator concreteOp = (HashPartitionExchangePOperator) physOp;
                    usedVariables.addAll(concreteOp.getHashFields());
                    break;
                }
                case HASH_PARTITION_MERGE_EXCHANGE: {
                    HashPartitionMergeExchangePOperator concreteOp = (HashPartitionMergeExchangePOperator) physOp;
                    usedVariables.addAll(concreteOp.getPartitionFields());
                    for (OrderColumn orderCol : concreteOp.getOrderColumns()) {
                        usedVariables.add(orderCol.getColumn());
                    }
                    break;
                }
                case SORT_MERGE_EXCHANGE: {
                    SortMergeExchangePOperator concreteOp = (SortMergeExchangePOperator) physOp;
                    for (OrderColumn orderCol : concreteOp.getSortColumns()) {
                        usedVariables.add(orderCol.getColumn());
                    }
                    break;
                }
                case RANGE_PARTITION_EXCHANGE: {
                    RangePartitionPOperator concreteOp = (RangePartitionPOperator) physOp;
                    for (OrderColumn partCol : concreteOp.getPartitioningFields()) {
                        usedVariables.add(partCol.getColumn());
                    }
                    break;
                }
                default: {
                    throw new AlgebricksException("Unhandled physical operator tag '" + physOp.getOperatorTag() + "'.");
                }
            }
        }
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        for (ILogicalPlan p : op.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                VariableUtilities.getUsedVariablesInDescendantsAndSelf(r.getValue(), usedVariables);
            }
        }
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
    public Void visitPartitioningSplitOperator(PartitioningSplitOperator op, Void arg) {
        for (Mutable<ILogicalExpression> e : op.getExpressions()) {
            e.getValue().getUsedVariables(usedVariables);
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
        for (ILogicalPlan p : op.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                VariableUtilities.getUsedVariablesInDescendantsAndSelf(r.getValue(), usedVariables);
            }
        }
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
    public Void visitUnnestMapOperator(UnnestMapOperator op, Void arg) {
        op.getExpressionRef().getValue().getUsedVariables(usedVariables);
        return null;
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
        return null;
    }

    @Override
    public Void visitInsertDeleteOperator(InsertDeleteOperator op, Void arg) {
        op.getPayloadExpression().getValue().getUsedVariables(usedVariables);
        for (Mutable<ILogicalExpression> e : op.getPrimaryKeyExpressions()) {
            e.getValue().getUsedVariables(usedVariables);
        }
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteOperator(IndexInsertDeleteOperator op, Void arg) {
        for (Mutable<ILogicalExpression> e : op.getPrimaryKeyExpressions()) {
            e.getValue().getUsedVariables(usedVariables);
        }
        for (Mutable<ILogicalExpression> e : op.getSecondaryKeyExpressions()) {
            e.getValue().getUsedVariables(usedVariables);
        }
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
    public Void visitExtensionOperator(ExtensionOperator op, Void arg) throws AlgebricksException {
        op.getDelegate().getUsedVariables(usedVariables);
        return null;
    }

}
