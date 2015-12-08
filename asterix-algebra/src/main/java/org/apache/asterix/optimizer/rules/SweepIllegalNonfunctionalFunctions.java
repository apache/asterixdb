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
package org.apache.asterix.optimizer.rules;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.PartitioningSplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.rules.AbstractExtractExprRule;

public class SweepIllegalNonfunctionalFunctions extends AbstractExtractExprRule implements IAlgebraicRewriteRule {

    private final IllegalNonfunctionalFunctionSweeperOperatorVisitor visitor;

    public SweepIllegalNonfunctionalFunctions() {
        visitor = new IllegalNonfunctionalFunctionSweeperOperatorVisitor();
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        op.accept(visitor, null);
        context.computeAndSetTypeEnvironmentForOperator(op);
        context.addToDontApplySet(this, op);
        return false;
    }

    private class IllegalNonfunctionalFunctionSweeperOperatorVisitor implements ILogicalOperatorVisitor<Void, Void> {

        private void sweepExpression(ILogicalExpression expr, ILogicalOperator op) throws AlgebricksException {
            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                if (!expr.isFunctional()) {
                    AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
                    throw new AlgebricksException(
                            "Found non-functional function " + fce.getFunctionIdentifier() + " in op " + op);
                }
            }
        }

        @Override
        public Void visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
            for (Mutable<ILogicalExpression> me : op.getExpressions()) {
                sweepExpression(me.getValue(), op);
            }
            List<Mutable<ILogicalExpression>> mergeExprs = op.getMergeExpressions();
            if (mergeExprs != null) {
                for (Mutable<ILogicalExpression> me : mergeExprs) {
                    sweepExpression(me.getValue(), op);
                }
            }
            return null;
        }

        @Override
        public Void visitRunningAggregateOperator(RunningAggregateOperator op, Void arg) throws AlgebricksException {
            for (Mutable<ILogicalExpression> me : op.getExpressions()) {
                sweepExpression(me.getValue(), op);
            }
            return null;
        }

        @Override
        public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
            for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : op.getGroupByList()) {
                sweepExpression(p.second.getValue(), op);
            }
            for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : op.getDecorList()) {
                sweepExpression(p.second.getValue(), op);
            }
            return null;
        }

        @Override
        public Void visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
            sweepExpression(op.getCondition().getValue(), op);
            return null;
        }

        @Override
        public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
            sweepExpression(op.getCondition().getValue(), op);
            return null;
        }

        @Override
        public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
            for (Pair<IOrder, Mutable<ILogicalExpression>> p : op.getOrderExpressions()) {
                sweepExpression(p.second.getValue(), op);
            }
            return null;
        }

        @Override
        public Void visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitExtensionOperator(ExtensionOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitPartitioningSplitOperator(PartitioningSplitOperator op, Void arg) throws AlgebricksException {
            for (Mutable<ILogicalExpression> expr : op.getExpressions()) {
                sweepExpression(expr.getValue(), op);
            }
            return null;
        }

        @Override
        public Void visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitSinkOperator(SinkOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitOuterUnnestOperator(OuterUnnestOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
            for (Mutable<ILogicalExpression> expr : op.getExpressions()) {
                sweepExpression(expr.getValue(), op);
            }
            return null;
        }

        @Override
        public Void visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitWriteOperator(WriteOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitDistributeResultOperator(DistributeResultOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitWriteResultOperator(WriteResultOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitInsertDeleteOperator(InsertDeleteOperator op, Void tag) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitIndexInsertDeleteOperator(IndexInsertDeleteOperator op, Void tag) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitTokenizeOperator(TokenizeOperator op, Void tag) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitExternalDataLookupOperator(ExternalDataLookupOperator op, Void arg)
                throws AlgebricksException {
            return null;
        }
    }

}
