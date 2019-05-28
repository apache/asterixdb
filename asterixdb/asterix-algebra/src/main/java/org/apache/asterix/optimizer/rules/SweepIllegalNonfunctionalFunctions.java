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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
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
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class SweepIllegalNonfunctionalFunctions implements IAlgebraicRewriteRule {

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

        private void sweepExpression(ILogicalExpression expr) throws AlgebricksException {
            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL && !expr.isFunctional()) {
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, fce.getSourceLocation(),
                        "Found non-functional function " + fce.getFunctionIdentifier());
            }
        }

        @Override
        public Void visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
            for (Mutable<ILogicalExpression> me : op.getExpressions()) {
                sweepExpression(me.getValue());
            }
            List<Mutable<ILogicalExpression>> mergeExprs = op.getMergeExpressions();
            if (mergeExprs != null) {
                for (Mutable<ILogicalExpression> me : mergeExprs) {
                    sweepExpression(me.getValue());
                }
            }
            return null;
        }

        @Override
        public Void visitRunningAggregateOperator(RunningAggregateOperator op, Void arg) throws AlgebricksException {
            for (Mutable<ILogicalExpression> me : op.getExpressions()) {
                sweepExpression(me.getValue());
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
                sweepExpression(p.second.getValue());
            }
            for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : op.getDecorList()) {
                sweepExpression(p.second.getValue());
            }
            return null;
        }

        @Override
        public Void visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
            sweepExpression(op.getCondition().getValue());
            return null;
        }

        @Override
        public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
            sweepExpression(op.getCondition().getValue());
            return null;
        }

        @Override
        public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
            for (Pair<IOrder, Mutable<ILogicalExpression>> p : op.getOrderExpressions()) {
                sweepExpression(p.second.getValue());
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
        public Void visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
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
        public Void visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg)
                throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
            for (Mutable<ILogicalExpression> expr : op.getExpressions()) {
                sweepExpression(expr.getValue());
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
        public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void tag)
                throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void tag)
                throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitTokenizeOperator(TokenizeOperator op, Void tag) throws AlgebricksException {
            return null;
        }

        @Override
        public Void visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
            sweepExpression(op.getSideDataExpression().getValue());
            return null;
        }

        @Override
        public Void visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
            for (Mutable<ILogicalExpression> me : op.getPartitionExpressions()) {
                sweepExpression(me.getValue());
            }
            for (Pair<IOrder, Mutable<ILogicalExpression>> p : op.getOrderExpressions()) {
                sweepExpression(p.second.getValue());
            }
            for (Pair<IOrder, Mutable<ILogicalExpression>> p : op.getFrameValueExpressions()) {
                sweepExpression(p.second.getValue());
            }
            for (Mutable<ILogicalExpression> me : op.getFrameStartExpressions()) {
                sweepExpression(me.getValue());
            }
            for (Mutable<ILogicalExpression> me : op.getFrameStartValidationExpressions()) {
                sweepExpression(me.getValue());
            }
            for (Mutable<ILogicalExpression> me : op.getFrameEndExpressions()) {
                sweepExpression(me.getValue());
            }
            for (Mutable<ILogicalExpression> me : op.getFrameEndValidationExpressions()) {
                sweepExpression(me.getValue());
            }
            for (Mutable<ILogicalExpression> me : op.getFrameExcludeExpressions()) {
                sweepExpression(me.getValue());
            }
            ILogicalExpression frameExcludeUnaryExpr = op.getFrameExcludeUnaryExpression().getValue();
            if (frameExcludeUnaryExpr != null) {
                sweepExpression(frameExcludeUnaryExpr);
            }
            ILogicalExpression frameOffsetExpr = op.getFrameOffsetExpression().getValue();
            if (frameOffsetExpr != null) {
                sweepExpression(frameOffsetExpr);
            }
            for (Mutable<ILogicalExpression> me : op.getExpressions()) {
                ILogicalExpression expr = me.getValue();
                if (isStatefulFunctionCall(expr)) {
                    for (Mutable<ILogicalExpression> fcallArg : ((AbstractFunctionCallExpression) expr)
                            .getArguments()) {
                        sweepExpression(fcallArg.getValue());
                    }
                } else {
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, op.getSourceLocation());
                }
            }
            return null;
        }

        private boolean isStatefulFunctionCall(ILogicalExpression expr) {
            return expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                    && ((AbstractFunctionCallExpression) expr)
                            .getKind() == AbstractFunctionCallExpression.FunctionKind.STATEFUL;
        }
    }
}
