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

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestNonMapOperator;
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
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class SubstituteVariableVisitor
        implements ILogicalOperatorVisitor<Void, Pair<LogicalVariable, LogicalVariable>> {

    private final boolean goThroughNts;
    private final ITypingContext ctx;

    public SubstituteVariableVisitor(boolean goThroughNts, ITypingContext ctx) {
        this.goThroughNts = goThroughNts;
        this.ctx = ctx;
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substAssignVariables(op.getVariables(), op.getExpressions(), pair);
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substAssignVariables(op.getVariables(), op.getExpressions(), pair);
        // Substitute variables stored in ordering property
        if (op.getExplicitOrderingProperty() != null) {
            List<OrderColumn> orderColumns = op.getExplicitOrderingProperty().getOrderColumns();
            for (int i = 0; i < orderColumns.size(); i++) {
                OrderColumn oc = orderColumns.get(i);
                if (oc.getColumn().equals(pair.first)) {
                    orderColumns.set(i, new OrderColumn(pair.second, oc.getOrder()));
                }
            }
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        List<LogicalVariable> variables = op.getVariables();
        for (int i = 0; i < variables.size(); i++) {
            if (variables.get(i) == pair.first) {
                variables.set(i, pair.second);
                return null;
            }
        }
        if (op.getSelectCondition() != null) {
            op.getSelectCondition().getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (Mutable<ILogicalExpression> eRef : op.getExpressions()) {
            eRef.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op,
            Pair<LogicalVariable, LogicalVariable> pair) {
        // does not use any variable
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, Pair<LogicalVariable, LogicalVariable> pair) {
        // does not use any variable
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        subst(pair.first, pair.second, op.getGroupByList());
        subst(pair.first, pair.second, op.getDecorList());
        substInNestedPlans(pair.first, pair.second, op);
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        op.getCondition().getValue().substituteVar(pair.first, pair.second);
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        op.getCondition().getValue().substituteVar(pair.first, pair.second);
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        op.getMaxObjects().getValue().substituteVar(pair.first, pair.second);
        ILogicalExpression offset = op.getOffset().getValue();
        if (offset != null) {
            offset.substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op,
            Pair<LogicalVariable, LogicalVariable> pair) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (Pair<IOrder, Mutable<ILogicalExpression>> oe : op.getOrderExpressions()) {
            oe.second.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        List<LogicalVariable> usedVariables = op.getVariables();
        int n = usedVariables.size();
        for (int i = 0; i < n; i++) {
            LogicalVariable v = usedVariables.get(i);
            if (v.equals(pair.first)) {
                usedVariables.set(i, pair.second);
            }
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substAssignVariables(op.getVariables(), op.getExpressions(), pair);
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substInArray(op.getInputVariables(), pair.first, pair.second);
        substInArray(op.getOutputVariables(), pair.first, pair.second);
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, Pair<LogicalVariable, LogicalVariable> pair) {
        op.getCondition().getValue().substituteVar(pair.first, pair.second);
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substInNestedPlans(pair.first, pair.second, op);
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap = op.getVariableMappings();
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> t : varMap) {
            if (t.first.equals(pair.first)) {
                t.first = pair.second;
            }
            if (t.second.equals(pair.first)) {
                t.second = pair.second;
            }
            if (t.third.equals(pair.first)) {
                t.third = pair.second;
            }
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean hasExtraVars = op.hasExtraVariables();
        substInArray(op.getOutputCompareVariables(), pair.first, pair.second);
        if (hasExtraVars) {
            substInArray(op.getOutputExtraVariables(), pair.first, pair.second);
        }
        for (int i = 0, n = op.getNumInput(); i < n; i++) {
            substInArray(op.getInputCompareVariables(i), pair.first, pair.second);
            if (hasExtraVars) {
                substInArray(op.getInputExtraVariables(i), pair.first, pair.second);
            }
        }
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substituteVarsForAbstractUnnestMapOp(op, pair);
        if (op.getSelectCondition() != null) {
            op.getSelectCondition().getValue().substituteVar(pair.first, pair.second);
        }
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op,
            Pair<LogicalVariable, LogicalVariable> pair) throws AlgebricksException {
        substituteVarsForAbstractUnnestMapOp(op, pair);
        return null;
    }

    private void substituteVarsForAbstractUnnestMapOp(AbstractUnnestMapOperator op,
            Pair<LogicalVariable, LogicalVariable> pair) throws AlgebricksException {
        List<LogicalVariable> variables = op.getVariables();
        for (int i = 0; i < variables.size(); i++) {
            if (variables.get(i) == pair.first) {
                variables.set(i, pair.second);
                return;
            }
        }
        op.getExpressionRef().getValue().substituteVar(pair.first, pair.second);
        substVarTypes(op, pair);
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        return visitUnnestNonMapOperator(op, pair);
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (Mutable<ILogicalExpression> e : op.getExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (Mutable<ILogicalExpression> e : op.getExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        op.getPayloadExpression().getValue().substituteVar(pair.first, pair.second);
        for (Mutable<ILogicalExpression> e : op.getKeyExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    private void subst(LogicalVariable v1, LogicalVariable v2,
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> varExprPairList) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : varExprPairList) {
            if (ve.first != null && ve.first.equals(v1)) {
                ve.first = v2;
                return;
            }
            ve.second.getValue().substituteVar(v1, v2);
        }
    }

    private void substInArray(List<LogicalVariable> varArray, LogicalVariable v1, LogicalVariable v2) {
        for (int i = 0; i < varArray.size(); i++) {
            LogicalVariable v = varArray.get(i);
            if (v == v1) {
                varArray.set(i, v2);
            }
        }
    }

    private void substInNestedPlans(LogicalVariable v1, LogicalVariable v2, AbstractOperatorWithNestedPlans op)
            throws AlgebricksException {
        for (ILogicalPlan p : op.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                VariableUtilities.substituteVariablesInDescendantsAndSelf(r.getValue(), v1, v2, ctx);
            }
        }
    }

    private void substAssignVariables(List<LogicalVariable> variables, List<Mutable<ILogicalExpression>> expressions,
            Pair<LogicalVariable, LogicalVariable> pair) {
        int n = variables.size();
        for (int i = 0; i < n; i++) {
            if (variables.get(i).equals(pair.first)) {
                variables.set(i, pair.second);
            } else {
                expressions.get(i).getValue().substituteVar(pair.first, pair.second);
            }
        }
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, Pair<LogicalVariable, LogicalVariable> arg)
            throws AlgebricksException {
        op.substituteVar(arg.first, arg.second);
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, Pair<LogicalVariable, LogicalVariable> arg)
            throws AlgebricksException {
        op.substituteVar(arg.first, arg.second);
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, Pair<LogicalVariable, LogicalVariable> arg)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op,
            Pair<LogicalVariable, LogicalVariable> pair) throws AlgebricksException {
        op.getPayloadExpression().getValue().substituteVar(pair.first, pair.second);
        for (Mutable<ILogicalExpression> e : op.getPrimaryKeyExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op,
            Pair<LogicalVariable, LogicalVariable> pair) throws AlgebricksException {
        for (Mutable<ILogicalExpression> e : op.getPrimaryKeyExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        for (Mutable<ILogicalExpression> e : op.getSecondaryKeyExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (Mutable<ILogicalExpression> e : op.getPrimaryKeyExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        for (Mutable<ILogicalExpression> e : op.getSecondaryKeyExpressions()) {
            e.getValue().substituteVar(pair.first, pair.second);
        }
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, Pair<LogicalVariable, LogicalVariable> arg)
            throws AlgebricksException {
        op.getSideDataExpression().getValue().substituteVar(arg.first, arg.second);
        substVarTypes(op, arg);
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        return null;
    }

    private void substVarTypes(ILogicalOperator op, Pair<LogicalVariable, LogicalVariable> arg)
            throws AlgebricksException {
        if (ctx == null) {
            return;
        }
        IVariableTypeEnvironment env = ctx.getOutputTypeEnvironment(op);
        if (env != null) {
            env.substituteProducedVariable(arg.first, arg.second);
        }
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, Pair<LogicalVariable, LogicalVariable> arg)
            throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        return visitUnnestNonMapOperator(op, pair);
    }

    private Void visitUnnestNonMapOperator(AbstractUnnestNonMapOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        List<LogicalVariable> variables = op.getVariables();
        for (int i = 0; i < variables.size(); i++) {
            if (variables.get(i) == pair.first) {
                variables.set(i, pair.second);
                return null;
            }
        }
        op.getExpressionRef().getValue().substituteVar(pair.first, pair.second);
        substVarTypes(op, pair);
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (Mutable<ILogicalExpression> expr : op.getPartitionExpressions()) {
            expr.getValue().substituteVar(pair.first, pair.second);
        }
        for (Pair<IOrder, Mutable<ILogicalExpression>> p : op.getOrderExpressions()) {
            p.second.getValue().substituteVar(pair.first, pair.second);
        }
        for (Pair<IOrder, Mutable<ILogicalExpression>> p : op.getFrameValueExpressions()) {
            p.second.getValue().substituteVar(pair.first, pair.second);
        }
        for (Mutable<ILogicalExpression> expr : op.getFrameStartExpressions()) {
            expr.getValue().substituteVar(pair.first, pair.second);
        }
        for (Mutable<ILogicalExpression> expr : op.getFrameStartValidationExpressions()) {
            expr.getValue().substituteVar(pair.first, pair.second);
        }
        for (Mutable<ILogicalExpression> expr : op.getFrameEndExpressions()) {
            expr.getValue().substituteVar(pair.first, pair.second);
        }
        for (Mutable<ILogicalExpression> expr : op.getFrameEndValidationExpressions()) {
            expr.getValue().substituteVar(pair.first, pair.second);
        }
        for (Mutable<ILogicalExpression> expr : op.getFrameExcludeExpressions()) {
            expr.getValue().substituteVar(pair.first, pair.second);
        }
        ILogicalExpression frameExcludeUnaryExpr = op.getFrameExcludeUnaryExpression().getValue();
        if (frameExcludeUnaryExpr != null) {
            frameExcludeUnaryExpr.substituteVar(pair.first, pair.second);
        }
        ILogicalExpression frameOffsetExpr = op.getFrameOffsetExpression().getValue();
        if (frameOffsetExpr != null) {
            frameOffsetExpr.substituteVar(pair.first, pair.second);
        }
        substAssignVariables(op.getVariables(), op.getExpressions(), pair);
        substInNestedPlans(pair.first, pair.second, op);
        substVarTypes(op, pair);
        return null;
    }
}
