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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SwitchOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class SubstituteVariableVisitor
        implements ILogicalOperatorVisitor<Void, Pair<LogicalVariable, LogicalVariable>> {

    private final ITypingContext ctx;

    //TODO(dmitry):unused -> remove
    private final boolean goThroughNts;

    public SubstituteVariableVisitor(boolean goThroughNts, ITypingContext ctx) {
        this.goThroughNts = goThroughNts;
        this.ctx = ctx;
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean producedVarFound =
                substAssignVariables(op.getVariables(), op.getExpressions(), pair.first, pair.second);
        if (producedVarFound) {
            substProducedVarInTypeEnvironment(op, pair);
        }
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean producedVarFound =
                substAssignVariables(op.getVariables(), op.getExpressions(), pair.first, pair.second);
        if (producedVarFound) {
            // Substitute variables stored in ordering property
            if (op.getExplicitOrderingProperty() != null) {
                List<OrderColumn> orderColumns = op.getExplicitOrderingProperty().getOrderColumns();
                List<OrderColumn> newOrderColumns = new ArrayList<>(orderColumns.size());
                for (OrderColumn oc : orderColumns) {
                    LogicalVariable columnVar = oc.getColumn();
                    LogicalVariable newColumnVar = columnVar.equals(pair.first) ? pair.second : columnVar;
                    newOrderColumns.add(new OrderColumn(newColumnVar, oc.getOrder()));
                }
                op.setExplicitOrderingProperty(new LocalOrderProperty(newOrderColumns));
            }

            substProducedVarInTypeEnvironment(op, pair);
        }
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean producedVarFound = substProducedVariables(op.getVariables(), pair.first, pair.second);
        if (!producedVarFound) {
            if (op.isProjectPushed()) {
                producedVarFound = substProducedVariables(op.getProjectVariables(), pair.first, pair.second);
            }
        }
        if (producedVarFound) {
            substProducedVarInTypeEnvironment(op, pair);
        } else {
            substUsedVariablesInExpr(op.getSelectCondition(), pair.first, pair.second);
            substUsedVariablesInExpr(op.getAdditionalFilteringExpressions(), pair.first, pair.second);
            substUsedVariables(op.getMinFilterVars(), pair.first, pair.second);
            substUsedVariables(op.getMaxFilterVars(), pair.first, pair.second);
        }
        op.getProjectionFiltrationInfo().substituteFilterVariable(pair.first, pair.second);
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substUsedVariablesInExpr(op.getExpressions(), pair.first, pair.second);
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op,
            Pair<LogicalVariable, LogicalVariable> pair) {
        // does not produce/use any variables
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, Pair<LogicalVariable, LogicalVariable> pair) {
        // does not produce/use any variables
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean producedVarFound = substGbyVariables(op.getGroupByList(), pair.first, pair.second);
        if (!producedVarFound) {
            producedVarFound = substGbyVariables(op.getDecorList(), pair.first, pair.second);
        }
        if (!producedVarFound) {
            substInNestedPlans(op, pair.first, pair.second);
        }
        // GROUP BY operator may add its used variables
        // to its own output type environment as produced variables
        // therefore we need perform variable substitution in its own type environment
        // TODO (dmitry): this needs to be revisited
        substProducedVarInTypeEnvironment(op, pair);
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substUsedVariablesInExpr(op.getCondition(), pair.first, pair.second);
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substUsedVariablesInExpr(op.getCondition(), pair.first, pair.second);
        // LEFT OUTER JOIN operator adds its right branch variables
        // to its own output type environment as 'correlatedMissableVariables'
        // therefore we need perform variable substitution in its own type environment
        substProducedVarInTypeEnvironment(op, pair);
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substUsedVariablesInExpr(op.getMaxObjects(), pair.first, pair.second);
        substUsedVariablesInExpr(op.getOffset(), pair.first, pair.second);
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op,
            Pair<LogicalVariable, LogicalVariable> pair) throws AlgebricksException {
        // does not produce/use any variables
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        for (Pair<IOrder, Mutable<ILogicalExpression>> oe : op.getOrderExpressions()) {
            substUsedVariablesInExpr(oe.second, pair.first, pair.second);
        }
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substUsedVariables(op.getVariables(), pair.first, pair.second);
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean producedVarFound =
                substAssignVariables(op.getVariables(), op.getExpressions(), pair.first, pair.second);
        if (producedVarFound) {
            substProducedVarInTypeEnvironment(op, pair);
        }
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean producedVarFound = substProducedVariables(op.getOutputVariables(), pair.first, pair.second);
        if (producedVarFound) {
            substProducedVarInTypeEnvironment(op, pair);
        } else {
            substUsedVariables(op.getInputVariables(), pair.first, pair.second);
        }
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substUsedVariablesInExpr(op.getCondition(), pair.first, pair.second);
        LogicalVariable missingPlaceholderVar = op.getMissingPlaceholderVariable();
        if (missingPlaceholderVar != null && missingPlaceholderVar.equals(pair.first)) {
            op.setMissingPlaceholderVar(pair.second);
        }
        // SELECT operator may add its used variable
        // to its own output type environment as 'nonMissableVariable' (not(is-missing($used_var))
        // therefore we need perform variable substitution in its own type environment
        substProducedVarInTypeEnvironment(op, pair);
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substInNestedPlans(op, pair.first, pair.second);
        // do not call substProducedVarInTypeEnvironment() because the variables are produced by nested plans
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean producedVarFound = substUnionAllVariables(op.getVariableMappings(), pair.first, pair.second);
        if (producedVarFound) {
            substProducedVarInTypeEnvironment(op, pair);
        }
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean producedVarFound = substProducedVariables(op.getOutputCompareVariables(), pair.first, pair.second);
        if (!producedVarFound) {
            if (op.hasExtraVariables()) {
                producedVarFound = substProducedVariables(op.getOutputExtraVariables(), pair.first, pair.second);
            }
        }
        if (producedVarFound) {
            substProducedVarInTypeEnvironment(op, pair);
        } else {
            for (int i = 0, n = op.getNumInput(); i < n; i++) {
                substUsedVariables(op.getInputCompareVariables(i), pair.first, pair.second);
                if (op.hasExtraVariables()) {
                    substUsedVariables(op.getInputExtraVariables(i), pair.first, pair.second);
                }
            }
        }
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean producedVarFound = substituteVarsForAbstractUnnestMapOp(op, pair.first, pair.second);
        if (producedVarFound) {
            substProducedVarInTypeEnvironment(op, pair);
        } else {
            substUsedVariablesInExpr(op.getSelectCondition(), pair.first, pair.second);
        }
        op.getProjectionFiltrationInfo().substituteFilterVariable(pair.first, pair.second);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op,
            Pair<LogicalVariable, LogicalVariable> pair) throws AlgebricksException {
        boolean producedVarFound = substituteVarsForAbstractUnnestMapOp(op, pair.first, pair.second);
        if (producedVarFound) {
            substProducedVarInTypeEnvironment(op, pair);
        }
        op.getProjectionFiltrationInfo().substituteFilterVariable(pair.first, pair.second);
        return null;
    }

    private boolean substituteVarsForAbstractUnnestMapOp(AbstractUnnestMapOperator op, LogicalVariable v1,
            LogicalVariable v2) {
        boolean producedVarFound = substProducedVariables(op.getVariables(), v1, v2);
        if (!producedVarFound) {
            substUsedVariablesInExpr(op.getExpressionRef(), v1, v2);
            substUsedVariablesInExpr(op.getAdditionalFilteringExpressions(), v1, v2);
            substUsedVariables(op.getMinFilterVars(), v1, v2);
            substUsedVariables(op.getMaxFilterVars(), v1, v2);
        }
        return producedVarFound;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean producedVarFound = substituteVarsForAbstractUnnestNonMapOp(op, pair.first, pair.second);
        if (producedVarFound) {
            substProducedVarInTypeEnvironment(op, pair);
        }
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean producedVarFound = substituteVarsForAbstractUnnestNonMapOp(op, pair.first, pair.second);
        if (producedVarFound) {
            substProducedVarInTypeEnvironment(op, pair);
        }
        return null;
    }

    private boolean substituteVarsForAbstractUnnestNonMapOp(AbstractUnnestNonMapOperator op, LogicalVariable v1,
            LogicalVariable v2) {
        boolean producedVarFound = substProducedVariables(op.getVariables(), v1, v2);
        if (!producedVarFound) {
            if (op.hasPositionalVariable() && op.getPositionalVariable().equals(v1)) {
                op.setPositionalVariable(v2);
                producedVarFound = true;
            }
        }
        if (!producedVarFound) {
            substUsedVariablesInExpr(op.getExpressionRef(), v1, v2);
        }
        return producedVarFound;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substUsedVariablesInExpr(op.getSourceExpression(), pair.first, pair.second);
        substUsedVariablesInExpr(op.getPathExpression(), pair.first, pair.second);
        substUsedVariablesInExpr(op.getPartitionExpressions(), pair.first, pair.second);
        for (Pair<IOrder, Mutable<ILogicalExpression>> orderExpr : op.getOrderExpressions()) {
            substUsedVariablesInExpr(orderExpr.second, pair.first, pair.second);
        }
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substUsedVariablesInExpr(op.getExpressions(), pair.first, pair.second);
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        // does not produce/use any variables
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substUsedVariablesInExpr(op.getBranchingExpression(), pair.first, pair.second);
        return null;
    }

    @Override
    public Void visitSwitchOperator(SwitchOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        // TODO (GLENN): Implement this logic
        throw new NotImplementedException();
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        // does not produce/use any variables
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op,
            Pair<LogicalVariable, LogicalVariable> pair) throws AlgebricksException {
        boolean producedVarFound = false;
        if (op.getOperation() == InsertDeleteUpsertOperator.Kind.UPSERT) {
            if (op.getOperationVar() != null && op.getOperationVar().equals(pair.first)) {
                op.setOperationVar(pair.second);
                producedVarFound = true;
            } else if (op.getBeforeOpRecordVar() != null && op.getBeforeOpRecordVar().equals(pair.first)) {
                op.setPrevRecordVar(pair.second);
                producedVarFound = true;
            } else if (op.getBeforeOpFilterVar() != null && op.getBeforeOpFilterVar().equals(pair.first)) {
                op.setPrevFilterVar(pair.second);
                producedVarFound = true;
            } else {
                producedVarFound =
                        substProducedVariables(op.getBeforeOpAdditionalNonFilteringVars(), pair.first, pair.second);
            }
        }
        if (producedVarFound) {
            substProducedVarInTypeEnvironment(op, pair);
        } else {
            substUsedVariablesInExpr(op.getPayloadExpression(), pair.first, pair.second);
            substUsedVariablesInExpr(op.getPrimaryKeyExpressions(), pair.first, pair.second);
            substUsedVariablesInExpr(op.getAdditionalFilteringExpressions(), pair.first, pair.second);
            substUsedVariablesInExpr(op.getAdditionalNonFilteringExpressions(), pair.first, pair.second);
        }
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op,
            Pair<LogicalVariable, LogicalVariable> pair) throws AlgebricksException {
        substUsedVariablesInExpr(op.getPrimaryKeyExpressions(), pair.first, pair.second);
        substUsedVariablesInExpr(op.getSecondaryKeyExpressions(), pair.first, pair.second);
        substUsedVariablesInExpr(op.getFilterExpression(), pair.first, pair.second);
        substUsedVariablesInExpr(op.getBeforeOpFilterExpression(), pair.first, pair.second);
        substUsedVariablesInExpr(op.getAdditionalFilteringExpressions(), pair.first, pair.second);
        substUsedVariablesInExpr(op.getOperationExpr(), pair.first, pair.second);
        substUsedVariablesInExpr(op.getPrevSecondaryKeyExprs(), pair.first, pair.second);
        substUsedVariablesInExpr(op.getPrevAdditionalFilteringExpression(), pair.first, pair.second);
        if (!op.getNestedPlans().isEmpty()) {
            substInNestedPlans(op, pair.first, pair.second);
        }
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean producedVarFound = substProducedVariables(op.getTokenizeVars(), pair.first, pair.second);
        if (producedVarFound) {
            substProducedVarInTypeEnvironment(op, pair);
        } else {
            substUsedVariablesInExpr(op.getPrimaryKeyExpressions(), pair.first, pair.second);
            substUsedVariablesInExpr(op.getSecondaryKeyExpressions(), pair.first, pair.second);
            substUsedVariablesInExpr(op.getFilterExpression(), pair.first, pair.second);
            substUsedVariablesInExpr(op.getAdditionalFilteringExpressions(), pair.first, pair.second);
        }
        return null;
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        substUsedVariablesInExpr(op.getSideDataExpression(), pair.first, pair.second);
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        // does not produce/use any variables
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, Pair<LogicalVariable, LogicalVariable> arg)
            throws AlgebricksException {
        // does not produce/use any variables
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        boolean producedVarFound =
                substAssignVariables(op.getVariables(), op.getExpressions(), pair.first, pair.second);
        if (producedVarFound) {
            substProducedVarInTypeEnvironment(op, pair);
        } else {
            substUsedVariablesInExpr(op.getPartitionExpressions(), pair.first, pair.second);
            for (Pair<IOrder, Mutable<ILogicalExpression>> p : op.getOrderExpressions()) {
                substUsedVariablesInExpr(p.second, pair.first, pair.second);
            }
            for (Pair<IOrder, Mutable<ILogicalExpression>> p : op.getFrameValueExpressions()) {
                substUsedVariablesInExpr(p.second, pair.first, pair.second);
            }
            substUsedVariablesInExpr(op.getFrameStartExpressions(), pair.first, pair.second);
            substUsedVariablesInExpr(op.getFrameStartValidationExpressions(), pair.first, pair.second);
            substUsedVariablesInExpr(op.getFrameEndExpressions(), pair.first, pair.second);
            substUsedVariablesInExpr(op.getFrameEndValidationExpressions(), pair.first, pair.second);
            substUsedVariablesInExpr(op.getFrameExcludeExpressions(), pair.first, pair.second);
            substUsedVariablesInExpr(op.getFrameExcludeUnaryExpression(), pair.first, pair.second);
            substUsedVariablesInExpr(op.getFrameOffsetExpression(), pair.first, pair.second);
            substInNestedPlans(op, pair.first, pair.second);
        }
        return null;
    }

    private void substUsedVariablesInExpr(Mutable<ILogicalExpression> exprRef, LogicalVariable v1, LogicalVariable v2) {
        if (exprRef != null && exprRef.getValue() != null) {
            exprRef.getValue().substituteVar(v1, v2);
        }
    }

    private void substUsedVariablesInExpr(List<Mutable<ILogicalExpression>> expressions, LogicalVariable v1,
            LogicalVariable v2) {
        if (expressions != null) {
            for (Mutable<ILogicalExpression> exprRef : expressions) {
                substUsedVariablesInExpr(exprRef, v1, v2);
            }
        }
    }

    private void substUsedVariables(List<LogicalVariable> variables, LogicalVariable v1, LogicalVariable v2) {
        if (variables != null) {
            for (int i = 0, n = variables.size(); i < n; i++) {
                if (variables.get(i).equals(v1)) {
                    variables.set(i, v2);
                }
            }
        }
    }

    private boolean substProducedVariables(List<LogicalVariable> variables, LogicalVariable v1, LogicalVariable v2) {
        if (variables != null) {
            for (int i = 0, n = variables.size(); i < n; i++) {
                if (variables.get(i).equals(v1)) {
                    variables.set(i, v2);
                    return true; // found produced var
                }
            }
        }
        return false;
    }

    private boolean substAssignVariables(List<LogicalVariable> variables, List<Mutable<ILogicalExpression>> expressions,
            LogicalVariable v1, LogicalVariable v2) {
        for (int i = 0, n = variables.size(); i < n; i++) {
            if (variables.get(i).equals(v1)) {
                variables.set(i, v2);
                return true; // found produced var
            } else {
                expressions.get(i).getValue().substituteVar(v1, v2);
            }
        }
        return false;
    }

    private boolean substGbyVariables(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyPairList,
            LogicalVariable v1, LogicalVariable v2) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : gbyPairList) {
            if (ve.first != null && ve.first.equals(v1)) {
                ve.first = v2;
                return true; // found produced var
            }
            ve.second.getValue().substituteVar(v1, v2);
        }
        return false;
    }

    private boolean substUnionAllVariables(List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap,
            LogicalVariable v1, LogicalVariable v2) {
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> t : varMap) {
            if (t.first.equals(v1)) {
                t.first = v2;
            }
            if (t.second.equals(v1)) {
                t.second = v2;
            }
            if (t.third.equals(v1)) {
                t.third = v2;
                return true; // found produced var
            }
        }
        return false;
    }

    private void substInNestedPlans(AbstractOperatorWithNestedPlans op, LogicalVariable v1, LogicalVariable v2)
            throws AlgebricksException {
        for (ILogicalPlan p : op.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                VariableUtilities.substituteVariablesInDescendantsAndSelf(r.getValue(), v1, v2, ctx);
            }
        }
    }

    private void substProducedVarInTypeEnvironment(ILogicalOperator op, Pair<LogicalVariable, LogicalVariable> pair)
            throws AlgebricksException {
        if (ctx == null) {
            return;
        }
        IVariableTypeEnvironment env = ctx.getOutputTypeEnvironment(op);
        if (env != null) {
            env.substituteProducedVariable(pair.first, pair.second);
        }
    }
}
