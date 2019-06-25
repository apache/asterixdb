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
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
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
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class OperatorDeepCopyVisitor implements ILogicalOperatorVisitor<ILogicalOperator, Void> {

    @Override
    public ILogicalOperator visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
        ArrayList<LogicalVariable> newList = new ArrayList<>();
        ArrayList<Mutable<ILogicalExpression>> newExpressions = new ArrayList<>();
        newList.addAll(op.getVariables());
        deepCopyExpressionRefs(newExpressions, op.getExpressions());
        return new AggregateOperator(newList, newExpressions);
    }

    @Override
    public ILogicalOperator visitRunningAggregateOperator(RunningAggregateOperator op, Void arg)
            throws AlgebricksException {
        ArrayList<LogicalVariable> newList = new ArrayList<>();
        ArrayList<Mutable<ILogicalExpression>> newExpressions = new ArrayList<>();
        newList.addAll(op.getVariables());
        deepCopyExpressionRefs(newExpressions, op.getExpressions());
        return new RunningAggregateOperator(newList, newExpressions);
    }

    @Override
    public ILogicalOperator visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg)
            throws AlgebricksException {
        return new EmptyTupleSourceOperator();
    }

    @Override
    public ILogicalOperator visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByList = new ArrayList<>();
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decoList = new ArrayList<>();
        ArrayList<ILogicalPlan> newSubplans = new ArrayList<>();
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> pair : op.getGroupByList()) {
            groupByList.add(new Pair<>(pair.first, deepCopyExpressionRef(pair.second)));
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> pair : op.getDecorList()) {
            decoList.add(new Pair<>(pair.first, deepCopyExpressionRef(pair.second)));
        }
        GroupByOperator gbyOp = new GroupByOperator(groupByList, decoList, newSubplans, op.isGroupAll());
        for (ILogicalPlan plan : op.getNestedPlans()) {
            newSubplans.add(OperatorManipulationUtil.deepCopy(plan, gbyOp));
        }
        return gbyOp;
    }

    @Override
    public ILogicalOperator visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        return new LimitOperator(deepCopyExpressionRef(op.getMaxObjects()).getValue(),
                deepCopyExpressionRef(op.getOffset()).getValue(), op.isTopmostLimitOp());
    }

    @Override
    public ILogicalOperator visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
        return new InnerJoinOperator(deepCopyExpressionRef(op.getCondition()), op.getInputs().get(0),
                op.getInputs().get(1));
    }

    @Override
    public ILogicalOperator visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
        return new LeftOuterJoinOperator(deepCopyExpressionRef(op.getCondition()), op.getInputs().get(0),
                op.getInputs().get(1));
    }

    @Override
    public ILogicalOperator visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg)
            throws AlgebricksException {
        return new NestedTupleSourceOperator(op.getDataSourceReference());
    }

    @Override
    public ILogicalOperator visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
        return new OrderOperator(this.deepCopyOrderAndExpression(op.getOrderExpressions()));
    }

    @Override
    public ILogicalOperator visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
        ArrayList<LogicalVariable> newList = new ArrayList<>();
        ArrayList<Mutable<ILogicalExpression>> newExpressions = new ArrayList<>();
        newList.addAll(op.getVariables());
        deepCopyExpressionRefs(newExpressions, op.getExpressions());
        return new AssignOperator(newList, newExpressions);
    }

    @Override
    public ILogicalOperator visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
        return new SelectOperator(deepCopyExpressionRef(op.getCondition()), op.getRetainMissing(),
                op.getMissingPlaceholderVariable());
    }

    @Override
    public ILogicalOperator visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
        ArrayList<LogicalVariable> newList = new ArrayList<>();
        newList.addAll(op.getVariables());
        return new ProjectOperator(newList);
    }

    @Override
    public ILogicalOperator visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        return new ReplicateOperator(op.getOutputArity());
    }

    @Override
    public ILogicalOperator visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
        return new SplitOperator(op.getOutputArity(), op.getBranchingExpression());
    }

    @Override
    public ILogicalOperator visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
        ArrayList<LogicalVariable> newInputList = new ArrayList<>();
        ArrayList<LogicalVariable> newOutputList = new ArrayList<>();
        newInputList.addAll(op.getInputVariables());
        newOutputList.addAll(op.getOutputVariables());
        return new ScriptOperator(op.getScriptDescription(), newInputList, newOutputList);
    }

    @Override
    public ILogicalOperator visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        ArrayList<ILogicalPlan> newSubplans = new ArrayList<>();
        SubplanOperator subplanOp = new SubplanOperator(newSubplans);
        for (ILogicalPlan plan : op.getNestedPlans()) {
            newSubplans.add(OperatorManipulationUtil.deepCopy(plan, subplanOp));
        }
        return subplanOp;
    }

    @Override
    public ILogicalOperator visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> newVarMap = new ArrayList<>();
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap = op.getVariableMappings();
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> triple : varMap) {
            newVarMap.add(new Triple<>(triple.first, triple.second, triple.third));
        }
        return new UnionAllOperator(newVarMap);
    }

    @Override
    public ILogicalOperator visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
        int nInput = op.getNumInput();
        boolean hasExtraVars = op.hasExtraVariables();
        List<LogicalVariable> newOutputCompareVars = new ArrayList<>(op.getOutputCompareVariables());
        List<LogicalVariable> newOutputExtraVars = hasExtraVars ? new ArrayList<>(op.getOutputExtraVariables()) : null;
        List<List<LogicalVariable>> newInputCompareVars = new ArrayList<>(nInput);
        List<List<LogicalVariable>> newInputExtraVars = hasExtraVars ? new ArrayList<>(nInput) : null;
        for (int i = 0; i < nInput; i++) {
            newInputCompareVars.add(new ArrayList<>(op.getInputCompareVariables(i)));
            if (hasExtraVars) {
                newInputExtraVars.add(new ArrayList<>(op.getInputExtraVariables(i)));
            }
        }
        return new IntersectOperator(newOutputCompareVars, newOutputExtraVars, newInputCompareVars, newInputExtraVars);
    }

    @Override
    public ILogicalOperator visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
        return new UnnestOperator(op.getVariable(), deepCopyExpressionRef(op.getExpressionRef()),
                op.getPositionalVariable(), op.getPositionalVariableType(), op.getPositionWriter());
    }

    @Override
    public ILogicalOperator visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
        ArrayList<LogicalVariable> newInputList = new ArrayList<>();
        newInputList.addAll(op.getVariables());
        Mutable<ILogicalExpression> newSelectCondition =
                op.getSelectCondition() != null ? deepCopyExpressionRef(op.getSelectCondition()) : null;
        return new UnnestMapOperator(newInputList, deepCopyExpressionRef(op.getExpressionRef()),
                new ArrayList<>(op.getVariableTypes()), op.propagatesInput(), newSelectCondition, op.getOutputLimit());
    }

    @Override
    public ILogicalOperator visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg)
            throws AlgebricksException {
        ArrayList<LogicalVariable> newInputList = new ArrayList<>();
        newInputList.addAll(op.getVariables());
        return new LeftOuterUnnestMapOperator(newInputList, deepCopyExpressionRef(op.getExpressionRef()),
                new ArrayList<>(op.getVariableTypes()), op.propagatesInput());
    }

    @Override
    public ILogicalOperator visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
        ArrayList<LogicalVariable> newInputList = new ArrayList<>();
        newInputList.addAll(op.getVariables());
        Mutable<ILogicalExpression> newSelectCondition =
                op.getSelectCondition() != null ? deepCopyExpressionRef(op.getSelectCondition()) : null;
        DataSourceScanOperator newOp =
                new DataSourceScanOperator(newInputList, op.getDataSource(), newSelectCondition, op.getOutputLimit());
        return newOp;
    }

    @Override
    public ILogicalOperator visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
        ArrayList<Mutable<ILogicalExpression>> newExpressions = new ArrayList<>();
        deepCopyExpressionRefs(newExpressions, op.getExpressions());
        return new DistinctOperator(newExpressions);
    }

    @Override
    public ILogicalOperator visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        return new ExchangeOperator();
    }

    @Override
    public ILogicalOperator visitWriteOperator(WriteOperator op, Void arg) throws AlgebricksException {
        ArrayList<Mutable<ILogicalExpression>> newExpressions = new ArrayList<>();
        deepCopyExpressionRefs(newExpressions, op.getExpressions());
        return new WriteOperator(newExpressions, op.getDataSink());
    }

    @Override
    public ILogicalOperator visitDistributeResultOperator(DistributeResultOperator op, Void arg)
            throws AlgebricksException {
        ArrayList<Mutable<ILogicalExpression>> newExpressions = new ArrayList<>();
        deepCopyExpressionRefs(newExpressions, op.getExpressions());
        return new DistributeResultOperator(newExpressions, op.getDataSink(), op.getResultMetadata());
    }

    @Override
    public ILogicalOperator visitWriteResultOperator(WriteResultOperator op, Void arg) throws AlgebricksException {
        ArrayList<Mutable<ILogicalExpression>> newKeyExpressions = new ArrayList<>();
        deepCopyExpressionRefs(newKeyExpressions, op.getKeyExpressions());
        List<Mutable<ILogicalExpression>> newLSMComponentFilterExpressions = new ArrayList<>();
        deepCopyExpressionRefs(newKeyExpressions, op.getAdditionalFilteringExpressions());
        WriteResultOperator writeResultOp = new WriteResultOperator(op.getDataSource(),
                deepCopyExpressionRef(op.getPayloadExpression()), newKeyExpressions);
        writeResultOp.setAdditionalFilteringExpressions(newLSMComponentFilterExpressions);
        return writeResultOp;
    }

    @Override
    public ILogicalOperator visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void arg)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> newKeyExpressions = new ArrayList<>();
        deepCopyExpressionRefs(newKeyExpressions, op.getPrimaryKeyExpressions());
        List<Mutable<ILogicalExpression>> newLSMComponentFilterExpressions = new ArrayList<>();
        deepCopyExpressionRefs(newKeyExpressions, op.getAdditionalFilteringExpressions());
        InsertDeleteUpsertOperator insertDeleteOp =
                new InsertDeleteUpsertOperator(op.getDataSource(), deepCopyExpressionRef(op.getPayloadExpression()),
                        newKeyExpressions, op.getOperation(), op.isBulkload());
        insertDeleteOp.setAdditionalFilteringExpressions(newLSMComponentFilterExpressions);
        return insertDeleteOp;
    }

    @Override
    public ILogicalOperator visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void arg)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> newPrimaryKeyExpressions = new ArrayList<>();
        deepCopyExpressionRefs(newPrimaryKeyExpressions, op.getPrimaryKeyExpressions());
        List<Mutable<ILogicalExpression>> newSecondaryKeyExpressions = new ArrayList<>();
        deepCopyExpressionRefs(newSecondaryKeyExpressions, op.getSecondaryKeyExpressions());
        Mutable<ILogicalExpression> newFilterExpression =
                new MutableObject<>(((AbstractLogicalExpression) op.getFilterExpression()).cloneExpression());
        List<Mutable<ILogicalExpression>> newLSMComponentFilterExpressions = new ArrayList<>();
        deepCopyExpressionRefs(newLSMComponentFilterExpressions, op.getAdditionalFilteringExpressions());
        IndexInsertDeleteUpsertOperator indexInsertDeleteOp = new IndexInsertDeleteUpsertOperator(
                op.getDataSourceIndex(), newPrimaryKeyExpressions, newSecondaryKeyExpressions, newFilterExpression,
                op.getOperation(), op.isBulkload(), op.getNumberOfAdditionalNonFilteringFields());
        indexInsertDeleteOp.setAdditionalFilteringExpressions(newLSMComponentFilterExpressions);
        return indexInsertDeleteOp;
    }

    @Override
    public ILogicalOperator visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> newPrimaryKeyExpressions = new ArrayList<>();
        deepCopyExpressionRefs(newPrimaryKeyExpressions, op.getPrimaryKeyExpressions());
        List<Mutable<ILogicalExpression>> newSecondaryKeyExpressions = new ArrayList<>();
        deepCopyExpressionRefs(newSecondaryKeyExpressions, op.getSecondaryKeyExpressions());
        List<LogicalVariable> newTokenizeVars = new ArrayList<>();
        deepCopyVars(newTokenizeVars, op.getTokenizeVars());
        Mutable<ILogicalExpression> newFilterExpression =
                new MutableObject<>(((AbstractLogicalExpression) op.getFilterExpression()).cloneExpression());
        List<Object> newTokenizeVarTypes = new ArrayList<>();
        deepCopyObjects(newTokenizeVarTypes, op.getTokenizeVarTypes());

        TokenizeOperator tokenizeOp = new TokenizeOperator(op.getDataSourceIndex(), newPrimaryKeyExpressions,
                newSecondaryKeyExpressions, newTokenizeVars, newFilterExpression, op.getOperation(), op.isBulkload(),
                op.isPartitioned(), newTokenizeVarTypes);
        return tokenizeOp;
    }

    @Override
    public ILogicalOperator visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
        return new ForwardOperator(op.getSideDataKey(), deepCopyExpressionRef(op.getSideDataExpression()));
    }

    @Override
    public ILogicalOperator visitSinkOperator(SinkOperator op, Void arg) throws AlgebricksException {
        return new SinkOperator();
    }

    private void deepCopyExpressionRefs(List<Mutable<ILogicalExpression>> newExprs,
            List<Mutable<ILogicalExpression>> oldExprs) {
        for (Mutable<ILogicalExpression> oldExpr : oldExprs) {
            newExprs.add(new MutableObject<>(((AbstractLogicalExpression) oldExpr.getValue()).cloneExpression()));
        }
    }

    private Mutable<ILogicalExpression> deepCopyExpressionRef(Mutable<ILogicalExpression> oldExprRef) {
        ILogicalExpression oldExpr = oldExprRef.getValue();
        if (oldExpr == null) {
            return new MutableObject<>(null);
        }
        return new MutableObject<>(oldExpr.cloneExpression());
    }

    private List<LogicalVariable> deepCopyVars(List<LogicalVariable> newVars, List<LogicalVariable> oldVars) {
        for (LogicalVariable oldVar : oldVars) {
            newVars.add(oldVar);
        }
        return newVars;
    }

    private List<Object> deepCopyObjects(List<Object> newObjs, List<Object> oldObjs) {
        for (Object oldObj : oldObjs) {
            newObjs.add(oldObj);
        }
        return newObjs;
    }

    private List<Pair<IOrder, Mutable<ILogicalExpression>>> deepCopyOrderAndExpression(
            List<Pair<IOrder, Mutable<ILogicalExpression>>> ordersAndExprs) {
        List<Pair<IOrder, Mutable<ILogicalExpression>>> newOrdersAndExprs = new ArrayList<>();
        for (Pair<IOrder, Mutable<ILogicalExpression>> pair : ordersAndExprs) {
            newOrdersAndExprs.add(new Pair<>(pair.first, deepCopyExpressionRef(pair.second)));
        }
        return newOrdersAndExprs;
    }

    @Override
    public ILogicalOperator visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
        return new DelegateOperator(op.getNewInstanceOfDelegateOperator());
    }

    @Override
    public ILogicalOperator visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        return new MaterializeOperator();
    }

    @Override
    public ILogicalOperator visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg)
            throws AlgebricksException {
        return new LeftOuterUnnestOperator(op.getVariable(), deepCopyExpressionRef(op.getExpressionRef()),
                op.getPositionalVariable(), op.getPositionalVariableType(), op.getPositionWriter());
    }

    @Override
    public ILogicalOperator visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> newPartitionExprs = new ArrayList<>();
        deepCopyExpressionRefs(op.getPartitionExpressions(), newPartitionExprs);
        List<Pair<IOrder, Mutable<ILogicalExpression>>> newOrderExprs =
                deepCopyOrderAndExpression(op.getOrderExpressions());
        List<Pair<IOrder, Mutable<ILogicalExpression>>> newFrameValueExprs =
                deepCopyOrderAndExpression(op.getFrameValueExpressions());
        List<Mutable<ILogicalExpression>> newFrameStartExprs = new ArrayList<>();
        deepCopyExpressionRefs(newFrameStartExprs, op.getFrameStartExpressions());
        List<Mutable<ILogicalExpression>> newFrameStartValidationExprs = new ArrayList<>();
        deepCopyExpressionRefs(newFrameStartValidationExprs, op.getFrameStartValidationExpressions());
        List<Mutable<ILogicalExpression>> newFrameEndExprs = new ArrayList<>();
        deepCopyExpressionRefs(newFrameEndExprs, op.getFrameEndExpressions());
        List<Mutable<ILogicalExpression>> newFrameEndValidationExprs = new ArrayList<>();
        deepCopyExpressionRefs(newFrameEndValidationExprs, op.getFrameEndValidationExpressions());
        List<Mutable<ILogicalExpression>> newFrameExclusionExprs = new ArrayList<>();
        deepCopyExpressionRefs(newFrameExclusionExprs, op.getFrameExcludeExpressions());
        ILogicalExpression newFrameExcludeUnaryExpr =
                deepCopyExpressionRef(op.getFrameExcludeUnaryExpression()).getValue();
        ILogicalExpression newFrameOffsetExpr = deepCopyExpressionRef(op.getFrameOffsetExpression()).getValue();
        List<LogicalVariable> newVariables = new ArrayList<>();
        deepCopyVars(newVariables, op.getVariables());
        List<Mutable<ILogicalExpression>> newExpressions = new ArrayList<>();
        deepCopyExpressionRefs(newExpressions, op.getExpressions());
        List<ILogicalPlan> newNestedPlans = new ArrayList<>();
        WindowOperator newWinOp = new WindowOperator(newPartitionExprs, newOrderExprs, newFrameValueExprs,
                newFrameStartExprs, newFrameStartValidationExprs, newFrameEndExprs, newFrameEndValidationExprs,
                newFrameExclusionExprs, op.getFrameExcludeNegationStartIdx(), newFrameExcludeUnaryExpr,
                newFrameOffsetExpr, op.getFrameMaxObjects(), newVariables, newExpressions, newNestedPlans);
        for (ILogicalPlan nestedPlan : op.getNestedPlans()) {
            newNestedPlans.add(OperatorManipulationUtil.deepCopy(nestedPlan, newWinOp));
        }
        return newWinOp;
    }
}
