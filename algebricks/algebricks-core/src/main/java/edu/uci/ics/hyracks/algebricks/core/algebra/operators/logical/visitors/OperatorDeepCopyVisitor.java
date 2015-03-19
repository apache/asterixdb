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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class OperatorDeepCopyVisitor implements ILogicalOperatorVisitor<ILogicalOperator, Void> {

    @Override
    public ILogicalOperator visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
        ArrayList<LogicalVariable> newList = new ArrayList<LogicalVariable>();
        ArrayList<Mutable<ILogicalExpression>> newExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        newList.addAll(op.getVariables());
        deepCopyExpressionRefs(newExpressions, op.getExpressions());
        return new AggregateOperator(newList, newExpressions);
    }

    @Override
    public ILogicalOperator visitRunningAggregateOperator(RunningAggregateOperator op, Void arg)
            throws AlgebricksException {
        ArrayList<LogicalVariable> newList = new ArrayList<LogicalVariable>();
        ArrayList<Mutable<ILogicalExpression>> newExpressions = new ArrayList<Mutable<ILogicalExpression>>();
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
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByList = new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>();
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decoList = new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>();
        ArrayList<ILogicalPlan> newSubplans = new ArrayList<ILogicalPlan>();
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> pair : op.getGroupByList())
            groupByList.add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(pair.first,
                    deepCopyExpressionRef(pair.second)));
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> pair : op.getDecorList())
            decoList.add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(pair.first,
                    deepCopyExpressionRef(pair.second)));
        for (ILogicalPlan plan : op.getNestedPlans()) {
            newSubplans.add(OperatorManipulationUtil.deepCopy(plan));
        }
        return new GroupByOperator(groupByList, decoList, newSubplans);
    }

    @Override
    public ILogicalOperator visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        return new LimitOperator(deepCopyExpressionRef(op.getMaxObjects()).getValue(), deepCopyExpressionRef(
                op.getOffset()).getValue(), op.isTopmostLimitOp());
    }

    @Override
    public ILogicalOperator visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
        return new InnerJoinOperator(deepCopyExpressionRef(op.getCondition()), op.getInputs().get(0), op.getInputs()
                .get(1));
    }

    @Override
    public ILogicalOperator visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
        return new LeftOuterJoinOperator(deepCopyExpressionRef(op.getCondition()), op.getInputs().get(0), op
                .getInputs().get(1));
    }

    @Override
    public ILogicalOperator visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg)
            throws AlgebricksException {
        return new NestedTupleSourceOperator(null);
    }

    @Override
    public ILogicalOperator visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
        return new OrderOperator(this.deepCopyOrderAndExpression(op.getOrderExpressions()));
    }

    @Override
    public ILogicalOperator visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
        ArrayList<LogicalVariable> newList = new ArrayList<LogicalVariable>();
        ArrayList<Mutable<ILogicalExpression>> newExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        newList.addAll(op.getVariables());
        deepCopyExpressionRefs(newExpressions, op.getExpressions());
        return new AssignOperator(newList, newExpressions);
    }

    @Override
    public ILogicalOperator visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
        return new SelectOperator(deepCopyExpressionRef(op.getCondition()), op.getRetainNull(),
                op.getNullPlaceholderVariable());
    }

    @Override
    public ILogicalOperator visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
        ArrayList<LogicalVariable> newList = new ArrayList<LogicalVariable>();
        newList.addAll(op.getVariables());
        return new ProjectOperator(newList);
    }

    @Override
    public ILogicalOperator visitPartitioningSplitOperator(PartitioningSplitOperator op, Void arg)
            throws AlgebricksException {
        ArrayList<Mutable<ILogicalExpression>> newExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        deepCopyExpressionRefs(newExpressions, op.getExpressions());
        return new PartitioningSplitOperator(newExpressions, op.getDefaultBranchIndex());
    }

    @Override
    public ILogicalOperator visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        return new ReplicateOperator(op.getOutputArity());
    }

    @Override
    public ILogicalOperator visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
        ArrayList<LogicalVariable> newInputList = new ArrayList<LogicalVariable>();
        ArrayList<LogicalVariable> newOutputList = new ArrayList<LogicalVariable>();
        newInputList.addAll(op.getInputVariables());
        newOutputList.addAll(op.getOutputVariables());
        return new ScriptOperator(op.getScriptDescription(), newInputList, newOutputList);
    }

    @Override
    public ILogicalOperator visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        ArrayList<ILogicalPlan> newSubplans = new ArrayList<ILogicalPlan>();
        for (ILogicalPlan plan : op.getNestedPlans()) {
            newSubplans.add(OperatorManipulationUtil.deepCopy(plan));
        }
        return new SubplanOperator(newSubplans);
    }

    @Override
    public ILogicalOperator visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> newVarMap = new ArrayList<Triple<LogicalVariable, LogicalVariable, LogicalVariable>>();
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap = op.getVariableMappings();
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> triple : varMap)
            newVarMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(triple.first, triple.second,
                    triple.third));
        return new UnionAllOperator(newVarMap);
    }

    @Override
    public ILogicalOperator visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
        return new UnnestOperator(op.getVariable(), deepCopyExpressionRef(op.getExpressionRef()),
                op.getPositionalVariable(), op.getPositionalVariableType(), op.getPositionWriter());
    }

    @Override
    public ILogicalOperator visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
        ArrayList<LogicalVariable> newInputList = new ArrayList<LogicalVariable>();
        newInputList.addAll(op.getVariables());
        return new UnnestMapOperator(newInputList, deepCopyExpressionRef(op.getExpressionRef()), new ArrayList<Object>(
                op.getVariableTypes()), op.propagatesInput());
    }

    @Override
    public ILogicalOperator visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
        ArrayList<LogicalVariable> newInputList = new ArrayList<LogicalVariable>();
        newInputList.addAll(op.getVariables());
        return new DataSourceScanOperator(newInputList, op.getDataSource());
    }

    @Override
    public ILogicalOperator visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
        ArrayList<Mutable<ILogicalExpression>> newExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        deepCopyExpressionRefs(newExpressions, op.getExpressions());
        return new DistinctOperator(newExpressions);
    }

    @Override
    public ILogicalOperator visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        return new ExchangeOperator();
    }

    @Override
    public ILogicalOperator visitWriteOperator(WriteOperator op, Void arg) throws AlgebricksException {
        ArrayList<Mutable<ILogicalExpression>> newExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        deepCopyExpressionRefs(newExpressions, op.getExpressions());
        return new WriteOperator(newExpressions, op.getDataSink());
    }

    @Override
    public ILogicalOperator visitDistributeResultOperator(DistributeResultOperator op, Void arg)
            throws AlgebricksException {
        ArrayList<Mutable<ILogicalExpression>> newExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        deepCopyExpressionRefs(newExpressions, op.getExpressions());
        return new DistributeResultOperator(newExpressions, op.getDataSink());
    }

    @Override
    public ILogicalOperator visitWriteResultOperator(WriteResultOperator op, Void arg) throws AlgebricksException {
        ArrayList<Mutable<ILogicalExpression>> newKeyExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        deepCopyExpressionRefs(newKeyExpressions, op.getKeyExpressions());
        List<Mutable<ILogicalExpression>> newLSMComponentFilterExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        deepCopyExpressionRefs(newKeyExpressions, op.getAdditionalFilteringExpressions());
        WriteResultOperator writeResultOp = new WriteResultOperator(op.getDataSource(),
                deepCopyExpressionRef(op.getPayloadExpression()), newKeyExpressions);
        writeResultOp.setAdditionalFilteringExpressions(newLSMComponentFilterExpressions);
        return writeResultOp;
    }

    @Override
    public ILogicalOperator visitInsertDeleteOperator(InsertDeleteOperator op, Void arg) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> newKeyExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        deepCopyExpressionRefs(newKeyExpressions, op.getPrimaryKeyExpressions());
        List<Mutable<ILogicalExpression>> newLSMComponentFilterExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        deepCopyExpressionRefs(newKeyExpressions, op.getAdditionalFilteringExpressions());
        InsertDeleteOperator insertDeleteOp = new InsertDeleteOperator(op.getDataSource(),
                deepCopyExpressionRef(op.getPayloadExpression()), newKeyExpressions, op.getOperation(), op.isBulkload());
        insertDeleteOp.setAdditionalFilteringExpressions(newLSMComponentFilterExpressions);
        return insertDeleteOp;
    }

    @Override
    public ILogicalOperator visitIndexInsertDeleteOperator(IndexInsertDeleteOperator op, Void arg)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> newPrimaryKeyExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        deepCopyExpressionRefs(newPrimaryKeyExpressions, op.getPrimaryKeyExpressions());
        List<Mutable<ILogicalExpression>> newSecondaryKeyExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        deepCopyExpressionRefs(newSecondaryKeyExpressions, op.getSecondaryKeyExpressions());
        Mutable<ILogicalExpression> newFilterExpression = new MutableObject<ILogicalExpression>(
                ((AbstractLogicalExpression) op.getFilterExpression()).cloneExpression());
        List<Mutable<ILogicalExpression>> newLSMComponentFilterExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        deepCopyExpressionRefs(newLSMComponentFilterExpressions, op.getAdditionalFilteringExpressions());
        IndexInsertDeleteOperator indexInsertDeleteOp = new IndexInsertDeleteOperator(op.getDataSourceIndex(),
                newPrimaryKeyExpressions, newSecondaryKeyExpressions, newFilterExpression, op.getOperation(),
                op.isBulkload());
        indexInsertDeleteOp.setAdditionalFilteringExpressions(newLSMComponentFilterExpressions);
        return indexInsertDeleteOp;
    }

    @Override
    public ILogicalOperator visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> newPrimaryKeyExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        deepCopyExpressionRefs(newPrimaryKeyExpressions, op.getPrimaryKeyExpressions());
        List<Mutable<ILogicalExpression>> newSecondaryKeyExpressions = new ArrayList<Mutable<ILogicalExpression>>();
        deepCopyExpressionRefs(newSecondaryKeyExpressions, op.getSecondaryKeyExpressions());
        List<LogicalVariable> newTokenizeVars = new ArrayList<LogicalVariable>();
        deepCopyVars(newTokenizeVars, op.getTokenizeVars());
        Mutable<ILogicalExpression> newFilterExpression = new MutableObject<ILogicalExpression>(
                ((AbstractLogicalExpression) op.getFilterExpression()).cloneExpression());
        List<Object> newTokenizeVarTypes = new ArrayList<Object>();
        deepCopyObjects(newTokenizeVarTypes, op.getTokenizeVarTypes());

        TokenizeOperator tokenizeOp = new TokenizeOperator(op.getDataSourceIndex(), newPrimaryKeyExpressions,
                newSecondaryKeyExpressions, newTokenizeVars, newFilterExpression, op.getOperation(), op.isBulkload(),
                op.isPartitioned(), newTokenizeVarTypes);
        return tokenizeOp;
    }

    @Override
    public ILogicalOperator visitSinkOperator(SinkOperator op, Void arg) throws AlgebricksException {
        return new SinkOperator();
    }

    private void deepCopyExpressionRefs(List<Mutable<ILogicalExpression>> newExprs,
            List<Mutable<ILogicalExpression>> oldExprs) {
        for (Mutable<ILogicalExpression> oldExpr : oldExprs)
            newExprs.add(new MutableObject<ILogicalExpression>(((AbstractLogicalExpression) oldExpr.getValue())
                    .cloneExpression()));
    }

    private Mutable<ILogicalExpression> deepCopyExpressionRef(Mutable<ILogicalExpression> oldExpr) {
        return new MutableObject<ILogicalExpression>(((AbstractLogicalExpression) oldExpr.getValue()).cloneExpression());
    }

    private List<LogicalVariable> deepCopyVars(List<LogicalVariable> newVars, List<LogicalVariable> oldVars) {
        for (LogicalVariable oldVar : oldVars)
            newVars.add(oldVar);
        return newVars;
    }

    private List<Object> deepCopyObjects(List<Object> newObjs, List<Object> oldObjs) {
        for (Object oldObj : oldObjs)
            newObjs.add(oldObj);
        return newObjs;
    }

    private List<Pair<IOrder, Mutable<ILogicalExpression>>> deepCopyOrderAndExpression(
            List<Pair<IOrder, Mutable<ILogicalExpression>>> ordersAndExprs) {
        List<Pair<IOrder, Mutable<ILogicalExpression>>> newOrdersAndExprs = new ArrayList<Pair<IOrder, Mutable<ILogicalExpression>>>();
        for (Pair<IOrder, Mutable<ILogicalExpression>> pair : ordersAndExprs)
            newOrdersAndExprs.add(new Pair<IOrder, Mutable<ILogicalExpression>>(pair.first,
                    deepCopyExpressionRef(pair.second)));
        return newOrdersAndExprs;
    }

    @Override
    public ILogicalOperator visitExtensionOperator(ExtensionOperator op, Void arg) throws AlgebricksException {
        return new ExtensionOperator(op.getNewInstanceOfDelegateOperator());
    }

    @Override
    public ILogicalOperator visitExternalDataLookupOperator(ExternalDataLookupOperator op, Void arg)
            throws AlgebricksException {
        ArrayList<LogicalVariable> newInputList = new ArrayList<LogicalVariable>();
        newInputList.addAll(op.getVariables());
        return new ExternalDataLookupOperator(newInputList, deepCopyExpressionRef(op.getExpressionRef()),
                new ArrayList<Object>(op.getVariableTypes()), op.isPropagateInput(), op.getDataSource());
    }

    @Override
    public ILogicalOperator visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        return new MaterializeOperator();
    }
}
