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
package org.apache.asterix.algebra.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
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
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class LogicalOperatorDeepCopyVisitor implements ILogicalOperatorVisitor<ILogicalOperator, ILogicalOperator> {
    private final Counter counter;
    private final LogicalExpressionDeepCopyVisitor exprDeepCopyVisitor;

    // Key: Variable in the original plan. Value: New variable replacing the
    // original one in the copied plan.
    private final Map<LogicalVariable, LogicalVariable> outVarMapping = new HashMap<LogicalVariable, LogicalVariable>();

    // Key: Variable in the original plan. Value: Variable with which to replace
    // original variable in the plan copy.
    private final Map<LogicalVariable, LogicalVariable> inVarMapping;

    public LogicalOperatorDeepCopyVisitor(Counter counter) {
        this.counter = counter;
        this.inVarMapping = Collections.emptyMap();
        exprDeepCopyVisitor = new LogicalExpressionDeepCopyVisitor(counter, inVarMapping, outVarMapping);
    }

    /**
     * @param counter
     *            Starting variable counter.
     * @param inVarMapping
     *            Variable mapping keyed by variables in the original plan.
     *            Those variables are replaced by their corresponding value in
     *            the map in the copied plan.
     */
    public LogicalOperatorDeepCopyVisitor(Counter counter, Map<LogicalVariable, LogicalVariable> inVarMapping) {
        this.counter = counter;
        this.inVarMapping = inVarMapping;
        exprDeepCopyVisitor = new LogicalExpressionDeepCopyVisitor(counter, inVarMapping, outVarMapping);
    }

    private void copyAnnotations(ILogicalOperator src, ILogicalOperator dest) {
        dest.getAnnotations().putAll(src.getAnnotations());
    }

    public ILogicalOperator deepCopy(ILogicalOperator op, ILogicalOperator arg) throws AlgebricksException {
        return op.accept(this, arg);
    }

    private void deepCopyInputs(ILogicalOperator src, ILogicalOperator dest, ILogicalOperator arg)
            throws AlgebricksException {
        List<Mutable<ILogicalOperator>> inputs = src.getInputs();
        List<Mutable<ILogicalOperator>> inputsCopy = dest.getInputs();
        for (Mutable<ILogicalOperator> input : inputs) {
            inputsCopy.add(deepCopyOperatorReference(input, arg));
        }
    }

    private Mutable<ILogicalOperator> deepCopyOperatorReference(Mutable<ILogicalOperator> opRef, ILogicalOperator arg)
            throws AlgebricksException {
        return new MutableObject<ILogicalOperator>(deepCopy(opRef.getValue(), arg));
    }

    private List<Mutable<ILogicalOperator>> deepCopyOperatorReferenceList(List<Mutable<ILogicalOperator>> list,
            ILogicalOperator arg) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> listCopy = new ArrayList<Mutable<ILogicalOperator>>(list.size());
        for (Mutable<ILogicalOperator> opRef : list) {
            listCopy.add(deepCopyOperatorReference(opRef, arg));
        }
        return listCopy;
    }

    private IOrder deepCopyOrder(IOrder order) {
        switch (order.getKind()) {
            case ASC:
            case DESC:
                return order;
            case FUNCTIONCALL:
            default:
                throw new UnsupportedOperationException();
        }
    }

    private List<Pair<IOrder, Mutable<ILogicalExpression>>> deepCopyOrderExpressionReferencePairList(
            List<Pair<IOrder, Mutable<ILogicalExpression>>> list) throws AlgebricksException {
        ArrayList<Pair<IOrder, Mutable<ILogicalExpression>>> listCopy = new ArrayList<Pair<IOrder, Mutable<ILogicalExpression>>>(
                list.size());
        for (Pair<IOrder, Mutable<ILogicalExpression>> pair : list) {
            listCopy.add(new Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>(deepCopyOrder(pair.first),
                    exprDeepCopyVisitor.deepCopyExpressionReference(pair.second)));
        }
        return listCopy;
    }

    private ILogicalPlan deepCopyPlan(ILogicalPlan plan, ILogicalOperator arg) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> rootsCopy = deepCopyOperatorReferenceList(plan.getRoots(), arg);
        ILogicalPlan planCopy = new ALogicalPlanImpl(rootsCopy);
        return planCopy;
    }

    private List<ILogicalPlan> deepCopyPlanList(List<ILogicalPlan> list, List<ILogicalPlan> listCopy,
            ILogicalOperator arg) throws AlgebricksException {
        for (ILogicalPlan plan : list) {
            listCopy.add(deepCopyPlan(plan, arg));
        }
        return listCopy;
    }

    private LogicalVariable deepCopyVariable(LogicalVariable var) {
        if (var == null) {
            return null;
        }
        LogicalVariable givenVarReplacement = inVarMapping.get(var);
        if (givenVarReplacement != null) {
            outVarMapping.put(var, givenVarReplacement);
            return givenVarReplacement;
        }
        LogicalVariable varCopy = outVarMapping.get(var);
        if (varCopy == null) {
            counter.inc();
            varCopy = new LogicalVariable(counter.get());
            outVarMapping.put(var, varCopy);
        }
        return varCopy;
    }

    private List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> deepCopyVariableExpressionReferencePairList(
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> list) throws AlgebricksException {
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> listCopy = new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>(
                list.size());
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> pair : list) {
            listCopy.add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(deepCopyVariable(pair.first),
                    exprDeepCopyVisitor.deepCopyExpressionReference(pair.second)));
        }
        return listCopy;
    }

    // TODO return List<...>
    private ArrayList<LogicalVariable> deepCopyVariableList(List<LogicalVariable> list) {
        ArrayList<LogicalVariable> listCopy = new ArrayList<LogicalVariable>(list.size());
        for (LogicalVariable var : list) {
            listCopy.add(deepCopyVariable(var));
        }
        return listCopy;
    }

    public void reset() {
        outVarMapping.clear();
    }

    public void updatePrimaryKeys(IOptimizationContext context) {
        for (Map.Entry<LogicalVariable, LogicalVariable> entry : outVarMapping.entrySet()) {
            List<LogicalVariable> primaryKey = context.findPrimaryKey(entry.getKey());
            if (primaryKey != null) {
                List<LogicalVariable> head = new ArrayList<LogicalVariable>();
                for (LogicalVariable variable : primaryKey) {
                    head.add(outVarMapping.get(variable));
                }
                List<LogicalVariable> tail = new ArrayList<LogicalVariable>(1);
                tail.add(entry.getValue());
                context.addPrimaryKey(new FunctionalDependency(head, tail));
            }
        }
    }

    public LogicalVariable varCopy(LogicalVariable var) throws AlgebricksException {
        return outVarMapping.get(var);
    }

    @Override
    public ILogicalOperator visitAggregateOperator(AggregateOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AggregateOperator opCopy = new AggregateOperator(deepCopyVariableList(op.getVariables()),
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getExpressions()));
        deepCopyInputs(op, opCopy, arg);
        copyAnnotations(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitAssignOperator(AssignOperator op, ILogicalOperator arg) throws AlgebricksException {
        AssignOperator opCopy = new AssignOperator(deepCopyVariableList(op.getVariables()),
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getExpressions()));
        deepCopyInputs(op, opCopy, arg);
        copyAnnotations(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitDataScanOperator(DataSourceScanOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        DataSourceScanOperator opCopy = new DataSourceScanOperator(deepCopyVariableList(op.getVariables()),
                op.getDataSource());
        deepCopyInputs(op, opCopy, arg);
        copyAnnotations(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitDistinctOperator(DistinctOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, ILogicalOperator arg) {
        EmptyTupleSourceOperator opCopy = new EmptyTupleSourceOperator();
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitExchangeOperator(ExchangeOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitGroupByOperator(GroupByOperator op, ILogicalOperator arg) throws AlgebricksException {
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByListCopy = deepCopyVariableExpressionReferencePairList(
                op.getGroupByList());
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decorListCopy = deepCopyVariableExpressionReferencePairList(
                op.getDecorList());
        List<ILogicalPlan> nestedPlansCopy = new ArrayList<ILogicalPlan>();

        GroupByOperator opCopy = new GroupByOperator(groupByListCopy, decorListCopy, nestedPlansCopy);
        deepCopyPlanList(op.getNestedPlans(), nestedPlansCopy, opCopy);
        deepCopyInputs(op, opCopy, arg);
        copyAnnotations(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitInnerJoinOperator(InnerJoinOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        InnerJoinOperator opCopy = new InnerJoinOperator(
                exprDeepCopyVisitor.deepCopyExpressionReference(op.getCondition()),
                deepCopyOperatorReference(op.getInputs().get(0), null),
                deepCopyOperatorReference(op.getInputs().get(1), null));
        copyAnnotations(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitLeftOuterJoinOperator(LeftOuterJoinOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitLimitOperator(LimitOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitNestedTupleSourceOperator(NestedTupleSourceOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        NestedTupleSourceOperator opCopy = new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(arg));
        deepCopyInputs(op, opCopy, arg);
        copyAnnotations(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitOrderOperator(OrderOperator op, ILogicalOperator arg) throws AlgebricksException {
        OrderOperator opCopy = new OrderOperator(deepCopyOrderExpressionReferencePairList(op.getOrderExpressions()));
        deepCopyInputs(op, opCopy, arg);
        copyAnnotations(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitPartitioningSplitOperator(PartitioningSplitOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitProjectOperator(ProjectOperator op, ILogicalOperator arg) throws AlgebricksException {
        ProjectOperator opCopy = new ProjectOperator(deepCopyVariableList(op.getVariables()));
        deepCopyInputs(op, opCopy, arg);
        copyAnnotations(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitReplicateOperator(ReplicateOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitMaterializeOperator(MaterializeOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitRunningAggregateOperator(RunningAggregateOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitScriptOperator(ScriptOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitSelectOperator(SelectOperator op, ILogicalOperator arg) throws AlgebricksException {
        SelectOperator opCopy = new SelectOperator(exprDeepCopyVisitor.deepCopyExpressionReference(op.getCondition()),
                op.getRetainNull(), deepCopyVariable(op.getNullPlaceholderVariable()));
        deepCopyInputs(op, opCopy, arg);
        copyAnnotations(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitSubplanOperator(SubplanOperator op, ILogicalOperator arg) throws AlgebricksException {
        List<ILogicalPlan> nestedPlansCopy = new ArrayList<ILogicalPlan>();

        SubplanOperator opCopy = new SubplanOperator(nestedPlansCopy);
        deepCopyPlanList(op.getNestedPlans(), nestedPlansCopy, opCopy);
        deepCopyInputs(op, opCopy, arg);
        copyAnnotations(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitUnionOperator(UnionAllOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitUnnestMapOperator(UnnestMapOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        UnnestMapOperator opCopy = new UnnestMapOperator(deepCopyVariableList(op.getVariables()),
                exprDeepCopyVisitor.deepCopyExpressionReference(op.getExpressionRef()), op.getVariableTypes(),
                op.propagatesInput());
        deepCopyInputs(op, opCopy, arg);
        copyAnnotations(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitUnnestOperator(UnnestOperator op, ILogicalOperator arg) throws AlgebricksException {
        UnnestOperator opCopy = new UnnestOperator(deepCopyVariable(op.getVariable()),
                exprDeepCopyVisitor.deepCopyExpressionReference(op.getExpressionRef()),
                deepCopyVariable(op.getPositionalVariable()), op.getPositionalVariableType(), op.getPositionWriter());
        deepCopyInputs(op, opCopy, arg);
        copyAnnotations(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitWriteOperator(WriteOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitDistributeResultOperator(DistributeResultOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitWriteResultOperator(WriteResultOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitInsertDeleteOperator(InsertDeleteOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitIndexInsertDeleteOperator(IndexInsertDeleteOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitTokenizeOperator(TokenizeOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitSinkOperator(SinkOperator op, ILogicalOperator arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitExtensionOperator(ExtensionOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    public Map<LogicalVariable, LogicalVariable> getVariableMapping() {
        return outVarMapping;
    }

    @Override
    public ILogicalOperator visitExternalDataLookupOperator(ExternalDataLookupOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

}
