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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class IsomorphismOperatorVisitor implements ILogicalOperatorVisitor<Boolean, ILogicalOperator> {

    private Map<LogicalVariable, LogicalVariable> variableMapping = new HashMap<LogicalVariable, LogicalVariable>();

    public IsomorphismOperatorVisitor() {
    }

    @Override
    public Boolean visitAggregateOperator(AggregateOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.AGGREGATE)
            return Boolean.FALSE;
        AggregateOperator aggOpArg = (AggregateOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(
                getPairList(op.getVariables(), op.getExpressions()),
                getPairList(aggOpArg.getVariables(), aggOpArg.getExpressions()));
        return isomorphic;
    }

    @Override
    public Boolean visitRunningAggregateOperator(RunningAggregateOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.RUNNINGAGGREGATE)
            return Boolean.FALSE;
        RunningAggregateOperator aggOpArg = (RunningAggregateOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(
                getPairList(op.getVariables(), op.getExpressions()),
                getPairList(aggOpArg.getVariables(), aggOpArg.getExpressions()));
        return isomorphic;
    }

    @Override
    public Boolean visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) copyAndSubstituteVar(op, arg);
        if (aop.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE)
            return Boolean.FALSE;
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitExtensionOperator(ExtensionOperator op, ILogicalOperator arg) throws AlgebricksException {
        ExtensionOperator aop = (ExtensionOperator) copyAndSubstituteVar(op, arg);
        if (aop.getOperatorTag() != LogicalOperatorTag.EXTENSION_OPERATOR)
            return Boolean.FALSE;
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitGroupByOperator(GroupByOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        // require the same physical operator, otherwise delivers different data
        // properties
        if (aop.getOperatorTag() != LogicalOperatorTag.GROUP
                || aop.getPhysicalOperator().getOperatorTag() != op.getPhysicalOperator().getOperatorTag())
            return Boolean.FALSE;

        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> keyLists = op.getGroupByList();
        GroupByOperator gbyOpArg = (GroupByOperator) copyAndSubstituteVar(op, arg);
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> keyListsArg = gbyOpArg.getGroupByList();

        List<Pair<LogicalVariable, ILogicalExpression>> listLeft = new ArrayList<Pair<LogicalVariable, ILogicalExpression>>();
        List<Pair<LogicalVariable, ILogicalExpression>> listRight = new ArrayList<Pair<LogicalVariable, ILogicalExpression>>();

        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> pair : keyLists)
            listLeft.add(new Pair<LogicalVariable, ILogicalExpression>(pair.first, pair.second.getValue()));
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> pair : keyListsArg)
            listRight.add(new Pair<LogicalVariable, ILogicalExpression>(pair.first, pair.second.getValue()));

        boolean isomorphic = VariableUtilities.varListEqualUnordered(listLeft, listRight);

        if (!isomorphic)
            return Boolean.FALSE;
        int sizeOp = op.getNestedPlans().size();
        int sizeArg = gbyOpArg.getNestedPlans().size();
        if (sizeOp != sizeArg)
            return Boolean.FALSE;

        GroupByOperator argOp = (GroupByOperator) arg;
        List<ILogicalPlan> plans = op.getNestedPlans();
        List<ILogicalPlan> plansArg = argOp.getNestedPlans();
        for (int i = 0; i < plans.size(); i++) {
            List<Mutable<ILogicalOperator>> roots = plans.get(i).getRoots();
            List<Mutable<ILogicalOperator>> rootsArg = plansArg.get(i).getRoots();
            if (roots.size() != rootsArg.size())
                return Boolean.FALSE;
            for (int j = 0; j < roots.size(); j++) {
                ILogicalOperator topOp1 = roots.get(j).getValue();
                ILogicalOperator topOp2 = rootsArg.get(j).getValue();
                isomorphic = this.checkBottomUp(topOp1, topOp2);
                if (!isomorphic)
                    return Boolean.FALSE;
            }
        }
        return isomorphic;
    }

    @Override
    public Boolean visitLimitOperator(LimitOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.LIMIT)
            return Boolean.FALSE;
        LimitOperator limitOpArg = (LimitOperator) copyAndSubstituteVar(op, arg);
        if (op.getOffset() != limitOpArg.getOffset())
            return Boolean.FALSE;
        boolean isomorphic = op.getMaxObjects().getValue().equals(limitOpArg.getMaxObjects().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitInnerJoinOperator(InnerJoinOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.INNERJOIN)
            return Boolean.FALSE;
        InnerJoinOperator joinOpArg = (InnerJoinOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = op.getCondition().getValue().equals(joinOpArg.getCondition().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitLeftOuterJoinOperator(LeftOuterJoinOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN)
            return Boolean.FALSE;
        LeftOuterJoinOperator joinOpArg = (LeftOuterJoinOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = op.getCondition().getValue().equals(joinOpArg.getCondition().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitNestedTupleSourceOperator(NestedTupleSourceOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE)
            return Boolean.FALSE;
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitOrderOperator(OrderOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.ORDER)
            return Boolean.FALSE;
        OrderOperator orderOpArg = (OrderOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = compareIOrderAndExpressions(op.getOrderExpressions(), orderOpArg.getOrderExpressions());
        return isomorphic;
    }

    @Override
    public Boolean visitAssignOperator(AssignOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.ASSIGN)
            return Boolean.FALSE;
        AssignOperator assignOpArg = (AssignOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(
                getPairList(op.getVariables(), op.getExpressions()),
                getPairList(assignOpArg.getVariables(), assignOpArg.getExpressions()));
        return isomorphic;
    }

    @Override
    public Boolean visitSelectOperator(SelectOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.SELECT)
            return Boolean.FALSE;
        SelectOperator selectOpArg = (SelectOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = op.getCondition().getValue().equals(selectOpArg.getCondition().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitProjectOperator(ProjectOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.PROJECT)
            return Boolean.FALSE;
        ProjectOperator projectOpArg = (ProjectOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), projectOpArg.getVariables());
        return isomorphic;
    }

    @Override
    public Boolean visitPartitioningSplitOperator(PartitioningSplitOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.PARTITIONINGSPLIT)
            return Boolean.FALSE;
        PartitioningSplitOperator partitionOpArg = (PartitioningSplitOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = compareExpressions(op.getExpressions(), partitionOpArg.getExpressions());
        return isomorphic;
    }

    @Override
    public Boolean visitReplicateOperator(ReplicateOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.REPLICATE)
            return Boolean.FALSE;
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitScriptOperator(ScriptOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.SCRIPT)
            return Boolean.FALSE;
        ScriptOperator scriptOpArg = (ScriptOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = op.getScriptDescription().equals(scriptOpArg.getScriptDescription());
        return isomorphic;
    }

    @Override
    public Boolean visitSubplanOperator(SubplanOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.SUBPLAN)
            return Boolean.FALSE;
        SubplanOperator subplanOpArg = (SubplanOperator) copyAndSubstituteVar(op, arg);
        List<ILogicalPlan> plans = op.getNestedPlans();
        List<ILogicalPlan> plansArg = subplanOpArg.getNestedPlans();
        for (int i = 0; i < plans.size(); i++) {
            List<Mutable<ILogicalOperator>> roots = plans.get(i).getRoots();
            List<Mutable<ILogicalOperator>> rootsArg = plansArg.get(i).getRoots();
            if (roots.size() == rootsArg.size())
                return Boolean.FALSE;
            for (int j = 0; j < roots.size(); j++) {
                ILogicalOperator topOp1 = roots.get(j).getValue();
                ILogicalOperator topOp2 = rootsArg.get(j).getValue();
                boolean isomorphic = this.checkBottomUp(topOp1, topOp2);
                if (!isomorphic)
                    return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitUnionOperator(UnionAllOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.UNIONALL)
            return Boolean.FALSE;
        UnionAllOperator unionOpArg = (UnionAllOperator) copyAndSubstituteVar(op, arg);
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> mapping = op.getVariableMappings();
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> mappingArg = unionOpArg.getVariableMappings();
        if (mapping.size() != mappingArg.size())
            return Boolean.FALSE;
        return VariableUtilities.varListEqualUnordered(mapping, mappingArg);
    }

    @Override
    public Boolean visitUnnestOperator(UnnestOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.UNNEST)
            return Boolean.FALSE;
        UnnestOperator unnestOpArg = (UnnestOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), unnestOpArg.getVariables())
                && variableEqual(op.getPositionalVariable(), unnestOpArg.getPositionalVariable());
        if (!isomorphic)
            return Boolean.FALSE;
        isomorphic = op.getExpressionRef().getValue().equals(unnestOpArg.getExpressionRef().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitUnnestMapOperator(UnnestMapOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.UNNEST_MAP)
            return Boolean.FALSE;
        UnnestMapOperator unnestOpArg = (UnnestMapOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), unnestOpArg.getVariables());
        if (!isomorphic)
            return Boolean.FALSE;
        isomorphic = op.getExpressionRef().getValue().equals(unnestOpArg.getExpressionRef().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitDataScanOperator(DataSourceScanOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN)
            return Boolean.FALSE;
        DataSourceScanOperator argScan = (DataSourceScanOperator) arg;
        if (!argScan.getDataSource().toString().equals(op.getDataSource().toString()))
            return Boolean.FALSE;
        DataSourceScanOperator scanOpArg = (DataSourceScanOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), scanOpArg.getVariables())
                && op.getDataSource().toString().equals(scanOpArg.getDataSource().toString());
        return isomorphic;
    }

    @Override
    public Boolean visitDistinctOperator(DistinctOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.DISTINCT)
            return Boolean.FALSE;
        DistinctOperator distinctOpArg = (DistinctOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = compareExpressions(op.getExpressions(), distinctOpArg.getExpressions());
        return isomorphic;
    }

    @Override
    public Boolean visitExchangeOperator(ExchangeOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.EXCHANGE)
            return Boolean.FALSE;
        // require the same partition property
        if (!(op.getPhysicalOperator().getOperatorTag() == aop.getPhysicalOperator().getOperatorTag()))
            return Boolean.FALSE;
        variableMapping.clear();
        IsomorphismUtilities.mapVariablesTopDown(op, arg, variableMapping);
        IPhysicalPropertiesVector properties = op.getPhysicalOperator().getDeliveredProperties();
        IPhysicalPropertiesVector propertiesArg = aop.getPhysicalOperator().getDeliveredProperties();
        if (properties == null && propertiesArg == null)
            return Boolean.TRUE;
        if (properties == null || propertiesArg == null)
            return Boolean.FALSE;
        IPartitioningProperty partProp = properties.getPartitioningProperty();
        IPartitioningProperty partPropArg = propertiesArg.getPartitioningProperty();
        if (!partProp.getPartitioningType().equals(partPropArg.getPartitioningType()))
            return Boolean.FALSE;
        List<LogicalVariable> columns = new ArrayList<LogicalVariable>();
        partProp.getColumns(columns);
        List<LogicalVariable> columnsArg = new ArrayList<LogicalVariable>();
        partPropArg.getColumns(columnsArg);
        if (columns.size() != columnsArg.size())
            return Boolean.FALSE;
        if (columns.size() == 0)
            return Boolean.TRUE;
        for (int i = 0; i < columnsArg.size(); i++) {
            LogicalVariable rightVar = columnsArg.get(i);
            LogicalVariable leftVar = variableMapping.get(rightVar);
            if (leftVar != null)
                columnsArg.set(i, leftVar);
        }
        return VariableUtilities.varListEqualUnordered(columns, columnsArg);
    }

    @Override
    public Boolean visitWriteOperator(WriteOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.WRITE)
            return Boolean.FALSE;
        WriteOperator writeOpArg = (WriteOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), writeOpArg.getSchema());
        return isomorphic;
    }

    @Override
    public Boolean visitDistributeResultOperator(DistributeResultOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT)
            return Boolean.FALSE;
        DistributeResultOperator writeOpArg = (DistributeResultOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), writeOpArg.getSchema());
        return isomorphic;
    }

    @Override
    public Boolean visitWriteResultOperator(WriteResultOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.WRITE_RESULT)
            return Boolean.FALSE;
        WriteResultOperator writeOpArg = (WriteResultOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), writeOpArg.getSchema());
        if (!op.getDataSource().equals(writeOpArg.getDataSource()))
            isomorphic = false;
        if (!op.getPayloadExpression().equals(writeOpArg.getPayloadExpression()))
            isomorphic = false;
        return isomorphic;
    }

    @Override
    public Boolean visitInsertDeleteOperator(InsertDeleteOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE)
            return Boolean.FALSE;
        InsertDeleteOperator insertOpArg = (InsertDeleteOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), insertOpArg.getSchema());
        if (!op.getDataSource().equals(insertOpArg.getDataSource()))
            isomorphic = false;
        if (!op.getPayloadExpression().equals(insertOpArg.getPayloadExpression()))
            isomorphic = false;
        return isomorphic;
    }

    @Override
    public Boolean visitIndexInsertDeleteOperator(IndexInsertDeleteOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.INDEX_INSERT_DELETE)
            return Boolean.FALSE;
        IndexInsertDeleteOperator insertOpArg = (IndexInsertDeleteOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), insertOpArg.getSchema());
        if (!op.getDataSourceIndex().equals(insertOpArg.getDataSourceIndex()))
            isomorphic = false;
        return isomorphic;
    }

    @Override
    public Boolean visitSinkOperator(SinkOperator op, ILogicalOperator arg) throws AlgebricksException {
        return true;
    }

    private Boolean compareExpressions(List<Mutable<ILogicalExpression>> opExprs,
            List<Mutable<ILogicalExpression>> argExprs) {
        if (opExprs.size() != argExprs.size())
            return Boolean.FALSE;
        for (int i = 0; i < opExprs.size(); i++) {
            boolean isomorphic = opExprs.get(i).getValue().equals(argExprs.get(i).getValue());
            if (!isomorphic)
                return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private Boolean compareIOrderAndExpressions(List<Pair<IOrder, Mutable<ILogicalExpression>>> opOrderExprs,
            List<Pair<IOrder, Mutable<ILogicalExpression>>> argOrderExprs) {
        if (opOrderExprs.size() != argOrderExprs.size())
            return Boolean.FALSE;
        for (int i = 0; i < opOrderExprs.size(); i++) {
            boolean isomorphic = opOrderExprs.get(i).first.equals(argOrderExprs.get(i).first);
            if (!isomorphic)
                return Boolean.FALSE;
            isomorphic = opOrderExprs.get(i).second.getValue().equals(argOrderExprs.get(i).second.getValue());
            if (!isomorphic)
                return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private Boolean checkBottomUp(ILogicalOperator op1, ILogicalOperator op2) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> inputs1 = op1.getInputs();
        List<Mutable<ILogicalOperator>> inputs2 = op2.getInputs();
        if (inputs1.size() != inputs2.size())
            return Boolean.FALSE;
        for (int i = 0; i < inputs1.size(); i++) {
            ILogicalOperator input1 = inputs1.get(i).getValue();
            ILogicalOperator input2 = inputs2.get(i).getValue();
            boolean isomorphic = checkBottomUp(input1, input2);
            if (!isomorphic)
                return Boolean.FALSE;
        }
        return IsomorphismUtilities.isOperatorIsomorphic(op1, op2);
    }

    private ILogicalOperator copyAndSubstituteVar(ILogicalOperator op, ILogicalOperator argOp)
            throws AlgebricksException {
        ILogicalOperator newOp = IsomorphismOperatorVisitor.deepCopy(argOp);
        variableMapping.clear();
        IsomorphismUtilities.mapVariablesTopDown(op, argOp, variableMapping);

        List<LogicalVariable> liveVars = new ArrayList<LogicalVariable>();
        if (argOp.getInputs().size() > 0)
            for (int i = 0; i < argOp.getInputs().size(); i++)
                VariableUtilities.getLiveVariables(argOp.getInputs().get(i).getValue(), liveVars);
        List<LogicalVariable> producedVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getProducedVariables(argOp, producedVars);
        List<LogicalVariable> producedVarsNew = new ArrayList<LogicalVariable>();
        VariableUtilities.getProducedVariables(op, producedVarsNew);

        if (producedVars.size() != producedVarsNew.size())
            return newOp;
        for (Entry<LogicalVariable, LogicalVariable> map : variableMapping.entrySet()) {
            if (liveVars.contains(map.getKey())) {
                VariableUtilities.substituteVariables(newOp, map.getKey(), map.getValue(), null);
            }
        }
        for (int i = 0; i < producedVars.size(); i++)
            VariableUtilities.substituteVariables(newOp, producedVars.get(i), producedVarsNew.get(i), null);
        return newOp;
    }

    public List<Pair<LogicalVariable, ILogicalExpression>> getPairList(List<LogicalVariable> vars,
            List<Mutable<ILogicalExpression>> exprs) throws AlgebricksException {
        List<Pair<LogicalVariable, ILogicalExpression>> list = new ArrayList<Pair<LogicalVariable, ILogicalExpression>>();
        if (vars.size() != exprs.size())
            throw new AlgebricksException("variable list size does not equal to expression list size ");
        for (int i = 0; i < vars.size(); i++) {
            list.add(new Pair<LogicalVariable, ILogicalExpression>(vars.get(i), exprs.get(i).getValue()));
        }
        return list;
    }

    private static ILogicalOperator deepCopy(ILogicalOperator op) throws AlgebricksException {
        OperatorDeepCopyVisitor visitor = new OperatorDeepCopyVisitor();
        return op.accept(visitor, null);
    }

    private static ILogicalPlan deepCopy(ILogicalPlan plan) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> roots = plan.getRoots();
        List<Mutable<ILogicalOperator>> newRoots = new ArrayList<Mutable<ILogicalOperator>>();
        for (Mutable<ILogicalOperator> opRef : roots)
            newRoots.add(new MutableObject<ILogicalOperator>(bottomUpCopyOperators(opRef.getValue())));
        return new ALogicalPlanImpl(newRoots);
    }

    private static ILogicalOperator bottomUpCopyOperators(ILogicalOperator op) throws AlgebricksException {
        ILogicalOperator newOp = deepCopy(op);
        newOp.getInputs().clear();
        for (Mutable<ILogicalOperator> child : op.getInputs())
            newOp.getInputs().add(new MutableObject<ILogicalOperator>(bottomUpCopyOperators(child.getValue())));
        return newOp;
    }

    private static boolean variableEqual(LogicalVariable var, LogicalVariable varArg) {
        if (var == null && varArg == null)
            return true;
        if (var.equals(varArg))
            return true;
        else
            return false;
    }

    private static class OperatorDeepCopyVisitor implements ILogicalOperatorVisitor<ILogicalOperator, Void> {

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
                newSubplans.add(IsomorphismOperatorVisitor.deepCopy(plan));
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
            return new InnerJoinOperator(deepCopyExpressionRef(op.getCondition()), op.getInputs().get(0), op
                    .getInputs().get(1));
        }

        @Override
        public ILogicalOperator visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg)
                throws AlgebricksException {
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
            return new SelectOperator(deepCopyExpressionRef(op.getCondition()));
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
                newSubplans.add(IsomorphismOperatorVisitor.deepCopy(plan));
            }
            return new SubplanOperator(newSubplans);
        }

        @Override
        public ILogicalOperator visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
            List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> newVarMap = new ArrayList<Triple<LogicalVariable, LogicalVariable, LogicalVariable>>();
            List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap = op.getVariableMappings();
            for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> triple : varMap)
                newVarMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(triple.first,
                        triple.second, triple.third));
            return new UnionAllOperator(newVarMap);
        }

        @Override
        public ILogicalOperator visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
            return new UnnestOperator(op.getVariable(), deepCopyExpressionRef(op.getExpressionRef()),
                    op.getPositionalVariable(), op.getPositionalVariableType());
        }

        @Override
        public ILogicalOperator visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
            ArrayList<LogicalVariable> newInputList = new ArrayList<LogicalVariable>();
            newInputList.addAll(op.getVariables());
            return new UnnestMapOperator(newInputList, deepCopyExpressionRef(op.getExpressionRef()),
                    new ArrayList<Object>(op.getVariableTypes()), op.propagatesInput());
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
            return new WriteResultOperator(op.getDataSource(), deepCopyExpressionRef(op.getPayloadExpression()),
                    newKeyExpressions);
        }

        @Override
        public ILogicalOperator visitInsertDeleteOperator(InsertDeleteOperator op, Void arg) throws AlgebricksException {
            List<Mutable<ILogicalExpression>> newKeyExpressions = new ArrayList<Mutable<ILogicalExpression>>();
            deepCopyExpressionRefs(newKeyExpressions, op.getPrimaryKeyExpressions());
            return new InsertDeleteOperator(op.getDataSource(), deepCopyExpressionRef(op.getPayloadExpression()),
                    newKeyExpressions, op.getOperation());
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
            return new IndexInsertDeleteOperator(op.getDataSourceIndex(), newPrimaryKeyExpressions,
                    newSecondaryKeyExpressions, newFilterExpression, op.getOperation());
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
            return new MutableObject<ILogicalExpression>(
                    ((AbstractLogicalExpression) oldExpr.getValue()).cloneExpression());
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
    }

}
