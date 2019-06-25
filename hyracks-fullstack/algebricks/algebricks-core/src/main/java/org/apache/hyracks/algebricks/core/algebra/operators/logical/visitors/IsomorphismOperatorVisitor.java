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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
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
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class IsomorphismOperatorVisitor implements ILogicalOperatorVisitor<Boolean, ILogicalOperator> {

    private final Map<LogicalVariable, LogicalVariable> variableMapping =
            new HashMap<LogicalVariable, LogicalVariable>();

    public IsomorphismOperatorVisitor() {
    }

    @Override
    public Boolean visitAggregateOperator(AggregateOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return Boolean.FALSE;
        }
        AggregateOperator aggOpArg = (AggregateOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic =
                VariableUtilities.varListEqualUnordered(getPairList(op.getVariables(), op.getExpressions()),
                        getPairList(aggOpArg.getVariables(), aggOpArg.getExpressions()));
        return isomorphic;
    }

    @Override
    public Boolean visitRunningAggregateOperator(RunningAggregateOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.RUNNINGAGGREGATE) {
            return Boolean.FALSE;
        }
        RunningAggregateOperator aggOpArg = (RunningAggregateOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic =
                VariableUtilities.varListEqualUnordered(getPairList(op.getVariables(), op.getExpressions()),
                        getPairList(aggOpArg.getVariables(), aggOpArg.getExpressions()));
        return isomorphic;
    }

    @Override
    public Boolean visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) copyAndSubstituteVar(op, arg);
        if (aop.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitDelegateOperator(DelegateOperator op, ILogicalOperator arg) throws AlgebricksException {
        DelegateOperator aop = (DelegateOperator) copyAndSubstituteVar(op, arg);
        if (aop.getOperatorTag() != LogicalOperatorTag.DELEGATE_OPERATOR) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitGroupByOperator(GroupByOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        // require the same physical operator, otherwise delivers different data
        // properties
        if (aop.getOperatorTag() != LogicalOperatorTag.GROUP || op.getPhysicalOperator() == null
                || aop.getPhysicalOperator() == null
                || op.getPhysicalOperator().getOperatorTag() != aop.getPhysicalOperator().getOperatorTag()) {
            return Boolean.FALSE;
        }

        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> keyLists = op.getGroupByList();
        GroupByOperator gbyOpArg = (GroupByOperator) copyAndSubstituteVar(op, arg);
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> keyListsArg = gbyOpArg.getGroupByList();

        List<Pair<LogicalVariable, ILogicalExpression>> listLeft =
                new ArrayList<Pair<LogicalVariable, ILogicalExpression>>();
        List<Pair<LogicalVariable, ILogicalExpression>> listRight =
                new ArrayList<Pair<LogicalVariable, ILogicalExpression>>();

        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> pair : keyLists) {
            listLeft.add(new Pair<LogicalVariable, ILogicalExpression>(pair.first, pair.second.getValue()));
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> pair : keyListsArg) {
            listRight.add(new Pair<LogicalVariable, ILogicalExpression>(pair.first, pair.second.getValue()));
        }

        boolean isomorphic = VariableUtilities.varListEqualUnordered(listLeft, listRight);

        if (!isomorphic) {
            return Boolean.FALSE;
        }
        int sizeOp = op.getNestedPlans().size();
        int sizeArg = gbyOpArg.getNestedPlans().size();
        if (sizeOp != sizeArg) {
            return Boolean.FALSE;
        }

        GroupByOperator argOp = (GroupByOperator) arg;
        List<ILogicalPlan> plans = op.getNestedPlans();
        List<ILogicalPlan> plansArg = argOp.getNestedPlans();
        for (int i = 0; i < plans.size(); i++) {
            List<Mutable<ILogicalOperator>> roots = plans.get(i).getRoots();
            List<Mutable<ILogicalOperator>> rootsArg = plansArg.get(i).getRoots();
            if (roots.size() != rootsArg.size()) {
                return Boolean.FALSE;
            }
            for (int j = 0; j < roots.size(); j++) {
                ILogicalOperator topOp1 = roots.get(j).getValue();
                ILogicalOperator topOp2 = rootsArg.get(j).getValue();
                isomorphic = IsomorphismUtilities.isOperatorIsomorphicPlanSegment(topOp1, topOp2);
                if (!isomorphic) {
                    return Boolean.FALSE;
                }
            }
        }
        return isomorphic;
    }

    @Override
    public Boolean visitLimitOperator(LimitOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.LIMIT) {
            return Boolean.FALSE;
        }
        LimitOperator limitOpArg = (LimitOperator) copyAndSubstituteVar(op, arg);
        if (!Objects.equals(op.getOffset().getValue(), limitOpArg.getOffset().getValue())) {
            return Boolean.FALSE;
        }
        boolean isomorphic = op.getMaxObjects().getValue().equals(limitOpArg.getMaxObjects().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitInnerJoinOperator(InnerJoinOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return Boolean.FALSE;
        }
        InnerJoinOperator joinOpArg = (InnerJoinOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = op.getCondition().getValue().equals(joinOpArg.getCondition().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitLeftOuterJoinOperator(LeftOuterJoinOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return Boolean.FALSE;
        }
        LeftOuterJoinOperator joinOpArg = (LeftOuterJoinOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = op.getCondition().getValue().equals(joinOpArg.getCondition().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitNestedTupleSourceOperator(NestedTupleSourceOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitOrderOperator(OrderOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.ORDER) {
            return Boolean.FALSE;
        }
        OrderOperator orderOpArg = (OrderOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = compareIOrderAndExpressions(op.getOrderExpressions(), orderOpArg.getOrderExpressions());
        return isomorphic;
    }

    @Override
    public Boolean visitAssignOperator(AssignOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return Boolean.FALSE;
        }
        AssignOperator assignOpArg = (AssignOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic =
                VariableUtilities.varListEqualUnordered(getPairList(op.getVariables(), op.getExpressions()),
                        getPairList(assignOpArg.getVariables(), assignOpArg.getExpressions()));
        return isomorphic;
    }

    @Override
    public Boolean visitSelectOperator(SelectOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return Boolean.FALSE;
        }
        SelectOperator selectOpArg = (SelectOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = op.getCondition().getValue().equals(selectOpArg.getCondition().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitProjectOperator(ProjectOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.PROJECT) {
            return Boolean.FALSE;
        }
        ProjectOperator projectOpArg = (ProjectOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), projectOpArg.getVariables());
        return isomorphic;
    }

    @Override
    public Boolean visitReplicateOperator(ReplicateOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.REPLICATE) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitSplitOperator(SplitOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.SPLIT) {
            return Boolean.FALSE;
        }
        SplitOperator sOpArg = (SplitOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = op.getBranchingExpression().getValue().equals(sOpArg.getBranchingExpression().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitMaterializeOperator(MaterializeOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.MATERIALIZE) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitScriptOperator(ScriptOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.SCRIPT) {
            return Boolean.FALSE;
        }
        ScriptOperator scriptOpArg = (ScriptOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = op.getScriptDescription().equals(scriptOpArg.getScriptDescription());
        return isomorphic;
    }

    @Override
    public Boolean visitSubplanOperator(SubplanOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return Boolean.FALSE;
        }
        SubplanOperator subplanOpArg = (SubplanOperator) copyAndSubstituteVar(op, arg);
        List<ILogicalPlan> plans = op.getNestedPlans();
        List<ILogicalPlan> plansArg = subplanOpArg.getNestedPlans();
        for (int i = 0; i < plans.size(); i++) {
            List<Mutable<ILogicalOperator>> roots = plans.get(i).getRoots();
            List<Mutable<ILogicalOperator>> rootsArg = plansArg.get(i).getRoots();
            if (roots.size() == rootsArg.size()) {
                return Boolean.FALSE;
            }
            for (int j = 0; j < roots.size(); j++) {
                ILogicalOperator topOp1 = roots.get(j).getValue();
                ILogicalOperator topOp2 = rootsArg.get(j).getValue();
                boolean isomorphic = IsomorphismUtilities.isOperatorIsomorphicPlanSegment(topOp1, topOp2);
                if (!isomorphic) {
                    return Boolean.FALSE;
                }
            }
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitUnionOperator(UnionAllOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.UNIONALL) {
            return Boolean.FALSE;
        }
        UnionAllOperator unionOpArg = (UnionAllOperator) copyAndSubstituteVar(op, arg);
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> mapping = op.getVariableMappings();
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> mappingArg = unionOpArg.getVariableMappings();
        if (mapping.size() != mappingArg.size()) {
            return Boolean.FALSE;
        }
        return VariableUtilities.varListEqualUnordered(mapping, mappingArg);
    }

    @Override
    public Boolean visitIntersectOperator(IntersectOperator op, ILogicalOperator arg) throws AlgebricksException {
        if (op.getOperatorTag() != LogicalOperatorTag.INTERSECT) {
            return Boolean.FALSE;
        }
        IntersectOperator intersetOpArg = (IntersectOperator) copyAndSubstituteVar(op, arg);
        List<LogicalVariable> outputCompareVars = op.getOutputCompareVariables();
        List<LogicalVariable> outputCompareVarsArg = intersetOpArg.getOutputCompareVariables();
        if (outputCompareVars.size() != outputCompareVarsArg.size()) {
            return Boolean.FALSE;
        }
        if (!VariableUtilities.varListEqualUnordered(outputCompareVars, outputCompareVarsArg)) {
            return Boolean.FALSE;
        }
        boolean hasExtraVars = op.hasExtraVariables();
        List<LogicalVariable> outputExtraVars = op.getOutputExtraVariables();
        List<LogicalVariable> outputExtraVarsArg = intersetOpArg.getOutputExtraVariables();
        if (outputExtraVars.size() != outputExtraVarsArg.size()) {
            return Boolean.FALSE;
        }
        if (!VariableUtilities.varListEqualUnordered(outputExtraVars, outputExtraVarsArg)) {
            return Boolean.FALSE;
        }

        int nInput = op.getNumInput();
        if (nInput != intersetOpArg.getNumInput()) {
            return Boolean.FALSE;
        }
        for (int i = 0; i < nInput; i++) {
            if (!VariableUtilities.varListEqualUnordered(op.getInputCompareVariables(i),
                    intersetOpArg.getInputCompareVariables(i))) {
                return Boolean.FALSE;
            }
            if (hasExtraVars && !VariableUtilities.varListEqualUnordered(op.getInputExtraVariables(i),
                    intersetOpArg.getInputExtraVariables(i))) {
                return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitUnnestOperator(UnnestOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return Boolean.FALSE;
        }
        UnnestOperator unnestOpArg = (UnnestOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), unnestOpArg.getVariables())
                && variableEqual(op.getPositionalVariable(), unnestOpArg.getPositionalVariable());
        if (!isomorphic) {
            return Boolean.FALSE;
        }
        isomorphic = op.getExpressionRef().getValue().equals(unnestOpArg.getExpressionRef().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitUnnestMapOperator(UnnestMapOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.UNNEST_MAP) {
            return Boolean.FALSE;
        }
        UnnestMapOperator unnestOpArg = (UnnestMapOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), unnestOpArg.getVariables());
        if (!isomorphic) {
            return Boolean.FALSE;
        }
        isomorphic = op.getExpressionRef().getValue().equals(unnestOpArg.getExpressionRef().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP) {
            return Boolean.FALSE;
        }
        LeftOuterUnnestMapOperator loUnnestOpArg = (LeftOuterUnnestMapOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), loUnnestOpArg.getVariables());
        if (!isomorphic) {
            return Boolean.FALSE;
        }
        isomorphic = op.getExpressionRef().getValue().equals(loUnnestOpArg.getExpressionRef().getValue());
        return isomorphic;
    }

    @Override
    public Boolean visitDataScanOperator(DataSourceScanOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return Boolean.FALSE;
        }
        DataSourceScanOperator argScan = (DataSourceScanOperator) arg;
        if (!argScan.getDataSource().toString().equals(op.getDataSource().toString())) {
            return Boolean.FALSE;
        }
        DataSourceScanOperator scanOpArg = (DataSourceScanOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), scanOpArg.getVariables())
                && op.getDataSource().toString().equals(scanOpArg.getDataSource().toString());
        return isomorphic;
    }

    @Override
    public Boolean visitDistinctOperator(DistinctOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.DISTINCT) {
            return Boolean.FALSE;
        }
        DistinctOperator distinctOpArg = (DistinctOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = compareExpressions(op.getExpressions(), distinctOpArg.getExpressions());
        return isomorphic;
    }

    @Override
    public Boolean visitExchangeOperator(ExchangeOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.EXCHANGE) {
            return Boolean.FALSE;
        }
        // require the same partition property
        if (op.getPhysicalOperator() == null || aop.getPhysicalOperator() == null
                || op.getPhysicalOperator().getOperatorTag() != aop.getPhysicalOperator().getOperatorTag()) {
            return Boolean.FALSE;
        }
        variableMapping.clear();
        IsomorphismUtilities.mapVariablesTopDown(op, arg, variableMapping);
        IPhysicalPropertiesVector properties = op.getPhysicalOperator().getDeliveredProperties();
        IPhysicalPropertiesVector propertiesArg = aop.getPhysicalOperator().getDeliveredProperties();
        if (properties == null && propertiesArg == null) {
            return Boolean.TRUE;
        }
        if (properties == null || propertiesArg == null) {
            return Boolean.FALSE;
        }
        IPartitioningProperty partProp = properties.getPartitioningProperty();
        IPartitioningProperty partPropArg = propertiesArg.getPartitioningProperty();
        if (!partProp.getPartitioningType().equals(partPropArg.getPartitioningType())) {
            return Boolean.FALSE;
        }
        List<LogicalVariable> columns = new ArrayList<LogicalVariable>();
        partProp.getColumns(columns);
        List<LogicalVariable> columnsArg = new ArrayList<LogicalVariable>();
        partPropArg.getColumns(columnsArg);
        if (columns.size() != columnsArg.size()) {
            return Boolean.FALSE;
        }
        if (columns.size() == 0) {
            return Boolean.TRUE;
        }
        for (int i = 0; i < columnsArg.size(); i++) {
            LogicalVariable rightVar = columnsArg.get(i);
            LogicalVariable leftVar = variableMapping.get(rightVar);
            if (leftVar != null) {
                columnsArg.set(i, leftVar);
            }
        }
        return columns.equals(columnsArg);
    }

    @Override
    public Boolean visitWriteOperator(WriteOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.WRITE) {
            return Boolean.FALSE;
        }
        WriteOperator writeOpArg = (WriteOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), writeOpArg.getSchema());
        return isomorphic;
    }

    @Override
    public Boolean visitDistributeResultOperator(DistributeResultOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT) {
            return Boolean.FALSE;
        }
        DistributeResultOperator writeOpArg = (DistributeResultOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), writeOpArg.getSchema());
        return isomorphic;
    }

    @Override
    public Boolean visitWriteResultOperator(WriteResultOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.WRITE_RESULT) {
            return Boolean.FALSE;
        }
        WriteResultOperator writeOpArg = (WriteResultOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), writeOpArg.getSchema());
        if (!op.getDataSource().equals(writeOpArg.getDataSource())) {
            isomorphic = false;
        }
        if (!op.getPayloadExpression().equals(writeOpArg.getPayloadExpression())) {
            isomorphic = false;
        }
        return isomorphic;
    }

    @Override
    public Boolean visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE_UPSERT) {
            return Boolean.FALSE;
        }
        InsertDeleteUpsertOperator insertOpArg = (InsertDeleteUpsertOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), insertOpArg.getSchema());
        if (!op.getDataSource().equals(insertOpArg.getDataSource())) {
            isomorphic = false;
        }
        if (!op.getPayloadExpression().equals(insertOpArg.getPayloadExpression())) {
            isomorphic = false;
        }
        return isomorphic;
    }

    @Override
    public Boolean visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.INDEX_INSERT_DELETE_UPSERT) {
            return Boolean.FALSE;
        }
        IndexInsertDeleteUpsertOperator insertOpArg = (IndexInsertDeleteUpsertOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), insertOpArg.getSchema());
        if (!op.getDataSourceIndex().equals(insertOpArg.getDataSourceIndex())) {
            isomorphic = false;
        }
        return isomorphic;
    }

    @Override
    public Boolean visitTokenizeOperator(TokenizeOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.TOKENIZE) {
            return Boolean.FALSE;
        }
        TokenizeOperator tokenizeOpArg = (TokenizeOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getSchema(), tokenizeOpArg.getSchema());
        if (!op.getDataSourceIndex().equals(tokenizeOpArg.getDataSourceIndex())) {
            isomorphic = false;
        }
        return isomorphic;
    }

    @Override
    public Boolean visitForwardOperator(ForwardOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator argOperator = (AbstractLogicalOperator) arg;
        if (argOperator.getOperatorTag() != LogicalOperatorTag.FORWARD) {
            return Boolean.FALSE;
        }
        ForwardOperator otherOp = (ForwardOperator) copyAndSubstituteVar(op, arg);
        ILogicalExpression rangeMapExp = op.getSideDataExpression().getValue();
        ILogicalExpression otherRangeMapExp = otherOp.getSideDataExpression().getValue();
        return rangeMapExp.equals(otherRangeMapExp) && op.getSideDataKey().equals(otherOp.getSideDataKey());
    }

    @Override
    public Boolean visitSinkOperator(SinkOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.SINK) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitWindowOperator(WindowOperator op, ILogicalOperator arg) throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.WINDOW) {
            return false;
        }
        WindowOperator windowOpArg = (WindowOperator) copyAndSubstituteVar(op, arg);
        if (!compareWindowPartitionSpec(op, windowOpArg)) {
            return false;
        }
        if (!compareWindowFrameSpec(op, windowOpArg)) {
            return false;
        }
        if (!VariableUtilities.varListEqualUnordered(getPairList(op.getVariables(), op.getExpressions()),
                getPairList(windowOpArg.getVariables(), windowOpArg.getExpressions()))) {
            return false;
        }
        List<ILogicalPlan> plans = op.getNestedPlans();
        List<ILogicalPlan> plansArg = windowOpArg.getNestedPlans();
        boolean isomorphic = compareSubplans(plans, plansArg);
        return isomorphic;
    }

    public static boolean compareWindowPartitionSpec(WindowOperator winOp1, WindowOperator winOp2) {
        return VariableUtilities.varListEqualUnordered(winOp1.getPartitionExpressions(),
                winOp2.getPartitionExpressions())
                && compareIOrderAndExpressions(winOp1.getOrderExpressions(), winOp2.getOrderExpressions());
    }

    public static boolean compareWindowFrameSpec(WindowOperator winOp1, WindowOperator winOp2) {
        return compareWindowFrameSpecExcludingMaxObjects(winOp1, winOp2)
                && winOp1.getFrameMaxObjects() == winOp2.getFrameMaxObjects();
    }

    public static boolean compareWindowFrameSpecExcludingMaxObjects(WindowOperator winOp1, WindowOperator winOp2) {
        return compareIOrderAndExpressions(winOp1.getFrameValueExpressions(), winOp2.getFrameValueExpressions())
                && compareExpressions(winOp1.getFrameStartExpressions(), winOp2.getFrameStartExpressions())
                && compareExpressions(winOp1.getFrameStartValidationExpressions(),
                        winOp2.getFrameStartValidationExpressions())
                && compareExpressions(winOp1.getFrameEndExpressions(), winOp2.getFrameEndExpressions())
                && compareExpressions(winOp1.getFrameEndValidationExpressions(),
                        winOp2.getFrameEndValidationExpressions())
                && compareExpressions(winOp1.getFrameExcludeExpressions(), winOp2.getFrameExcludeExpressions())
                && winOp1.getFrameExcludeNegationStartIdx() == winOp2.getFrameExcludeNegationStartIdx()
                && Objects.equals(winOp1.getFrameExcludeUnaryExpression().getValue(),
                        winOp2.getFrameExcludeUnaryExpression().getValue())
                && Objects.equals(winOp1.getFrameOffsetExpression().getValue(),
                        winOp2.getFrameOffsetExpression().getValue());
        // do not include WindowOperator.getFrameMaxObjects()
    }

    private static boolean compareExpressions(List<Mutable<ILogicalExpression>> opExprs,
            List<Mutable<ILogicalExpression>> argExprs) {
        if (opExprs.size() != argExprs.size()) {
            return false;
        }
        for (int i = 0; i < opExprs.size(); i++) {
            boolean isomorphic = opExprs.get(i).getValue().equals(argExprs.get(i).getValue());
            if (!isomorphic) {
                return false;
            }
        }
        return true;
    }

    private static boolean compareIOrderAndExpressions(List<Pair<IOrder, Mutable<ILogicalExpression>>> opOrderExprs,
            List<Pair<IOrder, Mutable<ILogicalExpression>>> argOrderExprs) {
        if (opOrderExprs.size() != argOrderExprs.size()) {
            return false;
        }
        for (int i = 0; i < opOrderExprs.size(); i++) {
            boolean isomorphic = opOrderExprs.get(i).first.equals(argOrderExprs.get(i).first);
            if (!isomorphic) {
                return false;
            }
            isomorphic = opOrderExprs.get(i).second.getValue().equals(argOrderExprs.get(i).second.getValue());
            if (!isomorphic) {
                return false;
            }
        }
        return true;
    }

    private boolean compareSubplans(List<ILogicalPlan> plans, List<ILogicalPlan> plansArg) throws AlgebricksException {
        int plansSize = plans.size();
        if (plansSize != plansArg.size()) {
            return false;
        }
        for (int i = 0; i < plansSize; i++) {
            if (!IsomorphismUtilities.isOperatorIsomorphicPlan(plans.get(i), plansArg.get(i))) {
                return false;
            }
        }
        return true;
    }

    private ILogicalOperator copyAndSubstituteVar(ILogicalOperator op, ILogicalOperator argOp)
            throws AlgebricksException {
        ILogicalOperator newOp = OperatorManipulationUtil.deepCopy(argOp);
        variableMapping.clear();
        IsomorphismUtilities.mapVariablesTopDown(op, argOp, variableMapping);

        List<LogicalVariable> liveVars = new ArrayList<>();
        for (int i = 0; i < argOp.getInputs().size(); i++) {
            VariableUtilities.getLiveVariables(argOp.getInputs().get(i).getValue(), liveVars);
        }
        List<LogicalVariable> producedVars = new ArrayList<>();
        VariableUtilities.getProducedVariables(argOp, producedVars);
        List<LogicalVariable> producedVarsNew = new ArrayList<>();
        VariableUtilities.getProducedVariables(op, producedVarsNew);

        if (producedVars.size() != producedVarsNew.size()) {
            return newOp;
        }
        for (Entry<LogicalVariable, LogicalVariable> map : variableMapping.entrySet()) {
            if (liveVars.contains(map.getKey())) {
                VariableUtilities.substituteVariables(newOp, map.getKey(), map.getValue(), null);
            }
        }
        for (int i = 0; i < producedVars.size(); i++) {
            VariableUtilities.substituteVariables(newOp, producedVars.get(i), producedVarsNew.get(i), null);
        }
        return newOp;
    }

    public List<Pair<LogicalVariable, ILogicalExpression>> getPairList(List<LogicalVariable> vars,
            List<Mutable<ILogicalExpression>> exprs) throws AlgebricksException {
        List<Pair<LogicalVariable, ILogicalExpression>> list =
                new ArrayList<Pair<LogicalVariable, ILogicalExpression>>();
        if (vars.size() != exprs.size()) {
            throw new AlgebricksException("variable list size does not equal to expression list size ");
        }
        for (int i = 0; i < vars.size(); i++) {
            list.add(new Pair<LogicalVariable, ILogicalExpression>(vars.get(i), exprs.get(i).getValue()));
        }
        return list;
    }

    private static boolean variableEqual(LogicalVariable var, LogicalVariable varArg) {
        return Objects.equals(var, varArg);
    }

    @Override
    public Boolean visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) arg;
        if (aop.getOperatorTag() != LogicalOperatorTag.LEFT_OUTER_UNNEST) {
            return Boolean.FALSE;
        }
        LeftOuterUnnestOperator unnestOpArg = (LeftOuterUnnestOperator) copyAndSubstituteVar(op, arg);
        boolean isomorphic = VariableUtilities.varListEqualUnordered(op.getVariables(), unnestOpArg.getVariables())
                && variableEqual(op.getPositionalVariable(), unnestOpArg.getPositionalVariable());
        if (!isomorphic) {
            return Boolean.FALSE;
        }
        isomorphic = op.getExpressionRef().getValue().equals(unnestOpArg.getExpressionRef().getValue());
        return isomorphic;
    }

}
