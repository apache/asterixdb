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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractAssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
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

public class IsomorphismVariableMappingVisitor implements ILogicalOperatorVisitor<Void, ILogicalOperator> {

    private final Map<ILogicalOperator, Set<ILogicalOperator>> alreadyMapped = new HashMap<>();
    private final Map<LogicalVariable, LogicalVariable> variableMapping;

    IsomorphismVariableMappingVisitor(Map<LogicalVariable, LogicalVariable> variableMapping) {
        this.variableMapping = variableMapping;
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapChildren(op, arg);
        mapVariablesForAbstractAssign(op, arg);
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        mapChildren(op, arg);
        mapVariablesForAbstractAssign(op, arg);
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapChildren(op, arg);
        mapVariablesForWindow(op, arg);
        mapVariablesInNestedPlans(op, arg);
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapChildren(op, arg);
        mapVariablesForGroupBy(op, arg);
        mapVariablesInNestedPlans(op, arg);
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        if (op.getOperatorTag() != arg.getOperatorTag()) {
            return null;
        }
        Set<ILogicalOperator> mappedOps = alreadyMapped.get(op);
        if (mappedOps != null && mappedOps.contains(arg)) {
            return null;
        }
        if (mappedOps == null) {
            mappedOps = new HashSet<>();
            alreadyMapped.put(op, mappedOps);
        }
        mappedOps.add(arg);
        ILogicalOperator inputToCreator1 = op.getSourceOperator();
        NestedTupleSourceOperator nts = (NestedTupleSourceOperator) arg;
        ILogicalOperator inputToCreator2 = nts.getSourceOperator();
        inputToCreator1.accept(this, inputToCreator2);
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapChildren(op, arg);
        mapVariablesForAbstractAssign(op, arg);
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapChildren(op, arg);
        mapVariablesInNestedPlans(op, arg);
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapChildren(op, arg);
        mapVariablesForUnion(op, arg);
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapChildren(op, arg);
        mapVariablesForIntersect(op, arg);
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, ILogicalOperator arg) throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        mapVariablesStandard(op, arg);
        return null;
    }

    private void mapChildren(ILogicalOperator op, ILogicalOperator opArg) throws AlgebricksException {
        if (op.getOperatorTag() != opArg.getOperatorTag()) {
            return;
        }
        List<Mutable<ILogicalOperator>> inputs = op.getInputs();
        List<Mutable<ILogicalOperator>> inputsArg = opArg.getInputs();
        if (inputs.size() != inputsArg.size()) {
            return;
        }
        for (int i = 0; i < inputs.size(); i++) {
            ILogicalOperator input = inputs.get(i).getValue();
            ILogicalOperator inputArg = inputsArg.get(i).getValue();
            input.accept(this, inputArg);
        }
    }

    private void mapVariables(ILogicalOperator left, ILogicalOperator right) throws AlgebricksException {
        if (left.getOperatorTag() != right.getOperatorTag()) {
            return;
        }
        List<LogicalVariable> producedVarLeft = new ArrayList<>();
        List<LogicalVariable> producedVarRight = new ArrayList<>();
        VariableUtilities.getProducedVariables(left, producedVarLeft);
        VariableUtilities.getProducedVariables(right, producedVarRight);
        mapVariables(producedVarLeft, producedVarRight);
    }

    private void mapVariables(List<LogicalVariable> variablesLeft, List<LogicalVariable> variablesRight) {
        if (variablesLeft.size() != variablesRight.size()) {
            return;
        }
        int size = variablesLeft.size();
        for (int i = 0; i < size; i++) {
            LogicalVariable left = variablesLeft.get(i);
            LogicalVariable right = variablesRight.get(i);
            variableMapping.put(right, left);
        }
    }

    private void mapVariablesForAbstractAssign(ILogicalOperator left, ILogicalOperator right)
            throws AlgebricksException {
        if (left.getOperatorTag() != right.getOperatorTag()) {
            return;
        }
        AbstractAssignOperator leftOp = (AbstractAssignOperator) left;
        AbstractAssignOperator rightOp = (AbstractAssignOperator) right;
        List<LogicalVariable> producedVarLeft = new ArrayList<>();
        List<LogicalVariable> producedVarRight = new ArrayList<>();
        VariableUtilities.getProducedVariables(left, producedVarLeft);
        VariableUtilities.getProducedVariables(right, producedVarRight);
        mapVariablesForAbstractAssign(producedVarLeft, leftOp.getExpressions(), producedVarRight,
                rightOp.getExpressions());
    }

    private void mapVariablesForGroupBy(ILogicalOperator left, ILogicalOperator right) {
        if (left.getOperatorTag() != right.getOperatorTag()) {
            return;
        }
        GroupByOperator leftOp = (GroupByOperator) left;
        GroupByOperator rightOp = (GroupByOperator) right;
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> leftPairs = leftOp.getGroupByList();
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> rightPairs = rightOp.getGroupByList();
        mapVarExprPairList(leftPairs, rightPairs);
        leftPairs = leftOp.getDecorList();
        rightPairs = rightOp.getDecorList();
        mapVarExprPairList(leftPairs, rightPairs);
    }

    private void mapVariablesForWindow(ILogicalOperator left, ILogicalOperator right) {
        if (left.getOperatorTag() != right.getOperatorTag()) {
            return;
        }
        WindowOperator leftOp = (WindowOperator) left;
        WindowOperator rightOp = (WindowOperator) right;
        mapVariablesForAbstractAssign(leftOp.getVariables(), leftOp.getExpressions(), rightOp.getVariables(),
                rightOp.getExpressions());
    }

    private void mapVarExprPairList(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> leftPairs,
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> rightPairs) {
        if (leftPairs.size() != rightPairs.size()) {
            return;
        }
        for (int i = 0; i < leftPairs.size(); i++) {
            ILogicalExpression exprLeft = leftPairs.get(i).second.getValue();
            LogicalVariable leftVar = leftPairs.get(i).first;
            for (int j = 0; j < leftPairs.size(); j++) {
                ILogicalExpression exprRight = copyExpressionAndSubtituteVars(rightPairs.get(j).second).getValue();
                if (exprLeft.equals(exprRight)) {
                    LogicalVariable rightVar = rightPairs.get(j).first;
                    if (rightVar != null && leftVar != null) {
                        variableMapping.put(rightVar, leftVar);
                    }
                    break;
                }
            }
        }
    }

    private void mapVariablesForAbstractAssign(List<LogicalVariable> variablesLeft,
            List<Mutable<ILogicalExpression>> exprsLeft, List<LogicalVariable> variablesRight,
            List<Mutable<ILogicalExpression>> exprsRight) {
        if (variablesLeft.size() != variablesRight.size()) {
            return;
        }
        int size = variablesLeft.size();
        // Keeps track of already matched right side variables.
        Set<LogicalVariable> matchedRightVars = new HashSet<>();
        for (int i = 0; i < size; i++) {
            ILogicalExpression exprLeft = exprsLeft.get(i).getValue();
            LogicalVariable left = variablesLeft.get(i);
            for (int j = 0; j < size; j++) {
                ILogicalExpression exprRight = copyExpressionAndSubtituteVars(exprsRight.get(j)).getValue();
                LogicalVariable right = variablesRight.get(j);
                if (exprLeft.equals(exprRight) && !matchedRightVars.contains(right)) {
                    variableMapping.put(right, left);
                    matchedRightVars.add(right); // The added variable will not be considered in next rounds.
                    break;
                }
            }
        }
    }

    private void mapVariablesInNestedPlans(AbstractOperatorWithNestedPlans op, ILogicalOperator arg)
            throws AlgebricksException {
        if (op.getOperatorTag() != arg.getOperatorTag()) {
            return;
        }
        AbstractOperatorWithNestedPlans argOp = (AbstractOperatorWithNestedPlans) arg;
        List<ILogicalPlan> plans = op.getNestedPlans();
        List<ILogicalPlan> plansArg = argOp.getNestedPlans();
        if (plans.size() != plansArg.size()) {
            return;
        }
        for (int i = 0; i < plans.size(); i++) {
            List<Mutable<ILogicalOperator>> roots = plans.get(i).getRoots();
            List<Mutable<ILogicalOperator>> rootsArg = plansArg.get(i).getRoots();
            if (roots.size() != rootsArg.size()) {
                return;
            }
            for (int j = 0; j < roots.size(); j++) {
                ILogicalOperator topOp1 = roots.get(j).getValue();
                ILogicalOperator topOp2 = rootsArg.get(j).getValue();
                topOp1.accept(this, topOp2);
            }
        }
    }

    private void mapVariablesStandard(ILogicalOperator op, ILogicalOperator arg) throws AlgebricksException {
        if (op.getOperatorTag() != arg.getOperatorTag()) {
            return;
        }
        mapChildren(op, arg);
        mapVariables(op, arg);
    }

    private Mutable<ILogicalExpression> copyExpressionAndSubtituteVars(Mutable<ILogicalExpression> expr) {
        ILogicalExpression copy = expr.getValue().cloneExpression();
        variableMapping.forEach(copy::substituteVar);
        return new MutableObject<>(copy);
    }

    private void mapVariablesForUnion(ILogicalOperator op, ILogicalOperator arg) {
        if (op.getOperatorTag() != arg.getOperatorTag()) {
            return;
        }
        UnionAllOperator union = (UnionAllOperator) op;
        UnionAllOperator unionArg = (UnionAllOperator) arg;
        mapVarTripleList(union.getVariableMappings(), unionArg.getVariableMappings());
    }

    private void mapVarTripleList(List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> leftTriples,
            List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> rightTriples) {
        if (leftTriples.size() != rightTriples.size()) {
            return;
        }
        for (int i = 0; i < leftTriples.size(); i++) {
            LogicalVariable leftFirstVar = leftTriples.get(i).first;
            LogicalVariable leftSecondVar = leftTriples.get(i).second;
            LogicalVariable leftThirdVar = leftTriples.get(i).third;
            for (int j = 0; j < rightTriples.size(); j++) {
                LogicalVariable rightFirstVar = rightTriples.get(j).first;
                LogicalVariable rightSecondVar = rightTriples.get(j).second;
                LogicalVariable rightThirdVar = rightTriples.get(j).third;
                if (varEquivalent(leftFirstVar, rightFirstVar) && varEquivalent(leftSecondVar, rightSecondVar)) {
                    variableMapping.put(rightThirdVar, leftThirdVar);
                    break;
                }
            }
        }
    }

    private void mapVariablesForIntersect(IntersectOperator op, ILogicalOperator arg) {
        if (op.getOperatorTag() != arg.getOperatorTag()) {
            return;
        }
        IntersectOperator opArg = (IntersectOperator) arg;
        int nInput = op.getNumInput();
        if (nInput != opArg.getNumInput()) {
            return;
        }
        boolean hasExtraVars = op.hasExtraVariables();
        if (hasExtraVars && !opArg.hasExtraVariables()) {
            return;
        }
        for (int i = 0; i < nInput; i++) {
            List<LogicalVariable> inputCompareVars = op.getInputCompareVariables(i);
            List<LogicalVariable> inputCompareVarsArg = opArg.getInputCompareVariables(i);
            for (int j = 0, n = inputCompareVars.size(); j < n; j++) {
                if (!varEquivalent(inputCompareVars.get(j), inputCompareVarsArg.get(j))) {
                    return;
                }
            }
            if (hasExtraVars) {
                List<LogicalVariable> inputExtraVars = op.getInputExtraVariables(i);
                List<LogicalVariable> inputExtraVarsArg = opArg.getInputExtraVariables(i);
                for (int j = 0, n = inputExtraVars.size(); j < n; j++) {
                    if (!varEquivalent(inputExtraVars.get(j), inputExtraVarsArg.get(j))) {
                        return;
                    }
                }
            }
        }

        mapVariables(op.getOutputCompareVariables(), opArg.getOutputCompareVariables());
        if (hasExtraVars) {
            mapVariables(op.getOutputExtraVariables(), opArg.getOutputExtraVariables());
        }
    }

    private boolean varEquivalent(LogicalVariable left, LogicalVariable right) {
        if (variableMapping.get(right) == null) {
            return false;
        }
        return variableMapping.get(right).equals(left);
    }
}
