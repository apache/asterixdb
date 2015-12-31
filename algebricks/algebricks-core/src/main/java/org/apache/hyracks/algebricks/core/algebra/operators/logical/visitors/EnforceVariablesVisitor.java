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

import java.util.Collection;
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
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExtensionOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.PartitioningSplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.IQueryOperatorVisitor;

/**
 * This visitor is to add back variables that are killed in the query plan rooted at an input operator.
 * After visiting, it also provides a variable map for variables that have been
 * mapped in the query plan, e.g., by group-by, assign, and union.
 */
class EnforceVariablesVisitor implements IQueryOperatorVisitor<ILogicalOperator, Collection<LogicalVariable>> {
    private final IOptimizationContext context;
    private final Map<LogicalVariable, LogicalVariable> inputVarToOutputVarMap = new HashMap<>();

    public EnforceVariablesVisitor(IOptimizationContext context) {
        this.context = context;
    }

    public Map<LogicalVariable, LogicalVariable> getInputVariableToOutputVariableMap() {
        return inputVarToOutputVarMap;
    }

    @Override
    public ILogicalOperator visitAggregateOperator(AggregateOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return rewriteAggregateOperator(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitRunningAggregateOperator(RunningAggregateOperator op,
            Collection<LogicalVariable> varsToRecover) throws AlgebricksException {
        return rewriteAggregateOperator(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op,
            Collection<LogicalVariable> varsToRecover) throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitGroupByOperator(GroupByOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        Set<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(op, liveVars);
        varsToRecover.removeAll(liveVars);

        // Maps group by key variables if the corresponding expressions are VariableReferenceExpressions.
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> keyVarExprRef : op.getGroupByList()) {
            ILogicalExpression expr = keyVarExprRef.second.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varExpr = (VariableReferenceExpression) expr;
                LogicalVariable sourceVar = varExpr.getVariableReference();
                updateVarMapping(sourceVar, keyVarExprRef.first);
                varsToRecover.remove(sourceVar);
            }
        }

        for (LogicalVariable varToRecover : varsToRecover) {
            // This limits the visitor can only be applied to a nested logical plan inside a Subplan operator,
            // where the varsToRecover forms a candidate key which can uniquely identify a tuple out of the nested-tuple-source.
            op.getDecorList().add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(null,
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(varToRecover))));
        }
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitLimitOperator(LimitOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitInnerJoinOperator(InnerJoinOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitLeftOuterJoinOperator(LeftOuterJoinOperator op,
            Collection<LogicalVariable> varsToRecover) throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitNestedTupleSourceOperator(NestedTupleSourceOperator op,
            Collection<LogicalVariable> varsToRecover) throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitOrderOperator(OrderOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitAssignOperator(AssignOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> assignedExprRefs = op.getExpressions();
        List<LogicalVariable> assignedVars = op.getVariables();

        // Maps assigning variables if assignment expressions are VariableReferenceExpressions.
        for (int index = 0; index < assignedVars.size(); ++index) {
            ILogicalExpression expr = assignedExprRefs.get(index).getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varExpr = (VariableReferenceExpression) expr;
                LogicalVariable sourceVar = varExpr.getVariableReference();
                updateVarMapping(sourceVar, assignedVars.get(index));
                varsToRecover.remove(sourceVar);
            }
        }
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitSelectOperator(SelectOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitExtensionOperator(ExtensionOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitProjectOperator(ProjectOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        varsToRecover.removeAll(op.getVariables());
        // Adds all missing variables that should propagates up.
        op.getVariables().addAll(varsToRecover);
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitPartitioningSplitOperator(PartitioningSplitOperator op,
            Collection<LogicalVariable> varsToRecover) throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitReplicateOperator(ReplicateOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitMaterializeOperator(MaterializeOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitScriptOperator(ScriptOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        throw new UnsupportedOperationException("Script operators in a subplan are not supported!");
    }

    @Override
    public ILogicalOperator visitSubplanOperator(SubplanOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitUnionOperator(UnionAllOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        // Update the variable mappings
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varTriples = op.getVariableMappings();
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> triple : varTriples) {
            updateVarMapping(triple.second, triple.first);
            updateVarMapping(triple.third, triple.first);
        }
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitUnnestOperator(UnnestOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitOuterUnnestOperator(OuterUnnestOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitUnnestMapOperator(UnnestMapOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        Set<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(op, liveVars);
        varsToRecover.remove(liveVars);
        if (!varsToRecover.isEmpty()) {
            op.setPropagatesInput(true);
            return visitsInputs(op, varsToRecover);
        }
        return op;
    }

    @Override
    public ILogicalOperator visitDataScanOperator(DataSourceScanOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitDistinctOperator(DistinctOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitExchangeOperator(ExchangeOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    @Override
    public ILogicalOperator visitExternalDataLookupOperator(ExternalDataLookupOperator op,
            Collection<LogicalVariable> varsToRecover) throws AlgebricksException {
        Set<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(op, liveVars);
        varsToRecover.retainAll(liveVars);
        if (!varsToRecover.isEmpty()) {
            op.setPropagateInput(true);
            return visitsInputs(op, varsToRecover);
        }
        return op;
    }

    @Override
    public ILogicalOperator visitTokenizeOperator(TokenizeOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        return visitsInputs(op, varsToRecover);
    }

    /**
     * Wraps an AggregateOperator or RunningAggregateOperator with a group-by operator where
     * the group-by keys are variables in varsToRecover.
     * Note that the function here prevents this visitor being used to rewrite arbitrary query plans.
     * Instead, it could only be used for rewriting a nested plan within a subplan operator.
     *
     * @param op
     *            the logical operator for aggregate or running aggregate.
     * @param varsToRecover
     *            the set of variables that needs to preserve.
     * @return the wrapped group-by operator if {@code varsToRecover} is not empty, and {@code op} otherwise.
     * @throws AlgebricksException
     */
    private ILogicalOperator rewriteAggregateOperator(ILogicalOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        Set<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(op, liveVars);
        varsToRecover.removeAll(liveVars);

        GroupByOperator gbyOp = new GroupByOperator();
        for (LogicalVariable varToRecover : varsToRecover) {
            // This limits the visitor can only be applied to a nested logical plan inside a Subplan operator,
            // where the varsToRecover forms a candidate key which can uniquely identify a tuple out of the nested-tuple-source.
            LogicalVariable newVar = context.newVar();
            gbyOp.getGroupByList().add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(newVar,
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(varToRecover))));
            updateVarMapping(varToRecover, newVar);
        }

        NestedTupleSourceOperator nts = new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(gbyOp));
        op.getInputs().clear();
        op.getInputs().add(new MutableObject<ILogicalOperator>(nts));

        ILogicalOperator inputOp = op.getInputs().get(0).getValue();
        ILogicalPlan nestedPlan = new ALogicalPlanImpl();
        nestedPlan.getRoots().add(new MutableObject<ILogicalOperator>(op));
        gbyOp.getNestedPlans().add(nestedPlan);
        gbyOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));

        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(op, context);
        return visitsInputs(gbyOp, varsToRecover);
    }

    private ILogicalOperator visitsInputs(ILogicalOperator op, Collection<LogicalVariable> varsToRecover)
            throws AlgebricksException {
        if (op.getInputs().size() == 0 || varsToRecover.isEmpty()) {
            return op;
        }
        Set<LogicalVariable> producedVars = new HashSet<>();
        VariableUtilities.getProducedVariables(op, producedVars);
        varsToRecover.removeAll(producedVars);
        if (!varsToRecover.isEmpty()) {
            if (op.getInputs().size() == 1) {
                // Deals with single input operators.
                ILogicalOperator newOp = op.getInputs().get(0).getValue().accept(this, varsToRecover);
                op.getInputs().get(0).setValue(newOp);
            } else {
                // Deals with multi-input operators.
                for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
                    ILogicalOperator child = childRef.getValue();
                    Set<LogicalVariable> varsToRecoverInChild = new HashSet<>();
                    VariableUtilities.getProducedVariablesInDescendantsAndSelf(child, varsToRecoverInChild);
                    // Obtains the variables that this particular child should propagate.
                    varsToRecoverInChild.retainAll(varsToRecover);
                    ILogicalOperator newChild = child.accept(this, varsToRecoverInChild);
                    childRef.setValue(newChild);
                }
            }
        }
        return op;
    }

    private void updateVarMapping(LogicalVariable oldVar, LogicalVariable newVar) {
        if (oldVar.equals(newVar)) {
            return;
        }
        LogicalVariable mappedVar = newVar;
        if (inputVarToOutputVarMap.containsKey(newVar)) {
            mappedVar = inputVarToOutputVarMap.get(newVar);
            inputVarToOutputVarMap.remove(newVar);
        }
        inputVarToOutputVarMap.put(oldVar, mappedVar);
    }

}
