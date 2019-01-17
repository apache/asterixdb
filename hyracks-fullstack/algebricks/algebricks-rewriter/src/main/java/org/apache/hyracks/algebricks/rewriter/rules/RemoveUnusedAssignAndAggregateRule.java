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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Removes unused variables from Assign, Unnest, Aggregate, UnionAll, and Group-by operators.
 */
public class RemoveUnusedAssignAndAggregateRule implements IAlgebraicRewriteRule {

    // Keep the variables that are produced by ASSIGN, UNNEST, AGGREGATE, UNION, WINDOW,
    // and GROUP operators.
    private Map<Mutable<ILogicalOperator>, Set<LogicalVariable>> assignedVarMap = new LinkedHashMap<>();
    private Set<LogicalVariable> assignedVarSet = new HashSet<>();

    // Keep the variables that are used after ASSIGN, UNNEST, AGGREGATE, UNION, WINDOW,
    // and GROUP operators.
    private Map<Mutable<ILogicalOperator>, Set<LogicalVariable>> accumulatedUsedVarFromRootMap = new LinkedHashMap<>();

    private boolean isTransformed = false;

    // Keep the variable-mapping of a UNION operator.
    // This is required to keep the variables of the left or right branch of the UNION operator
    // if the output variable of the UNION operator is survived.
    private Set<LogicalVariable> survivedUnionSourceVarSet = new HashSet<>();

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }

        clear();
        Set<LogicalVariable> accumulatedUsedVarFromRootSet = new HashSet<>();
        collectUnusedAssignedVars(opRef, accumulatedUsedVarFromRootSet, true, context);

        // If there are ASSIGN, UNNEST, AGGREGATE, UNION, and GROUP operators in the plan,
        // we try to remove these operators if the produced variables from these
        // operators are not used.
        if (!assignedVarMap.isEmpty()) {
            removeUnusedAssigns(opRef, context);
        }

        return isTransformed;
    }

    /**
     * Collect the information from the given operator and removes assigned
     * variables if they are used afterwards.
     */
    private Set<LogicalVariable> removeAssignVarFromConsideration(Mutable<ILogicalOperator> opRef) {
        Set<LogicalVariable> assignVarsSetForThisOp = null;
        Set<LogicalVariable> usedVarsSetForThisOp = null;

        if (accumulatedUsedVarFromRootMap.containsKey(opRef)) {
            usedVarsSetForThisOp = accumulatedUsedVarFromRootMap.get(opRef);
        }

        if (assignedVarMap.containsKey(opRef)) {
            assignVarsSetForThisOp = assignedVarMap.get(opRef);
        }

        if (assignVarsSetForThisOp != null && !assignVarsSetForThisOp.isEmpty()) {
            Iterator<LogicalVariable> varIter = assignVarsSetForThisOp.iterator();
            while (varIter.hasNext()) {
                LogicalVariable v = varIter.next();
                if ((usedVarsSetForThisOp != null && usedVarsSetForThisOp.contains(v))
                        || survivedUnionSourceVarSet.contains(v)) {
                    varIter.remove();
                }
            }
        }

        // The source variables of the UNIONALL operator should be survived
        // since we are sure that the output of UNIONALL operator is used
        // afterwards.
        if (opRef.getValue().getOperatorTag() == LogicalOperatorTag.UNIONALL) {
            Iterator<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> iter =
                    ((UnionAllOperator) opRef.getValue()).getVariableMappings().iterator();
            while (iter.hasNext()) {
                Triple<LogicalVariable, LogicalVariable, LogicalVariable> varMapping = iter.next();
                survivedUnionSourceVarSet.add(varMapping.first);
                survivedUnionSourceVarSet.add(varMapping.second);
            }
        }

        return assignVarsSetForThisOp;
    }

    private void removeUnusedAssigns(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        Set<LogicalVariable> assignVarsSetForThisOp = removeAssignVarFromConsideration(opRef);

        while (removeFromAssigns(op, assignVarsSetForThisOp, context) == 0) {
            // UnionAllOperator cannot be removed since it has two branches.
            if (op.getOperatorTag() == LogicalOperatorTag.AGGREGATE
                    || op.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                break;
            }
            op = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
            opRef.setValue(op);
            assignVarsSetForThisOp = removeAssignVarFromConsideration(opRef);
            isTransformed = true;
        }

        Iterator<Mutable<ILogicalOperator>> childIter = op.getInputs().iterator();
        while (childIter.hasNext()) {
            Mutable<ILogicalOperator> cRef = childIter.next();
            removeUnusedAssigns(cRef, context);
        }

        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opWithNest = (AbstractOperatorWithNestedPlans) op;
            Iterator<ILogicalPlan> planIter = opWithNest.getNestedPlans().iterator();
            while (planIter.hasNext()) {
                ILogicalPlan p = planIter.next();
                for (Mutable<ILogicalOperator> r : p.getRoots()) {
                    removeUnusedAssigns(r, context);
                }
            }

            // Removes redundant nested plans that produces nothing
            for (int i = opWithNest.getNestedPlans().size() - 1; i >= 0; i--) {
                ILogicalPlan nestedPlan = opWithNest.getNestedPlans().get(i);
                List<Mutable<ILogicalOperator>> rootsToBeRemoved = new ArrayList<Mutable<ILogicalOperator>>();
                for (Mutable<ILogicalOperator> r : nestedPlan.getRoots()) {
                    ILogicalOperator topOp = r.getValue();
                    Set<LogicalVariable> producedVars = new ListSet<LogicalVariable>();
                    VariableUtilities.getProducedVariablesInDescendantsAndSelf(topOp, producedVars);
                    if (producedVars.size() == 0) {
                        rootsToBeRemoved.add(r);
                    }
                }
                // Makes sure the operator should have at least ONE nested plan even it is empty
                // (because a lot of places uses this assumption,  TODO(yingyib): clean them up).
                if (nestedPlan.getRoots().size() == rootsToBeRemoved.size() && opWithNest.getNestedPlans().size() > 1) {
                    nestedPlan.getRoots().removeAll(rootsToBeRemoved);
                    opWithNest.getNestedPlans().remove(nestedPlan);
                }
            }
        }
    }

    private int removeFromAssigns(AbstractLogicalOperator op, Set<LogicalVariable> toRemove,
            IOptimizationContext context) throws AlgebricksException {
        switch (op.getOperatorTag()) {
            case ASSIGN:
                AssignOperator assign = (AssignOperator) op;
                if (removeUnusedVarsAndExprs(toRemove, assign.getVariables(), assign.getExpressions())) {
                    context.computeAndSetTypeEnvironmentForOperator(assign);
                    isTransformed = true;
                }
                return assign.getVariables().size();
            case AGGREGATE:
                AggregateOperator agg = (AggregateOperator) op;
                if (removeUnusedVarsAndExprs(toRemove, agg.getVariables(), agg.getExpressions())) {
                    context.computeAndSetTypeEnvironmentForOperator(agg);
                    isTransformed = true;
                }
                return agg.getVariables().size();
            case UNNEST:
                UnnestOperator uOp = (UnnestOperator) op;
                LogicalVariable pVar = uOp.getPositionalVariable();
                if (pVar != null && toRemove != null && toRemove.contains(pVar)) {
                    uOp.setPositionalVariable(null);
                    assignedVarSet.remove(pVar);
                    isTransformed = true;
                }
                break;
            case UNIONALL:
                UnionAllOperator unionOp = (UnionAllOperator) op;
                if (removeUnusedVarsFromUnionAll(unionOp, toRemove)) {
                    context.computeAndSetTypeEnvironmentForOperator(unionOp);
                    isTransformed = true;
                }
                return unionOp.getVariableMappings().size();
            case GROUP:
                GroupByOperator groupByOp = (GroupByOperator) op;
                if (removeUnusedVarsFromGroupBy(groupByOp, toRemove)) {
                    context.computeAndSetTypeEnvironmentForOperator(groupByOp);
                    isTransformed = true;
                }
                return groupByOp.getGroupByList().size() + groupByOp.getNestedPlans().size()
                        + groupByOp.getDecorList().size();
            case WINDOW:
                WindowOperator winOp = (WindowOperator) op;
                if (removeUnusedVarsAndExprs(toRemove, winOp.getVariables(), winOp.getExpressions())) {
                    context.computeAndSetTypeEnvironmentForOperator(winOp);
                    isTransformed = true;
                }
                return winOp.getVariables().size() + winOp.getNestedPlans().size();
            default:
                break;
        }
        return -1;
    }

    private boolean removeUnusedVarsFromUnionAll(UnionAllOperator unionOp, Set<LogicalVariable> toRemove) {
        Iterator<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> iter =
                unionOp.getVariableMappings().iterator();
        boolean modified = false;
        if (toRemove != null && !toRemove.isEmpty()) {
            while (iter.hasNext()) {
                Triple<LogicalVariable, LogicalVariable, LogicalVariable> varMapping = iter.next();
                if (toRemove.contains(varMapping.third)) {
                    iter.remove();
                    assignedVarSet.remove(varMapping.third);
                    modified = true;
                } else {
                    // In case when the output variable of Union is survived,
                    // the source variables should not be removed.
                    survivedUnionSourceVarSet.add(varMapping.first);
                    survivedUnionSourceVarSet.add(varMapping.second);
                }
            }
        }
        return modified;
    }

    private boolean removeUnusedVarsFromGroupBy(GroupByOperator groupByOp, Set<LogicalVariable> toRemove) {
        if (toRemove == null || toRemove.isEmpty()) {
            return false;
        }
        Iterator<Pair<LogicalVariable, Mutable<ILogicalExpression>>> iter = groupByOp.getDecorList().iterator();
        boolean modified = false;
        while (iter.hasNext()) {
            Pair<LogicalVariable, Mutable<ILogicalExpression>> varMapping = iter.next();
            LogicalVariable decorVar = varMapping.first;
            // A decor var mapping can have a variable reference expression without a new variable definition,
            // which is for rebinding the referred variable.
            VariableReferenceExpression varExpr = (VariableReferenceExpression) varMapping.second.getValue();
            LogicalVariable decorReferredVar = varExpr.getVariableReference();
            boolean removeReBoundDecorVar = toRemove.contains(decorReferredVar);
            if ((decorVar != null && toRemove.contains(decorVar)) || removeReBoundDecorVar) {
                iter.remove();
                modified = true;
                if (removeReBoundDecorVar) {
                    // Do not need to remove that in the children pipeline.
                    toRemove.remove(decorReferredVar);
                }
            }
        }
        return modified;
    }

    private boolean removeUnusedVarsAndExprs(Set<LogicalVariable> toRemove, List<LogicalVariable> varList,
            List<Mutable<ILogicalExpression>> exprList) {
        boolean changed = false;
        if (toRemove != null && !toRemove.isEmpty()) {
            Iterator<LogicalVariable> varIter = varList.iterator();
            Iterator<Mutable<ILogicalExpression>> exprIter = exprList.iterator();
            while (varIter.hasNext()) {
                LogicalVariable v = varIter.next();
                exprIter.next();
                if (toRemove.contains(v)) {
                    varIter.remove();
                    exprIter.remove();
                    assignedVarSet.remove(v);
                    changed = true;
                }
            }
        }
        return changed;
    }

    private void collectUnusedAssignedVars(Mutable<ILogicalOperator> opRef,
            Set<LogicalVariable> accumulatedUsedVarFromRootSet, boolean first, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (!first) {
            context.addToDontApplySet(this, op);
        }
        Set<LogicalVariable> assignVarsSetInThisOp = new HashSet<>();
        Set<LogicalVariable> usedVarsSetInThisOp = new HashSet<>();

        // Add used variables in this operator to the accumulated used variables set?
        boolean addUsedVarsInThisOp = true;
        // ASSIGN, AGGREGATE, UNNEST, UNIONALL, or GROUP operator found?
        boolean targetOpFound = false;

        switch (op.getOperatorTag()) {
            case ASSIGN:
                AssignOperator assign = (AssignOperator) op;
                assignVarsSetInThisOp.addAll(assign.getVariables());
                targetOpFound = true;
                break;
            case AGGREGATE:
                AggregateOperator agg = (AggregateOperator) op;
                assignVarsSetInThisOp.addAll(agg.getVariables());
                targetOpFound = true;
                break;
            case UNNEST:
                UnnestOperator uOp = (UnnestOperator) op;
                LogicalVariable pVar = uOp.getPositionalVariable();
                if (pVar != null) {
                    assignVarsSetInThisOp.add(pVar);
                    targetOpFound = true;
                }
                break;
            case UNIONALL:
                UnionAllOperator unionOp = (UnionAllOperator) op;
                for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> varMapping : unionOp
                        .getVariableMappings()) {
                    assignVarsSetInThisOp.add(varMapping.third);
                }
                targetOpFound = true;
                // Don't add used variables in UNIONALL.
                addUsedVarsInThisOp = false;
                break;
            case GROUP:
                GroupByOperator groupByOp = (GroupByOperator) op;
                for (Pair<LogicalVariable, Mutable<ILogicalExpression>> decorMapping : groupByOp.getDecorList()) {
                    LogicalVariable decorVar = decorMapping.first;
                    if (decorVar != null) {
                        assignVarsSetInThisOp.add(decorVar);
                        targetOpFound = true;
                    } else {
                        // A decor var mapping can have a variable reference expression without a new variable
                        // definition, which is for rebinding the referred variable.
                        VariableReferenceExpression varExpr =
                                (VariableReferenceExpression) decorMapping.second.getValue();
                        LogicalVariable reboundDecorVar = varExpr.getVariableReference();
                        assignVarsSetInThisOp.add(reboundDecorVar);
                    }
                }
                break;
            case WINDOW:
                WindowOperator winOp = (WindowOperator) op;
                assignVarsSetInThisOp.addAll(winOp.getVariables());
                targetOpFound = true;
                break;
        }

        if (targetOpFound) {
            assignedVarMap.put(opRef, assignVarsSetInThisOp);
            assignedVarSet.addAll(assignVarsSetInThisOp);
        }

        if (addUsedVarsInThisOp) {
            VariableUtilities.getUsedVariables(op, usedVarsSetInThisOp);
            accumulatedUsedVarFromRootSet.addAll(usedVarsSetInThisOp);
            // We may have visited this operator before if there are multiple
            // paths in the plan.
            if (accumulatedUsedVarFromRootMap.containsKey(opRef)) {
                accumulatedUsedVarFromRootMap.get(opRef).addAll(accumulatedUsedVarFromRootSet);
            } else {
                accumulatedUsedVarFromRootMap.put(opRef, new HashSet<LogicalVariable>(accumulatedUsedVarFromRootSet));
            }
        } else {
            accumulatedUsedVarFromRootMap.put(opRef, new HashSet<LogicalVariable>(accumulatedUsedVarFromRootSet));
        }

        for (Mutable<ILogicalOperator> c : op.getInputs()) {
            collectUnusedAssignedVars(c, new HashSet<LogicalVariable>(accumulatedUsedVarFromRootSet), false, context);
        }

        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opWithNested = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan plan : opWithNested.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r : plan.getRoots()) {
                    collectUnusedAssignedVars(r, new HashSet<LogicalVariable>(accumulatedUsedVarFromRootSet), false,
                            context);
                }
            }
        }
    }

    private void clear() {
        assignedVarMap.clear();
        assignedVarSet.clear();
        accumulatedUsedVarFromRootMap.clear();
        survivedUnionSourceVarSet.clear();
        isTransformed = false;
    }
}
