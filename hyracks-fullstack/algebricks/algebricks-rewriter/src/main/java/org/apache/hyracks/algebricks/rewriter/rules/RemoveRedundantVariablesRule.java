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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Replaces redundant variable references with their bottom-most equivalent representative.
 * Does a DFS sweep over the plan keeping track of variable equivalence classes.
 * For example, this rule would perform the following rewrite.
 * Before Plan:
 * select (function-call: func, Args:[%0->$$11])
 * project [$11]
 * assign [$$11] <- [$$10]
 * assign [$$10] <- [$$9]
 * assign [$$9] <- ...
 * ...
 * After Plan:
 * select (function-call: func, Args:[%0->$$9])
 * project [$9]
 * assign [$$11] <- [$$9]
 * assign [$$10] <- [$$9]
 * assign [$$9] <- ...
 * ...
 */
public class RemoveRedundantVariablesRule implements IAlgebraicRewriteRule {

    private final VariableSubstitutionVisitor substVisitor = new VariableSubstitutionVisitor();
    private final Map<LogicalVariable, List<LogicalVariable>> equivalentVarsMap =
            new HashMap<LogicalVariable, List<LogicalVariable>>();

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        boolean modified = removeRedundantVariables(opRef, context);
        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(opRef.getValue());
        }
        return modified;
    }

    private void updateEquivalenceClassMap(LogicalVariable lhs, LogicalVariable rhs) {
        List<LogicalVariable> equivalentVars = equivalentVarsMap.get(rhs);
        if (equivalentVars == null) {
            equivalentVars = new ArrayList<LogicalVariable>();
            // The first element in the list is the bottom-most representative which will replace all equivalent vars.
            equivalentVars.add(rhs);
            equivalentVars.add(lhs);
            equivalentVarsMap.put(rhs, equivalentVars);
        }
        equivalentVarsMap.put(lhs, equivalentVars);
    }

    private boolean removeRedundantVariables(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        LogicalOperatorTag opTag = op.getOperatorTag();
        boolean modified = false;

        // Update equivalence class map.
        if (opTag == LogicalOperatorTag.ASSIGN) {
            AssignOperator assignOp = (AssignOperator) op;
            int numVars = assignOp.getVariables().size();
            for (int i = 0; i < numVars; i++) {
                ILogicalExpression expr = assignOp.getExpressions().get(i).getValue();
                if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                    continue;
                }
                VariableReferenceExpression rhsVarRefExpr = (VariableReferenceExpression) expr;
                // Update equivalence class map.
                LogicalVariable lhs = assignOp.getVariables().get(i);
                LogicalVariable rhs = rhsVarRefExpr.getVariableReference();
                updateEquivalenceClassMap(lhs, rhs);
            }
        }

        // Replace variable references with their first representative.
        if (opTag == LogicalOperatorTag.PROJECT) {
            // The project operator does not use expressions, so we need to replace it's variables manually.
            if (replaceProjectVars((ProjectOperator) op)) {
                modified = true;
            }
        } else if (op.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
            // Replace redundant variables manually in the UnionAll operator.
            if (replaceUnionAllVars((UnionAllOperator) op)) {
                modified = true;
            }
        } else {
            if (op.acceptExpressionTransform(substVisitor)) {
                modified = true;
            }
        }

        // Perform variable replacement in nested plans.
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans opWithNestedPlan = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan nestedPlan : opWithNestedPlan.getNestedPlans()) {
                for (Mutable<ILogicalOperator> rootRef : nestedPlan.getRoots()) {
                    if (removeRedundantVariables(rootRef, context)) {
                        modified = true;
                    }
                }
            }
        }

        // Deal with re-mapping of variables in group by.
        if (opTag == LogicalOperatorTag.GROUP) {
            if (handleGroupByVarRemapping((GroupByOperator) op)) {
                modified = true;
            }
        }

        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(op);
            context.addToDontApplySet(this, op);
        }

        // Clears the equivalent variable map if the current operator is the root operator
        // in the query plan.
        if (opTag == LogicalOperatorTag.DISTRIBUTE_RESULT || opTag == LogicalOperatorTag.SINK) {
            equivalentVarsMap.clear();
        }
        return modified;
    }

    private boolean handleGroupByVarRemapping(GroupByOperator groupOp) {
        boolean modified = false;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> gp : groupOp.getGroupByList()) {
            if (gp.first == null || gp.second.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                continue;
            }
            LogicalVariable groupByVar = ((VariableReferenceExpression) gp.second.getValue()).getVariableReference();
            Iterator<Pair<LogicalVariable, Mutable<ILogicalExpression>>> iter = groupOp.getDecorList().iterator();
            while (iter.hasNext()) {
                Pair<LogicalVariable, Mutable<ILogicalExpression>> dp = iter.next();
                if (dp.first != null || dp.second.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                    continue;
                }
                LogicalVariable dv = ((VariableReferenceExpression) dp.second.getValue()).getVariableReference();
                if (dv == groupByVar) {
                    // The decor variable is redundant, since it is propagated as a grouping variable.
                    List<LogicalVariable> equivalentVars = equivalentVarsMap.get(groupByVar);
                    if (equivalentVars != null) {
                        // Change representative of this equivalence class.
                        equivalentVars.set(0, gp.first);
                        equivalentVarsMap.put(gp.first, equivalentVars);
                    } else {
                        updateEquivalenceClassMap(gp.first, groupByVar);
                    }
                    iter.remove();
                    modified = true;
                    break;
                }
            }
        }
        // find the redundant variables within the decor list
        Map<LogicalVariable, LogicalVariable> variableToFirstDecorMap = new HashMap<LogicalVariable, LogicalVariable>();
        Iterator<Pair<LogicalVariable, Mutable<ILogicalExpression>>> iter = groupOp.getDecorList().iterator();
        while (iter.hasNext()) {
            Pair<LogicalVariable, Mutable<ILogicalExpression>> dp = iter.next();
            if (dp.first == null || dp.second.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                continue;
            }
            LogicalVariable dv = ((VariableReferenceExpression) dp.second.getValue()).getVariableReference();
            LogicalVariable firstDecor = variableToFirstDecorMap.get(dv);
            if (firstDecor == null) {
                variableToFirstDecorMap.put(dv, dp.first);
            } else {
                // The decor variable dp.first is redundant since firstDecor is exactly the same.
                updateEquivalenceClassMap(dp.first, firstDecor);
                iter.remove();
                modified = true;
            }
        }
        return modified;
    }

    /**
     * Replace the projects's variables with their corresponding representative
     * from the equivalence class map (if any).
     * We cannot use the VariableSubstitutionVisitor here because the project ops
     * maintain their variables as a list and not as expressions.
     */
    private boolean replaceProjectVars(ProjectOperator op) throws AlgebricksException {
        List<LogicalVariable> vars = op.getVariables();
        int size = vars.size();
        boolean modified = false;
        for (int i = 0; i < size; i++) {
            LogicalVariable var = vars.get(i);
            List<LogicalVariable> equivalentVars = equivalentVarsMap.get(var);
            if (equivalentVars == null) {
                continue;
            }
            // Replace with equivalence class representative.
            LogicalVariable representative = equivalentVars.get(0);
            if (representative != var) {
                vars.set(i, equivalentVars.get(0));
                modified = true;
            }
        }
        return modified;
    }

    private boolean replaceUnionAllVars(UnionAllOperator op) throws AlgebricksException {
        boolean modified = false;
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> varMapping : op.getVariableMappings()) {
            List<LogicalVariable> firstEquivalentVars = equivalentVarsMap.get(varMapping.first);
            List<LogicalVariable> secondEquivalentVars = equivalentVarsMap.get(varMapping.second);
            // Replace variables with their representative.
            if (firstEquivalentVars != null) {
                varMapping.first = firstEquivalentVars.get(0);
                modified = true;
            }
            if (secondEquivalentVars != null) {
                varMapping.second = secondEquivalentVars.get(0);
                modified = true;
            }
        }
        return modified;
    }

    private class VariableSubstitutionVisitor implements ILogicalExpressionReferenceTransform {
        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) {
            ILogicalExpression e = exprRef.getValue();
            switch (((AbstractLogicalExpression) e).getExpressionTag()) {
                case VARIABLE: {
                    // Replace variable references with their equivalent representative in the equivalence class map.
                    VariableReferenceExpression varRefExpr = (VariableReferenceExpression) e;
                    LogicalVariable var = varRefExpr.getVariableReference();
                    List<LogicalVariable> equivalentVars = equivalentVarsMap.get(var);
                    if (equivalentVars == null) {
                        return false;
                    }
                    LogicalVariable representative = equivalentVars.get(0);
                    if (representative != var) {
                        varRefExpr.setVariable(representative);
                        return true;
                    }
                    return false;
                }
                case FUNCTION_CALL: {
                    AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) e;
                    boolean modified = false;
                    for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                        if (transform(arg)) {
                            modified = true;
                        }
                    }
                    return modified;
                }
                default: {
                    return false;
                }
            }
        }
    }
}
