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
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Removes duplicate and/or unnecessary variables from a group-by operator's decor list.
 */
public class RemoveRedundantGroupByDecorVarsRule implements IAlgebraicRewriteRule {

    private Set<LogicalVariable> usedVars = new HashSet<>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        // Begin from the root operator to collect used variables after a possible group-by operator.
        if (op.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT
                && op.getOperatorTag() != LogicalOperatorTag.SINK) {
            return false;
        }
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        usedVars.clear();
        boolean planTransformed = checkAndApplyTheRule(opRef, context);

        return planTransformed;
    }

    /**
     * Collect used variables in each operator in the plan until the optimizer sees a GroupBy operator.
     * It first removes duplicated variables in the decor list.
     * Then, it eliminates useless variables in the decor list that are not going to be used
     * after the given groupBy operator.
     */
    protected boolean checkAndApplyTheRule(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        Set<LogicalVariable> usedVarsFromThisOp = new HashSet<>();
        Set<LogicalVariable> collectedUsedVarsBeforeThisOpFromRoot = new HashSet<>();
        boolean redundantVarsRemoved = false;
        boolean uselessVarsRemoved = false;

        // Found Group-By operator?
        if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
            GroupByOperator groupByOp = (GroupByOperator) op;
            Set<LogicalVariable> decorVars = new HashSet<>();

            // First, get rid of duplicated variables from a group-by operator's decor list.
            Iterator<Pair<LogicalVariable, Mutable<ILogicalExpression>>> iter = groupByOp.getDecorList().iterator();
            while (iter.hasNext()) {
                Pair<LogicalVariable, Mutable<ILogicalExpression>> decor = iter.next();
                if (decor.first != null
                        || decor.second.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                    continue;
                }
                VariableReferenceExpression varRefExpr = (VariableReferenceExpression) decor.second.getValue();
                LogicalVariable var = varRefExpr.getVariableReference();
                if (decorVars.contains(var)) {
                    iter.remove();
                    redundantVarsRemoved = true;
                } else {
                    decorVars.add(var);
                }
            }

            // Next, get rid of useless decor variables in the GROUP-BY operator.
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> newDecorList = new ArrayList<>();
            for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : groupByOp.getDecorList()) {
                LogicalVariable decorVar = GroupByOperator.getDecorVariable(p);
                // If a variable in the decor list will not be used after this operator, then it needs to be removed.
                if (!usedVars.contains(decorVar)) {
                    uselessVarsRemoved = true;
                } else {
                    // Maintain the variable since it will be used.
                    newDecorList.add(p);
                }
            }

            // If we have identified useless decor variables,
            // then the decor list needs to be reset without those variables.
            if (uselessVarsRemoved) {
                groupByOp.getDecorList().clear();
                groupByOp.getDecorList().addAll(newDecorList);
            }

            // If the plan transformation is successful, we don't need to traverse the plan any more,
            // since if there are more GROUP-BY operators, the next trigger on this plan will find them.
            if (redundantVarsRemoved || uselessVarsRemoved) {
                context.computeAndSetTypeEnvironmentForOperator(groupByOp);
                context.addToDontApplySet(this, op);
                return redundantVarsRemoved || uselessVarsRemoved;
            }
        }

        // Either we have found a GroupBy operator but no removal is happened or
        // there we haven't found a GroupBy operator yet. Thus, we add used variables for this operator
        // and keep traversing the plan.
        VariableUtilities.getUsedVariables(op, usedVarsFromThisOp);
        collectedUsedVarsBeforeThisOpFromRoot.addAll(usedVars);
        usedVars.addAll(usedVarsFromThisOp);

        // Recursively check the plan and try to optimize it.
        for (int i = 0; i < op.getInputs().size(); i++) {
            boolean groupByChanged = checkAndApplyTheRule(op.getInputs().get(i), context);
            if (groupByChanged) {
                return true;
            }
        }

        // This rule can't be applied to this operator or its descendants.
        // Thus, remove the effects of this operator so that the depth-first-search can return to the parent.
        usedVars.clear();
        usedVars.addAll(collectedUsedVarsBeforeThisOpFromRoot);

        return false;
    }

}
