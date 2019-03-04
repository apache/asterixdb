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

package org.apache.hyracks.algebricks.rewriter.rules.subplan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.IsomorphismUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * <pre>
 * This rules searches for
 *   SUBPLAN_1 { v1 = ... } (directly followed by)
 *   SUBPLAN_2 { v2 = ... }
 * and eliminates nested plans from subplan_1 that are isomorphic to nested plans defined by subplan_2.
 * The variables produced by eliminated nested plans are then ASSIGN-ed to variables produced by
 * matching nested plans in subplan_2:
 *   ASSIGN { v1 = v2 }
 *   SUBPLAN_2 { v2 = ...}
 *
 * Note: SUBPLAN_1 will remain in the plan (below ASSIGN) if some of its nested plans could not be eliminated.
 * </pre>
 */
public class EliminateIsomorphicSubplanRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan1 = (SubplanOperator) op1;

        Mutable<ILogicalOperator> op2Ref = subplan1.getInputs().get(0);
        ILogicalOperator op2 = op2Ref.getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan2 = (SubplanOperator) op2;

        Map<LogicalVariable, LogicalVariable> assignVarMap = new LinkedHashMap<>();
        Map<LogicalVariable, LogicalVariable> tmpVarMap = new LinkedHashMap<>();
        List<LogicalVariable> tmpVarList = new ArrayList<>();

        for (Iterator<ILogicalPlan> nestedPlanIter = subplan1.getNestedPlans().iterator(); nestedPlanIter.hasNext();) {
            ILogicalPlan nestedPlan = nestedPlanIter.next();
            for (Iterator<Mutable<ILogicalOperator>> rootOpIter = nestedPlan.getRoots().iterator(); rootOpIter
                    .hasNext();) {
                ILogicalOperator rootOp = rootOpIter.next().getValue();
                if (findIsomorphicPlanRoot(rootOp, subplan2, assignVarMap, tmpVarList, tmpVarMap)) {
                    rootOpIter.remove();
                }
            }
            if (nestedPlan.getRoots().isEmpty()) {
                nestedPlanIter.remove();
            }
        }

        int assignVarCount = assignVarMap.size();
        if (assignVarCount == 0) {
            return false;
        }

        List<LogicalVariable> assignVars = new ArrayList<>(assignVarCount);
        List<Mutable<ILogicalExpression>> assignExprs = new ArrayList<>(assignVarCount);

        for (Map.Entry<LogicalVariable, LogicalVariable> me : assignVarMap.entrySet()) {
            LogicalVariable subplan1Var = me.getKey();

            LogicalVariable subplan2Var = me.getValue();
            VariableReferenceExpression subplan2VarRef = new VariableReferenceExpression(subplan2Var);
            subplan2VarRef.setSourceLocation(subplan2.getSourceLocation());

            assignVars.add(subplan1Var);
            assignExprs.add(new MutableObject<>(subplan2VarRef));
        }

        Mutable<ILogicalOperator> assignInputOp;
        if (subplan1.getNestedPlans().isEmpty()) {
            assignInputOp = op2Ref;
        } else {
            // some nested plans were removed from subplan1 -> recompute its type environment
            context.computeAndSetTypeEnvironmentForOperator(subplan1);
            assignInputOp = new MutableObject<>(subplan1);
        }

        AssignOperator assignOp = new AssignOperator(assignVars, assignExprs);
        assignOp.setSourceLocation(subplan1.getSourceLocation());
        assignOp.getInputs().add(assignInputOp);

        context.computeAndSetTypeEnvironmentForOperator(assignOp);
        opRef.setValue(assignOp);

        return true;
    }

    /**
     * Finds nested plan root in given subplan that is isomorphic to given operator
     * and returns their variable mappings
     */
    private boolean findIsomorphicPlanRoot(ILogicalOperator op, SubplanOperator subplanOp,
            Map<LogicalVariable, LogicalVariable> outVarMap, List<LogicalVariable> tmpVarList,
            Map<LogicalVariable, LogicalVariable> tmpVarMap) throws AlgebricksException {
        for (ILogicalPlan nestedPlan : subplanOp.getNestedPlans()) {
            for (Mutable<ILogicalOperator> rootOpRef : nestedPlan.getRoots()) {
                ILogicalOperator rootOp = rootOpRef.getValue();
                if (IsomorphismUtilities.isOperatorIsomorphicPlanSegment(op, rootOp)) {
                    tmpVarList.clear();
                    VariableUtilities.getProducedVariables(op, tmpVarList);
                    tmpVarMap.clear();
                    IsomorphismUtilities.mapVariablesTopDown(rootOp, op, tmpVarMap);
                    tmpVarMap.keySet().retainAll(tmpVarList);
                    if (tmpVarMap.size() == tmpVarList.size()) {
                        outVarMap.putAll(tmpVarMap);
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }
}
