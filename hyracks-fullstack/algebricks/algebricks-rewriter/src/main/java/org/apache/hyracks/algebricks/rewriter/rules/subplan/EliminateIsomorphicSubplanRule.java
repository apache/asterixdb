/*
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.IsomorphismUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule consolidates two subplan operators into a single subplan operator.
 * It searches for two adjacent subplan operators in the plan
 *
 * <pre>
 * SUBPLAN_1 {
 *   AGGREGATE_1 [v1=...]
 *   ASSIGN_i_j (zero or more)
 *   rest_ops_1
 * }
 * SUBPLAN_2 {
 *   AGGREGATE_2 [v2=...]
 *   ASSIGN_m_n (zero or more)
 *   rest_ops_2
 * }
 * </pre>
 *
 * If {@code rest_ops_1} segment is isomorphic with {@code rest_ops_2} segment then
 * this rule consolidates both subplans into a single (lower) one.
 * Variables produced {@code rest_ops_1} and used by AGGREGATE_1 / ASSIGN_1_i
 * are replaced with variables produced by {@code rest_ops_2}
 *
 * <pre>
 * SUBPLAN_2 {
 *   AGGREGATE [v1=..., v2=...]
 *   ASSIGN_i_j (zero or more)
 *   ASSIGN_m_n (zero or more)
 *   rest_ops_2
 * }
 * </pre>
 *
 * Note: this rule keeps {@code SUBPLAN_1} if it had several nested plans and
 * some of those nested plans could not be moved into the lower subplan operator.
 * </pre>
 */
public final class EliminateIsomorphicSubplanRule implements IAlgebraicRewriteRule {

    private List<AggregateOperator> targetSubplan1Roots;

    private List<AggregateOperator> targetSubplan2Roots;

    private List<Map<LogicalVariable, LogicalVariable>> targetVarMaps;

    private Map<LogicalVariable, LogicalVariable> tmpVarMap;

    private Mutable<AggregateOperator> tmpAggOpRef;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }

        boolean applied = false;
        for (;;) {
            context.addToDontApplySet(this, opRef.getValue());
            Pair<Boolean, Mutable<ILogicalOperator>> p = mergeSubplanIntoChildSubplan(opRef, context);
            if (p == null) {
                break;
            }
            applied |= p.first;
            opRef = p.second;
        }

        return applied;
    }

    /**
     * Returns {@code null} if given operator's child operator is not a SUBPLAN.
     * Otherwise attempts to merge the given SUBPLAN into its child and returns a pair of values.
     * The first value in the pair is a boolean indicating whether the rewriting succeeded or not,
     * the second value is a reference to the lower subplan (always returned even if the rewriting did not happen)
     */
    private Pair<Boolean, Mutable<ILogicalOperator>> mergeSubplanIntoChildSubplan(Mutable<ILogicalOperator> op1Ref,
            IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) op1Ref.getValue();
        SubplanOperator subplan1 = (SubplanOperator) op1;
        Mutable<ILogicalOperator> op2Ref = subplan1.getInputs().get(0);
        ILogicalOperator op2 = op2Ref.getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return null;
        }
        SubplanOperator subplan2 = (SubplanOperator) op2;

        reset();

        for (Iterator<ILogicalPlan> nestedPlanIter = subplan1.getNestedPlans().iterator(); nestedPlanIter.hasNext();) {
            ILogicalPlan nestedPlan = nestedPlanIter.next();
            for (Iterator<Mutable<ILogicalOperator>> rootOpIter = nestedPlan.getRoots().iterator(); rootOpIter
                    .hasNext();) {
                ILogicalOperator rootOp = rootOpIter.next().getValue();
                if (findIsomorphicPlanSegment(rootOp, subplan2, tmpAggOpRef, tmpVarMap)) {
                    targetSubplan1Roots.add((AggregateOperator) rootOp);
                    targetSubplan2Roots.add(Objects.requireNonNull(tmpAggOpRef.getValue()));
                    targetVarMaps.add(new HashMap<>(tmpVarMap));
                    rootOpIter.remove();
                }
            }
            if (nestedPlan.getRoots().isEmpty()) {
                nestedPlanIter.remove();
            }
        }

        if (targetSubplan1Roots.isEmpty()) {
            return new Pair<>(false, op2Ref);
        }

        for (int i = 0, n = targetSubplan1Roots.size(); i < n; i++) {
            AggregateOperator targetSubplan1Root = targetSubplan1Roots.get(i);
            AggregateOperator targetSubplan2Root = targetSubplan2Roots.get(i);
            Map<LogicalVariable, LogicalVariable> targetVarMap = targetVarMaps.get(i);
            consolidateSubplans(targetSubplan1Root, targetSubplan2Root, targetVarMap, context);
        }

        context.computeAndSetTypeEnvironmentForOperator(subplan2);

        if (subplan1.getNestedPlans().isEmpty()) {
            // remove subplan1 from the tree
            op1Ref.setValue(subplan2);
            return new Pair<>(true, op1Ref);
        } else {
            // some nested plans were removed from subplan1 -> recompute its type environment
            context.computeAndSetTypeEnvironmentForOperator(subplan1);
            return new Pair<>(true, op2Ref);
        }
    }

    /**
     * Finds nested plan root in given subplan that is isomorphic to given operator
     * and returns their variable mappings
     */
    private static boolean findIsomorphicPlanSegment(ILogicalOperator op, SubplanOperator subplanOp,
            Mutable<AggregateOperator> outSubplanRootOpRef, Map<LogicalVariable, LogicalVariable> outVarMap)
            throws AlgebricksException {
        if (op.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggOp = (AggregateOperator) op;
        if (aggOp.getMergeExpressions() != null) {
            return false;
        }

        Set<LogicalVariable> freeVars = new ListSet<>();
        OperatorPropertiesUtil.getFreeVariablesInSelfOrDesc(aggOp, freeVars);

        // find first non-ASSIGN child. It'll be the root for the isomorphic segment search.
        ILogicalOperator opChildIsomorphicCandidate = aggOp.getInputs().get(0).getValue();
        while (opChildIsomorphicCandidate.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            if (!OperatorPropertiesUtil.isMovable(opChildIsomorphicCandidate)) {
                return false;
            }
            opChildIsomorphicCandidate = opChildIsomorphicCandidate.getInputs().get(0).getValue();
        }

        for (ILogicalPlan nestedPlan : subplanOp.getNestedPlans()) {
            for (Mutable<ILogicalOperator> rootOpRef : nestedPlan.getRoots()) {
                ILogicalOperator rootOp = rootOpRef.getValue();
                if (rootOp.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
                    continue;
                }
                AggregateOperator aggRootOp = (AggregateOperator) rootOp;
                if (aggRootOp.getMergeExpressions() != null) {
                    continue;
                }
                if (!OperatorPropertiesUtil.disjoint(freeVars, aggRootOp.getVariables())) {
                    // upper subplan uses variables from this subplan -> exit
                    continue;
                }

                // find first non-ASSIGN child. It'll be the root for the isomorphic segment search.
                ILogicalOperator rootOpChildIsomorphicCandidate = aggRootOp.getInputs().get(0).getValue();
                while (rootOpChildIsomorphicCandidate.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                    rootOpChildIsomorphicCandidate = rootOpChildIsomorphicCandidate.getInputs().get(0).getValue();
                }

                if (IsomorphismUtilities.isOperatorIsomorphicPlanSegment(opChildIsomorphicCandidate,
                        rootOpChildIsomorphicCandidate)) {
                    outSubplanRootOpRef.setValue(aggRootOp);
                    IsomorphismUtilities.mapVariablesTopDown(rootOpChildIsomorphicCandidate, opChildIsomorphicCandidate,
                            outVarMap, false);
                    return true;
                }
            }
        }
        return false;
    }

    private static void consolidateSubplans(AggregateOperator upperAggRootOp, AggregateOperator targetAggRootOp,
            Map<LogicalVariable, LogicalVariable> varMap, IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator upperChildOp = upperAggRootOp.getInputs().get(0).getValue();
        if (upperChildOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            Deque<AssignOperator> upperAssignQueue = new ArrayDeque<>();
            do {
                upperAssignQueue.push((AssignOperator) upperChildOp);
                upperChildOp = upperChildOp.getInputs().get(0).getValue();
            } while (upperChildOp.getOperatorTag() == LogicalOperatorTag.ASSIGN);

            ILogicalOperator targetChildOp = targetAggRootOp.getInputs().get(0).getValue();

            AssignOperator upperAssignOp;
            while ((upperAssignOp = upperAssignQueue.poll()) != null) {
                AssignOperator upperAssignOpCopy = (AssignOperator) OperatorManipulationUtil.deepCopy(upperAssignOp);
                upperAssignOpCopy.getInputs().clear();
                VariableUtilities.substituteVariables(upperAssignOpCopy, varMap, null);

                upperAssignOpCopy.getInputs().add(new MutableObject<>(targetChildOp));
                context.computeAndSetTypeEnvironmentForOperator(upperAssignOpCopy);
                targetChildOp = upperAssignOpCopy;
            }

            targetAggRootOp.getInputs().clear();
            targetAggRootOp.getInputs().add(new MutableObject<>(targetChildOp));
        }

        AggregateOperator upperAggRootOpCopy = (AggregateOperator) OperatorManipulationUtil.deepCopy(upperAggRootOp);
        upperAggRootOpCopy.getInputs().clear();
        VariableUtilities.substituteVariables(upperAggRootOpCopy, varMap, null);

        targetAggRootOp.getVariables().addAll(upperAggRootOpCopy.getVariables());
        targetAggRootOp.getExpressions().addAll(upperAggRootOpCopy.getExpressions());

        context.computeAndSetTypeEnvironmentForOperator(targetAggRootOp);
    }

    private void reset() {
        if (targetSubplan1Roots == null) {
            targetSubplan1Roots = new ArrayList<>();
        } else {
            targetSubplan1Roots.clear();
        }
        if (targetSubplan2Roots == null) {
            targetSubplan2Roots = new ArrayList<>();
        } else {
            targetSubplan2Roots.clear();
        }
        if (targetVarMaps == null) {
            targetVarMaps = new ArrayList<>();
        } else {
            targetVarMaps.clear();
        }
        if (tmpVarMap == null) {
            tmpVarMap = new HashMap<>();
        } else {
            tmpVarMap.clear();
        }
        if (tmpAggOpRef == null) {
            tmpAggOpRef = new MutableObject<>();
        } else {
            tmpAggOpRef.setValue(null);
        }
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }
}
