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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * This rule pushes an array of subplans on top of a group-by or a subplan into the
 * nested plan of the group-by / subplan
 *
 * @author yingyib
 *
 * TODO(dmitry):rename to PushSubplanIntoGroupByOrSubplanRule
 */
public class PushSubplanIntoGroupByRule implements IAlgebraicRewriteRule {
    /** The pointer to the topmost operator */
    private Mutable<ILogicalOperator> rootRef;
    /** Whether the rule has ever been invoked */
    private boolean invoked = false;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (!invoked) {
            rootRef = opRef;
            invoked = true;
        }
        return rewriteForOperator(rootRef, opRef.getValue(), context);
    }

    // The core rewriting function for an operator.
    private boolean rewriteForOperator(Mutable<ILogicalOperator> rootRef, ILogicalOperator parentOperator,
            IOptimizationContext context) throws AlgebricksException {
        boolean changed = false;
        List<Mutable<ILogicalOperator>> parentInputs = parentOperator.getInputs();
        for (int i = 0, n = parentInputs.size(); i < n; i++) {
            Mutable<ILogicalOperator> ref = parentInputs.get(i);
            ILogicalOperator op = ref.getValue();
            // Only processes subplan operator.
            if (op.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
                // Recursively rewrites the child plan.
                changed |= rewriteForOperator(rootRef, op, context);
                continue;
            }
            Deque<SubplanOperator> subplans = new ArrayDeque<>();
            while (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                SubplanOperator currentSubplan = (SubplanOperator) op;
                // Recursively rewrites the pipelines inside a nested subplan.
                for (ILogicalPlan subplan : currentSubplan.getNestedPlans()) {
                    for (Mutable<ILogicalOperator> nestedRootRef : subplan.getRoots()) {
                        changed |= rewriteForOperator(nestedRootRef, nestedRootRef.getValue(), context);
                    }
                }
                subplans.addFirst(currentSubplan);
                op = op.getInputs().get(0).getValue();
            }
            if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
                // Process the case a group-by operator is the input of the subplan operators.
                GroupByOperator gby = (GroupByOperator) op;
                // Recursively rewrites the pipelines inside a nested subplan.
                for (ILogicalPlan gbyNestedPlan : gby.getNestedPlans()) {
                    for (Mutable<ILogicalOperator> gbyNestedPlanRootRef : gbyNestedPlan.getRoots()) {
                        changed |= rewriteForOperator(gbyNestedPlanRootRef, gbyNestedPlanRootRef.getValue(), context);
                    }
                }
                changed |= pushSubplansIntoGroupByOrSubplan(rootRef, parentOperator, i, subplans, gby, context);
            } else if (subplans.size() > 1 && context.getPhysicalOptimizationConfig().getSubplanMerge()) {
                // Process the case a subplan operator is the input of the subplan operators.
                SubplanOperator destOp = subplans.removeFirst();
                if (!context.checkIfInDontApplySet(this, destOp)) {
                    changed |= pushSubplansIntoGroupByOrSubplan(rootRef, parentOperator, i, subplans, destOp, context);
                }
            }
        }
        return changed;
    }

    // Pushes subplans into operator with nested plans (group by or subplan).
    private boolean pushSubplansIntoGroupByOrSubplan(Mutable<ILogicalOperator> currentRootRef,
            ILogicalOperator parentOperator, int parentChildIdx, Deque<SubplanOperator> subplans,
            AbstractOperatorWithNestedPlans destOp, IOptimizationContext context) throws AlgebricksException {
        boolean changed = false;
        List<ILogicalPlan> newDestOpNestedPlans = new ArrayList<>();

        Deque<Set<LogicalVariable>> freeVarsInSubplans = new ArrayDeque<>(subplans.size());
        for (SubplanOperator subplan : subplans) {
            Set<LogicalVariable> freeVarsInSubplan = new ListSet<>();
            OperatorPropertiesUtil.getFreeVariablesInSubplans(subplan, freeVarsInSubplan);
            freeVarsInSubplans.addLast(freeVarsInSubplan);
        }

        Set<LogicalVariable> liveVarsBeforeDestOp = new ListSet<>();
        ILogicalOperator destOpInput = destOp.getInputs().get(0).getValue();
        VariableUtilities.getLiveVariables(destOpInput, liveVarsBeforeDestOp);

        Set<LogicalVariable> liveVarsInDestOpNestedPlan = new ListSet<>();
        Set<LogicalVariable> upperSubplanFreeVarsTmp = new ListSet<>();

        // Tries to push subplans into the destination operator (destop)
        for (Iterator<SubplanOperator> subplanOperatorIterator = subplans.iterator(); subplanOperatorIterator
                .hasNext();) {
            SubplanOperator upperSubplan = subplanOperatorIterator.next();
            Set<LogicalVariable> upperSubplanFreeVars = freeVarsInSubplans.removeFirst();

            for (Iterator<ILogicalPlan> upperSubplanNestedPlanIter =
                    upperSubplan.getNestedPlans().iterator(); upperSubplanNestedPlanIter.hasNext();) {
                ILogicalPlan upperSubplanNestedPlan = upperSubplanNestedPlanIter.next();
                List<Mutable<ILogicalOperator>> upperSubplanRootRefs = upperSubplanNestedPlan.getRoots();
                for (Iterator<Mutable<ILogicalOperator>> upperSubplanRootRefIter =
                        upperSubplanRootRefs.iterator(); upperSubplanRootRefIter.hasNext();) {
                    Mutable<ILogicalOperator> upperSubplanRootRef = upperSubplanRootRefIter.next();
                    Mutable<ILogicalOperator> upperSubplanNtsRef = downToNts(upperSubplanRootRef);
                    if (upperSubplanNtsRef == null) {
                        continue;
                    }

                    loop_dest_op_nested_plans: for (ILogicalPlan originalDestOpNestedPlan : destOp.getNestedPlans()) {
                        for (Mutable<ILogicalOperator> originalDestOpNestedPlanRootRef : originalDestOpNestedPlan
                                .getRoots()) {
                            if (downToNts(originalDestOpNestedPlanRootRef) == null) {
                                continue;
                            }

                            // Check that the upper subplan's freeVars contains only
                            // 1. live variables from the current destop's nested plan (must have, otherwise we exit),
                            // 2. (optionally) live variables before the destop.
                            // If yes, then the subplan could be pushed into the destop's nested plan.
                            // In case #2 above we push upper plan into the nested plan as a subplan,
                            // otherwise (no live variables before destop are used by upper subplan)
                            // we merge the upper supplan into the destop's nested plan
                            upperSubplanFreeVarsTmp.clear();
                            upperSubplanFreeVarsTmp.addAll(upperSubplanFreeVars);

                            liveVarsInDestOpNestedPlan.clear();
                            VariableUtilities.getLiveVariables(originalDestOpNestedPlanRootRef.getValue(),
                                    liveVarsInDestOpNestedPlan);
                            if (!upperSubplanFreeVarsTmp.removeAll(liveVarsInDestOpNestedPlan)) {
                                // upper subplan's freeVars doesn't contain any live variables of
                                // the current destop's nested plan => exit
                                continue;
                            }

                            boolean needInnerSubplan = false;
                            if (!upperSubplanFreeVarsTmp.isEmpty()) {
                                if (!context.getPhysicalOptimizationConfig().getSubplanNestedPushdown()) {
                                    continue;
                                }
                                upperSubplanFreeVarsTmp.removeAll(liveVarsBeforeDestOp);
                                if (!upperSubplanFreeVarsTmp.isEmpty()) {
                                    continue;
                                }
                                needInnerSubplan = true;
                            }

                            // We can push the current subplan into the destop's nested plan

                            // Copy the original nested pipeline inside the group-by.
                            // (don't compute type environment for each operator, we'll do it later)
                            Pair<ILogicalOperator, Map<LogicalVariable, LogicalVariable>> copiedDestOpNestedPlanRootRefAndVarMap =
                                    OperatorManipulationUtil.deepCopyWithNewVars(
                                            originalDestOpNestedPlanRootRef.getValue(), context, false);
                            ILogicalOperator copiedDestOpNestedPlanRootRef =
                                    copiedDestOpNestedPlanRootRefAndVarMap.first;

                            AggregateOperator originalAggOp =
                                    (AggregateOperator) originalDestOpNestedPlanRootRef.getValue();
                            AggregateOperator copiedAggOp = (AggregateOperator) copiedDestOpNestedPlanRootRef;
                            for (int varIndex = originalAggOp.getVariables().size() - 1; varIndex >= 0; varIndex--) {
                                if (!upperSubplanFreeVars.contains(originalAggOp.getVariables().get(varIndex))) {
                                    copiedAggOp.getVariables().remove(varIndex);
                                    copiedAggOp.getExpressions().remove(varIndex);
                                }
                            }

                            // Substitutes variables in the upper nested plan.
                            ILogicalOperator upperSubplanRoot = upperSubplanRootRef.getValue();
                            VariableUtilities.substituteVariablesInDescendantsAndSelf(upperSubplanRoot,
                                    copiedDestOpNestedPlanRootRefAndVarMap.second, context);

                            // Does the actual push.
                            Mutable<ILogicalOperator> copiedDestOpNestedPlanNtsRef = Objects
                                    .requireNonNull(downToNts(new MutableObject<>(copiedDestOpNestedPlanRootRef)));
                            NestedTupleSourceOperator copiedDestOpNestedPlanNts =
                                    (NestedTupleSourceOperator) copiedDestOpNestedPlanNtsRef.getValue();

                            if (needInnerSubplan) {
                                SubplanOperator newInnerSubplan = new SubplanOperator(copiedDestOpNestedPlanRootRef);
                                NestedTupleSourceOperator newNts =
                                        new NestedTupleSourceOperator(new MutableObject<>(destOp));
                                newInnerSubplan.getInputs().add(new MutableObject<>(newNts));
                                copiedDestOpNestedPlanNts.setDataSourceReference(new MutableObject<>(newInnerSubplan));
                                upperSubplanNtsRef.setValue(newInnerSubplan);
                            } else {
                                copiedDestOpNestedPlanNts.setDataSourceReference(new MutableObject<>(destOp));
                                upperSubplanNtsRef.setValue(copiedDestOpNestedPlanRootRef);
                            }

                            newDestOpNestedPlans.add(new ALogicalPlanImpl(new MutableObject<>(upperSubplanRoot)));
                            upperSubplanRootRefIter.remove();
                            changed = true;
                            break loop_dest_op_nested_plans;
                        }
                    }
                }

                if (upperSubplanRootRefs.isEmpty()) {
                    upperSubplanNestedPlanIter.remove();
                    changed = true;
                }
            }
            if (upperSubplan.getNestedPlans().isEmpty()) {
                subplanOperatorIterator.remove();
                changed = true;
            }
        }

        if (!changed) {
            return false;
        }

        // Connects the destination operator with its parent operator.
        ILogicalOperator parent;
        int childIdx;
        if (!subplans.isEmpty()) {
            parent = subplans.getFirst();
            childIdx = 0;
        } else {
            parent = parentOperator;
            childIdx = parentChildIdx;
        }
        parent.getInputs().get(childIdx).setValue(destOp);

        // Add new nested plans into the destination operator
        destOp.getNestedPlans().addAll(newDestOpNestedPlans);
        // Remove unnecessary nested plans from the destination operator.
        cleanup(currentRootRef.getValue(), destOp);

        // If subplan pushdown results in destop subplan operator with multiple roots then
        // we break destop subplan operator into separate subplan operators.
        if (destOp.getOperatorTag() == LogicalOperatorTag.SUBPLAN && destOp.getNumberOfRoots() > 1) {
            splitMultiRootSubplan((SubplanOperator) destOp, context);
        }

        // Computes type environments.
        for (ILogicalPlan nestedPlan : destOp.getNestedPlans()) {
            for (Mutable<ILogicalOperator> rootOp : nestedPlan.getRoots()) {
                OperatorManipulationUtil.computeTypeEnvironmentBottomUp(rootOp.getValue(), context);
            }
        }
        context.computeAndSetTypeEnvironmentForOperator(destOp);
        context.computeAndSetTypeEnvironmentForOperator(parent);

        return true;
    }

    /**
     * Removes unused aggregation variables (and expressions)
     *
     * @param rootOp,
     *            the root operator of a plan or nested plan.
     * @param destOp,
     *            the operator with nested plans that needs to be cleaned up.
     */
    private void cleanup(ILogicalOperator rootOp, AbstractOperatorWithNestedPlans destOp) throws AlgebricksException {
        Set<LogicalVariable> freeVars = new HashSet<>();
        OperatorPropertiesUtil.getFreeVariablesInPath(rootOp, destOp, freeVars);
        for (Iterator<ILogicalPlan> nestedPlanIter = destOp.getNestedPlans().iterator(); nestedPlanIter.hasNext();) {
            ILogicalPlan nestedPlan = nestedPlanIter.next();
            Iterator<Mutable<ILogicalOperator>> nestRootRefIterator = nestedPlan.getRoots().iterator();
            while (nestRootRefIterator.hasNext()) {
                Mutable<ILogicalOperator> nestRootRef = nestRootRefIterator.next();
                AggregateOperator aggOp = (AggregateOperator) nestRootRef.getValue();
                for (int varIndex = aggOp.getVariables().size() - 1; varIndex >= 0; varIndex--) {
                    if (!freeVars.contains(aggOp.getVariables().get(varIndex))) {
                        aggOp.getVariables().remove(varIndex);
                        aggOp.getExpressions().remove(varIndex);
                    }
                }
                if (aggOp.getVariables().isEmpty()) {
                    nestRootRefIterator.remove();
                }
            }
            if (nestedPlan.getRoots().isEmpty()) {
                nestedPlanIter.remove();
            }
        }
    }

    /**
     * Split multi-root subplan operator into separate subplan operators each with a single root
     */
    private void splitMultiRootSubplan(SubplanOperator destOp, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator currentInputOp = destOp.getInputs().get(0).getValue();
        LinkedList<Mutable<ILogicalOperator>> destOpRootRefs = destOp.allRootsInReverseOrder();
        for (;;) {
            Mutable<ILogicalOperator> destOpRootRef = destOpRootRefs.removeFirst();
            ILogicalOperator destOpRoot = destOpRootRef.getValue();
            if (!destOpRootRefs.isEmpty()) {
                SubplanOperator newSubplanOp = new SubplanOperator(destOpRoot);
                newSubplanOp.setSourceLocation(destOp.getSourceLocation());
                newSubplanOp.getInputs().add(new MutableObject<>(currentInputOp));

                Mutable<ILogicalOperator> ntsRef = downToNts(destOpRootRef);
                if (ntsRef == null) {
                    throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE, destOpRoot.getSourceLocation(), "");
                }
                ((NestedTupleSourceOperator) ntsRef.getValue()).getDataSourceReference().setValue(newSubplanOp);

                OperatorManipulationUtil.computeTypeEnvironmentBottomUp(destOpRoot, context);
                context.computeAndSetTypeEnvironmentForOperator(newSubplanOp);
                context.addToDontApplySet(this, newSubplanOp);
                currentInputOp = newSubplanOp;
            } else {
                destOp.getNestedPlans().clear();
                destOp.getNestedPlans().add(new ALogicalPlanImpl(new MutableObject<>(destOpRoot)));
                destOp.getInputs().clear();
                destOp.getInputs().add(new MutableObject<>(currentInputOp));
                context.addToDontApplySet(this, destOp);
                break;
            }
        }
    }

    private Mutable<ILogicalOperator> downToNts(Mutable<ILogicalOperator> opRef) {
        List<Mutable<ILogicalOperator>> leafOps = OperatorManipulationUtil.findLeafDescendantsOrSelf(opRef);
        if (leafOps.size() == 1) {
            Mutable<ILogicalOperator> leafOp = leafOps.get(0);
            if (leafOp.getValue().getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                return leafOp;
            }
        }
        return null;
    }
}