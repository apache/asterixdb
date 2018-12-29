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
import java.util.List;
import java.util.Map;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule pushes an array of subplans on top of a group-by into the
 * nested plan of the group-by.
 *
 * @author yingyib
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
        return rewriteForOperator(rootRef, opRef, context);
    }

    // The core rewriting function for an operator.
    private boolean rewriteForOperator(Mutable<ILogicalOperator> rootRef, Mutable<ILogicalOperator> opRef,
            IOptimizationContext context) throws AlgebricksException {
        boolean changed = false;
        ILogicalOperator parentOperator = opRef.getValue();
        for (Mutable<ILogicalOperator> ref : parentOperator.getInputs()) {
            ILogicalOperator op = ref.getValue();
            // Only processes subplan operator.
            Deque<SubplanOperator> subplans = new ArrayDeque<>();
            if (op.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
                // Recursively rewrites the child plan.
                changed |= rewriteForOperator(rootRef, ref, context);
                continue;
            }
            while (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                SubplanOperator currentSubplan = (SubplanOperator) op;
                // Recursively rewrites the pipelines inside a nested subplan.
                for (ILogicalPlan subplan : currentSubplan.getNestedPlans()) {
                    for (Mutable<ILogicalOperator> nestedRootRef : subplan.getRoots()) {
                        changed |= rewriteForOperator(nestedRootRef, nestedRootRef, context);
                    }
                }
                subplans.addFirst(currentSubplan);
                op = op.getInputs().get(0).getValue();
            }
            // Only processes the case a group-by operator is the input of the subplan operators.
            if (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
                continue;
            }
            GroupByOperator gby = (GroupByOperator) op;
            // Recursively rewrites the pipelines inside a nested subplan.
            for (ILogicalPlan subplan : gby.getNestedPlans()) {
                for (Mutable<ILogicalOperator> nestedRootRef : subplan.getRoots()) {
                    changed |= rewriteForOperator(nestedRootRef, nestedRootRef, context);
                }
            }
            changed |= pushSubplansIntoGroupBy(rootRef, parentOperator, subplans, gby, context);
        }
        return changed;
    }

    // Pushes subplans into the group by operator.
    private boolean pushSubplansIntoGroupBy(Mutable<ILogicalOperator> currentRootRef, ILogicalOperator parentOperator,
            Deque<SubplanOperator> subplans, GroupByOperator gby, IOptimizationContext context)
            throws AlgebricksException {
        boolean changed = false;
        List<ILogicalPlan> newGbyNestedPlans = new ArrayList<>();
        List<ILogicalPlan> originalNestedPlansInGby = gby.getNestedPlans();

        // Adds all original subplans from the group by.
        for (ILogicalPlan gbyNestedPlanOriginal : originalNestedPlansInGby) {
            newGbyNestedPlans.add(gbyNestedPlanOriginal);
        }

        // Tries to push subplans into the group by.
        Iterator<SubplanOperator> subplanOperatorIterator = subplans.iterator();
        while (subplanOperatorIterator.hasNext()) {
            SubplanOperator subplan = subplanOperatorIterator.next();
            Iterator<ILogicalPlan> subplanNestedPlanIterator = subplan.getNestedPlans().iterator();
            while (subplanNestedPlanIterator.hasNext()) {
                ILogicalPlan subplanNestedPlan = subplanNestedPlanIterator.next();
                List<Mutable<ILogicalOperator>> upperSubplanRootRefs = subplanNestedPlan.getRoots();
                Iterator<Mutable<ILogicalOperator>> upperSubplanRootRefIterator = upperSubplanRootRefs.iterator();
                while (upperSubplanRootRefIterator.hasNext()) {
                    Mutable<ILogicalOperator> rootOpRef = upperSubplanRootRefIterator.next();

                    if (downToNts(rootOpRef) == null) {
                        continue;
                    }

                    // Collects free variables in the root operator of a nested plan and its descent.
                    Set<LogicalVariable> freeVars = new ListSet<>();
                    OperatorPropertiesUtil.getFreeVariablesInSelfOrDesc((AbstractLogicalOperator) rootOpRef.getValue(),
                            freeVars);

                    // Checks whether the above freeVars are all contained in live variables * of one nested plan
                    // inside the group-by operator. If yes, then the subplan can be pushed into the nested plan
                    // of the group-by.
                    for (ILogicalPlan gbyNestedPlanOriginal : originalNestedPlansInGby) {
                        ILogicalPlan gbyNestedPlan = OperatorManipulationUtil.deepCopy(gbyNestedPlanOriginal, context);
                        List<Mutable<ILogicalOperator>> gbyRootOpRefs = gbyNestedPlan.getRoots();
                        for (int rootIndex = 0; rootIndex < gbyRootOpRefs.size(); rootIndex++) {
                            // Sets the nts for a original subplan.
                            Mutable<ILogicalOperator> originalGbyRootOpRef = gbyNestedPlan.getRoots().get(rootIndex);
                            Mutable<ILogicalOperator> originalGbyNtsRef = downToNts(originalGbyRootOpRef);
                            if (originalGbyNtsRef == null) {
                                continue;
                            }
                            NestedTupleSourceOperator originalNts =
                                    (NestedTupleSourceOperator) originalGbyNtsRef.getValue();
                            originalNts.setDataSourceReference(new MutableObject<>(gby));

                            // Pushes a new subplan if possible.
                            Mutable<ILogicalOperator> gbyRootOpRef = gbyRootOpRefs.get(rootIndex);
                            Set<LogicalVariable> liveVars = new ListSet<>();
                            VariableUtilities.getLiveVariables(gbyRootOpRef.getValue(), liveVars);
                            if (!liveVars.containsAll(freeVars)) {
                                continue;
                            }

                            AggregateOperator aggOp = (AggregateOperator) gbyRootOpRef.getValue();
                            for (int varIndex = aggOp.getVariables().size() - 1; varIndex >= 0; varIndex--) {
                                if (!freeVars.contains(aggOp.getVariables().get(varIndex))) {
                                    aggOp.getVariables().remove(varIndex);
                                    aggOp.getExpressions().remove(varIndex);
                                }
                            }

                            // Copy the original nested pipeline inside the group-by.
                            Pair<ILogicalOperator, Map<LogicalVariable, LogicalVariable>> copiedAggOpAndVarMap =
                                    OperatorManipulationUtil.deepCopyWithNewVars(aggOp, context);
                            ILogicalOperator newBottomAgg = copiedAggOpAndVarMap.first;

                            // Substitutes variables in the upper nested pipe line.
                            VariableUtilities.substituteVariablesInDescendantsAndSelf(rootOpRef.getValue(),
                                    copiedAggOpAndVarMap.second, context);

                            // Does the actual push.
                            Mutable<ILogicalOperator> ntsRef = downToNts(rootOpRef);
                            ntsRef.setValue(newBottomAgg);
                            gbyRootOpRef.setValue(rootOpRef.getValue());

                            // Sets the nts for a new pushed plan.
                            Mutable<ILogicalOperator> oldGbyNtsRef = downToNts(new MutableObject<>(newBottomAgg));
                            NestedTupleSourceOperator nts = (NestedTupleSourceOperator) oldGbyNtsRef.getValue();
                            nts.setDataSourceReference(new MutableObject<>(gby));

                            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(rootOpRef.getValue(), context);
                            newGbyNestedPlans.add(new ALogicalPlanImpl(rootOpRef));

                            upperSubplanRootRefIterator.remove();
                            changed |= true;
                            break;
                        }
                    }
                }

                if (upperSubplanRootRefs.isEmpty()) {
                    subplanNestedPlanIterator.remove();
                }
            }
            if (subplan.getNestedPlans().isEmpty()) {
                subplanOperatorIterator.remove();
            }
        }

        // Resets the nested subplans for the group-by operator.
        gby.getNestedPlans().clear();
        gby.getNestedPlans().addAll(newGbyNestedPlans);

        // Connects the group-by operator with its parent operator.
        ILogicalOperator parent = !subplans.isEmpty() ? subplans.getFirst() : parentOperator;
        parent.getInputs().get(0).setValue(gby);

        // Removes unnecessary pipelines inside the group by operator.
        cleanup(currentRootRef.getValue(), gby);

        // Computes type environments.
        context.computeAndSetTypeEnvironmentForOperator(gby);
        context.computeAndSetTypeEnvironmentForOperator(parent);
        return changed;
    }

    /**
     * Removes unused aggregation variables (and expressions)
     *
     * @param rootOp,
     *            the root operator of a plan or nested plan.
     * @param gby,
     *            the group-by operator.
     * @throws AlgebricksException
     */
    private void cleanup(ILogicalOperator rootOp, GroupByOperator gby) throws AlgebricksException {
        Set<LogicalVariable> freeVars = new HashSet<>();
        OperatorPropertiesUtil.getFreeVariablesInPath(rootOp, gby, freeVars);
        Iterator<ILogicalPlan> nestedPlanIterator = gby.getNestedPlans().iterator();
        while (nestedPlanIterator.hasNext()) {
            ILogicalPlan nestedPlan = nestedPlanIterator.next();
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
                nestedPlanIterator.remove();
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
