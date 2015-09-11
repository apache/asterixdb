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
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
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
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule pushes an array of subplans on top of a group-by into the
 * nested plan of the group-by.
 * 
 * @author yingyib
 */

public class PushSubplanIntoGroupByRule implements IAlgebraicRewriteRule {
    /** Stores used variables above the current operator. */
    private final Set<LogicalVariable> usedVarsSoFar = new HashSet<LogicalVariable>();

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator parentOperator = opRef.getValue();
        if (context.checkIfInDontApplySet(this, parentOperator)) {
            return false;
        }
        context.addToDontApplySet(this, parentOperator);
        VariableUtilities.getUsedVariables(parentOperator, usedVarsSoFar);
        if (parentOperator.getInputs().size() <= 0) {
            return false;
        }
        boolean changed = false;
        GroupByOperator gby = null;
        for (Mutable<ILogicalOperator> ref : parentOperator.getInputs()) {
            AbstractLogicalOperator op = (AbstractLogicalOperator) ref.getValue();
            /** Only processes subplan operator. */
            List<SubplanOperator> subplans = new ArrayList<SubplanOperator>();
            if (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                while (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                    SubplanOperator currentSubplan = (SubplanOperator) op;
                    subplans.add(currentSubplan);
                    op = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
                }
                /** Only processes the case a group-by operator is the input of the subplan operators. */
                if (op.getOperatorTag() == LogicalOperatorTag.GROUP) {
                    gby = (GroupByOperator) op;
                    List<ILogicalPlan> newGbyNestedPlans = new ArrayList<ILogicalPlan>();
                    for (SubplanOperator subplan : subplans) {
                        List<ILogicalPlan> subplanNestedPlans = subplan.getNestedPlans();
                        List<ILogicalPlan> gbyNestedPlans = gby.getNestedPlans();
                        List<ILogicalPlan> subplanNestedPlansToRemove = new ArrayList<ILogicalPlan>();
                        for (ILogicalPlan subplanNestedPlan : subplanNestedPlans) {
                            List<Mutable<ILogicalOperator>> rootOpRefs = subplanNestedPlan.getRoots();
                            List<Mutable<ILogicalOperator>> rootOpRefsToRemove = new ArrayList<Mutable<ILogicalOperator>>();
                            for (Mutable<ILogicalOperator> rootOpRef : rootOpRefs) {
                                /** Gets free variables in the root operator of a nested plan and its descent. */
                                Set<LogicalVariable> freeVars = new ListSet<LogicalVariable>();
                                VariableUtilities.getUsedVariablesInDescendantsAndSelf(rootOpRef.getValue(), freeVars);
                                Set<LogicalVariable> producedVars = new ListSet<LogicalVariable>();
                                VariableUtilities.getProducedVariablesInDescendantsAndSelf(rootOpRef.getValue(),
                                        producedVars);
                                freeVars.removeAll(producedVars);
                                /** * Checks whether the above freeVars are all contained in live variables * of one nested plan inside the group-by operator. * If yes, then the subplan can be pushed into the nested plan of the group-by. */
                                for (ILogicalPlan gbyNestedPlanOriginal : gbyNestedPlans) {
                                    // add a subplan in the original gby
                                    if (!newGbyNestedPlans.contains(gbyNestedPlanOriginal)) {
                                        newGbyNestedPlans.add(gbyNestedPlanOriginal);
                                    }

                                    // add a pushed subplan
                                    ILogicalPlan gbyNestedPlan = OperatorManipulationUtil.deepCopy(
                                            gbyNestedPlanOriginal, context);
                                    List<Mutable<ILogicalOperator>> gbyRootOpRefs = gbyNestedPlan.getRoots();
                                    for (int rootIndex = 0; rootIndex < gbyRootOpRefs.size(); rootIndex++) {
                                        //set the nts for a original subplan
                                        Mutable<ILogicalOperator> originalGbyRootOpRef = gbyNestedPlanOriginal
                                                .getRoots().get(rootIndex);
                                        Mutable<ILogicalOperator> originalGbyNtsRef = downToNts(originalGbyRootOpRef);
                                        NestedTupleSourceOperator originalNts = (NestedTupleSourceOperator) originalGbyNtsRef
                                                .getValue();
                                        originalNts.setDataSourceReference(new MutableObject<ILogicalOperator>(gby));

                                        //push a new subplan if possible
                                        Mutable<ILogicalOperator> gbyRootOpRef = gbyRootOpRefs.get(rootIndex);
                                        Set<LogicalVariable> liveVars = new ListSet<LogicalVariable>();
                                        VariableUtilities.getLiveVariables(gbyRootOpRef.getValue(), liveVars);
                                        if (liveVars.containsAll(freeVars)) {
                                            /** Does the actual push. */
                                            Mutable<ILogicalOperator> ntsRef = downToNts(rootOpRef);
                                            ntsRef.setValue(gbyRootOpRef.getValue());
                                            // Removes unused vars.
                                            AggregateOperator aggOp = (AggregateOperator) gbyRootOpRef.getValue();
                                            for (int varIndex = aggOp.getVariables().size() - 1; varIndex >= 0; varIndex--) {
                                                if (!freeVars.contains(aggOp.getVariables().get(varIndex))) {
                                                    aggOp.getVariables().remove(varIndex);
                                                    aggOp.getExpressions().remove(varIndex);
                                                }
                                            }

                                            gbyRootOpRef.setValue(rootOpRef.getValue());
                                            rootOpRefsToRemove.add(rootOpRef);

                                            // Sets the nts for a new pushed plan.
                                            Mutable<ILogicalOperator> oldGbyNtsRef = downToNts(gbyRootOpRef);
                                            NestedTupleSourceOperator nts = (NestedTupleSourceOperator) oldGbyNtsRef
                                                    .getValue();
                                            nts.setDataSourceReference(new MutableObject<ILogicalOperator>(gby));

                                            newGbyNestedPlans.add(gbyNestedPlan);
                                            changed = true;
                                            continue;
                                        }
                                    }
                                }
                            }
                            rootOpRefs.removeAll(rootOpRefsToRemove);
                            if (rootOpRefs.size() == 0) {
                                subplanNestedPlansToRemove.add(subplanNestedPlan);
                            }
                        }
                        subplanNestedPlans.removeAll(subplanNestedPlansToRemove);
                    }
                    if (changed) {
                        ref.setValue(gby);
                        gby.getNestedPlans().clear();
                        gby.getNestedPlans().addAll(newGbyNestedPlans);
                    }
                }
            }
        }
        if (changed) {
            cleanup(gby);
        }
        return changed;
    }

    /**
     * Removes unused aggregation variables (and expressions)
     * 
     * @param gby
     * @throws AlgebricksException
     */
    private void cleanup(GroupByOperator gby) throws AlgebricksException {
        for (ILogicalPlan nestedPlan : gby.getNestedPlans()) {
            for (Mutable<ILogicalOperator> rootRef : nestedPlan.getRoots()) {
                AggregateOperator aggOp = (AggregateOperator) rootRef.getValue();
                for (int varIndex = aggOp.getVariables().size() - 1; varIndex >= 0; varIndex--) {
                    if (!usedVarsSoFar.contains(aggOp.getVariables().get(varIndex))) {
                        aggOp.getVariables().remove(varIndex);
                        aggOp.getExpressions().remove(varIndex);
                    }
                }
            }

        }
    }

    private Mutable<ILogicalOperator> downToNts(Mutable<ILogicalOperator> opRef) {
        Mutable<ILogicalOperator> currentOpRef = opRef;
        while (currentOpRef.getValue().getInputs().size() > 0) {
            currentOpRef = currentOpRef.getValue().getInputs().get(0);
        }
        return currentOpRef;
    }

}