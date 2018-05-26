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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Complex rewrite rule for producing joins from unnests.
 * This rule is limited to creating left-deep trees.
 */
public class ComplexUnnestToProductRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN
                && op.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }

        //stop rewriting if the operators originates from a nested tuple source
        if (insideSubplan(opRef)) {
            return false;
        }

        // We may pull selects above the join we create in order to eliminate possible dependencies between
        // the outer and inner input plans of the join.
        List<ILogicalOperator> topSelects = new ArrayList<ILogicalOperator>();

        // Keep track of the operators and used variables participating in the inner input plan.
        HashSet<LogicalVariable> innerUsedVars = new HashSet<LogicalVariable>();
        List<ILogicalOperator> innerOps = new ArrayList<ILogicalOperator>();
        HashSet<LogicalVariable> outerUsedVars = new HashSet<LogicalVariable>();
        List<ILogicalOperator> outerOps = new ArrayList<ILogicalOperator>();
        innerOps.add(op);
        VariableUtilities.getUsedVariables(op, innerUsedVars);

        Mutable<ILogicalOperator> opRef2 = op.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();

        // Find an unnest or join and partition the plan between the first unnest and that operator into independent parts.
        if (!findPlanPartition(op2, innerUsedVars, outerUsedVars, innerOps, outerOps, topSelects, false)) {
            // We could not find an unnest or join.
            return false;
        }
        // The last operator must be an unnest or join.
        AbstractLogicalOperator unnestOrJoin = (AbstractLogicalOperator) outerOps.get(outerOps.size() - 1);

        ILogicalOperator outerRoot = null;
        ILogicalOperator innerRoot = null;
        EmptyTupleSourceOperator ets = new EmptyTupleSourceOperator();
        // If we found a join, simply use it as the outer root.
        if (unnestOrJoin.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && unnestOrJoin.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            // We've found a second unnest. First, sanity check that the unnest does not output any live variables
            // that are used by the plan above (until the first unnest).
            List<LogicalVariable> liveVars = new ArrayList<>();
            VariableUtilities.getLiveVariables(unnestOrJoin, liveVars);
            for (LogicalVariable liveVar : liveVars) {
                if (innerUsedVars.contains(liveVar)) {
                    return false;
                }
            }
            // Continue finding a partitioning of the plan such that the inner and outer partitions are independent, in order to feed a join.
            // Now, we look below the second unnest or join.
            VariableUtilities.getUsedVariables(unnestOrJoin, outerUsedVars);
            AbstractLogicalOperator unnestChild = (AbstractLogicalOperator) unnestOrJoin.getInputs().get(0).getValue();
            if (!findPlanPartition(unnestChild, innerUsedVars, outerUsedVars, innerOps, outerOps, topSelects, true)) {
                // We could not find a suitable partitioning.
                return false;
            }
        }
        innerRoot = buildOperatorChain(innerOps, ets, context);
        context.computeAndSetTypeEnvironmentForOperator(innerRoot);
        outerRoot = buildOperatorChain(outerOps, null, context);
        context.computeAndSetTypeEnvironmentForOperator(outerRoot);

        InnerJoinOperator product =
                new InnerJoinOperator(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
        product.setSourceLocation(op.getSourceLocation());
        // Outer branch.
        product.getInputs().add(new MutableObject<ILogicalOperator>(outerRoot));
        // Inner branch.
        product.getInputs().add(new MutableObject<ILogicalOperator>(innerRoot));
        context.computeAndSetTypeEnvironmentForOperator(product);
        // Put the selects on top of the join.
        ILogicalOperator topOp = product;
        if (!topSelects.isEmpty()) {
            topOp = buildOperatorChain(topSelects, product, context);
        }
        // Plug the selects + product in the plan.
        opRef.setValue(topOp);
        context.computeAndSetTypeEnvironmentForOperator(topOp);
        return true;
    }

    private ILogicalOperator buildOperatorChain(List<ILogicalOperator> ops, ILogicalOperator bottomOp,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator root = ops.get(0);
        ILogicalOperator prevOp = root;
        for (int i = 1; i < ops.size(); i++) {
            ILogicalOperator inputOp = ops.get(i);
            prevOp.getInputs().clear();
            prevOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
            prevOp = inputOp;
        }
        if (bottomOp != null) {
            context.computeAndSetTypeEnvironmentForOperator(bottomOp);
            prevOp.getInputs().clear();
            prevOp.getInputs().add(new MutableObject<ILogicalOperator>(bottomOp));
        }
        return root;
    }

    private boolean findPlanPartition(AbstractLogicalOperator op, HashSet<LogicalVariable> innerUsedVars,
            HashSet<LogicalVariable> outerUsedVars, List<ILogicalOperator> innerOps, List<ILogicalOperator> outerOps,
            List<ILogicalOperator> topSelects, boolean belowSecondUnnest) throws AlgebricksException {
        if (belowSecondUnnest && innerUsedVars.isEmpty()) {
            // Trivially joinable.
            return true;
        }
        if (!belowSecondUnnest) {
            // Bail on the following operators.
            switch (op.getOperatorTag()) {
                case AGGREGATE:
                case SUBPLAN:
                case GROUP:
                case UNNEST_MAP:
                    return false;
            }
        }
        switch (op.getOperatorTag()) {
            case UNNEST:
            case DATASOURCESCAN: {
                // We may have reached this state by descending through a subplan.
                outerOps.add(op);
                return true;
            }
            case INNERJOIN:
            case LEFTOUTERJOIN: {
                // Make sure that no variables that are live under this join are needed by the inner.
                List<LogicalVariable> liveVars = new ArrayList<LogicalVariable>();
                VariableUtilities.getLiveVariables(op, liveVars);
                for (LogicalVariable liveVar : liveVars) {
                    if (innerUsedVars.contains(liveVar)) {
                        return false;
                    }
                }
                outerOps.add(op);
                return true;
            }
            case SELECT: {
                // Remember this select to pulling it above the join.
                if (innerUsedVars.isEmpty()) {
                    outerOps.add(op);
                } else {
                    topSelects.add(op);
                }
                break;
            }
            case PROJECT: {
                // Throw away projects from the plan since we are pulling selects up.
                break;
            }
            case EMPTYTUPLESOURCE:
            case NESTEDTUPLESOURCE: {
                if (belowSecondUnnest) {
                    // We have successfully partitioned the plan into independent parts to be plugged into the join.
                    return true;
                } else {
                    // We could not find a second unnest or a join.
                    return false;
                }
            }
            default: {
                // The inner is trivially independent.
                if (!belowSecondUnnest && innerUsedVars.isEmpty()) {
                    outerOps.add(op);
                    break;
                }

                // Examine produced vars to determine which partition uses them.
                List<LogicalVariable> producedVars = new ArrayList<LogicalVariable>();
                VariableUtilities.getProducedVariables(op, producedVars);
                int outerMatches = 0;
                int innerMatches = 0;
                for (LogicalVariable producedVar : producedVars) {
                    if (outerUsedVars.contains(producedVar)) {
                        outerMatches++;
                    }
                    if (innerUsedVars.contains(producedVar)) {
                        innerMatches++;
                    }
                }

                HashSet<LogicalVariable> targetUsedVars = null;
                if (outerMatches == producedVars.size() && !producedVars.isEmpty()) {
                    // All produced vars used by outer partition.
                    outerOps.add(op);
                    targetUsedVars = outerUsedVars;
                }
                if (innerMatches == producedVars.size() && !producedVars.isEmpty()) {
                    // All produced vars used by inner partition.
                    innerOps.add(op);
                    targetUsedVars = innerUsedVars;
                }
                if (innerMatches == 0 && outerMatches == 0) {
                    // Op produces variables that are not used in the part of the plan we've seen (or it doesn't produce any vars).
                    // Try to figure out where it belongs by analyzing the used variables.
                    List<LogicalVariable> usedVars = new ArrayList<LogicalVariable>();
                    VariableUtilities.getUsedVariables(op, usedVars);
                    for (LogicalVariable usedVar : usedVars) {
                        boolean canBreak = false;
                        if (outerUsedVars.contains(usedVar)) {
                            outerOps.add(op);
                            targetUsedVars = outerUsedVars;
                            canBreak = true;
                        }
                        if (innerUsedVars.contains(usedVar)) {
                            innerOps.add(op);
                            targetUsedVars = innerUsedVars;
                            canBreak = true;
                        }
                        if (canBreak) {
                            break;
                        }
                    }
                } else if (innerMatches != 0 && outerMatches != 0) {
                    // The current operator produces variables that are used by both partitions, so the inner and outer are not independent and, therefore, we cannot create a join.
                    // TODO: We may still be able to split the operator to create a viable partitioning.
                    return false;
                }

                // TODO: For now we bail here, but we could remember such ops and determine their target partition at a later point.
                if (targetUsedVars == null) {
                    return false;
                }

                // Update used variables of partition that op belongs to.
                if (op.hasNestedPlans() && op.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
                    AbstractOperatorWithNestedPlans opWithNestedPlans = (AbstractOperatorWithNestedPlans) op;
                    opWithNestedPlans.getUsedVariablesExceptNestedPlans(targetUsedVars);
                } else {
                    VariableUtilities.getUsedVariables(op, targetUsedVars);
                }
                break;
            }
        }
        if (!op.hasInputs()) {
            if (!belowSecondUnnest) {
                // We could not find a second unnest or a join.
                return false;
            } else {
                // We have successfully partitioned the plan into independent parts to be plugged into the join.
                return true;
            }
        }
        return findPlanPartition((AbstractLogicalOperator) op.getInputs().get(0).getValue(), innerUsedVars,
                outerUsedVars, innerOps, outerOps, topSelects, belowSecondUnnest);
    }

    /**
     * check whether the operator is inside a sub-plan
     *
     * @param nestedRootRef
     * @return true-if it is; false otherwise.
     */
    private boolean insideSubplan(Mutable<ILogicalOperator> nestedRootRef) {
        AbstractLogicalOperator nestedRoot = (AbstractLogicalOperator) nestedRootRef.getValue();
        if (nestedRoot.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return true;
        }
        List<Mutable<ILogicalOperator>> inputs = nestedRoot.getInputs();
        for (Mutable<ILogicalOperator> input : inputs) {
            if (insideSubplan(input)) {
                return true;
            }
        }
        return false;
    }
}
