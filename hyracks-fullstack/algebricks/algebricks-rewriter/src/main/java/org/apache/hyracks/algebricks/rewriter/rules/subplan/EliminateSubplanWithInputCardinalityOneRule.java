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

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule eliminates a subplan with the following pattern:
 * -- SUBPLAN
 * -- OP (where OP produces exactly one tuple)
 * The live variables at OP will not be used after SUBPLAN.
 * Note: This rule must be applied after
 * the RemoveRedundantVariablesRule (to avoid the lineage analysis of variable cardinality).
 *
 * @author yingyib
 */
public class EliminateSubplanWithInputCardinalityOneRule implements IAlgebraicRewriteRule {
    /** The pointer to the topmost operator */
    private Mutable<ILogicalOperator> rootRef;
    /** Whether the rule has even been invoked */
    private boolean invoked = false;

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (!invoked) {
            rootRef = opRef;
            invoked = true;
        }
        return rewriteForOperator(rootRef, opRef, context);
    }

    private boolean rewriteForOperator(Mutable<ILogicalOperator> rootRef, Mutable<ILogicalOperator> opRef,
            IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getInputs().size() <= 0) {
            return false;
        }
        boolean changed = false;

        for (Mutable<ILogicalOperator> currentOpRef : op.getInputs()) {
            AbstractLogicalOperator op1 = (AbstractLogicalOperator) currentOpRef.getValue();
            if (op1.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
                changed |= rewriteForOperator(rootRef, currentOpRef, context);
                continue;
            }

            SubplanOperator subplan = (SubplanOperator) op1;
            // TODO(buyingyi): figure out the rewriting for subplan operators with multiple subplans.
            if (subplan.getNestedPlans().size() != 1) {
                continue;
            }

            Set<LogicalVariable> usedVarsUp = new ListSet<>();
            OperatorPropertiesUtil.getFreeVariablesInPath(rootRef.getValue(), subplan, usedVarsUp);

            // Recursively rewrites the pipelines inside a nested subplan.
            for (Mutable<ILogicalOperator> nestedRootRef : subplan.getNestedPlans().get(0).getRoots()) {
                changed |= rewriteForOperator(nestedRootRef, nestedRootRef, context);
            }

            Mutable<ILogicalOperator> subplanInputOperatorRef = subplan.getInputs().get(0);
            ILogicalOperator subplanInputOperator = subplanInputOperatorRef.getValue();
            Set<LogicalVariable> subplanInputVars = new ListSet<>();
            VariableUtilities.getLiveVariables(subplanInputOperator, subplanInputVars);
            // Make sure that subplan input vars are only used inside the subplan (i.e. not used above the subplan op)
            if (!OperatorPropertiesUtil.disjoint(subplanInputVars, usedVarsUp)) {
                continue;
            }
            if (!OperatorPropertiesUtil.isCardinalityExactOne(subplanInputOperator)) {
                continue;
            }
            /* The subplan can be removed. */
            ILogicalPlan plan = subplan.getNestedPlans().get(0);
            List<Mutable<ILogicalOperator>> rootRefs = plan.getRoots();
            // TODO(buyingyi): investigate the case of multi-root plans.
            if (rootRefs.size() != 1) {
                continue;
            }

            // Replaces all Nts' in the nested plan with the Subplan input operator or its deep copy.
            ILogicalOperator topOperator = rootRefs.get(0).getValue();
            ReplaceNtsWithSubplanInputOperatorVisitor visitor =
                    new ReplaceNtsWithSubplanInputOperatorVisitor(context, subplan);
            ILogicalOperator newTopOperator = topOperator.accept(visitor, null);
            currentOpRef.setValue(newTopOperator);
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(newTopOperator, context);
            changed = true;
        }

        return changed;
    }
}
