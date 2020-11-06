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
package org.apache.hyracks.algebricks.core.rewriter.base;

import java.util.Collection;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.util.LogRedactionUtil;

public abstract class AbstractRuleController {

    protected final boolean isTraceEnabled;

    protected boolean isSanityCheckEnabled;

    protected IOptimizationContext context;

    public AbstractRuleController() {
        isTraceEnabled = AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled();
    }

    public void setContext(IOptimizationContext context) {
        this.context = context;
        this.isSanityCheckEnabled = context.getPhysicalOptimizationConfig().isSanityCheckEnabled();
    }

    /**
     * Each rewriting strategy may differ in the
     *
     * @param root
     * @param rules
     * @return true iff one of the rules in the collection fired
     */
    public abstract boolean rewriteWithRuleCollection(Mutable<ILogicalOperator> root,
            Collection<IAlgebraicRewriteRule> rules) throws AlgebricksException;

    /**
     * @param opRef
     * @param rule
     * @return true if any rewrite was fired, either on opRef or any operator
     *         under it.
     */
    protected boolean rewriteOperatorRef(Mutable<ILogicalOperator> opRef, IAlgebraicRewriteRule rule)
            throws AlgebricksException {
        return rewriteOperatorRef(opRef, rule, true, false);
    }

    protected boolean rewriteOperatorRef(Mutable<ILogicalOperator> opRef, IAlgebraicRewriteRule rule,
            boolean enterNestedPlans, boolean fullDFS) throws AlgebricksException {

        String preBeforePlan = getPlanString(opRef);
        sanityCheckBeforeRewrite(rule, opRef);
        if (rule.rewritePre(opRef, context)) {
            String preAfterPlan = getPlanString(opRef);
            printRuleApplication(rule, "fired", preBeforePlan, preAfterPlan);
            sanityCheckAfterRewrite(rule, opRef, true, preBeforePlan);
            return true;
        } else {
            sanityCheckAfterRewrite(rule, opRef, false, preBeforePlan);
        }

        boolean rewritten = false;
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        for (Mutable<ILogicalOperator> inp : op.getInputs()) {
            if (rewriteOperatorRef(inp, rule, enterNestedPlans, fullDFS)) {
                rewritten = true;
                if (!fullDFS) {
                    break;
                }
            }
        }

        if (op.hasNestedPlans() && enterNestedPlans) {
            AbstractOperatorWithNestedPlans o2 = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : o2.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r : p.getRoots()) {
                    if (rewriteOperatorRef(r, rule, enterNestedPlans, fullDFS)) {
                        rewritten = true;
                        if (!fullDFS) {
                            break;
                        }
                    }
                }
                if (rewritten && !fullDFS) {
                    break;
                }
            }
        }

        String postBeforePlan = getPlanString(opRef);
        sanityCheckBeforeRewrite(rule, opRef);
        if (rule.rewritePost(opRef, context)) {
            String postAfterPlan = getPlanString(opRef);
            printRuleApplication(rule, "fired", postBeforePlan, postAfterPlan);
            sanityCheckAfterRewrite(rule, opRef, true, postBeforePlan);
            return true;
        } else {
            sanityCheckAfterRewrite(rule, opRef, false, postBeforePlan);
        }

        return rewritten;
    }

    private void sanityCheckBeforeRewrite(IAlgebraicRewriteRule rule, Mutable<ILogicalOperator> opRef)
            throws AlgebricksException {
        if (isSanityCheckEnabled) {
            sanityCheckBeforeRewriteImpl(rule, opRef);
        }
    }

    private void sanityCheckAfterRewrite(IAlgebraicRewriteRule rule, Mutable<ILogicalOperator> opRef, boolean rewritten,
            String beforePlan) throws AlgebricksException {
        if (isSanityCheckEnabled) {
            sanityCheckAfterRewriteImpl(rule, opRef, rewritten, beforePlan);
        }
    }

    private void sanityCheckBeforeRewriteImpl(IAlgebraicRewriteRule rule, Mutable<ILogicalOperator> opRef)
            throws AlgebricksException {
        try {
            context.getPlanStabilityVerifier().recordPlanSignature(opRef);
        } catch (AlgebricksException e) {
            throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE,
                    String.format("Illegal state before rule %s. %s", rule.getClass().getName(), e.getMessage()));
        }
    }

    private void sanityCheckAfterRewriteImpl(IAlgebraicRewriteRule rule, Mutable<ILogicalOperator> opRef,
            boolean rewritten, String beforePlan) throws AlgebricksException {
        if (rewritten) {
            try {
                context.getPlanStructureVerifier().verifyPlanStructure(opRef);
            } catch (AlgebricksException e) {
                throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE,
                        String.format("Fired rule %s produced illegal %s", rule.getClass().getName(), e.getMessage()));
            }
        } else {
            try {
                context.getPlanStabilityVerifier().comparePlanSignature(opRef);
            } catch (AlgebricksException e) {
                if (isTraceEnabled) {
                    printRuleApplication(rule, "not fired, but failed sanity check: " + e.getMessage(), beforePlan,
                            getPlanString(opRef));
                }
                throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE,
                        String.format("Non-fired rule %s unexpectedly %s", rule.getClass().getName(), e.getMessage()));
            }
        }
        context.getPlanStabilityVerifier().discardPlanSignature();
    }

    private String getPlanString(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
        if (isTraceEnabled && context != null) {
            IPlanPrettyPrinter prettyPrinter = context.getPrettyPrinter();
            return prettyPrinter.reset().printOperator((AbstractLogicalOperator) opRef.getValue()).toString();
        }
        return null;
    }

    private void printRuleApplication(IAlgebraicRewriteRule rule, String status, String beforePlan, String afterPlan) {
        if (isTraceEnabled) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace(">> Rule " + rule.getClass().getName() + " " + status + "\n");
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace(">> Before plan\n" + LogRedactionUtil.userData(beforePlan) + "\n");
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace(">> After plan\n" + LogRedactionUtil.userData(afterPlan) + "\n");
        }
    }
}
