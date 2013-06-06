/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.rewriter.base;

import java.util.Collection;
import java.util.logging.Level;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import edu.uci.ics.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import edu.uci.ics.hyracks.algebricks.core.config.AlgebricksConfig;

public abstract class AbstractRuleController {

    protected IOptimizationContext context;
    private LogicalOperatorPrettyPrintVisitor pvisitor = new LogicalOperatorPrettyPrintVisitor();

    public AbstractRuleController() {
    }

    public void setContext(IOptimizationContext context) {
        this.context = context;
    }

    /**
     * Each rewriting strategy may differ in the
     * 
     * @param root
     * @param ruleClasses
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

    private String getPlanString(Mutable<ILogicalOperator> opRef) throws AlgebricksException {
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isLoggable(Level.FINE)) {
            StringBuilder sb = new StringBuilder();
            PlanPrettyPrinter.printOperator((AbstractLogicalOperator) opRef.getValue(), sb, pvisitor, 0);
            return sb.toString();
        }
        return null;
    }

    private void printRuleApplication(IAlgebraicRewriteRule rule, String beforePlan, String afterPlan)
            throws AlgebricksException {
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isLoggable(Level.FINE)) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.fine(">>>> Rule " + rule.getClass() + " fired.\n");
            AlgebricksConfig.ALGEBRICKS_LOGGER.fine(">>>> Before plan\n" + beforePlan + "\n");
            AlgebricksConfig.ALGEBRICKS_LOGGER.fine(">>>> After plan\n" + afterPlan + "\n");
        }
    }

    protected boolean rewriteOperatorRef(Mutable<ILogicalOperator> opRef, IAlgebraicRewriteRule rule,
            boolean enterNestedPlans, boolean fullDFS) throws AlgebricksException {

        String preBeforePlan = getPlanString(opRef);
        if (rule.rewritePre(opRef, context)) {
            String preAfterPlan = getPlanString(opRef);
            printRuleApplication(rule, preBeforePlan, preAfterPlan);
            return true;
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
        if (rule.rewritePost(opRef, context)) {
            String postAfterPlan = getPlanString(opRef);
            printRuleApplication(rule, postBeforePlan, postAfterPlan);
            return true;
        }

        return rewritten;
    }
}
