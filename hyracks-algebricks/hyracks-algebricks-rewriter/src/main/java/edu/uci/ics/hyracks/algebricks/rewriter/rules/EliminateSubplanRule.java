/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import java.util.LinkedList;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class EliminateSubplanRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) {
        return false;
    }

    /**
     * Eliminate Subplan above ETS
     * 
     * and Subplan that has only ops. with one input and no free vars. (could we
     * modify it to consider free vars which are sources of Unnest or Assign, if
     * there are no aggregates?)
     */
    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getOperator();
        if (op.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op;

        LogicalOperatorReference outerRef = subplan.getInputs().get(0);
        AbstractLogicalOperator outerRefOp = (AbstractLogicalOperator) outerRef.getOperator();
        if (outerRefOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
            elimSubplanOverEts(opRef, context);
            return true;
        }
        if (subplan.getNestedPlans().size() == 1 && subplan.getNestedPlans().get(0).getRoots().size() == 1
                && !OperatorPropertiesUtil.hasFreeVariables(subplan)) {
            if (elimOneSubplanWithNoFreeVars(opRef)) {
                return true;
            }
        }

        return false;
    }

    private boolean elimOneSubplanWithNoFreeVars(LogicalOperatorReference opRef) {
        SubplanOperator subplan = (SubplanOperator) opRef.getOperator();
        AbstractLogicalOperator rootOp = (AbstractLogicalOperator) subplan.getNestedPlans().get(0).getRoots().get(0)
                .getOperator();
        if (rootOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
            opRef.setOperator(subplan.getInputs().get(0).getOperator());
            return true;
        } else {
            AbstractLogicalOperator botOp = rootOp;
            if (botOp.getInputs().size() != 1) {
                return false;
            }
            do {
                LogicalOperatorReference botRef = botOp.getInputs().get(0);
                botOp = (AbstractLogicalOperator) botRef.getOperator();
                if (botOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                    botRef.setOperator(subplan.getInputs().get(0).getOperator());
                    opRef.setOperator(rootOp);
                    return true;
                }
            } while (botOp.getInputs().size() == 1);
            return false;
        }
    }

    private void elimSubplanOverEts(LogicalOperatorReference opRef, IOptimizationContext ctx)
            throws AlgebricksException {
        SubplanOperator subplan = (SubplanOperator) opRef.getOperator();
        for (ILogicalPlan p : subplan.getNestedPlans()) {
            for (LogicalOperatorReference r : p.getRoots()) {
                OperatorManipulationUtil.ntsToEts(r, ctx);
            }
        }
        LinkedList<LogicalOperatorReference> allRoots = subplan.allRootsInReverseOrder();
        if (allRoots.size() == 1) {
            opRef.setOperator(allRoots.get(0).getOperator());
        } else {
            ILogicalOperator topOp = null;
            for (LogicalOperatorReference r : allRoots) {
                if (topOp == null) {
                    topOp = r.getOperator();
                } else {
                    LeftOuterJoinOperator j = new LeftOuterJoinOperator(new LogicalExpressionReference(
                            ConstantExpression.TRUE));
                    j.getInputs().add(new LogicalOperatorReference(topOp));
                    j.getInputs().add(r);
                    ctx.setOutputTypeEnvironment(j, j.computeOutputTypeEnvironment(ctx));
                    topOp = j;
                }
            }
            opRef.setOperator(topOp);
        }
    }
}
