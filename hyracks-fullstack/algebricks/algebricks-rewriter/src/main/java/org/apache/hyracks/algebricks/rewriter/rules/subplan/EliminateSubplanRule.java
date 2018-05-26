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

import java.util.LinkedList;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class EliminateSubplanRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    /**
     * Eliminate Subplan above ETS
     * and Subplan that has only ops. with one input and no free vars.
     */
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op;

        Mutable<ILogicalOperator> outerRef = subplan.getInputs().get(0);
        AbstractLogicalOperator outerRefOp = (AbstractLogicalOperator) outerRef.getValue();
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

    private boolean elimOneSubplanWithNoFreeVars(Mutable<ILogicalOperator> opRef) {
        SubplanOperator subplan = (SubplanOperator) opRef.getValue();
        AbstractLogicalOperator rootOp =
                (AbstractLogicalOperator) subplan.getNestedPlans().get(0).getRoots().get(0).getValue();
        if (rootOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE
                || rootOp.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            opRef.setValue(subplan.getInputs().get(0).getValue());
            return true;
        } else {
            AbstractLogicalOperator botOp = rootOp;
            if (botOp.getInputs().size() != 1) {
                return false;
            }
            do {
                Mutable<ILogicalOperator> botRef = botOp.getInputs().get(0);
                botOp = (AbstractLogicalOperator) botRef.getValue();
                if (botOp.getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
                    botRef.setValue(subplan.getInputs().get(0).getValue());
                    opRef.setValue(rootOp);
                    return true;
                }
            } while (botOp.getInputs().size() == 1);
            return false;
        }
    }

    private void elimSubplanOverEts(Mutable<ILogicalOperator> opRef, IOptimizationContext ctx)
            throws AlgebricksException {
        SubplanOperator subplan = (SubplanOperator) opRef.getValue();
        SourceLocation sourceLoc = subplan.getSourceLocation();
        for (ILogicalPlan p : subplan.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                OperatorManipulationUtil.ntsToEts(r, ctx);
            }
        }
        LinkedList<Mutable<ILogicalOperator>> allRoots = subplan.allRootsInReverseOrder();
        if (allRoots.size() == 1) {
            opRef.setValue(allRoots.get(0).getValue());
        } else {
            ILogicalOperator topOp = null;
            for (Mutable<ILogicalOperator> r : allRoots) {
                if (topOp == null) {
                    topOp = r.getValue();
                } else {
                    InnerJoinOperator j =
                            new InnerJoinOperator(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
                    j.setSourceLocation(sourceLoc);
                    j.getInputs().add(new MutableObject<ILogicalOperator>(topOp));
                    j.getInputs().add(r);
                    ctx.setOutputTypeEnvironment(j, j.computeOutputTypeEnvironment(ctx));
                    topOp = j;
                }
            }
            opRef.setValue(topOp);
        }
    }
}
