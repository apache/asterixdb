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

import java.util.Iterator;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class InsertOuterJoinRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getOperator();
        if (op0.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op0;

        Iterator<ILogicalPlan> plansIter = subplan.getNestedPlans().iterator();
        ILogicalPlan p = null;
        while (plansIter.hasNext()) {
            p = plansIter.next();
        }
        if (p == null) {
            return false;
        }
        if (p.getRoots().size() != 1) {
            return false;
        }
        LogicalOperatorReference subplanRoot = p.getRoots().get(0);
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) subplanRoot.getOperator();
        LogicalOperatorReference opUnder = subplan.getInputs().get(0);

        if (OperatorPropertiesUtil.isNullTest((AbstractLogicalOperator) opUnder.getOperator())) {
            return false;
        }

        switch (op1.getOperatorTag()) {
            case INNERJOIN: {
                InnerJoinOperator join = (InnerJoinOperator) op1;
                LogicalOperatorReference leftRef = join.getInputs().get(0);
                LogicalOperatorReference rightRef = join.getInputs().get(1);
                LogicalOperatorReference ntsRef = getNtsAtEndOfPipeline(leftRef);
                if (ntsRef == null) {
                    ntsRef = getNtsAtEndOfPipeline(rightRef);
                    if (ntsRef == null) {
                        return false;
                    } else {
                        LogicalOperatorReference t = leftRef;
                        leftRef = rightRef;
                        rightRef = t;
                    }
                }
                ntsRef.setOperator(opUnder.getOperator());
                LeftOuterJoinOperator loj = new LeftOuterJoinOperator(join.getCondition());
                loj.getInputs().add(leftRef);
                loj.getInputs().add(rightRef);
                opRef.setOperator(loj);
                context.computeAndSetTypeEnvironmentForOperator(loj);
                return true;
            }
            case LEFTOUTERJOIN: {
                LeftOuterJoinOperator join = (LeftOuterJoinOperator) op1;
                LogicalOperatorReference leftRef = join.getInputs().get(0);
                LogicalOperatorReference ntsRef = getNtsAtEndOfPipeline(leftRef);
                if (ntsRef == null) {
                    return false;
                }
                ntsRef.setOperator(opUnder.getOperator());
                opRef.setOperator(join);
                context.computeAndSetTypeEnvironmentForOperator(join);
                return true;
            }
            default: {
                return false;
            }
        }
    }

    private LogicalOperatorReference getNtsAtEndOfPipeline(LogicalOperatorReference opRef) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getOperator();
        if (op.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return opRef;
        }
        if (op.getInputs().size() != 1) {
            return null;
        }
        return getNtsAtEndOfPipeline(op.getInputs().get(0));
    }

}
