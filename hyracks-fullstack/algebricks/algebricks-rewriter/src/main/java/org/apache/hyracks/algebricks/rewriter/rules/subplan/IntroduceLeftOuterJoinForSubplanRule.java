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

import java.util.Iterator;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IntroduceLeftOuterJoinForSubplanRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getValue();
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
        Mutable<ILogicalOperator> subplanRoot = p.getRoots().get(0);
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) subplanRoot.getValue();
        Mutable<ILogicalOperator> opUnder = subplan.getInputs().get(0);

        if (OperatorPropertiesUtil.isMissingTest((AbstractLogicalOperator) opUnder.getValue())) {
            return false;
        }

        switch (op1.getOperatorTag()) {
            case INNERJOIN: {
                InnerJoinOperator join = (InnerJoinOperator) op1;
                Mutable<ILogicalOperator> leftRef = join.getInputs().get(0);
                Mutable<ILogicalOperator> rightRef = join.getInputs().get(1);

                Mutable<ILogicalOperator> ntsRef = getNtsAtEndOfPipeline(leftRef);
                if (ntsRef == null) {
                    ntsRef = getNtsAtEndOfPipeline(rightRef);
                    if (ntsRef == null) {
                        return false;
                    } else {
                        Mutable<ILogicalOperator> t = leftRef;
                        leftRef = rightRef;
                        rightRef = t;
                    }
                }
                ntsRef.setValue(opUnder.getValue());
                LeftOuterJoinOperator loj = new LeftOuterJoinOperator(join.getCondition());
                loj.setSourceLocation(join.getSourceLocation());
                loj.getInputs().add(leftRef);
                loj.getInputs().add(rightRef);
                opRef.setValue(loj);
                context.computeAndSetTypeEnvironmentForOperator(loj);
                return true;
            }
            case LEFTOUTERJOIN: {
                LeftOuterJoinOperator join = (LeftOuterJoinOperator) op1;
                Mutable<ILogicalOperator> leftRef = join.getInputs().get(0);
                Mutable<ILogicalOperator> ntsRef = getNtsAtEndOfPipeline(leftRef);
                if (ntsRef == null) {
                    return false;
                }
                ntsRef.setValue(opUnder.getValue());
                opRef.setValue(join);
                context.computeAndSetTypeEnvironmentForOperator(join);
                return true;
            }
            default: {
                return false;
            }
        }
    }

    private Mutable<ILogicalOperator> getNtsAtEndOfPipeline(Mutable<ILogicalOperator> opRef) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            return opRef;
        }
        if (op.getInputs().size() != 1) {
            return null;
        }
        return getNtsAtEndOfPipeline(op.getInputs().get(0));
    }

}
