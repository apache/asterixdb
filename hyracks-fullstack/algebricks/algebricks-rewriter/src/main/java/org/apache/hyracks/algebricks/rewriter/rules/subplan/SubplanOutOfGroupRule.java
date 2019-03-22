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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Looks for a nested group-by plan ending in
 * subplan {
 * ...
 * }
 * select (function-call: algebricks:not, Args:[function-call:
 * algebricks:is-null, Args:[...]])
 * nested tuple source -- |UNPARTITIONED|
 */

public class SubplanOutOfGroupRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getValue();
        if (op0.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        GroupByOperator gby = (GroupByOperator) op0;

        Iterator<ILogicalPlan> plansIter = gby.getNestedPlans().iterator();
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
        Mutable<ILogicalOperator> op1Ref = p.getRoots().get(0);
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) op1Ref.getValue();
        boolean found = false;
        while (op1.getInputs().size() == 1) {
            if (op1.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                SubplanOperator subplan = (SubplanOperator) op1;
                AbstractLogicalOperator op2 = (AbstractLogicalOperator) subplan.getInputs().get(0).getValue();
                if (OperatorPropertiesUtil.isMissingTest(op2)) {
                    if (subplan.getNestedPlans().size() == 1) {
                        ILogicalPlan p1 = subplan.getNestedPlans().get(0);
                        if (p1.getRoots().size() == 1) {
                            AbstractLogicalOperator r1 = (AbstractLogicalOperator) p1.getRoots().get(0).getValue();
                            if (r1.getOperatorTag() == LogicalOperatorTag.INNERJOIN
                                    || r1.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN) {
                                // now, check that it propagates all variables,
                                // so it can be pushed
                                List<LogicalVariable> op2Vars = new ArrayList<LogicalVariable>();
                                VariableUtilities.getLiveVariables(op2, op2Vars);
                                List<LogicalVariable> op1Vars = new ArrayList<LogicalVariable>();
                                VariableUtilities.getLiveVariables(subplan, op1Vars);
                                if (op1Vars.containsAll(op2Vars)) {
                                    found = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            op1Ref = op1.getInputs().get(0);
            op1 = (AbstractLogicalOperator) op1Ref.getValue();
        }
        if (!found) {
            return false;
        }

        ILogicalOperator subplan = op1;
        ILogicalOperator op2 = op1.getInputs().get(0).getValue();
        op1Ref.setValue(op2);
        Mutable<ILogicalOperator> opUnderRef = gby.getInputs().get(0);
        ILogicalOperator opUnder = opUnderRef.getValue();
        subplan.getInputs().clear();
        subplan.getInputs().add(new MutableObject<ILogicalOperator>(opUnder));
        opUnderRef.setValue(subplan);

        return true;
    }
}
