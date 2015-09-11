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

import java.util.HashSet;
import java.util.ListIterator;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * The rule searches for operators that can be moved outside the subplan.
 *
 * <pre>
 * Before
 * 
 *   %PARENT_PLAN
 *   SUBPLAN{
 *     %NESTED_OPERATORS_B+
 *     ASSIGN || %SUBPLAN
 *     %NESTED_OPERATORS_A*
 *     NESTEDTUPLESOURCE
 *   }
 *   %CHILD_PLAN
 * 
 *   where
 *     %SUBPLAN has one nested plan with a root AGGREGATE operator.
 * 
 * After
 * 
 *   %PARENT_PLAN
 *   SUBPLAN{
 *     %NESTED_OPERATORS_B+
 *     %NESTED_OPERATORS_A*
 *     NESTEDTUPLESOURCE
 *   }
 *   ASSIGN || %SUBPLAN
 *   %CHILD_PLAN
 * </pre>
 */
public class MoveFreeVariableOperatorOutOfSubplanRule extends AbstractDecorrelationRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getValue();
        if (op0.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op0;

        Mutable<ILogicalOperator> leftRef = subplan.getInputs().get(0);
        if (((AbstractLogicalOperator) leftRef.getValue()).getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
            return false;
        }

        ListIterator<ILogicalPlan> plansIter = subplan.getNestedPlans().listIterator();
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
        Mutable<ILogicalOperator> opRef1 = p.getRoots().get(0);

        //The root operator will not be movable. Start with the second op
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef1.getValue();
        if (op1.getInputs().size() != 1) {
            return false;
        }
        Mutable<ILogicalOperator> op2Ref = op1.getInputs().get(0);

        //Get all variables that come from outside of the loop
        Set<LogicalVariable> free = new HashSet<LogicalVariable>();
        OperatorPropertiesUtil.getFreeVariablesInSelfOrDesc(op1, free);

        while (op2Ref != null) {
            //Get the operator that we want to look at
            AbstractLogicalOperator op2 = (AbstractLogicalOperator) op2Ref.getValue();

            //Make sure we are looking at subplan with a scan/join
            if (op2.getInputs().size() != 1 || !descOrSelfIsScanOrJoin(op2)) {
                return false;
            }
            boolean notApplicable = false;

            //Get its used variables
            Set<LogicalVariable> used = new HashSet<LogicalVariable>();

            //not movable if the operator is not an assign or subplan
            //Might be helpful in the future for other operations in the future
            if (movableOperator(op2.getOperatorTag())) {
                if (op2.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                    VariableUtilities.getUsedVariables(op2, used);
                } else if (op2.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                    // Nested plan must have an aggregate root.
                    ListIterator<ILogicalPlan> subplansIter = ((SubplanOperator) op2).getNestedPlans().listIterator();
                    ILogicalPlan plan = null;
                    while (subplansIter.hasNext()) {
                        plan = subplansIter.next();
                    }
                    if (plan == null) {
                        return false;
                    }
                    if (plan.getRoots().size() != 1) {
                        return false;
                    }
                    ILogicalOperator op3 = plan.getRoots().get(0).getValue();
                    if (op3.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
                        return false;
                    }
                    // Used variables do not include ones created in the subplan.
                    VariableUtilities.getUsedVariables(op2, used);
                    Set<LogicalVariable> subplanProducedAndDown = new HashSet<LogicalVariable>();
                    VariableUtilities.getProducedVariablesInDescendantsAndSelf(op3, subplanProducedAndDown);
                    used.removeAll(subplanProducedAndDown);
                } else {
                    notApplicable = true;
                }
            } else {
                notApplicable = true;
            }

            //Make sure that all of its used variables come from outside
            for (LogicalVariable var : used) {
                if (!free.contains(var)) {
                    notApplicable = true;
                }
            }

            if (notApplicable) {
                op2Ref = op2.getInputs().get(0);
            } else {
                //Make the input of op2 be the input of op1
                op2Ref.setValue(op2.getInputs().get(0).getValue());

                //Make the outside of the subplan the input of op2
                Mutable<ILogicalOperator> outsideRef = op2.getInputs().get(0);
                outsideRef.setValue(op0.getInputs().get(0).getValue());

                //Make op2 the input of the subplan
                Mutable<ILogicalOperator> op2OutsideRef = op0.getInputs().get(0);
                op2OutsideRef.setValue(op2);

                return true;
            }

        }
        return false;
    }

    protected boolean movableOperator(LogicalOperatorTag operatorTag) {
        return (operatorTag == LogicalOperatorTag.ASSIGN || operatorTag == LogicalOperatorTag.SUBPLAN);
    }
}
