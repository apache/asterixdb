/*
 * Copyright 2009-2014 by The Regents of the University of California
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

import java.util.HashSet;
import java.util.ListIterator;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

public class MoveFreeVariableOperatorOutOfSubplanRule extends AbstractDecorrelationRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        /*
         * This rule looks for an assign within a subplan that uses only 
         * variables from outside of the subplan
         * 
         * It moves this assign outside of the subplan
         * 
         */
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
            VariableUtilities.getUsedVariables(op2, used);

            //not movable if the operator is not an assign
            //Might be helpful in the future for other operations in the future
            if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
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

}
