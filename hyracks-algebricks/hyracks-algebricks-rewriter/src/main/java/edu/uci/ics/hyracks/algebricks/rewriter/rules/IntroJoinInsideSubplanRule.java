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

import java.util.HashSet;
import java.util.ListIterator;
import java.util.Set;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class IntroJoinInsideSubplanRule extends AbstractDecorrelationRule {

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getOperator();
        if (op0.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op0;

        LogicalOperatorReference leftRef = subplan.getInputs().get(0);
        if (((AbstractLogicalOperator) leftRef.getOperator()).getOperatorTag() == LogicalOperatorTag.EMPTYTUPLESOURCE) {
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
        LogicalOperatorReference opRef1 = p.getRoots().get(0);

        while (true) {
            AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef1.getOperator();
            if (op1.getInputs().size() != 1) {
                return false;
            }
            if (op1.getOperatorTag() == LogicalOperatorTag.SELECT) {
                LogicalOperatorReference op2Ref = op1.getInputs().get(0);
                AbstractLogicalOperator op2 = (AbstractLogicalOperator) op2Ref.getOperator();
                if (op2.getOperatorTag() != LogicalOperatorTag.SELECT && descOrSelfIsScanOrJoin(op2)) {
                    Set<LogicalVariable> free2 = new HashSet<LogicalVariable>();
                    OperatorPropertiesUtil.getFreeVariablesInSelfOrDesc(op2, free2);
                    if (free2.isEmpty()) {
                        Set<LogicalVariable> free1 = new HashSet<LogicalVariable>();
                        OperatorPropertiesUtil.getFreeVariablesInSelfOrDesc(op1, free1);
                        if (!free1.isEmpty()) {
                            OperatorManipulationUtil.ntsToEts(op2Ref, context);
                            NestedTupleSourceOperator nts = new NestedTupleSourceOperator(new LogicalOperatorReference(
                                    subplan));
                            LogicalOperatorReference ntsRef = new LogicalOperatorReference(nts);
                            LogicalOperatorReference innerRef = new LogicalOperatorReference(op2);
                            InnerJoinOperator join = new InnerJoinOperator(new LogicalExpressionReference(
                                    ConstantExpression.TRUE), ntsRef, innerRef);
                            op2Ref.setOperator(join);
                            context.computeAndSetTypeEnvironmentForOperator(nts);
                            context.computeAndSetTypeEnvironmentForOperator(join);
                            return true;
                        }
                    }
                }
            }
            opRef1 = op1.getInputs().get(0);
        }
    }

}
