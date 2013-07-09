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
package edu.uci.ics.asterix.optimizer.rules;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IntroduceTransactionCommitByAssignOpRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator selectOperator = (SelectOperator) op;

        Mutable<ILogicalOperator> childOfSelect = selectOperator.getInputs().get(0);

        //[Direction] SelectOp(cond1)<--ChildOps... ==> SelectOp(booleanValue of cond1)<--NewAssignOp(cond1)<--ChildOps... 
        //#. Create an assign-operator with a new local variable and the condition of the select-operator. 
        //#. Set the input(child operator) of the new assign-operator to input(child operator) of the select-operator.
        //	 (Later, the newly created assign-operator will apply the condition on behalf of the select-operator, 
        //    and set the variable of the assign-operator to a boolean value according to the condition evaluation.)
        //#. Give the select-operator the result boolean value created by the newly created child assign-operator.

        //create an assignOp with a variable and the condition of the select-operator. 
        LogicalVariable v = context.newVar();
        AssignOperator assignOperator = new AssignOperator(v, new MutableObject<ILogicalExpression>(selectOperator
                .getCondition().getValue()));

        //set the input of the new assign-operator to the input of the select-operator.
        assignOperator.getInputs().add(childOfSelect);
        
        //set the result value of the assign-operator to the condition of the select-operator
        selectOperator.getCondition().setValue(new VariableReferenceExpression(v));//scalarFunctionCallExpression);
        selectOperator.getInputs().set(0, new MutableObject<ILogicalOperator>(assignOperator));

        context.computeAndSetTypeEnvironmentForOperator(assignOperator);

        //Once this rule is fired, don't apply again.
        context.addToDontApplySet(this, selectOperator);
        return true;
    }
}
