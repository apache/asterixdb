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
package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.ListSet;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule is to convert an outer join into an inner join when possible.
 * 
 * The specific pattern this rule will invoke for is:
 * select not(is-null($v)) // $v is from the right branch of the left outer join below
 *   left-outer-join
 * 
 * The pattern will be rewritten to:
 * inner-join
 * 
 * @author yingyib
 */
public class LeftOuterJoinToInnerJoinRule implements IAlgebraicRewriteRule {

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
        Mutable<ILogicalOperator> op2Ref = op.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op2Ref.getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }
        SelectOperator selectOp = (SelectOperator) op;
        LeftOuterJoinOperator joinOp = (LeftOuterJoinOperator) op2;
        ILogicalExpression condition = selectOp.getCondition().getValue();
        if (condition.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        ScalarFunctionCallExpression func = (ScalarFunctionCallExpression) condition;
        /** check if the filter condition on top of the LOJ is not(is-null($v)), where $v is from the right child of LOJ */
        if (!convertable(func, joinOp)) {
            return false;
        }
        ILogicalOperator newJoin = new InnerJoinOperator(joinOp.getCondition(), joinOp.getInputs().get(0), joinOp
                .getInputs().get(1));
        opRef.setValue(newJoin);
        context.computeAndSetTypeEnvironmentForOperator(newJoin);
        return true;
    }

    /**
     * check if the condition is not(is-null(var)) and var is from the right branch of the join
     */
    private boolean convertable(ScalarFunctionCallExpression func, LeftOuterJoinOperator join)
            throws AlgebricksException {
        if (func.getFunctionIdentifier() != AlgebricksBuiltinFunctions.NOT) {
            return false;
        }
        ILogicalExpression arg = func.getArguments().get(0).getValue();
        if (arg.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        ScalarFunctionCallExpression func2 = (ScalarFunctionCallExpression) arg;
        if (func2.getFunctionIdentifier() != AlgebricksBuiltinFunctions.IS_NULL) {
            return false;
        }
        ILogicalExpression arg2 = func2.getArguments().get(0).getValue();
        if (arg2.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        VariableReferenceExpression varExpr = (VariableReferenceExpression) arg2;
        LogicalVariable var = varExpr.getVariableReference();
        ListSet<LogicalVariable> leftVars = new ListSet<LogicalVariable>();
        ListSet<LogicalVariable> rightVars = new ListSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(join.getInputs().get(0).getValue(), leftVars);
        VariableUtilities.getLiveVariables(join.getInputs().get(1).getValue(), rightVars);
        if (!rightVars.contains(var)) {
            return false;
        }
        return true;
    }

}
