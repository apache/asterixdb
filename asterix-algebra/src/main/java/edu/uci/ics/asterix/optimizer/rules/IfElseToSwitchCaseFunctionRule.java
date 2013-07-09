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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IfElseToSwitchCaseFunctionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.ASSIGN)
            return false;

        AssignOperator assignOp = (AssignOperator) op1;
        List<Mutable<ILogicalExpression>> assignExprs = assignOp.getExpressions();
        if (assignExprs.size() > 1)
            return false;
        ILogicalExpression expr = assignExprs.get(0).getValue();
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            if (!funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.CONCAT_NON_NULL))
                return false;
        }

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.SUBPLAN)
            return false;

        SubplanOperator subplan = (SubplanOperator) op2;
        List<ILogicalPlan> subPlans = subplan.getNestedPlans();
        List<Mutable<ILogicalExpression>> arguments = new ArrayList<Mutable<ILogicalExpression>>();
        for (ILogicalPlan plan : subPlans) {
            List<Mutable<ILogicalOperator>> roots = plan.getRoots();

            AbstractLogicalOperator nestedRoot = (AbstractLogicalOperator) roots.get(0).getValue();
            if (nestedRoot.getOperatorTag() != LogicalOperatorTag.SELECT)
                return false;
            SelectOperator selectOp = (SelectOperator) nestedRoot;

            AbstractLogicalOperator nestedNextOp = (AbstractLogicalOperator) nestedRoot.getInputs().get(0).getValue();
            if (nestedNextOp.getOperatorTag() != LogicalOperatorTag.ASSIGN)
                return false;
            AssignOperator assignRoot = (AssignOperator) nestedNextOp;
            Mutable<ILogicalExpression> actionExprRef = assignRoot.getExpressions().get(0);

            arguments.add(selectOp.getCondition());
            arguments.add(actionExprRef);
            AbstractLogicalOperator nestedBottomOp = (AbstractLogicalOperator) assignRoot.getInputs().get(0).getValue();

            if (nestedBottomOp.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE)
                return false;
        }

        AbstractLogicalOperator op3 = (AbstractLogicalOperator) op2.getInputs().get(0).getValue();
        if (op3.getOperatorTag() != LogicalOperatorTag.ASSIGN)
            return false;

        AssignOperator bottomAssign = (AssignOperator) op3;
        LogicalVariable conditionVar = bottomAssign.getVariables().get(0);
        Mutable<ILogicalExpression> switchCondition = new MutableObject<ILogicalExpression>(
                new VariableReferenceExpression(conditionVar));
        List<Mutable<ILogicalExpression>> argumentRefs = new ArrayList<Mutable<ILogicalExpression>>();
        argumentRefs.add(switchCondition);
        argumentRefs.addAll(arguments);

        /** replace the branch conditions */
        for (int i = 0; i < arguments.size(); i += 2) {
            if (arguments.get(i).getValue().equals(switchCondition.getValue())) {
                arguments.get(i).setValue(ConstantExpression.TRUE);
            } else {
                arguments.get(i).setValue(ConstantExpression.FALSE);
            }
        }

        ILogicalExpression callExpr = new ScalarFunctionCallExpression(
                FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.SWITCH_CASE), argumentRefs);

        assignOp.getInputs().get(0).setValue(op3);
        assignOp.getExpressions().get(0).setValue(callExpr);
        context.computeAndSetTypeEnvironmentForOperator(assignOp);
        return true;
    }
}
