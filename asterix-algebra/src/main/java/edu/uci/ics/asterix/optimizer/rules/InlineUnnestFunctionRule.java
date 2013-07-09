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

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule is to inline unnest functions that are hold by variables.
 * This rule is to fix issue 201.
 */
public class InlineUnnestFunctionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op1))
            return false;
        context.addToDontApplySet(this, op1);
        if (op1.getOperatorTag() != LogicalOperatorTag.UNNEST)
            return false;
        UnnestOperator unnestOperator = (UnnestOperator) op1;
        AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) unnestOperator.getExpressionRef()
                .getValue();
        //we only inline for the scan-collection function
        if (expr.getFunctionIdentifier() != AsterixBuiltinFunctions.SCAN_COLLECTION)
            return false;

        // inline all variables from an unnesting function call
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        for (int i = 0; i < args.size(); i++) {
            ILogicalExpression argExpr = args.get(i).getValue();
            if (argExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varExpr = (VariableReferenceExpression) argExpr;
                inlineVariable(varExpr.getVariableReference(), unnestOperator);
            }
        }
        return true;
    }

    /**
     * This method is to inline one variable
     * 
     * @param usedVar
     *            A variable that is used by the scan-collection function in the unnest operator
     * @param unnestOp
     *            The unnest operator.
     * @throws AlgebricksException
     */
    private void inlineVariable(LogicalVariable usedVar, UnnestOperator unnestOp) throws AlgebricksException {
        AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) unnestOp.getExpressionRef().getValue();
        List<Pair<AbstractFunctionCallExpression, Integer>> parentAndIndexList = new ArrayList<Pair<AbstractFunctionCallExpression, Integer>>();
        getParentFunctionExpression(usedVar, expr, parentAndIndexList);
        ILogicalExpression usedVarOrginExpr = findUsedVarOrigin(usedVar, unnestOp, (AbstractLogicalOperator) unnestOp
                .getInputs().get(0).getValue());
        if (usedVarOrginExpr != null) {
            for (Pair<AbstractFunctionCallExpression, Integer> parentAndIndex : parentAndIndexList) {
                //we only rewrite the top scan-collection function
                if (parentAndIndex.first.getFunctionIdentifier() == AsterixBuiltinFunctions.SCAN_COLLECTION
                        && parentAndIndex.first == expr) {
                    unnestOp.getExpressionRef().setValue(usedVarOrginExpr);
                }
            }
        }
    }

    private void getParentFunctionExpression(LogicalVariable usedVar, ILogicalExpression expr,
            List<Pair<AbstractFunctionCallExpression, Integer>> parentAndIndexList) {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        for (int i = 0; i < args.size(); i++) {
            ILogicalExpression argExpr = args.get(i).getValue();
            if (argExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varExpr = (VariableReferenceExpression) argExpr;
                if (varExpr.getVariableReference().equals(usedVar))
                    parentAndIndexList.add(new Pair<AbstractFunctionCallExpression, Integer>(funcExpr, i));
            }
            if (argExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                getParentFunctionExpression(usedVar, argExpr, parentAndIndexList);
            }
        }
    }

    private ILogicalExpression findUsedVarOrigin(LogicalVariable usedVar, AbstractLogicalOperator parentOp,
            AbstractLogicalOperator currentOp) throws AlgebricksException {
        ILogicalExpression ret = null;
        if (currentOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            List<LogicalVariable> producedVars = new ArrayList<LogicalVariable>();
            VariableUtilities.getProducedVariables(currentOp, producedVars);
            if (producedVars.contains(usedVar)) {
                AssignOperator assignOp = (AssignOperator) currentOp;
                int index = assignOp.getVariables().indexOf(usedVar);
                ILogicalExpression returnedExpr = assignOp.getExpressions().get(index).getValue();
                if (returnedExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) returnedExpr;
                    if (AsterixBuiltinFunctions.isBuiltinUnnestingFunction(funcExpr.getFunctionIdentifier())) {
                        // we only inline for unnest functions
                        removeUnecessaryAssign(parentOp, currentOp, assignOp, index);
                        ret = returnedExpr;
                    }
                } else if (returnedExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    //recusively inline
                    VariableReferenceExpression varExpr = (VariableReferenceExpression) returnedExpr;
                    LogicalVariable var = varExpr.getVariableReference();
                    ILogicalExpression finalExpr = findUsedVarOrigin(var, currentOp,
                            (AbstractLogicalOperator) currentOp.getInputs().get(0).getValue());
                    if (finalExpr != null) {
                        removeUnecessaryAssign(parentOp, currentOp, assignOp, index);
                        ret = finalExpr;
                    }
                }
            }
        } else {
            for (Mutable<ILogicalOperator> child : currentOp.getInputs()) {
                ILogicalExpression expr = findUsedVarOrigin(usedVar, currentOp,
                        (AbstractLogicalOperator) child.getValue());
                if (expr != null) {
                    ret = expr;
                }
            }
        }
        return ret;
    }

    private void removeUnecessaryAssign(AbstractLogicalOperator parentOp, AbstractLogicalOperator currentOp,
            AssignOperator assignOp, int index) {
        assignOp.getVariables().remove(index);
        assignOp.getExpressions().remove(index);
        if (assignOp.getVariables().size() == 0) {
            int opIndex = parentOp.getInputs().indexOf(new MutableObject<ILogicalOperator>(currentOp));
            parentOp.getInputs().get(opIndex).setValue(assignOp.getInputs().get(0).getValue());
        }
    }
}
