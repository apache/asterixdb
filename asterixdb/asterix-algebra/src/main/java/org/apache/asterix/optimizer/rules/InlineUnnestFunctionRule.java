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

package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule is to inline unnest functions that are hold by variables.
 * This rule is to fix issue 201.
 */
public class InlineUnnestFunctionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op1)) {
            return false;
        }
        context.addToDontApplySet(this, op1);
        if (op1.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnestOperator = (UnnestOperator) op1;
        AbstractFunctionCallExpression expr =
                (AbstractFunctionCallExpression) unnestOperator.getExpressionRef().getValue();
        // we only inline for the scan-collection function
        if (expr.getFunctionIdentifier() != BuiltinFunctions.SCAN_COLLECTION) {
            return false;
        }

        // inline all variables from an unnesting function call
        List<Mutable<ILogicalExpression>> args = expr.getArguments();
        boolean changed = false;
        for (int i = 0; i < args.size(); i++) {
            ILogicalExpression argExpr = args.get(i).getValue();
            if (argExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varExpr = (VariableReferenceExpression) argExpr;
                changed |= inlineVariable(varExpr.getVariableReference(), unnestOperator);
            }
        }
        return changed;
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
    private boolean inlineVariable(LogicalVariable usedVar, UnnestOperator unnestOp) throws AlgebricksException {
        AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) unnestOp.getExpressionRef().getValue();
        List<Pair<AbstractFunctionCallExpression, Integer>> parentAndIndexList = new ArrayList<>();
        getParentFunctionExpression(usedVar, expr, parentAndIndexList);
        ILogicalExpression usedVarOrginExpr =
                findUsedVarOrigin(usedVar, unnestOp, (AbstractLogicalOperator) unnestOp.getInputs().get(0).getValue());
        if (usedVarOrginExpr != null) {
            for (Pair<AbstractFunctionCallExpression, Integer> parentAndIndex : parentAndIndexList) {
                // we only rewrite the top scan-collection function
                if (parentAndIndex.first.getFunctionIdentifier() == BuiltinFunctions.SCAN_COLLECTION
                        && parentAndIndex.first == expr) {
                    unnestOp.getExpressionRef().setValue(usedVarOrginExpr);
                }
            }
            return true;
        }
        return false;
    }

    private void getParentFunctionExpression(LogicalVariable usedVar, AbstractFunctionCallExpression funcExpr,
            List<Pair<AbstractFunctionCallExpression, Integer>> parentAndIndexList) {
        List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
        for (int i = 0; i < args.size(); i++) {
            ILogicalExpression argExpr = args.get(i).getValue();
            if (argExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varExpr = (VariableReferenceExpression) argExpr;
                if (varExpr.getVariableReference().equals(usedVar)) {
                    parentAndIndexList.add(new Pair<>(funcExpr, i));
                }
            }
            if (argExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                getParentFunctionExpression(usedVar, (AbstractFunctionCallExpression) argExpr, parentAndIndexList);
            }
        }
    }

    private ILogicalExpression findUsedVarOrigin(LogicalVariable usedVar, AbstractLogicalOperator parentOp,
            AbstractLogicalOperator currentOp) throws AlgebricksException {
        ILogicalExpression ret = null;
        if (currentOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            List<LogicalVariable> producedVars = new ArrayList<>();
            VariableUtilities.getProducedVariables(currentOp, producedVars);
            if (producedVars.contains(usedVar)) {
                AssignOperator assignOp = (AssignOperator) currentOp;
                int index = assignOp.getVariables().indexOf(usedVar);
                ILogicalExpression returnedExpr = assignOp.getExpressions().get(index).getValue();
                if (returnedExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) returnedExpr;
                    if (BuiltinFunctions.isBuiltinUnnestingFunction(funcExpr.getFunctionIdentifier())) {
                        // we only inline for unnest functions
                        removeUnecessaryAssign(parentOp, currentOp, assignOp, index);
                        ret = returnedExpr;
                    }
                } else if (returnedExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    // recursively inline
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
                ILogicalExpression expr =
                        findUsedVarOrigin(usedVar, currentOp, (AbstractLogicalOperator) child.getValue());
                if (expr != null) {
                    ret = expr;
                }
            }
        }
        return ret;
    }

    private void removeUnecessaryAssign(AbstractLogicalOperator parentOp, AbstractLogicalOperator currentOp,
            AssignOperator assignOp, int index) {
        // TODO: how come the assign variable is removed before checking that other operators might be using it?
        assignOp.getVariables().remove(index);
        assignOp.getExpressions().remove(index);
        if (assignOp.getVariables().size() == 0) {
            int opIndex = parentOp.getInputs().indexOf(new MutableObject<ILogicalOperator>(currentOp));
            parentOp.getInputs().get(opIndex).setValue(assignOp.getInputs().get(0).getValue());
        }
    }
}
