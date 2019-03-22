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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Factors out function expressions from each comparison function or similarity function in join condition by
 * assigning them to a variables, and replacing the function expressions with references to those variables.
 * Examples:
 * Plan with function expressions in comparison or similarity condition of join expression.
 * Generates one assign operator per extracted function expression.
 *
 * <pre>
 * Before plan:
 *
 *   join ( eq( funcX($$1), funcX($$2) ) )
 *
 * After plan:
 *
 *   join (eq($$3,$$4))
 *   assign [$$4] <- [funcY($$2)]
 *   assign [$$3] <- [funcX($$1)]
 * </pre>
 */
public class ExtractFunctionsFromJoinConditionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && op.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        ILogicalExpression expr = joinOp.getCondition().getValue();

        return assignFunctionExpressions(joinOp, expr, context);

    }

    private boolean assignFunctionExpressions(AbstractLogicalOperator joinOp, ILogicalExpression expr,
            IOptimizationContext context) throws AlgebricksException {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier fi = fexp.getFunctionIdentifier();

        boolean modified = false;
        if (fi.equals(AlgebricksBuiltinFunctions.AND) || fi.equals(AlgebricksBuiltinFunctions.OR)
                || processArgumentsToFunction(fi)) {
            for (Mutable<ILogicalExpression> a : fexp.getArguments()) {
                if (assignFunctionExpressions(joinOp, a.getValue(), context)) {
                    modified = true;
                }
            }
            return modified;
        } else if (AlgebricksBuiltinFunctions.isComparisonFunction(fi) || isComparisonFunction(fi)) {
            for (Mutable<ILogicalExpression> exprRef : fexp.getArguments()) {
                if (exprRef.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    SourceLocation exprRefSourceLoc = exprRef.getValue().getSourceLocation();
                    LogicalVariable newVar = context.newVar();
                    AssignOperator newAssign = new AssignOperator(newVar,
                            new MutableObject<ILogicalExpression>(exprRef.getValue().cloneExpression()));
                    newAssign.setSourceLocation(exprRefSourceLoc);
                    newAssign.setExecutionMode(joinOp.getExecutionMode());

                    // Place assign below joinOp.
                    List<LogicalVariable> used = new ArrayList<LogicalVariable>();
                    VariableUtilities.getUsedVariables(newAssign, used);

                    Mutable<ILogicalOperator> leftBranchRef = joinOp.getInputs().get(0);
                    ILogicalOperator leftBranch = leftBranchRef.getValue();
                    List<LogicalVariable> leftBranchVariables = new ArrayList<LogicalVariable>();
                    VariableUtilities.getLiveVariables(leftBranch, leftBranchVariables);
                    if (leftBranchVariables.containsAll(used)) {
                        // place assign on left branch
                        newAssign.getInputs().add(new MutableObject<ILogicalOperator>(leftBranch));
                        leftBranchRef.setValue(newAssign);
                        modified = true;
                    } else {
                        Mutable<ILogicalOperator> rightBranchRef = joinOp.getInputs().get(1);
                        ILogicalOperator rightBranch = rightBranchRef.getValue();
                        List<LogicalVariable> rightBranchVariables = new ArrayList<LogicalVariable>();
                        VariableUtilities.getLiveVariables(rightBranch, rightBranchVariables);
                        if (rightBranchVariables.containsAll(used)) {
                            // place assign on right branch
                            newAssign.getInputs().add(new MutableObject<ILogicalOperator>(rightBranch));
                            rightBranchRef.setValue(newAssign);
                            modified = true;
                        }
                    }

                    if (modified) {
                        // Replace original expr with variable reference.
                        VariableReferenceExpression newVarRef = new VariableReferenceExpression(newVar);
                        newVarRef.setSourceLocation(exprRefSourceLoc);
                        exprRef.setValue(newVarRef);
                        context.computeAndSetTypeEnvironmentForOperator(newAssign);
                        context.computeAndSetTypeEnvironmentForOperator(joinOp);
                    }
                }
            }
            return modified;
        } else {
            return false;
        }
    }

    protected boolean processArgumentsToFunction(FunctionIdentifier fi) {
        return false;
    }

    protected boolean isComparisonFunction(FunctionIdentifier fi) {
        return false;
    }

}
