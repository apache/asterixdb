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
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Factors out function expressions from each comparison function or similarity function in join condition by assigning them to a variables, and replacing the function expressions with references to those variables.
 * Examples:
 * Plan with function expressions in comparison or similarity condition of join expression. Generates one assign operator per extracted function expression.
 * Example
 * Before plan:
 * join ( eq( funcX($$1), funcX($$2) ) )
 * After plan:
 * join (eq($$3,$$4))
 * assign [$$4] <- [funcY($$2)]
 * assign [$$3] <- [funcX($$1)]
 */
public class ExtractFunctionsFromJoinConditionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
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
                || fi.equals(AsterixBuiltinFunctions.GET_ITEM)) {
            for (Mutable<ILogicalExpression> a : fexp.getArguments()) {
                if (assignFunctionExpressions(joinOp, a.getValue(), context)) {
                    modified = true;
                }
            }
            return modified;
        } else if (AlgebricksBuiltinFunctions.isComparisonFunction(fi)
                || AsterixBuiltinFunctions.isSimilarityFunction(fi)) {
            for (Mutable<ILogicalExpression> exprRef : fexp.getArguments()) {
                if (exprRef.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    LogicalVariable newVar = context.newVar();
                    AssignOperator newAssign = new AssignOperator(newVar, new MutableObject<ILogicalExpression>(exprRef
                            .getValue().cloneExpression()));
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
                        exprRef.setValue(new VariableReferenceExpression(newVar));
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

}
