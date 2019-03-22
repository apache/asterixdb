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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Pushes function-call expressions below a join if possible.
 * Assigns the result of such function-calls expressions to new variables, and replaces the original
 * expression with a corresponding variable reference expression.
 * This rule can help reduce the cost of computing expensive functions by pushing them below
 * a join (which may blow up the cardinality).
 * Also, this rule may help to enable other rules such as common subexpression elimination, again to reduce
 * the number of calls to expensive functions.
 *
 * Example: (we are pushing pushMeFunc)
 *
 * Before plan:
 * assign [$$10] <- [funcA(funcB(pushMeFunc($$3, $$4)))]
 *   join (some condition)
 *     join_branch_0 where $$3 and $$4 are not live
 *       ...
 *     join_branch_1 where $$3 and $$4 are live
 *       ...
 *
 * After plan:
 * assign [$$10] <- [funcA(funcB($$11))]
 *   join (some condition)
 *     join_branch_0 where $$3 and $$4 are not live
 *       ...
 *     join_branch_1 where $$3 and $$4 are live
 *       assign[$$11] <- [pushMeFunc($$3, $$4)]
 *         ...
 */
public class PushFunctionsBelowJoin implements IAlgebraicRewriteRule {

    private final Set<FunctionIdentifier> toPushFuncIdents;
    private final List<Mutable<ILogicalExpression>> funcExprs = new ArrayList<Mutable<ILogicalExpression>>();
    private final List<LogicalVariable> usedVars = new ArrayList<LogicalVariable>();
    private final List<LogicalVariable> liveVars = new ArrayList<LogicalVariable>();

    public PushFunctionsBelowJoin(Set<FunctionIdentifier> toPushFuncIdents) {
        this.toPushFuncIdents = toPushFuncIdents;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assignOp = (AssignOperator) op;

        // Find a join operator below this assign.
        Mutable<ILogicalOperator> joinOpRef = findJoinOp(assignOp.getInputs().get(0));
        if (joinOpRef == null) {
            return false;
        }
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) joinOpRef.getValue();

        // Check if the assign uses a function that we wish to push below the join if possible.
        funcExprs.clear();
        gatherFunctionCalls(assignOp, funcExprs);
        if (funcExprs.isEmpty()) {
            return false;
        }

        // Try to push the functions down the input branches of the join.
        boolean modified = false;
        if (pushDownFunctions(joinOp, 0, funcExprs, context)) {
            modified = true;
        }
        if (pushDownFunctions(joinOp, 1, funcExprs, context)) {
            modified = true;
        }
        if (modified) {
            context.computeAndSetTypeEnvironmentForOperator(joinOp);
        }
        return modified;
    }

    private Mutable<ILogicalOperator> findJoinOp(Mutable<ILogicalOperator> opRef) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        switch (op.getOperatorTag()) {
            case INNERJOIN:
            case LEFTOUTERJOIN: {
                return opRef;
            }
            // Bail on these operators.
            case GROUP:
            case AGGREGATE:
            case DISTINCT:
            case UNNEST_MAP: {
                return null;
            }
            // Traverse children.
            default: {
                for (Mutable<ILogicalOperator> childOpRef : op.getInputs()) {
                    return findJoinOp(childOpRef);
                }
            }
        }
        return null;
    }

    private void gatherFunctionCalls(AssignOperator assignOp, List<Mutable<ILogicalExpression>> funcExprs) {
        for (Mutable<ILogicalExpression> exprRef : assignOp.getExpressions()) {
            gatherFunctionCalls(exprRef, funcExprs);
        }
    }

    private void gatherFunctionCalls(Mutable<ILogicalExpression> exprRef, List<Mutable<ILogicalExpression>> funcExprs) {
        AbstractLogicalExpression expr = (AbstractLogicalExpression) exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }
        // Check whether the function is a function we want to push.
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        if (toPushFuncIdents.contains(funcExpr.getFunctionIdentifier())) {
            funcExprs.add(exprRef);
        }
        // Traverse arguments.
        for (Mutable<ILogicalExpression> funcArg : funcExpr.getArguments()) {
            gatherFunctionCalls(funcArg, funcExprs);
        }
    }

    private boolean pushDownFunctions(AbstractBinaryJoinOperator joinOp, int inputIndex,
            List<Mutable<ILogicalExpression>> funcExprs, IOptimizationContext context) throws AlgebricksException {
        ILogicalOperator joinInputOp = joinOp.getInputs().get(inputIndex).getValue();
        liveVars.clear();
        VariableUtilities.getLiveVariables(joinInputOp, liveVars);
        Iterator<Mutable<ILogicalExpression>> funcIter = funcExprs.iterator();
        List<LogicalVariable> assignVars = null;
        List<Mutable<ILogicalExpression>> assignExprs = null;
        while (funcIter.hasNext()) {
            Mutable<ILogicalExpression> funcExprRef = funcIter.next();
            ILogicalExpression funcExpr = funcExprRef.getValue();
            usedVars.clear();
            funcExpr.getUsedVariables(usedVars);
            // Check if we can push the function down this branch.
            if (liveVars.containsAll(usedVars)) {
                if (assignVars == null) {
                    assignVars = new ArrayList<LogicalVariable>();
                    assignExprs = new ArrayList<Mutable<ILogicalExpression>>();
                }
                // Replace the original expression with a variable reference expression.
                LogicalVariable replacementVar = context.newVar();
                assignVars.add(replacementVar);
                assignExprs.add(new MutableObject<ILogicalExpression>(funcExpr));
                VariableReferenceExpression replacementVarRef = new VariableReferenceExpression(replacementVar);
                replacementVarRef.setSourceLocation(funcExpr.getSourceLocation());
                funcExprRef.setValue(replacementVarRef);
                funcIter.remove();
            }
        }
        // Create new assign operator below the join if any functions can be pushed.
        if (assignVars != null) {
            AssignOperator newAssign = new AssignOperator(assignVars, assignExprs);
            newAssign.getInputs().add(new MutableObject<ILogicalOperator>(joinInputOp));
            newAssign.setExecutionMode(joinOp.getExecutionMode());
            joinOp.getInputs().get(inputIndex).setValue(newAssign);
            context.computeAndSetTypeEnvironmentForOperator(newAssign);
            return true;
        }
        return false;
    }
}
