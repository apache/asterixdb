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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ExtractRedundantVariablesInJoinRule implements IAlgebraicRewriteRule {
    private final Map<LogicalVariable, List<Mutable<ILogicalExpression>>> variableToExpressionsMap = new HashMap<>();
    private final Set<LogicalVariable> leftLiveVars = new HashSet<>();

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && op.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }

        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        if (!ensureAndExtractVarAndExpr(joinOp.getCondition().getValue())) {
            return false;
        }

        setLeftLiveVariables(joinOp);

        List<LogicalVariable> leftAssignVars = new ArrayList<>();
        List<Mutable<ILogicalExpression>> leftAssignExprs = new ArrayList<>();

        List<LogicalVariable> rightAssignVars = new ArrayList<>();
        List<Mutable<ILogicalExpression>> rightAssignExprs = new ArrayList<>();

        for (Map.Entry<LogicalVariable, List<Mutable<ILogicalExpression>>> kv : variableToExpressionsMap.entrySet()) {
            LogicalVariable repeatedVariable = kv.getKey();
            List<Mutable<ILogicalExpression>> repeatedReferences = kv.getValue();

            if (leftLiveVars.contains(repeatedVariable)) {
                reassignRepeatedVariables(context, repeatedVariable, repeatedReferences, leftAssignVars,
                        leftAssignExprs);
            } else {
                reassignRepeatedVariables(context, repeatedVariable, repeatedReferences, rightAssignVars,
                        rightAssignExprs);
            }
        }

        SourceLocation sourceLocation = joinOp.getSourceLocation();
        if (!leftAssignVars.isEmpty()) {
            createAndSetAssign(context, sourceLocation, joinOp.getInputs().get(0), leftAssignVars, leftAssignExprs);
        }

        if (!rightAssignVars.isEmpty()) {
            createAndSetAssign(context, sourceLocation, joinOp.getInputs().get(1), rightAssignVars, rightAssignExprs);
        }

        context.computeAndSetTypeEnvironmentForOperator(joinOp);

        return true;
    }

    private void createAndSetAssign(IOptimizationContext context, SourceLocation sourceLocation,
            Mutable<ILogicalOperator> joinInputRef, List<LogicalVariable> assignVars,
            List<Mutable<ILogicalExpression>> assignExprs) throws AlgebricksException {
        AssignOperator assignOp = new AssignOperator(assignVars, assignExprs);
        assignOp.setSourceLocation(sourceLocation);
        assignOp.getInputs().add(new MutableObject<>(joinInputRef.getValue()));
        joinInputRef.setValue(assignOp);
        context.computeAndSetTypeEnvironmentForOperator(assignOp);
    }

    private void setLeftLiveVariables(AbstractBinaryJoinOperator op) throws AlgebricksException {
        ILogicalOperator leftOp = op.getInputs().get(0).getValue();
        leftLiveVars.clear();
        VariableUtilities.getLiveVariables(leftOp, leftLiveVars);
    }

    private void reassignRepeatedVariables(IOptimizationContext context, LogicalVariable repeatedVariable,
            List<Mutable<ILogicalExpression>> repeatedReferences, List<LogicalVariable> assignVars,
            List<Mutable<ILogicalExpression>> assignExprs) {

        // keep one of the repeated references and reassign the others
        for (int i = 1; i < repeatedReferences.size(); i++) {
            Mutable<ILogicalExpression> exprRef = repeatedReferences.get(i);
            SourceLocation sourceLocation = exprRef.getValue().getSourceLocation();
            LogicalVariable newVar = context.newVar();

            exprRef.setValue(new VariableReferenceExpression(newVar, sourceLocation));

            assignVars.add(newVar);
            assignExprs.add(new MutableObject<>(new VariableReferenceExpression(repeatedVariable, sourceLocation)));

            // Prevent inlining the variable
            context.addNotToBeInlinedVar(newVar);
        }
    }

    private boolean ensureAndExtractVarAndExpr(ILogicalExpression expr) {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        if (!AlgebricksBuiltinFunctions.AND.equals(funcExpr.getFunctionIdentifier())) {
            return false;
        }

        variableToExpressionsMap.clear();
        boolean containsRepeatedReferences = false;
        for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
            ILogicalExpression arg = argRef.getValue();
            if (arg.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return false;
            }

            AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) arg;
            if (!AlgebricksBuiltinFunctions.EQ.equals(argFuncExpr.getFunctionIdentifier())) {
                return false;
            }

            List<Mutable<ILogicalExpression>> eqArgs = argFuncExpr.getArguments();
            Mutable<ILogicalExpression> leftRef = eqArgs.get(0);
            Mutable<ILogicalExpression> rightRef = eqArgs.get(1);

            ILogicalExpression left = leftRef.getValue();
            ILogicalExpression right = rightRef.getValue();

            LogicalVariable leftVar = VariableUtilities.getVariable(left);
            LogicalVariable rightVar = VariableUtilities.getVariable(right);

            // shouldn't be possible. But here for sanity check
            if (leftVar == null || rightVar == null) {
                return false;
            }

            List<Mutable<ILogicalExpression>> leftList =
                    variableToExpressionsMap.computeIfAbsent(leftVar, k -> new ArrayList<>());
            leftList.add(leftRef);

            List<Mutable<ILogicalExpression>> rightList =
                    variableToExpressionsMap.computeIfAbsent(rightVar, k -> new ArrayList<>());
            rightList.add(rightRef);

            containsRepeatedReferences |= leftList.size() > 1 || rightList.size() > 1;
        }

        // return true only if there's a repeated reference to a variable
        return containsRepeatedReferences;
    }
}
