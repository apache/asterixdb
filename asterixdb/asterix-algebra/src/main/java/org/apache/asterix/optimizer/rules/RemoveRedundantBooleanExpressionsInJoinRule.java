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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.FDsAndEquivClassesVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

public class RemoveRedundantBooleanExpressionsInJoinRule extends InlineAndRemoveRedundantBooleanExpressionsRule {
    private final FDsAndEquivClassesVisitor visitor = new FDsAndEquivClassesVisitor();
    private final Map<LogicalVariable, LogicalVariable> normalizedVariables = new HashMap<>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        LogicalOperatorTag opTag = op.getOperatorTag();

        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        if (opTag != LogicalOperatorTag.INNERJOIN && opTag != LogicalOperatorTag.LEFTOUTERJOIN) {
            // TODO FDsAndEquivClassesVisitor alters the distinct variables? We have seen bugs with distinct
            // not sure if that related
            if (op.getOperatorTag() != LogicalOperatorTag.DISTINCT) {
                // Compute the equivalent classes for op
                op.accept(visitor, context);
            }
            context.addToDontApplySet(this, op);
            return false;
        }

        boolean changed = normalize(context, op);
        // compute equivalent classes for the join op
        op.accept(visitor, context);
        context.addToDontApplySet(this, op);
        return changed;
    }

    private boolean normalize(IOptimizationContext context, ILogicalOperator op) {
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        ILogicalOperator leftOp = joinOp.getInputs().get(0).getValue();
        ILogicalOperator rightOp = joinOp.getInputs().get(1).getValue();

        Map<LogicalVariable, EquivalenceClass> leftEqMap = context.getEquivalenceClassMap(leftOp);
        Map<LogicalVariable, EquivalenceClass> rightEqMap = context.getEquivalenceClassMap(rightOp);

        normalizedVariables.clear();

        Mutable<ILogicalExpression> joinCondRef = joinOp.getCondition();
        Mutable<ILogicalExpression> clonedCondition = new MutableObject<>(joinCondRef.getValue().cloneExpression());

        if (normalizeVariables(leftEqMap, rightEqMap, clonedCondition) && transform(clonedCondition)) {
            // replace the join condition iff the normalization led to a minimized circuit
            joinCondRef.setValue(clonedCondition.getValue());
            return true;
        }

        return false;
    }

    private boolean normalizeVariables(Map<LogicalVariable, EquivalenceClass> leftEqMap,
            Map<LogicalVariable, EquivalenceClass> rightEqMap, Mutable<ILogicalExpression> exprRef) {
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            return processFunction(leftEqMap, rightEqMap, (AbstractFunctionCallExpression) expr);
        } else if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            // TODO is this possible in joins?
            return false;
        }

        LogicalVariable toNormalizeVariable = VariableUtilities.getVariable(expr);
        LogicalVariable normalized =
                getNormalizedVariableAndSetEquivalentsIfAny(leftEqMap, rightEqMap, toNormalizeVariable);

        if (normalized == toNormalizeVariable) {
            // both are the same, do nothing
            return false;
        }

        // we need to replace the variable expression using the normalized expression
        exprRef.setValue(new VariableReferenceExpression(normalized));
        return true;
    }

    private LogicalVariable getNormalizedVariableAndSetEquivalentsIfAny(
            Map<LogicalVariable, EquivalenceClass> leftEqMap, Map<LogicalVariable, EquivalenceClass> rightEqMap,
            LogicalVariable toNormalizeVariable) {
        if (normalizedVariables.containsKey(toNormalizeVariable)) {
            // get the normalized variable
            return normalizedVariables.get(toNormalizeVariable);
        } else if (leftEqMap != null && leftEqMap.containsKey(toNormalizeVariable)) {
            setNormalizedVariables(toNormalizeVariable, leftEqMap.get(toNormalizeVariable));
        } else if (rightEqMap != null && rightEqMap.containsKey(toNormalizeVariable)) {
            setNormalizedVariables(toNormalizeVariable, rightEqMap.get(toNormalizeVariable));
        }

        return toNormalizeVariable;
    }

    private void setNormalizedVariables(LogicalVariable toNormalizeVariable, EquivalenceClass equivalenceClass) {
        for (LogicalVariable eqVar : equivalenceClass.getMembers()) {
            normalizedVariables.put(eqVar, toNormalizeVariable);
        }
    }

    private boolean processFunction(Map<LogicalVariable, EquivalenceClass> leftEqMap,
            Map<LogicalVariable, EquivalenceClass> rightEqMap, AbstractFunctionCallExpression funcExpr) {

        boolean changed = false;
        for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
            changed |= normalizeVariables(leftEqMap, rightEqMap, argRef);
        }

        return changed;
    }
}
