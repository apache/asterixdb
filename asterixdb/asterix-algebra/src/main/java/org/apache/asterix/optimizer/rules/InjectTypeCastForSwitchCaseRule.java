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
import java.util.Collections;
import java.util.List;

import org.apache.asterix.dataflow.data.common.TypeResolverUtil;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule injects cast functions for "THEN" and "ELSE" branches of a switch-case function if
 * different "THEN" and "ELSE" branches have heterogeneous return types.
 */
public class InjectTypeCastForSwitchCaseRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getInputs().isEmpty()) {
            return false;
        }
        // Populates the latest type information.
        context.computeAndSetTypeEnvironmentForOperator(op);
        if (op.acceptExpressionTransform(exprRef -> injectTypeCast(op, exprRef, context))) {
            // Generates the up-to-date type information.
            context.computeAndSetTypeEnvironmentForOperator(op);
            return true;
        }
        return false;
    }

    // Injects type casts to cast return expressions' return types to a generalized type that conforms to every
    // return type.
    private boolean injectTypeCast(ILogicalOperator op, Mutable<ILogicalExpression> exprRef,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        boolean rewritten = false;
        AbstractFunctionCallExpression func = (AbstractFunctionCallExpression) expr;
        for (Mutable<ILogicalExpression> argRef : func.getArguments()) {
            // Recursively rewrites arguments.
            if (injectTypeCast(op, argRef, context)) {
                context.computeAndSetTypeEnvironmentForOperator(op);
                rewritten = true;
            }
        }
        if (!func.getFunctionIdentifier().equals(BuiltinFunctions.SWITCH_CASE)) {
            return rewritten;
        }
        return rewriteSwitchCase(op, func, context);
    }

    // Injects casts that cast types for different "THEN" and "ELSE" branches.
    private boolean rewriteSwitchCase(ILogicalOperator op, AbstractFunctionCallExpression func,
            IOptimizationContext context) throws AlgebricksException {
        IVariableTypeEnvironment env = context.getOutputTypeEnvironment(op.getInputs().get(0).getValue());
        IAType producedType = (IAType) env.getType(func);
        List<Mutable<ILogicalExpression>> argRefs = func.getArguments();
        int argSize = argRefs.size();
        boolean rewritten = false;
        for (int argIndex = 2; argIndex < argSize; argIndex += (argIndex + 2 == argSize) ? 1 : 2) {
            Mutable<ILogicalExpression> argRef = argRefs.get(argIndex);
            IAType type = (IAType) env.getType(argRefs.get(argIndex).getValue());
            if (TypeResolverUtil.needsCast(producedType, type)) {
                ILogicalExpression argExpr = argRef.getValue();
                // Injects a cast call to cast the data type to the produced type of the switch-case function call.
                ScalarFunctionCallExpression castFunc = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.CAST_TYPE),
                        new ArrayList<>(Collections.singletonList(new MutableObject<>(argExpr))));
                TypeCastUtils.setRequiredAndInputTypes(castFunc, producedType, type);
                argRef.setValue(castFunc);
                rewritten = true;
            }
        }
        return rewritten;
    }

}
