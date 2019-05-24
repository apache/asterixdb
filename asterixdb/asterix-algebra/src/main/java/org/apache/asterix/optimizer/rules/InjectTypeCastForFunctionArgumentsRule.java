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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntBinaryOperator;
import java.util.function.IntPredicate;

import org.apache.asterix.dataflow.data.common.TypeResolverUtil;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ATypeTag;
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
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule injects casts for function parameters if they have heterogeneous return types:
 * <ul>
 *     <li>for "THEN" and "ELSE" branches of a switch-case function</li>
 *     <li>for parameters of "if missing/null" functions  (if-missing(), if-null(), if-missing-or-null())</li>
 * </ul>
 */
public class InjectTypeCastForFunctionArgumentsRule implements IAlgebraicRewriteRule {

    private static final Map<FunctionIdentifier, IntPredicate> FUN_TO_ARG_CHECKER = new HashMap<>();

    static {
        addFunctionAndArgChecker(BuiltinFunctions.IF_MISSING, null);
        addFunctionAndArgChecker(BuiltinFunctions.IF_NULL, null);
        addFunctionAndArgChecker(BuiltinFunctions.IF_MISSING_OR_NULL, null);
        addFunctionAndArgChecker(BuiltinFunctions.IF_SYSTEM_NULL, null);
    }

    // allows the rule to check other functions in addition to the ones specified here
    public static void addFunctionAndArgChecker(FunctionIdentifier function, IntPredicate argChecker) {
        FUN_TO_ARG_CHECKER.put(function, argChecker);
    }

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
        FunctionIdentifier funcId = func.getFunctionIdentifier();
        if (funcId.equals(BuiltinFunctions.SWITCH_CASE)) {
            rewritten |= rewriteFunction(op, func, null, context, 2,
                    InjectTypeCastForFunctionArgumentsRule::switchIncrement);
        } else if (FUN_TO_ARG_CHECKER.containsKey(funcId)) {
            rewritten |= rewriteFunction(op, func, FUN_TO_ARG_CHECKER.get(funcId), context, 0,
                    InjectTypeCastForFunctionArgumentsRule::increment);
        }
        return rewritten;
    }

    // Injects casts that cast types for all function parameters
    private boolean rewriteFunction(ILogicalOperator op, AbstractFunctionCallExpression func, IntPredicate argChecker,
            IOptimizationContext context, int argStartIdx, IntBinaryOperator increment) throws AlgebricksException {
        IVariableTypeEnvironment env = op.computeInputTypeEnvironment(context);
        IAType producedType = (IAType) env.getType(func);
        if (!argumentsNeedCasting(producedType)) {
            return false;
        }
        List<Mutable<ILogicalExpression>> argRefs = func.getArguments();
        int argSize = argRefs.size();
        boolean rewritten = false;
        for (int argIndex = argStartIdx; argIndex < argSize; argIndex += increment.applyAsInt(argIndex, argSize)) {
            if (argChecker == null || argChecker.test(argIndex)) {
                rewritten |= rewriteFunctionArgument(argRefs.get(argIndex), producedType, env);
            }
        }
        return rewritten;
    }

    private boolean rewriteFunctionArgument(Mutable<ILogicalExpression> argRef, IAType funcOutputType,
            IVariableTypeEnvironment env) throws AlgebricksException {
        ILogicalExpression argExpr = argRef.getValue();
        IAType type = (IAType) env.getType(argExpr);
        if (TypeResolverUtil.needsCast(funcOutputType, type)) {
            // Injects a cast call to cast the data type to the produced type of the function call.
            ScalarFunctionCallExpression castFunc =
                    new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.CAST_TYPE),
                            new ArrayList<>(Collections.singletonList(new MutableObject<>(argExpr))));
            castFunc.setSourceLocation(argExpr.getSourceLocation());
            TypeCastUtils.setRequiredAndInputTypes(castFunc, funcOutputType, type);
            argRef.setValue(castFunc);
            return true;
        }
        return false;
    }

    private static boolean argumentsNeedCasting(IAType functionProducedType) {
        ATypeTag functionProducedTag = TypeComputeUtils.getActualType(functionProducedType).getTypeTag();
        return functionProducedTag == ATypeTag.ANY || functionProducedTag.isDerivedType();
    }

    private static int switchIncrement(int currentArgIndex, int numArguments) {
        return currentArgIndex + 2 == numArguments ? 1 : 2;
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    private static int increment(int currentArgIndex, int numArguments) {
        return 1;
    }
}
