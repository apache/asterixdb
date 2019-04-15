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

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.functions.ExternalScalarFunctionInfo;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule provides the same type-casting handling as the IntroduceDynamicTypeCastRule does.
 * The only difference is that this rule is intended for external functions (User-Defined Functions).
 * Refer to IntroduceDynamicTypeCastRule for the detail.
 */
public class IntroduceDynamicTypeCastForExternalFunctionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    private boolean rewriteFunctionArgs(ILogicalOperator op, Mutable<ILogicalExpression> expRef,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalExpression expr = expRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        boolean changed = false;
        // go over all arguments recursively
        AbstractFunctionCallExpression funcCallExpr = (AbstractFunctionCallExpression) expr;
        for (Mutable<ILogicalExpression> functionArgRef : funcCallExpr.getArguments()) {
            if (rewriteFunctionArgs(op, functionArgRef, context)) {
                changed = true;
            }
        }
        // if the current function is builtin function, skip the type casting
        if (BuiltinFunctions.getBuiltinFunctionIdentifier(funcCallExpr.getFunctionIdentifier()) != null) {
            return changed;
        }
        IAType inputType;
        IAType reqArgType;
        boolean castFlag;
        for (int i = 0; i < funcCallExpr.getArguments().size(); i++) {
            Mutable<ILogicalExpression> argExpr = funcCallExpr.getArguments().get(i);
            inputType = (IAType) op.computeOutputTypeEnvironment(context).getType(argExpr.getValue());
            reqArgType = ((ExternalScalarFunctionInfo) funcCallExpr.getFunctionInfo()).getArgumentTypes().get(i);

            if (reqArgType.getTypeTag() == ATypeTag.OBJECT) {
                castFlag = !IntroduceDynamicTypeCastRule.compatible((ARecordType) reqArgType, inputType,
                        argExpr.getValue().getSourceLocation());
            } else {
                castFlag = !reqArgType.equals(inputType);
            }
            /**
             * the input record type can be an union type
             * for the case when it comes from a subplan or left-outer join
             */
            boolean checkUnknown = false;
            while (NonTaggedFormatUtil.isOptional(inputType)) {
                /** while-loop for the case there is a nested multi-level union */
                inputType = ((AUnionType) inputType).getActualType();
                checkUnknown = true;
            }
            if (castFlag || checkUnknown) {
                AbstractFunctionCallExpression castFunc =
                        new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.CAST_TYPE));
                castFunc.setSourceLocation(argExpr.getValue().getSourceLocation());
                castFunc.getArguments().add(argExpr);
                TypeCastUtils.setRequiredAndInputTypes(castFunc, reqArgType, inputType);
                funcCallExpr.getArguments().set(i, new MutableObject<>(castFunc));
                changed = true;
            }
        }
        return changed;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        return op.acceptExpressionTransform(expr -> rewriteFunctionArgs(op, expr, context));
    }
}
