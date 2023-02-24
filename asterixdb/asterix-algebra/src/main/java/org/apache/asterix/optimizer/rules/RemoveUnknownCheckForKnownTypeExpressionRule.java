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

import java.util.List;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Removes unnecessary unknown checks (e.g., not(is-unknown(expr))) for known types
 * For example:
 * <p>
 * Before:
 * select (not(is-unknown(uid))
 * * assign [$$uid] <- [uuid()]
 * After:
 * select (true) <-- will be removed by another rule later
 * * assign [$$uid] <- [uuid()]
 */
public class RemoveUnknownCheckForKnownTypeExpressionRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }

        SelectOperator selectOp = (SelectOperator) op;
        return processExpression(context, selectOp, selectOp.getCondition());
    }

    private boolean processExpressions(IOptimizationContext context, ILogicalOperator op,
            List<Mutable<ILogicalExpression>> expressions) throws AlgebricksException {
        boolean changed = false;
        for (Mutable<ILogicalExpression> exprRef : expressions) {
            changed |= processExpression(context, op, exprRef);
        }
        return changed;
    }

    private boolean processExpression(IOptimizationContext context, ILogicalOperator op,
            Mutable<ILogicalExpression> exprRef) throws AlgebricksException {

        AbstractFunctionCallExpression notFuncExpr = getFunctionExpression(exprRef);
        if (notFuncExpr == null) {
            return false;
        }
        FunctionIdentifier fid = notFuncExpr.getFunctionIdentifier();
        if (!BuiltinFunctions.NOT.equals(fid)) {
            return processExpressions(context, op, notFuncExpr.getArguments());
        }

        AbstractFunctionCallExpression unknownCheckFuncExpr = getFunctionExpression(notFuncExpr.getArguments().get(0));
        if (unknownCheckFuncExpr == null || !isNullOrIsMissingOrIsUnknownCheck(unknownCheckFuncExpr)) {
            return false;
        }

        ILogicalExpression unknownCheckArg = unknownCheckFuncExpr.getArguments().get(0).getValue();
        IVariableTypeEnvironment env = op.computeInputTypeEnvironment(context);

        IAType type = (IAType) env.getType(unknownCheckArg);
        ATypeTag typeTag = type.getTypeTag();
        if (typeTag == ATypeTag.ANY || typeTag == ATypeTag.UNION && ((AUnionType) type).isUnknownableType()) {
            // Stop if it is ANY, or it is actually an unknown-able type
            return false;
        }

        // Set the expression to true and allow the constant folding to remove the SELECT if possible
        exprRef.setValue(ConstantExpression.TRUE);
        return true;
    }

    private boolean isNullOrIsMissingOrIsUnknownCheck(AbstractFunctionCallExpression funcExpr) {
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();
        return BuiltinFunctions.IS_NULL.equals(fid) || BuiltinFunctions.IS_MISSING.equals(fid)
                || BuiltinFunctions.IS_UNKNOWN.equals(fid);
    }

    private AbstractFunctionCallExpression getFunctionExpression(Mutable<ILogicalExpression> exprRef) {
        ILogicalExpression expr = exprRef.getValue();

        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }

        return (AbstractFunctionCallExpression) expr;
    }
}
