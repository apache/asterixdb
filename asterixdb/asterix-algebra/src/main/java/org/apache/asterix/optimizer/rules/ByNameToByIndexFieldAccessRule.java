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

import org.apache.asterix.algebra.base.OperatorAnnotation;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ByNameToByIndexFieldAccessRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.acceptExpressionTransform(exprRef -> rewriteExpressionReference(op, exprRef, context))) {
            op.removeAnnotation(OperatorAnnotation.PUSHED_FIELD_ACCESS);
            context.computeAndSetTypeEnvironmentForOperator(op);
            return true;
        }
        return false;
    }

    // Recursively rewrites expression reference.
    private boolean rewriteExpressionReference(ILogicalOperator op, Mutable<ILogicalExpression> exprRef,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        boolean changed = false;
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        for (Mutable<ILogicalExpression> funcArgRef : funcExpr.getArguments()) {
            if (rewriteExpressionReference(op, funcArgRef, context)) {
                changed = true;
            }
        }
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
        if (fce.getFunctionIdentifier() != BuiltinFunctions.FIELD_ACCESS_BY_NAME) {
            return changed;
        }
        changed |= extractFirstArg(fce, op, context);
        IVariableTypeEnvironment env = context.getOutputTypeEnvironment(op.getInputs().get(0).getValue());
        IAType t = (IAType) env.getType(fce.getArguments().get(0).getValue());
        changed |= rewriteFieldAccess(exprRef, fce, getActualType(t));
        return changed;
    }

    // Extracts the first argument of a field-access expression into an separate assign operator.
    private boolean extractFirstArg(AbstractFunctionCallExpression fce, ILogicalOperator op,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalExpression firstArg = fce.getArguments().get(0).getValue();
        if (firstArg.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        SourceLocation sourceLoc = firstArg.getSourceLocation();
        LogicalVariable var1 = context.newVar();
        AssignOperator assignOp = new AssignOperator(new ArrayList<>(Collections.singletonList(var1)),
                new ArrayList<>(Collections.singletonList(new MutableObject<>(firstArg))));
        assignOp.setSourceLocation(sourceLoc);
        VariableReferenceExpression var1Ref = new VariableReferenceExpression(var1);
        var1Ref.setSourceLocation(sourceLoc);
        fce.getArguments().get(0).setValue(var1Ref);
        assignOp.getInputs().add(new MutableObject<>(op.getInputs().get(0).getValue()));
        op.getInputs().get(0).setValue(assignOp);
        context.computeAndSetTypeEnvironmentForOperator(assignOp);
        return true;
    }

    // Rewrites field-access-by-name into field-access-by-index if possible.
    private boolean rewriteFieldAccess(Mutable<ILogicalExpression> exprRef, AbstractFunctionCallExpression fce,
            IAType t) throws AlgebricksException {
        if (t.getTypeTag() != ATypeTag.OBJECT) {
            return false;
        }
        ILogicalExpression fai = createFieldAccessByIndex((ARecordType) t, fce);
        boolean changed = fai != null;
        if (changed) {
            exprRef.setValue(fai);
        }
        return changed;
    }

    // Gets the actual type of a given type.
    private IAType getActualType(IAType t) throws AlgebricksException {
        switch (t.getTypeTag()) {
            case ANY:
            case OBJECT:
                return t;
            case UNION:
                return ((AUnionType) t).getActualType();
            default:
                throw new AlgebricksException("Cannot call field-access on data of type " + t);
        }
    }

    @SuppressWarnings("unchecked")
    private static ILogicalExpression createFieldAccessByIndex(ARecordType recType,
            AbstractFunctionCallExpression fce) {
        String s = ConstantExpressionUtil.getStringArgument(fce, 1);
        if (s == null) {
            return null;
        }
        int k = recType.getFieldIndex(s);
        if (k < 0) {
            return null;
        }
        ScalarFunctionCallExpression faExpr = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_INDEX), fce.getArguments().get(0),
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(k)))));
        faExpr.setSourceLocation(fce.getSourceLocation());
        return faExpr;
    }
}
