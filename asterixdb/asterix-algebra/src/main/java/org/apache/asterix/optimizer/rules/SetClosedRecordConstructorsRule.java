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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.AbstractConstVarFunVisitor;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.util.LogRedactionUtil;

/**
 * open-record-constructor() becomes closed-record-constructor() if all the
 * branches below lead to dataset scans for closed record types
 */

public class SetClosedRecordConstructorsRule implements IAlgebraicRewriteRule {

    final private SettingClosedRecordVisitor recordVisitor;

    public SetClosedRecordConstructorsRule() {
        this.recordVisitor = new SettingClosedRecordVisitor();
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext ctx) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (ctx.checkIfInDontApplySet(this, op)) {
            return false;
        }
        ctx.addToDontApplySet(this, op);
        this.recordVisitor.setOptimizationContext(ctx, op.computeInputTypeEnvironment(ctx));
        boolean res = op.acceptExpressionTransform(recordVisitor);
        if (res) {
            ctx.computeAndSetTypeEnvironmentForOperator(op);
        }
        return res;
    }

    private static class SettingClosedRecordVisitor extends AbstractConstVarFunVisitor<ClosedDataInfo, Void>
            implements ILogicalExpressionReferenceTransform {

        private IOptimizationContext context;
        private IVariableTypeEnvironment env;

        public void setOptimizationContext(IOptimizationContext context, IVariableTypeEnvironment env) {
            this.context = context;
            this.env = env;
        }

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            AbstractLogicalExpression expr = (AbstractLogicalExpression) exprRef.getValue();
            ClosedDataInfo cdi = expr.accept(this, null);
            if (cdi.expressionChanged) {
                exprRef.setValue(cdi.expression);
            }
            return cdi.expressionChanged;
        }

        @Override
        public ClosedDataInfo visitConstantExpression(ConstantExpression expr, Void arg) throws AlgebricksException {
            return new ClosedDataInfo(false, hasClosedType(expr), expr);
        }

        @Override
        public ClosedDataInfo visitFunctionCallExpression(AbstractFunctionCallExpression expr, Void arg)
                throws AlgebricksException {
            boolean allClosed = true;
            boolean changed = false;
            if (expr.getFunctionIdentifier().equals(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)) {
                ARecordType reqType = (ARecordType) TypeCastUtils.getRequiredType(expr);
                if (reqType == null || !reqType.isOpen()) {
                    int n = expr.getArguments().size();
                    if (n % 2 > 0) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, expr.getSourceLocation(),
                                "Record constructor expected to have an even number of arguments: "
                                        + LogRedactionUtil.userData(expr.toString()));
                    }
                    for (int i = 0; i < n / 2; i++) {
                        ILogicalExpression a0 = expr.getArguments().get(2 * i).getValue();
                        if (a0.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                            allClosed = false;
                        }
                        Mutable<ILogicalExpression> aRef1 = expr.getArguments().get(2 * i + 1);
                        ILogicalExpression a1 = aRef1.getValue();
                        ClosedDataInfo cdi = a1.accept(this, arg);
                        if (!cdi.dataIsClosed) {
                            allClosed = false;
                        }
                        if (cdi.expressionChanged) {
                            aRef1.setValue(cdi.expression);
                            changed = true;
                        }
                    }
                    if (allClosed) {
                        expr.setFunctionInfo(FunctionUtil.getFunctionInfo(BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR));
                        AlgebricksConfig.ALGEBRICKS_LOGGER.trace(() -> "Switching to CLOSED record constructor in "
                                + LogRedactionUtil.userData(expr.toString()) + ".\n");
                        changed = true;
                    }
                }
            } else {
                boolean rewrite = true;
                if (expr.getFunctionIdentifier().equals(BuiltinFunctions.ORDERED_LIST_CONSTRUCTOR)
                        || (expr.getFunctionIdentifier().equals(BuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR))) {
                    IAType reqType = TypeCastUtils.getRequiredType(expr);
                    if (reqType == null) {
                        rewrite = false;
                    }
                }
                if (rewrite) {
                    for (Mutable<ILogicalExpression> e : expr.getArguments()) {
                        ILogicalExpression ale = e.getValue();
                        ClosedDataInfo cdi = ale.accept(this, arg);
                        if (!cdi.dataIsClosed) {
                            allClosed = false;
                        }
                        if (cdi.expressionChanged) {
                            e.setValue(cdi.expression);
                            changed = true;
                        }
                    }
                }
            }
            return new ClosedDataInfo(changed, hasClosedType(expr), expr);
        }

        @Override
        public ClosedDataInfo visitVariableReferenceExpression(VariableReferenceExpression expr, Void arg)
                throws AlgebricksException {
            Object varType = env.getVarType(expr.getVariableReference());
            if (varType == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, expr.getSourceLocation(),
                        "Could not infer type for variable '" + expr.getVariableReference() + "'.");
            }
            return new ClosedDataInfo(false, TypeHelper.isClosed((IAType) varType), expr);
        }

        private boolean hasClosedType(ILogicalExpression expr) throws AlgebricksException {
            IAType t = (IAType) context.getExpressionTypeComputer().getType(expr, context.getMetadataProvider(), env);
            return TypeHelper.isClosed(t);
        }
    }

    private static class ClosedDataInfo {
        boolean expressionChanged;
        boolean dataIsClosed;
        ILogicalExpression expression;

        public ClosedDataInfo(boolean expressionChanged, boolean dataIsClosed, ILogicalExpression expression) {
            this.expressionChanged = expressionChanged;
            this.dataIsClosed = dataIsClosed;
            this.expression = expression;
        }
    }
}
