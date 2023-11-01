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
import java.util.List;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.rules.visitor.ConstantFoldingVisitor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class NormalizeWritingPathRule implements IAlgebraicRewriteRule {
    private final ConstantFoldingVisitor cfv;
    private boolean checked = false;

    public NormalizeWritingPathRule(ICcApplicationContext appCtx) {
        cfv = new ConstantFoldingVisitor(appCtx);
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (checked) {
            return false;
        }

        checked = true;
        ILogicalOperator distributeResultOp = opRef.getValue();
        ILogicalOperator op = distributeResultOp.getInputs().get(0).getValue();
        if (distributeResultOp.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT
                || op.getOperatorTag() != LogicalOperatorTag.WRITE) {
            return false;
        }

        cfv.reset(context);
        WriteOperator writeOp = (WriteOperator) op;
        Mutable<ILogicalExpression> pathExprRef = writeOp.getPathExpression();
        IVariableTypeEnvironment typeEnv = context.getOutputTypeEnvironment(distributeResultOp);
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        IFunctionInfo info = metadataProvider.lookupFunction(BuiltinFunctions.TO_STRING);
        boolean changed = normalize(pathExprRef, info, typeEnv);

        context.computeAndSetTypeEnvironmentForOperator(distributeResultOp);
        return changed;
    }

    private boolean normalize(Mutable<ILogicalExpression> exprRef, IFunctionInfo toStringInfo,
            IVariableTypeEnvironment typeEnv) throws AlgebricksException {
        ILogicalExpression expression = exprRef.getValue();
        if (expression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return wrapInToStringIfNeeded(exprRef, toStringInfo, typeEnv);
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        if (!BuiltinFunctions.STRING_CONCAT.equals(funcExpr.getFunctionIdentifier())) {
            return wrapInToStringIfNeeded(exprRef, toStringInfo, typeEnv);
        }

        Mutable<ILogicalExpression> concatArgRef = funcExpr.getArguments().get(0);
        ILogicalExpression concatArg = concatArgRef.getValue();
        if (concatArg.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return wrapInToStringIfNeeded(concatArgRef, toStringInfo, typeEnv);
        }

        AbstractFunctionCallExpression listConst = (AbstractFunctionCallExpression) concatArg;
        List<Mutable<ILogicalExpression>> args = listConst.getArguments();
        List<Mutable<ILogicalExpression>> newArgs = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        int foldedConst = 0;
        boolean changed = false;
        SourceLocation constSrcLocation = null;
        for (int i = 0; i < args.size(); i++) {
            Mutable<ILogicalExpression> argRef = args.get(i);
            changed |= wrapInToStringIfNeeded(argRef, toStringInfo, typeEnv);
            ILogicalExpression argExpr = argRef.getValue();
            ATypeTag typeTag = evaluateAndAppendString(argExpr, builder, typeEnv);

            if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
                // Fail early as no data will be written
                throw new CompilationException(ErrorCode.NON_STRING_WRITE_PATH, argExpr.getSourceLocation(), typeTag);
            } else if (typeTag == ATypeTag.STRING) {
                if (foldedConst++ == 0) {
                    constSrcLocation = argExpr.getSourceLocation();
                }
            } else {
                changed |= addFoldedArgument(foldedConst, builder, args, i, newArgs, constSrcLocation);
                newArgs.add(argRef);
            }
        }

        changed |= addFoldedArgument(foldedConst, builder, args, args.size(), newArgs, constSrcLocation);
        args.clear();
        args.addAll(newArgs);

        return changed;
    }

    private boolean addFoldedArgument(int foldedConst, StringBuilder builder, List<Mutable<ILogicalExpression>> args,
            int i, List<Mutable<ILogicalExpression>> newArgs, SourceLocation constSrcLocation) {
        boolean changed = false;
        if (foldedConst == 1) {
            newArgs.add(args.get(i - 1));
            builder.setLength(0);
        } else if (foldedConst > 1) {
            ILogicalExpression contExpr = ConstantExpressionUtil.create(builder.toString(), constSrcLocation);
            newArgs.add(new MutableObject<>(contExpr));
            builder.setLength(0);
            changed = true;
        }
        return changed;
    }

    private ATypeTag evaluateAndAppendString(ILogicalExpression argExpr, StringBuilder builder,
            IVariableTypeEnvironment typeEnv) throws AlgebricksException {
        Pair<Boolean, ILogicalExpression> pair = argExpr.accept(cfv, null);
        ILogicalExpression foldedExpr = pair.getSecond();

        if (foldedExpr == null || foldedExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return ATypeTag.ANY;
        }

        ATypeTag typeTag = ((IAType) typeEnv.getType(foldedExpr)).getTypeTag();
        if (typeTag == ATypeTag.STRING) {
            builder.append(ConstantExpressionUtil.getStringConstant(foldedExpr));
        }

        return typeTag;
    }

    private boolean wrapInToStringIfNeeded(Mutable<ILogicalExpression> exprRef, IFunctionInfo fInfo,
            IVariableTypeEnvironment typeEnv) throws AlgebricksException {
        ILogicalExpression expr = exprRef.getValue();
        IAType type = (IAType) typeEnv.getType(expr);
        if (isString(type) || isNullOrMissing(type)) {
            return false;
        }

        ScalarFunctionCallExpression toString = new ScalarFunctionCallExpression(fInfo, new MutableObject<>(expr));
        exprRef.setValue(toString);
        return true;
    }

    private boolean isNullOrMissing(IAType type) {
        ATypeTag typeTag = type.getTypeTag();
        return typeTag == ATypeTag.NULL || typeTag == ATypeTag.MISSING;
    }

    private boolean isString(IAType type) {
        IAType actualType = type;
        if (actualType.getTypeTag() == ATypeTag.UNION) {
            actualType = ((AUnionType) actualType).getActualType();
        }
        return actualType.getTypeTag() == ATypeTag.STRING;
    }

}
