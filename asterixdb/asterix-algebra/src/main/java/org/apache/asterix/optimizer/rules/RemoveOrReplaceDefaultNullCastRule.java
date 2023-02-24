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
import java.util.List;
import java.util.Map;

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule removes unnecessary default-null casts functions for known types
 * For example:
 * <p>
 * Before:
 * assign [$$uid] <- [uuid-default-null(uuid())]
 * After:
 * assign [$$uid] <- [uuid()]
 * <p>
 * Before:
 * assign [$$uid] <- [string-default-null(uuid())]
 * After:
 * assign [$$uid] <- [string(uuid())]
 * <p>
 * It is known that uuid() will not produce a null value. Hence, casting it using uuid-default-null() or
 * string-default-null() are useless
 */
public class RemoveOrReplaceDefaultNullCastRule implements IAlgebraicRewriteRule {
    private static final Map<FunctionIdentifier, FunctionIdentifier> CAST_MAP;

    static {
        CAST_MAP = new HashMap<>();
        CAST_MAP.put(BuiltinFunctions.BOOLEAN_DEFAULT_NULL_CONSTRUCTOR, BuiltinFunctions.BOOLEAN_CONSTRUCTOR);

        CAST_MAP.put(BuiltinFunctions.INT8_DEFAULT_NULL_CONSTRUCTOR, BuiltinFunctions.INT8_CONSTRUCTOR);
        CAST_MAP.put(BuiltinFunctions.INT16_DEFAULT_NULL_CONSTRUCTOR, BuiltinFunctions.INT16_CONSTRUCTOR);
        CAST_MAP.put(BuiltinFunctions.INT32_DEFAULT_NULL_CONSTRUCTOR, BuiltinFunctions.INT32_CONSTRUCTOR);
        CAST_MAP.put(BuiltinFunctions.INT64_DEFAULT_NULL_CONSTRUCTOR, BuiltinFunctions.INT64_CONSTRUCTOR);

        CAST_MAP.put(BuiltinFunctions.FLOAT_DEFAULT_NULL_CONSTRUCTOR, BuiltinFunctions.FLOAT_CONSTRUCTOR);
        CAST_MAP.put(BuiltinFunctions.DOUBLE_DEFAULT_NULL_CONSTRUCTOR, BuiltinFunctions.DOUBLE_CONSTRUCTOR);

        // *_DEFAULT_NULL_WITH_FORMAT_CONSTRUCTOR are not considered here as format may differ from the original value
        CAST_MAP.put(BuiltinFunctions.DATE_DEFAULT_NULL_CONSTRUCTOR, BuiltinFunctions.DATE_CONSTRUCTOR);
        CAST_MAP.put(BuiltinFunctions.TIME_DEFAULT_NULL_CONSTRUCTOR, BuiltinFunctions.TIME_CONSTRUCTOR);
        CAST_MAP.put(BuiltinFunctions.DATETIME_DEFAULT_NULL_CONSTRUCTOR, BuiltinFunctions.DATETIME_CONSTRUCTOR);

        CAST_MAP.put(BuiltinFunctions.DURATION_DEFAULT_NULL_CONSTRUCTOR, BuiltinFunctions.DURATION_CONSTRUCTOR);
        CAST_MAP.put(BuiltinFunctions.DAY_TIME_DURATION_DEFAULT_NULL_CONSTRUCTOR,
                BuiltinFunctions.DAY_TIME_DURATION_CONSTRUCTOR);
        CAST_MAP.put(BuiltinFunctions.YEAR_MONTH_DURATION_DEFAULT_NULL_CONSTRUCTOR,
                BuiltinFunctions.YEAR_MONTH_DURATION_CONSTRUCTOR);

        CAST_MAP.put(BuiltinFunctions.STRING_DEFAULT_NULL_CONSTRUCTOR, BuiltinFunctions.STRING_CONSTRUCTOR);

        CAST_MAP.put(BuiltinFunctions.BINARY_BASE64_DEFAULT_NULL_CONSTRUCTOR,
                BuiltinFunctions.BINARY_BASE64_CONSTRUCTOR);

        CAST_MAP.put(BuiltinFunctions.UUID_DEFAULT_NULL_CONSTRUCTOR, BuiltinFunctions.UUID_CONSTRUCTOR);
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();

        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            //process assign
            AssignOperator assignOp = (AssignOperator) op;
            return processExpressions(context, assignOp, assignOp.getExpressions());
        } else if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            //process select
            SelectOperator selectOp = (SelectOperator) op;
            return processExpression(context, selectOp, selectOp.getCondition());
        }
        return false;
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
        ILogicalExpression expr = exprRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();

        // First, process arguments to handle nested casts
        boolean changed = processExpressions(context, op, funcExpr.getArguments());
        if (!CAST_MAP.containsKey(fid)) {
            return changed;
        }

        ILogicalExpression castArgExpr = funcExpr.getArguments().get(0).getValue();
        IVariableTypeEnvironment env = context.getOutputTypeEnvironment(op);

        IAType outputType = ((AUnionType) env.getType(funcExpr)).getActualType();
        IAType argType = (IAType) env.getType(castArgExpr);

        //If arg type is a union type (or unknowable), then the function removed as below
        if (isDerivedOrAny(argType) || !outputType.equals(argType) && !isConvertableType(fid, outputType, argType)) {
            // The types of cast and its argument are different
            // Also, the cast function isn't a string function
            return changed;
        }

        if (outputType.equals(argType)) {
            exprRef.setValue(castArgExpr);
        } else {
            MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
            IFunctionInfo functionInfo = metadataProvider.lookupFunction(CAST_MAP.get(fid));
            funcExpr.setFunctionInfo(functionInfo);
            context.computeAndSetTypeEnvironmentForOperator(op);
        }
        return true;
    }

    private boolean isDerivedOrAny(IAType argType) {
        ATypeTag argTypeTag = argType.getTypeTag();
        return argTypeTag.isDerivedType() || argTypeTag == ATypeTag.ANY;
    }

    private boolean isConvertableType(FunctionIdentifier fid, IAType outputType, IAType argType) {
        ATypeTag outputTypeTag = outputType.getTypeTag();
        ATypeTag argTypeTag = argType.getTypeTag();

        boolean convertableNumeric = ATypeHierarchy.getTypeDomain(outputTypeTag) == ATypeHierarchy.Domain.NUMERIC
                && ATypeHierarchy.getTypeDomain(argTypeTag) == ATypeHierarchy.Domain.NUMERIC
                && (ATypeHierarchy.canPromote(argTypeTag, outputTypeTag)
                        || ATypeHierarchy.canDemote(argTypeTag, outputTypeTag));

        // converting to string is suitable for all non-derived types
        return BuiltinFunctions.STRING_DEFAULT_NULL_CONSTRUCTOR.equals(fid) || convertableNumeric;
    }
}
