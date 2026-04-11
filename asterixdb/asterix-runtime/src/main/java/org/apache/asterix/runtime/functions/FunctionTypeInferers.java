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

package org.apache.asterix.runtime.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * Implementations of {@link IFunctionTypeInferer} for built-in functions
 */
public final class FunctionTypeInferers {

    private FunctionTypeInferers() {
    }

    public static final IFunctionTypeInferer SET_EXPRESSION_TYPE = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            fd.setImmutableStates(context.getType(expr));
        }
    };

    public static final IFunctionTypeInferer SET_ARGUMENT_TYPE = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            fd.setImmutableStates(TypeComputeUtils.getActualType(t));
        }
    };

    /** Sets the types of the function arguments */
    public static final IFunctionTypeInferer SET_ARGUMENTS_TYPE = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            fd.setImmutableStates((Object[]) getArgumentsTypes(fce, context));
        }
    };

    public static final IFunctionTypeInferer SET_SORTING_PARAMETERS = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment ctx,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            // sets the type of the input range map produced by the local sampling expression and types of sort fields
            AbstractFunctionCallExpression funExp = (AbstractFunctionCallExpression) expr;
            Object[] sortingParameters = funExp.getOpaqueParameters();
            fd.setImmutableStates(sortingParameters[0], sortingParameters[1], getArgumentsTypes(funExp, ctx));
        }
    };

    public static final IFunctionTypeInferer SET_NUM_SAMPLES = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression funCallExpr = (AbstractFunctionCallExpression) expr;
            Object[] samplingParameters = funCallExpr.getOpaqueParameters();
            fd.setImmutableStates(samplingParameters[0]);
        }
    };

    public static final IFunctionTypeInferer LISTIFY_INFERER = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression listifyExpression = (AbstractFunctionCallExpression) expr;
            IAType outputListType = (IAType) context.getType(listifyExpression);
            IAType itemType = (IAType) context.getType(listifyExpression.getArguments().get(0).getValue());
            fd.setImmutableStates(outputListType, TypeComputeUtils.getActualType(itemType));
        }
    };

    public static final IFunctionTypeInferer MEDIAN_MEMORY = (expr, fd, context, compilerProps, metadataProvider) -> fd
            .setImmutableStates(compilerProps.getSortMemoryFrames());

    public static final IFunctionTypeInferer RECORD_MODIFY_INFERER =
            (expr, fd, context, compilerProps, metadataProvider) -> {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
                IAType outType = (IAType) context.getType(expr);
                IAType inType = (IAType) context.getType(f.getArguments().get(0).getValue());
                if (inType.getTypeTag().equals(ATypeTag.ANY)) {
                    inType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                }
                fd.setImmutableStates(outType, inType);
            };
    public static final IFunctionTypeInferer SET_OR_TYPE = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            int hashBasedThreshold;
            Map<String, Object> config = mp.getConfig();
            Object hashBasedOptionFromQuery = config.get(CompilerProperties.COMPILER_DISJUNCTION_HASH_THRESHOLD);
            if (hashBasedOptionFromQuery != null) {
                hashBasedThreshold = Integer.parseInt(String.valueOf(hashBasedOptionFromQuery));
            } else {
                hashBasedThreshold = compilerProps.getHashBasedORThreshold();
            }

            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType elementType = useHashBased(fce, hashBasedThreshold);
            if (elementType != null) {
                fd.setImmutableStates((Object[]) new IAType[] { elementType });
            }
        }
    };

    /**
     * Determines if the OR expression can be optimized using hash-based evaluation.
     * <p>Using the hash-based OR is only possible if we have:
     * <ul>
     *   <li>All arguments are equality comparisons (EQ function calls with exactly 2 arguments)</li>
     *   <li>Each EQ has exactly one constant operand and one non-constant operand</li>
     *   <li>All non-constant operands are identical across all EQs</li>
     *   <li>All constant operands have mutually compatible types that can share the same hash function</li>
     * </ul>
     * <p>Examples of non-optimizable expressions:
     * <ul>
     *   <li>{@code EQ(a, 1) OR EQ(b = 2)} (different variables)</li>
     *   <li>{@code EQ(a, 1) OR EQ(a = "2")} (incompatible constant types)</li>
     *   <li>{@code EQ(a, 1) OR EQ(a = null)} (null is not compatible with integer)</li>
     * </ul>
     *
     * @param funcExpr the OR function expression
     * @return the element type for hash-based comparison, or null if optimization is not applicable
     */
    private static IAType useHashBased(AbstractFunctionCallExpression funcExpr, int hashBasedThreshold) {
        List<Mutable<ILogicalExpression>> orArgs = funcExpr.getArguments();
        if (hashBasedThreshold < 0 || orArgs.size() < 2 || orArgs.size() < hashBasedThreshold) {
            return null;
        }
        LogicalVariable commonVar = null;
        AbstractFunctionCallExpression commonFunExpr = null;
        IAType type = null;
        boolean usesVar = false;
        boolean usesFun = false;

        for (Mutable<ILogicalExpression> arg : orArgs) {
            ILogicalExpression argExpr = arg.getValue();
            if (!apply(argExpr)) {
                return null;
            }

            AbstractFunctionCallExpression eqExpr = (AbstractFunctionCallExpression) argExpr;
            List<Mutable<ILogicalExpression>> eqArgs = eqExpr.getArguments();
            ILogicalExpression left = eqArgs.get(0).getValue();
            ILogicalExpression right = eqArgs.get(1).getValue();

            ConstantExpression constExpr;
            VariableReferenceExpression varExpr = null;
            AbstractFunctionCallExpression currentFunExpr = null;

            if (left.getExpressionTag() == LogicalExpressionTag.VARIABLE
                    && right.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                varExpr = (VariableReferenceExpression) left;
                constExpr = (ConstantExpression) right;
            } else if (right.getExpressionTag() == LogicalExpressionTag.VARIABLE
                    && left.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                varExpr = (VariableReferenceExpression) right;
                constExpr = (ConstantExpression) left;
            } else if (left.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                    && right.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                currentFunExpr = (AbstractFunctionCallExpression) left;
                constExpr = (ConstantExpression) right;
            } else if (right.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                    && left.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                currentFunExpr = (AbstractFunctionCallExpression) right;
                constExpr = (ConstantExpression) left;
            } else {
                return null;
            }

            if (type == null) {
                type = ((AsterixConstantValue) constExpr.getValue()).getObject().getType();
            } else if (!ATypeHierarchy.isCompatible(type.getTypeTag(),
                    ((AsterixConstantValue) constExpr.getValue()).getObject().getType().getTypeTag())) {
                return null;
            }

            if (varExpr != null) {
                if (usesFun) {
                    return null;
                }
                usesVar = true;
                LogicalVariable var = varExpr.getVariableReference();
                if (commonVar == null) {
                    commonVar = var;
                } else if (!commonVar.equals(var)) {
                    return null;
                }
            } else {
                if (usesVar) {
                    return null;
                }
                usesFun = true;
                if (commonFunExpr == null) {
                    commonFunExpr = currentFunExpr;
                } else if (!commonFunExpr.equals(currentFunExpr)) {
                    return null;
                }
            }
        }

        if (!usesVar && !usesFun) {
            return null;
        }
        return type;

    }

    private static boolean apply(ILogicalExpression argExpr) {
        return argExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                && ((AbstractFunctionCallExpression) argExpr).getFunctionIdentifier()
                        .equals(AlgebricksBuiltinFunctions.EQ)
                && ((AbstractFunctionCallExpression) argExpr).getArguments().size() == 2;
    }

    public static final class CastTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            IAType reqType = TypeCastUtils.getRequiredType(funcExpr);
            IAType inputType = (IAType) context.getType(funcExpr.getArguments().get(0).getValue());
            // If reqType or inputType is null it indicates there is a bug in the compiler.
            if (reqType == null || inputType == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, expr.getSourceLocation(),
                        "Invalid types for casting, required type " + reqType + ", input type " + inputType);
            }
            IAType rt = TypeComputeUtils.getActualType(reqType);
            IAType it = TypeComputeUtils.getActualType(inputType);
            fd.setImmutableStates(rt, it);
        }
    }

    public static final class DeepEqualityTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
            IAType type1 = (IAType) context.getType(f.getArguments().get(1).getValue());
            fd.setImmutableStates(type0, type1);
        }
    }

    public static final class FieldAccessByIndexTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            switch (t.getTypeTag()) {
                case OBJECT:
                    fd.setImmutableStates(t);
                    break;
                case UNION:
                    AUnionType unionT = (AUnionType) t;
                    if (unionT.isUnknownableType()) {
                        IAType t2 = unionT.getActualType();
                        if (t2.getTypeTag() == ATypeTag.OBJECT) {
                            fd.setImmutableStates(t2);
                        }
                    }
                    break;
            }
        }
    }

    public static final class FieldAccessNestedTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            // arg 1 should always be a constant array of strings
            AOrderedList fieldPath =
                    (AOrderedList) (((AsterixConstantValue) ((ConstantExpression) fce.getArguments().get(1).getValue())
                            .getValue()).getObject());
            List<String> listFieldPath = new ArrayList<>();
            for (int i = 0; i < fieldPath.size(); i++) {
                listFieldPath.add(((AString) fieldPath.getItem(i)).getStringValue());
            }
            IAType t = TypeComputeUtils.getActualType((IAType) context.getType(fce.getArguments().get(0).getValue()));
            switch (t.getTypeTag()) {
                case OBJECT:
                    fd.setImmutableStates(t, listFieldPath);
                    break;
                default:
                    fd.setImmutableStates(RecordUtil.FULLY_OPEN_RECORD_TYPE, listFieldPath);
                    break;
            }
        }
    }

    public static final class RecordAccessorTypeInferer implements IFunctionTypeInferer {

        public static final IFunctionTypeInferer INSTANCE_STRICT = new RecordAccessorTypeInferer(true);

        public static final IFunctionTypeInferer INSTANCE_LAX = new RecordAccessorTypeInferer(false);

        private final boolean strict;

        private RecordAccessorTypeInferer(boolean strict) {
            this.strict = strict;
        }

        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = TypeComputeUtils.getActualType((IAType) context.getType(fce.getArguments().get(0).getValue()));
            ATypeTag typeTag = t.getTypeTag();
            switch (typeTag) {
                case OBJECT:
                    fd.setImmutableStates(t);
                    break;
                case ANY:
                    fd.setImmutableStates(RecordUtil.FULLY_OPEN_RECORD_TYPE);
                    break;
                default:
                    if (strict) {
                        throw new NotImplementedException(fd.getIdentifier().getName() + " for data of type " + t);
                    } else {
                        fd.setImmutableStates(new Object[] { null });
                    }
                    break;
            }
        }
    }

    public static final class OpenRecordConstructorTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            ARecordType rt = (ARecordType) context.getType(expr);
            fd.setImmutableStates(rt, computeOpenFields((AbstractFunctionCallExpression) expr, rt));
        }

        private boolean[] computeOpenFields(AbstractFunctionCallExpression expr, ARecordType recType) {
            int n = expr.getArguments().size() / 2;
            boolean[] open = new boolean[n];
            for (int i = 0; i < n; i++) {
                Mutable<ILogicalExpression> argRef = expr.getArguments().get(2 * i);
                ILogicalExpression arg = argRef.getValue();
                open[i] = true;
                final String fn = ConstantExpressionUtil.getStringConstant(arg);
                if (fn != null) {
                    for (String s : recType.getFieldNames()) {
                        if (s.equals(fn)) {
                            open[i] = false;
                            break;
                        }
                    }
                }
            }
            return open;
        }
    }

    public static final class RecordAddFieldsTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            IAType outType = (IAType) context.getType(expr);
            IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
            ILogicalExpression listExpr = f.getArguments().get(1).getValue();
            IAType type1 = (IAType) context.getType(listExpr);
            if (type0.getTypeTag().equals(ATypeTag.ANY)) {
                type0 = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            }
            if (type1.getTypeTag().equals(ATypeTag.ANY)) {
                type1 = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
            }
            fd.setImmutableStates(outType, type0, type1);
        }
    }

    public static final class RecordMergeTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            IAType outType = (IAType) context.getType(expr);
            IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
            IAType type1 = (IAType) context.getType(f.getArguments().get(1).getValue());
            fd.setImmutableStates(outType, type0, type1);
        }
    }

    public static final class RecordRemoveFieldsTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            IAType outType = (IAType) context.getType(expr);
            IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
            ILogicalExpression le = f.getArguments().get(1).getValue();
            IAType type1 = (IAType) context.getType(le);
            if (type0.getTypeTag().equals(ATypeTag.ANY)) {
                type0 = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            } else if (type0.getTypeTag().equals(ATypeTag.UNION)) {
                type0 = ((AUnionType) type0).getActualType();
            }
            if (type1.getTypeTag().equals(ATypeTag.ANY)) {
                type1 = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
            }
            fd.setImmutableStates(outType, type0, type1);
        }
    }

    public static final class RecordConcatTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            List<Mutable<ILogicalExpression>> args = f.getArguments();
            int n = args.size();
            ARecordType[] argRecordTypes = new ARecordType[n];
            ARecordType listItemRecordType = null;
            if (n == 1) {
                // check and handle if it's the single argument list case
                IAType t = getExprActualType(args.get(0).getValue(), context);
                if (t.getTypeTag().isListType()) {
                    listItemRecordType = getListItemRecordType(t);
                } else if (t.getTypeTag() == ATypeTag.OBJECT) {
                    argRecordTypes[0] = (ARecordType) t;
                }
            } else {
                for (int i = 0; i < n; i++) {
                    IAType t = getExprActualType(args.get(i).getValue(), context);
                    if (t.getTypeTag() == ATypeTag.OBJECT) {
                        argRecordTypes[i] = (ARecordType) t;
                    }
                }
            }
            fd.setImmutableStates(argRecordTypes, listItemRecordType);
        }

        private static IAType getExprActualType(ILogicalExpression expr, IVariableTypeEnvironment ctx)
                throws AlgebricksException {
            return TypeComputeUtils.getActualType((IAType) ctx.getType(expr));
        }

        private static ARecordType getListItemRecordType(IAType listType) {
            IAType itemType = ((AbstractCollectionType) listType).getItemType();
            return itemType.getTypeTag() == ATypeTag.OBJECT ? (ARecordType) itemType : null;
        }
    }

    public static final class FullTextContainsTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            // get the full-text config evaluator from the expr which is set in FullTextContainsParameterCheckAndSetRule
            fd.setImmutableStates(funcExpr.getOpaqueParameters()[0]);
        }
    }

    public static final class PutAutogeneratedKeyTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            IAType outType = (IAType) context.getType(expr);
            IAType incRecType = (IAType) context.getType(f.getArguments().get(0).getValue());
            fd.setImmutableStates(outType, incRecType);
        }
    }

    private static IAType[] getArgumentsTypes(AbstractFunctionCallExpression funExp, IVariableTypeEnvironment ctx)
            throws AlgebricksException {
        IAType[] argsTypes = new IAType[funExp.getArguments().size()];
        int i = 0;
        for (Mutable<ILogicalExpression> arg : funExp.getArguments()) {
            argsTypes[i] = TypeComputeUtils.getActualType((IAType) ctx.getType(arg.getValue()));
            i++;
        }
        return argsTypes;
    }

    public static final class ToObjectVarStrTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps, IMetadataProvider<?, ?> mp) throws AlgebricksException {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            List<Mutable<ILogicalExpression>> args = f.getArguments();
            fd.setImmutableStates(context.getType(expr),
                    TypeComputeUtils.getActualType((IAType) context.getType(args.get(0).getValue())));
        }

    }
}
