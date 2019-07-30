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

import org.apache.asterix.common.config.CompilerProperties;
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
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;

/**
 * Implementations of {@link IFunctionTypeInferer} for built-in functions
 */
public final class FunctionTypeInferers {

    private FunctionTypeInferers() {
    }

    public static final IFunctionTypeInferer SET_EXPRESSION_TYPE = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps) throws AlgebricksException {
            fd.setImmutableStates(context.getType(expr));
        }
    };

    public static final IFunctionTypeInferer SET_STRING_OFFSET = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps) {
            fd.setImmutableStates(compilerProps.getStringOffset());
        }
    };

    public static final IFunctionTypeInferer SET_ARGUMENT_TYPE = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            fd.setImmutableStates(TypeComputeUtils.getActualType(t));
        }
    };

    /** Sets the types of the function arguments */
    public static final IFunctionTypeInferer SET_ARGUMENTS_TYPE = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            fd.setImmutableStates((Object[]) getArgumentsTypes(fce, context));
        }
    };

    public static final IFunctionTypeInferer SET_SORTING_PARAMETERS = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment ctx,
                CompilerProperties compilerProps) throws AlgebricksException {
            // sets the type of the input range map produced by the local sampling expression and types of sort fields
            AbstractFunctionCallExpression funExp = (AbstractFunctionCallExpression) expr;
            Object[] sortingParameters = funExp.getOpaqueParameters();
            fd.setImmutableStates(sortingParameters[0], sortingParameters[1], getArgumentsTypes(funExp, ctx));
        }
    };

    public static final IFunctionTypeInferer SET_NUM_SAMPLES = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression funCallExpr = (AbstractFunctionCallExpression) expr;
            Object[] samplingParameters = funCallExpr.getOpaqueParameters();
            fd.setImmutableStates(samplingParameters[0]);
        }
    };

    public static final IFunctionTypeInferer LISTIFY_INFERER = new IFunctionTypeInferer() {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression listifyExpression = (AbstractFunctionCallExpression) expr;
            IAType outputListType = (IAType) context.getType(listifyExpression);
            IAType itemType = (IAType) context.getType(listifyExpression.getArguments().get(0).getValue());
            fd.setImmutableStates(outputListType, TypeComputeUtils.getActualType(itemType));
        }
    };

    public static final class CastTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            IAType rt = TypeCastUtils.getRequiredType(funcExpr);
            IAType it = (IAType) context.getType(funcExpr.getArguments().get(0).getValue());
            fd.setImmutableStates(rt, it);
        }
    }

    public static final class DeepEqualityTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
            IAType type1 = (IAType) context.getType(f.getArguments().get(1).getValue());
            fd.setImmutableStates(type0, type1);
        }
    }

    public static final class FieldAccessByIndexTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps) throws AlgebricksException {
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
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            AOrderedList fieldPath =
                    (AOrderedList) (((AsterixConstantValue) ((ConstantExpression) fce.getArguments().get(1).getValue())
                            .getValue()).getObject());
            List<String> listFieldPath = new ArrayList<>();
            for (int i = 0; i < fieldPath.size(); i++) {
                listFieldPath.add(((AString) fieldPath.getItem(i)).getStringValue());
            }

            switch (t.getTypeTag()) {
                case OBJECT:
                    fd.setImmutableStates(t, listFieldPath);
                    break;
                case ANY:
                    fd.setImmutableStates(RecordUtil.FULLY_OPEN_RECORD_TYPE, listFieldPath);
                    break;
                default:
                    fd.setImmutableStates(null, listFieldPath);
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
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
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
                CompilerProperties compilerProps) throws AlgebricksException {
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
                CompilerProperties compilerProps) throws AlgebricksException {
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
                CompilerProperties compilerProps) throws AlgebricksException {
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
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            IAType outType = (IAType) context.getType(expr);
            IAType type0 = (IAType) context.getType(f.getArguments().get(0).getValue());
            ILogicalExpression le = f.getArguments().get(1).getValue();
            IAType type1 = (IAType) context.getType(le);
            if (type0.getTypeTag().equals(ATypeTag.ANY)) {
                type0 = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
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
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
            List<Mutable<ILogicalExpression>> args = f.getArguments();
            int n = args.size();
            ARecordType[] argRecordTypes = new ARecordType[n];
            for (int i = 0; i < n; i++) {
                IAType argType = (IAType) context.getType(args.get(i).getValue());
                IAType t = TypeComputeUtils.getActualType(argType);
                if (t.getTypeTag() == ATypeTag.OBJECT) {
                    argRecordTypes[i] = (ARecordType) t;
                }
            }
            fd.setImmutableStates((Object[]) argRecordTypes);
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
}
