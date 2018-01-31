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

import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
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

import java.util.ArrayList;
import java.util.List;

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
                case OBJECT: {
                    fd.setImmutableStates(t);
                    break;
                }
                case UNION: {
                    AUnionType unionT = (AUnionType) t;
                    if (unionT.isUnknownableType()) {
                        IAType t2 = unionT.getActualType();
                        if (t2.getTypeTag() == ATypeTag.OBJECT) {
                            fd.setImmutableStates(t2);
                            break;
                        }
                    }
                    throw new NotImplementedException("field-access-by-index for data of type " + t);
                }
                default: {
                    throw new NotImplementedException("field-access-by-index for data of type " + t);
                }
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
                case OBJECT: {
                    fd.setImmutableStates(t, listFieldPath);
                    break;
                }
                case ANY:
                    fd.setImmutableStates(RecordUtil.FULLY_OPEN_RECORD_TYPE, listFieldPath);
                    break;
                default: {
                    throw new NotImplementedException("field-access-nested for data of type " + t);
                }
            }
        }
    }

    public static final class GetRecordFieldsTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            ATypeTag typeTag = t.getTypeTag();
            if (typeTag.equals(ATypeTag.OBJECT)) {
                fd.setImmutableStates(t);
            } else if (typeTag.equals(ATypeTag.ANY)) {
                fd.setImmutableStates(RecordUtil.FULLY_OPEN_RECORD_TYPE);
            } else {
                throw new NotImplementedException("get-record-fields for data of type " + t);
            }
        }
    }

    public static final class GetRecordFieldValueTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            ATypeTag typeTag = t.getTypeTag();
            if (typeTag.equals(ATypeTag.OBJECT)) {
                fd.setImmutableStates(t);
            } else if (typeTag.equals(ATypeTag.ANY)) {
                fd.setImmutableStates(RecordUtil.FULLY_OPEN_RECORD_TYPE);
            } else {
                throw new NotImplementedException("get-record-field-value for data of type " + t);
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

    public static final class RecordPairsTypeInferer implements IFunctionTypeInferer {
        @Override
        public void infer(ILogicalExpression expr, IFunctionDescriptor fd, IVariableTypeEnvironment context,
                CompilerProperties compilerProps) throws AlgebricksException {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            IAType t = (IAType) context.getType(fce.getArguments().get(0).getValue());
            ATypeTag typeTag = t.getTypeTag();
            if (typeTag.equals(ATypeTag.OBJECT)) {
                fd.setImmutableStates(t);
            } else if (typeTag.equals(ATypeTag.ANY)) {
                fd.setImmutableStates(RecordUtil.FULLY_OPEN_RECORD_TYPE);
            } else {
                throw new NotImplementedException("record-fields with data of type " + t);
            }
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
}
