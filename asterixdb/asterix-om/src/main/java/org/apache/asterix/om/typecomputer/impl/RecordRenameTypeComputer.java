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
package org.apache.asterix.om.typecomputer.impl;

import java.util.Arrays;

import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;

public class RecordRenameTypeComputer extends AbstractRecordFunctionTypeComputer {
    public static final RecordRenameTypeComputer INSTANCE = new RecordRenameTypeComputer();

    private RecordRenameTypeComputer() {
    }

    @Override
    public IAType computeTypeImpl(AbstractFunctionCallExpression functionCallExpression, IVariableTypeEnvironment env,
            ARecordType inputRecordType, boolean isOutputMissable, boolean isOutputNullable)
            throws AlgebricksException {
        // Our third argument should be of type "string".
        ILogicalExpression arg2 = functionCallExpression.getArguments().get(2).getValue();
        IAType type2 = (IAType) env.getType(arg2);
        IAType actualType2 = TypeComputeUtils.getActualType(type2);
        ATypeTag tag2 = actualType2.getTypeTag();
        if (tag2 == ATypeTag.ANY) {
            // We cannot infer the type of our third argument-- our output may be MISSING or NULL.
            return AUnionType.createUnknownableType(inputRecordType, inputRecordType.getTypeName() + "?");
        } else if (tag2 == ATypeTag.MISSING) {
            // Our output is always going to be MISSING.
            return BuiltinType.AMISSING;
        } else if (tag2 != ATypeTag.STRING) {
            // Our output is always going to be NULL.
            return BuiltinType.ANULL;
        }
        isOutputMissable |= TypeHelper.canBeMissing(type2);
        isOutputNullable |= TypeHelper.canBeNull(type2);

        // We expect a CONSTANT expression for both arguments. Otherwise, defer the replacement to runtime.
        ILogicalExpression arg1 = functionCallExpression.getArguments().get(1).getValue();
        if (arg1.getExpressionTag() != LogicalExpressionTag.CONSTANT
                || arg2.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return wrapTypeWithUnknown(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, isOutputMissable,
                    isOutputNullable);
        }
        ConstantExpression arg1ConstantExpression = (ConstantExpression) arg1;
        ConstantExpression arg2ConstantExpression = (ConstantExpression) arg2;
        AsterixConstantValue arg1ConstantValue = (AsterixConstantValue) arg1ConstantExpression.getValue();
        AsterixConstantValue arg2ConstantValue = (AsterixConstantValue) arg2ConstantExpression.getValue();
        String oldFieldName = ((AString) arg1ConstantValue.getObject()).getStringValue();
        String newFieldName = ((AString) arg2ConstantValue.getObject()).getStringValue();

        // If our field is found, replace it. Otherwise, return the original record type.
        Mutable<Boolean> fieldFound = new MutableObject<>(false);
        String[] newFieldNames = Arrays.stream(inputRecordType.getFieldNames()).map(f -> {
            if (f.equals(oldFieldName)) {
                fieldFound.setValue(true);
                return newFieldName;
            } else {
                return f;
            }
        }).toArray(String[]::new);
        ARecordType outputRecordType;
        if (!fieldFound.getValue()) {
            outputRecordType = inputRecordType;
        } else {
            String inputTypeName = inputRecordType.getTypeName();
            String outputTypeName = inputTypeName != null ? inputTypeName + "_replaced_" + oldFieldName : null;
            outputRecordType = new ARecordType(outputTypeName, newFieldNames, inputRecordType.getFieldTypes(),
                    inputRecordType.isOpen());
        }
        return wrapTypeWithUnknown(outputRecordType, isOutputMissable, isOutputNullable);
    }
}
