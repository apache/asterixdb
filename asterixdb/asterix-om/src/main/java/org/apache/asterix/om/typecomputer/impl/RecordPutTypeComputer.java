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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;

public class RecordPutTypeComputer extends AbstractRecordFunctionTypeComputer {
    public static final RecordPutTypeComputer INSTANCE = new RecordPutTypeComputer();

    private RecordPutTypeComputer() {
    }

    @Override
    public IAType computeTypeImpl(AbstractFunctionCallExpression functionCallExpression, IVariableTypeEnvironment env,
            ARecordType inputRecordType, boolean isOutputMissable, boolean isOutputNullable)
            throws AlgebricksException {
        // Extract the type of our third argument. If it is MISSING, then we are performing a field removal.
        ILogicalExpression arg2 = functionCallExpression.getArguments().get(2).getValue();
        IAType type2 = (IAType) env.getType(arg2);
        IAType actualType2 = TypeComputeUtils.getActualType(type2);
        boolean isFieldRemoval = actualType2.getTypeTag() == ATypeTag.MISSING;

        // We expect a constant for our second argument.
        ILogicalExpression arg1 = functionCallExpression.getArguments().get(1).getValue();
        if (arg1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return wrapTypeWithUnknown(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, isOutputMissable,
                    isOutputNullable);
        }
        ConstantExpression constantExpression = (ConstantExpression) arg1;
        AsterixConstantValue constantValue = (AsterixConstantValue) constantExpression.getValue();
        String newFieldName = ((AString) constantValue.getObject()).getStringValue();

        // Remove or replace our field name and type (dependent on the type of our third argument).
        boolean fieldFound = false;
        List<String> outputFieldNames = new ArrayList<>();
        List<IAType> outputFieldTypes = new ArrayList<>();
        for (int i = 0; i < inputRecordType.getFieldNames().length; i++) {
            String inputFieldName = inputRecordType.getFieldNames()[i];
            IAType inputFieldType = inputRecordType.getFieldTypes()[i];
            if (!inputFieldName.equals(newFieldName)) {
                outputFieldNames.add(inputFieldName);
                outputFieldTypes.add(inputFieldType);

            } else {
                fieldFound = true;
                if (!isFieldRemoval) {
                    // Replace our input field type.
                    outputFieldNames.add(inputFieldName);
                    outputFieldTypes.add(type2);
                }
            }
        }

        // Build our output record type.
        ARecordType outputRecordType;
        String inputTypeName = inputRecordType.getTypeName();
        boolean doesRecordHaveTypeName = inputTypeName != null;
        if (fieldFound && isFieldRemoval) {
            // We have removed our argument field.
            String outputTypeName = doesRecordHaveTypeName ? inputTypeName + "_remove_" + newFieldName : null;
            outputRecordType = new ARecordType(outputTypeName, outputFieldNames.toArray(String[]::new),
                    outputFieldTypes.toArray(IAType[]::new), inputRecordType.isOpen());
        } else if (fieldFound) { // && !isFieldRemoval
            // We have replaced our argument field.
            String outputTypeName = doesRecordHaveTypeName ? inputTypeName + "_replaced_" + newFieldName : null;
            outputRecordType = new ARecordType(outputTypeName, outputFieldNames.toArray(String[]::new),
                    outputFieldTypes.toArray(IAType[]::new), inputRecordType.isOpen());
        } else if (!isFieldRemoval) { // && !wasFieldFound
            // We need to insert our argument field.
            outputFieldNames.add(newFieldName);
            outputFieldTypes.add(type2);
            String outputTypeName = doesRecordHaveTypeName ? inputTypeName + "_add_" + newFieldName : null;
            outputRecordType = new ARecordType(outputTypeName, outputFieldNames.toArray(String[]::new),
                    outputFieldTypes.toArray(IAType[]::new), inputRecordType.isOpen());
        } else { // isFieldRemoval && !wasFieldFound
            // We have not found the field to remove.
            outputRecordType = inputRecordType;
        }
        return wrapTypeWithUnknown(outputRecordType, isOutputMissable, isOutputNullable);
    }
}
