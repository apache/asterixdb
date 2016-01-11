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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class RecordAddFieldsTypeComputer implements IResultTypeComputer {
    public static final RecordAddFieldsTypeComputer INSTANCE = new RecordAddFieldsTypeComputer();

    private static final String FIELD_NAME_NAME = "field-name";
    private static final String FIELD_VALUE_VALUE = "field-value";

    private RecordAddFieldsTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        IAType type0 = (IAType) env.getType(funcExpr.getArguments().get(0).getValue());

        ARecordType inputRecordType = TypeComputerUtils.extractRecordType(type0);
        if (inputRecordType == null) {
            throw new AlgebricksException("Input record cannot be null");
        }

        AbstractLogicalExpression arg1 = (AbstractLogicalExpression) funcExpr.getArguments().get(1).getValue();
        IAType type1 = (IAType) env.getType(arg1);
        AOrderedListType inputOrderedListType = TypeComputerUtils.extractOrderedListType(type1);
        if (inputOrderedListType == null) {
            return inputRecordType;
        }

        boolean nullable = TypeHelper.canBeNull(type0) || TypeHelper.canBeNull(type1);
        Map<String, IAType> additionalFields = new HashMap<>();
        List<String> resultFieldNames = new ArrayList<>();
        List<IAType> resultFieldTypes = new ArrayList<>();

        resultFieldNames.addAll(Arrays.asList(inputRecordType.getFieldNames()));
        Collections.sort(resultFieldNames);

        for (String fieldName : resultFieldNames) {
            try {
                if (inputRecordType.getFieldType(fieldName).getTypeTag() == ATypeTag.RECORD) {
                    ARecordType nestedType = (ARecordType) inputRecordType.getFieldType(fieldName);
                    //Deep Copy prevents altering of input types
                    resultFieldTypes.add(nestedType.deepCopy(nestedType));
                } else {
                    resultFieldTypes.add(inputRecordType.getFieldType(fieldName));
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        if (!containsVariable(arg1)) {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) arg1;
            List<Mutable<ILogicalExpression>> args = f.getArguments();

            AString fieldName = null;
            IAType fieldType = null;

            // Iterating through the orderlist input
            for (Mutable<ILogicalExpression> arg : args) {
                AbstractFunctionCallExpression recConsExpr = (AbstractFunctionCallExpression) arg.getValue();
                ARecordType rtype = TypeComputerUtils.extractRecordType((IAType) env.getType(recConsExpr));
                if (rtype != null) {
                    String[] fn = rtype.getFieldNames();
                    IAType[] ft = rtype.getFieldTypes();
                    for (int j = 0; j < fn.length; j++) {
                        if (fn[j].equals(FIELD_NAME_NAME)) {
                            ILogicalExpression fieldNameExpr = recConsExpr.getArguments().get(j).getValue();
                            switch (fieldNameExpr.getExpressionTag()) {
                                case CONSTANT: // Top fields only
                                    IAObject object = ((AsterixConstantValue) ((ConstantExpression) fieldNameExpr)
                                            .getValue()).getObject();
                                    if (object.getType().getTypeTag() == ATypeTag.STRING) {
                                        // Get the actual "field-name" string
                                        ILogicalExpression recFieldExpr = recConsExpr.getArguments().get(j + 1)
                                                .getValue();
                                        if (recFieldExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                                            fieldName = (AString) ((AsterixConstantValue) ((ConstantExpression) recFieldExpr)
                                                    .getValue()).getObject();
                                        }
                                    }
                                    break;
                                default:
                                    throw new AlgebricksException(fieldNameExpr + " is not supported.");
                            }
                        } else if (fn[j].equals(FIELD_VALUE_VALUE)) {
                            fieldType = ft[j];
                        }
                    }
                    if (fieldName != null) {
                        additionalFields.put(fieldName.getStringValue(), fieldType);
                    }
                }
            }

            if (!additionalFields.isEmpty()) {
                Iterator<Map.Entry<String, IAType>> it = additionalFields.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, IAType> entry = it.next();
                    resultFieldNames.add(entry.getKey());
                    resultFieldTypes.add(entry.getValue());
                }
            }
        } // If variable ignore, deal with the addition at runtime

        String resultTypeName = "appended(" + inputRecordType.getTypeName() + ")";
        int n = resultFieldNames.size();
        IAType resultType = new ARecordType(resultTypeName, resultFieldNames.toArray(new String[n]),
                resultFieldTypes.toArray(new IAType[n]), true);
        if (nullable) {
            resultType = AUnionType.createNullableType(resultType);
        }
        return resultType;
    }

    // Handle variable as input
    private boolean containsVariable(ILogicalExpression expression) {
        if (expression.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
            List<Mutable<ILogicalExpression>> args = f.getArguments();
            for (Mutable<ILogicalExpression> arg : args) {
                ILogicalExpression subExpression = arg.getValue();
                switch (subExpression.getExpressionTag()) {
                    case VARIABLE:
                        return true;
                    case CONSTANT:
                        return false;
                    default: //FUNCTION_CALL
                        return containsVariable(subExpression);
                }
            }
        }
        return true;
    }

}
