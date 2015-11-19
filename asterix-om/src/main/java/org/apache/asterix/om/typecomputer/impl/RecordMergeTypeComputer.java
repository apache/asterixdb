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
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class RecordMergeTypeComputer implements IResultTypeComputer {
    public static final RecordMergeTypeComputer INSTANCE = new RecordMergeTypeComputer();

    private RecordMergeTypeComputer() {
    }

    public static ARecordType extractRecordType(IAType t) {
        if (t.getTypeTag() == ATypeTag.RECORD) {
            return (ARecordType) t;
        }

        if (t.getTypeTag() == ATypeTag.UNION) {
            IAType innerType = ((AUnionType) t).getNullableType();
            if (innerType.getTypeTag() == ATypeTag.RECORD) {
                return (ARecordType) innerType;
            }
        }

        return null;
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        IAType t0 = (IAType) env.getType(f.getArguments().get(0).getValue());
        IAType t1 = (IAType) env.getType(f.getArguments().get(1).getValue());
        boolean nullable = TypeHelper.canBeNull(t0) || TypeHelper.canBeNull(t1);
        ARecordType recType0 = extractRecordType(t0);
        ARecordType recType1 = extractRecordType(t1);

        if (recType0 == null || recType1 == null) {
            throw new AlgebricksException(
                    "record-merge expects possibly NULL records as arguments, but got (" + t0 + ", " + t1 + ")");
        }

        List<String> resultFieldNames = new ArrayList<>();
        for (String fieldName : recType0.getFieldNames()) {
            resultFieldNames.add(fieldName);
        }
        Collections.sort(resultFieldNames);
        List<IAType> resultFieldTypes = new ArrayList<>();
        for (String fieldName : resultFieldNames) {
            try {
                if (recType0.getFieldType(fieldName).getTypeTag() == ATypeTag.RECORD) {
                    ARecordType nestedType = (ARecordType) recType0.getFieldType(fieldName);
                    //Deep Copy prevents altering of input types
                    resultFieldTypes.add(nestedType.deepCopy(nestedType));
                } else {
                    resultFieldTypes.add(recType0.getFieldType(fieldName));
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        List<String> additionalFieldNames = new ArrayList<>();
        List<IAType> additionalFieldTypes = new ArrayList<>();
        for (int i = 0; i < recType1.getFieldNames().length; ++i) {
            String fieldName = recType1.getFieldNames()[i];
            IAType fieldType = recType1.getFieldTypes()[i];
            int pos = Collections.binarySearch(resultFieldNames, fieldName);
            if (pos >= 0) {
                try {
                    resultFieldTypes.set(pos, mergedNestedType(fieldType, resultFieldTypes.get(pos)));
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }

            } else {
                additionalFieldNames.add(fieldName);
                additionalFieldTypes.add(fieldType);
            }
        }

        resultFieldNames.addAll(additionalFieldNames);
        resultFieldTypes.addAll(additionalFieldTypes);
        String resultTypeName = "merged(" + recType0.getTypeName() + ", " + recType1.getTypeName() + ")";
        boolean isOpen = recType0.isOpen() || recType1.isOpen();
        IAType resultType = null;
        try {
            resultType = new ARecordType(resultTypeName, resultFieldNames.toArray(new String[] {}),
                    resultFieldTypes.toArray(new IAType[] {}), isOpen);
        } catch (AsterixException | HyracksDataException e) {
            throw new AlgebricksException(e);
        };

        if (nullable) {
            resultType = AUnionType.createNullableType(resultType);
        }
        return resultType;
    }

    IAType mergedNestedType(IAType fieldType1, IAType fieldType0) throws AlgebricksException, AsterixException {
        if (fieldType1.getTypeTag() != ATypeTag.RECORD || fieldType0.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Duplicate field \"" + fieldType1.getTypeName() + "\" encountered");
        }

        ARecordType returnType = (ARecordType) fieldType0;
        ARecordType fieldType1Copy = (ARecordType) fieldType1;

        for (int i = 0; i < fieldType1Copy.getFieldTypes().length; i++) {
            try {
                int pos = returnType.getFieldIndex(fieldType1Copy.getFieldNames()[i]);
                if (pos >= 0) {
                    if (fieldType1Copy.getFieldTypes()[i].getTypeTag() != ATypeTag.RECORD) {
                        break;
                    }
                    IAType[] oldTypes = returnType.getFieldTypes();
                    oldTypes[pos] = mergedNestedType(fieldType1Copy.getFieldTypes()[i],
                            returnType.getFieldTypes()[pos]);
                    returnType = new ARecordType(returnType.getTypeName(), returnType.getFieldNames(), oldTypes,
                            returnType.isOpen());
                } else {
                    IAType[] combinedFieldTypes = ArrayUtils.addAll(returnType.getFieldTypes().clone(),
                            fieldType1Copy.getFieldTypes()[i]);
                    returnType = new ARecordType(returnType.getTypeName(),
                            ArrayUtils.addAll(returnType.getFieldNames(), fieldType1Copy.getFieldNames()[i]),
                            combinedFieldTypes, returnType.isOpen());
                }

            } catch (IOException | AsterixException e) {
                throw new AlgebricksException(e);
            }
        }

        return returnType;
    }
}
