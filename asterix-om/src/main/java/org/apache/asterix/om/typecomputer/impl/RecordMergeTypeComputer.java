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

public class RecordMergeTypeComputer implements IResultTypeComputer {
    public static final RecordMergeTypeComputer INSTANCE = new RecordMergeTypeComputer();

    private RecordMergeTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        IAType t0 = (IAType) env.getType(f.getArguments().get(0).getValue());
        IAType t1 = (IAType) env.getType(f.getArguments().get(1).getValue());
        boolean nullable = TypeHelper.canBeNull(t0) || TypeHelper.canBeNull(t1);
        ARecordType recType0 = TypeComputerUtils.extractRecordType(t0);
        ARecordType recType1 = TypeComputerUtils.extractRecordType(t1);

        if (recType0 == null || recType1 == null) {
            throw new AlgebricksException("record-merge expects possibly NULL records as arguments, but got (" + t0
                    + ", " + t1 + ")");
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
        String fieldNames[] = recType1.getFieldNames();
        IAType fieldTypes[] = recType1.getFieldTypes();
        for (int i = 0; i < fieldNames.length; ++i) {
            int pos = Collections.binarySearch(resultFieldNames, fieldNames[i]);
            if (pos >= 0) {
                IAType resultFieldType = resultFieldTypes.get(pos);
                if (resultFieldType.getTypeTag() != fieldTypes[i].getTypeTag()) {
                    throw new AlgebricksException("Duplicate field " + fieldNames[i] + " encountered");
                }
                try {
                    // Assuming fieldTypes[i].getTypeTag() = resultFieldType.getTypeTag()
                    if (fieldTypes[i].getTypeTag() == ATypeTag.RECORD) {
                        resultFieldTypes.set(pos, mergedNestedType(fieldTypes[i], resultFieldType));
                    }
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }

            } else {
                additionalFieldNames.add(fieldNames[i]);
                additionalFieldTypes.add(fieldTypes[i]);
            }
        }

        resultFieldNames.addAll(additionalFieldNames);
        resultFieldTypes.addAll(additionalFieldTypes);
        String resultTypeName = "merged(" + recType0.getTypeName() + ", " + recType1.getTypeName() + ")";
        boolean isOpen = recType0.isOpen() || recType1.isOpen();

        IAType resultType = new ARecordType(resultTypeName, resultFieldNames.toArray(new String[] {}),
                    resultFieldTypes.toArray(new IAType[] {}), isOpen);

        if (nullable) {
            resultType = AUnionType.createNullableType(resultType);
        }
        return resultType;
    }

    private IAType mergedNestedType(IAType fieldType1, IAType fieldType0) throws AlgebricksException, AsterixException {
        if (fieldType1.getTypeTag() != ATypeTag.RECORD || fieldType0.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Duplicate field " + fieldType1.getTypeName() + " encountered");
        }

        ARecordType resultType = (ARecordType) fieldType0;
        ARecordType fieldType1Copy = (ARecordType) fieldType1;

        for (int i = 0; i < fieldType1Copy.getFieldTypes().length; i++) {
            try {
                int pos = resultType.getFieldIndex(fieldType1Copy.getFieldNames()[i]);
                if (pos >= 0) {
                    // If a sub-record do merge, else ignore and let the values decide what to do
                    if (fieldType1Copy.getFieldTypes()[i].getTypeTag() == ATypeTag.RECORD) {
                        IAType[] oldTypes = resultType.getFieldTypes();
                        oldTypes[pos] = mergedNestedType(fieldType1Copy.getFieldTypes()[i],
                                resultType.getFieldTypes()[pos]);
                        resultType = new ARecordType(resultType.getTypeName(), resultType.getFieldNames(), oldTypes,
                                resultType.isOpen());
                    }
                } else {
                    IAType[] combinedFieldTypes = ArrayUtils.addAll(resultType.getFieldTypes().clone(),
                            fieldType1Copy.getFieldTypes()[i]);
                    resultType = new ARecordType(resultType.getTypeName(), ArrayUtils.addAll(
                            resultType.getFieldNames(), fieldType1Copy.getFieldNames()[i]), combinedFieldTypes,
                            resultType.isOpen());
                }

            } catch (IOException | AsterixException e) {
                throw new AlgebricksException(e);
            }
        }

        return resultType;
    }
}
