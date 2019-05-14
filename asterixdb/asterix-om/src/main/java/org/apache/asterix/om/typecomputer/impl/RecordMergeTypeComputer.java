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
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.exceptions.TypeMismatchException;
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
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class RecordMergeTypeComputer implements IResultTypeComputer {

    public static final RecordMergeTypeComputer INSTANCE = new RecordMergeTypeComputer(false);
    public static final RecordMergeTypeComputer INSTANCE_IGNORE_DUPLICATES = new RecordMergeTypeComputer(true);

    private RecordMergeTypeComputer(boolean isIgnoreDuplicates) {
        this.isIgnoreDuplicates = isIgnoreDuplicates;
    }

    private final boolean isIgnoreDuplicates;

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        FunctionIdentifier funcId = f.getFunctionIdentifier();

        IAType t0 = (IAType) env.getType(f.getArguments().get(0).getValue());
        IAType t1 = (IAType) env.getType(f.getArguments().get(1).getValue());
        boolean unknownable = TypeHelper.canBeUnknown(t0) || TypeHelper.canBeUnknown(t1);
        ARecordType recType0 = TypeComputeUtils.extractRecordType(t0);
        if (recType0 == null) {
            throw new TypeMismatchException(f.getSourceLocation(), funcId, 0, t0.getTypeTag(), ATypeTag.OBJECT);
        }

        ARecordType recType1 = TypeComputeUtils.extractRecordType(t1);
        if (recType1 == null) {
            throw new TypeMismatchException(f.getSourceLocation(), funcId, 1, t1.getTypeTag(), ATypeTag.OBJECT);
        }

        List<String> resultFieldNames = new ArrayList<>();
        for (String fieldName : recType0.getFieldNames()) {
            resultFieldNames.add(fieldName);
        }
        Collections.sort(resultFieldNames);

        List<IAType> resultFieldTypes = new ArrayList<>();
        for (String fieldName : resultFieldNames) {
            if (recType0.getFieldType(fieldName).getTypeTag() == ATypeTag.OBJECT) {
                ARecordType nestedType = (ARecordType) recType0.getFieldType(fieldName);
                //Deep Copy prevents altering of input types
                resultFieldTypes.add(nestedType.deepCopy(nestedType));
            } else {
                resultFieldTypes.add(recType0.getFieldType(fieldName));
            }
        }

        List<String> additionalFieldNames = new ArrayList<>();
        List<IAType> additionalFieldTypes = new ArrayList<>();
        String[] fieldNames = recType1.getFieldNames();
        IAType[] fieldTypes = recType1.getFieldTypes();
        for (int i = 0; i < fieldNames.length; ++i) {

            // For each field on the right record, we check if a field with matching name exists on the left record
            int pos = Collections.binarySearch(resultFieldNames, fieldNames[i]);
            if (pos >= 0) {

                // If we're here, it means we found 2 fields with a matching field name
                IAType resultFieldType = resultFieldTypes.get(pos);

                // This is for fields with matching names, but different type tags.
                if (resultFieldType.getTypeTag() != fieldTypes[i].getTypeTag()) {

                    // If the ignore duplicates flag is set, we ignore the duplicate fields on the right record
                    if (isIgnoreDuplicates) {
                        continue;
                    }
                    // If the ignore duplicates flag is not set, we throw a duplicate field exception
                    else {
                        throw new CompilationException(ErrorCode.COMPILATION_DUPLICATE_FIELD_NAME,
                                f.getSourceLocation(), fieldNames[i]);
                    }
                }

                // This is for fields with matching names, matching types, type ARecord, do nested merge
                if (fieldTypes[i].getTypeTag() == ATypeTag.OBJECT) {
                    resultFieldTypes.set(pos,
                            mergedNestedType(fieldNames[i], fieldTypes[i], resultFieldType, f.getSourceLocation()));
                }
            } else {
                // If no field was found with a matching name, we simply add the field to be merged
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

        if (unknownable) {
            resultType = AUnionType.createUnknownableType(resultType);
        }
        return resultType;
    }

    private IAType mergedNestedType(String fieldName, IAType fieldType1, IAType fieldType0, SourceLocation sourceLoc)
            throws AlgebricksException {
        if (fieldType1.getTypeTag() != ATypeTag.OBJECT || fieldType0.getTypeTag() != ATypeTag.OBJECT) {
            throw new CompilationException(ErrorCode.COMPILATION_DUPLICATE_FIELD_NAME, sourceLoc, fieldName);
        }

        ARecordType resultType = (ARecordType) fieldType0;
        ARecordType fieldType1Copy = (ARecordType) fieldType1;

        for (int i = 0; i < fieldType1Copy.getFieldTypes().length; i++) {
            String fname = fieldType1Copy.getFieldNames()[i];
            int pos = resultType.getFieldIndex(fname);
            if (pos >= 0) {
                // If a sub-record do merge, else ignore and let the values decide what to do
                if (fieldType1Copy.getFieldTypes()[i].getTypeTag() == ATypeTag.OBJECT) {
                    IAType[] oldTypes = resultType.getFieldTypes();
                    oldTypes[pos] = mergedNestedType(fname, fieldType1Copy.getFieldTypes()[i],
                            resultType.getFieldTypes()[pos], sourceLoc);
                    resultType = new ARecordType(resultType.getTypeName(), resultType.getFieldNames(), oldTypes,
                            resultType.isOpen());
                }
            } else {
                IAType[] combinedFieldTypes =
                        ArrayUtils.addAll(resultType.getFieldTypes().clone(), fieldType1Copy.getFieldTypes()[i]);
                resultType = new ARecordType(resultType.getTypeName(),
                        ArrayUtils.addAll(resultType.getFieldNames(), fieldType1Copy.getFieldNames()[i]),
                        combinedFieldTypes, resultType.isOpen());
            }
        }

        return resultType;
    }
}
