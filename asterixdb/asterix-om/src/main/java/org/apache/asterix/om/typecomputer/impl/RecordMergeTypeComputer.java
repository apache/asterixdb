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
import org.apache.asterix.om.types.BuiltinType;
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
import org.apache.hyracks.util.LogRedactionUtil;

public class RecordMergeTypeComputer implements IResultTypeComputer {
    private static final SourceLocation DUMMY_LOCATION = new SourceLocation(0, 0);
    public static final RecordMergeTypeComputer INSTANCE = new RecordMergeTypeComputer(false, false, false);
    public static final RecordMergeTypeComputer INSTANCE_IGNORE_DUPLICATES =
            new RecordMergeTypeComputer(true, false, false);
    public static final RecordMergeTypeComputer INSTANCE_IGNORE_DUPLICATES_MERGE_ON_TRANSFORM_RECORDS =
            new RecordMergeTypeComputer(true, false, true);
    public static final RecordMergeTypeComputer INSTANCE_IGNORE_DUPLICATES_HANDLE_DELETIONS =
            new RecordMergeTypeComputer(true, true, false);

    protected RecordMergeTypeComputer(boolean isIgnoreDuplicates, boolean handleLeftRecordAsDeletions,
            boolean mergeOnTransformRecords) {
        this.isIgnoreDuplicates = isIgnoreDuplicates;
        this.handleLeftRecordAsDeletions = handleLeftRecordAsDeletions;
        this.mergeOnTransformRecords = mergeOnTransformRecords;
    }

    private final boolean isIgnoreDuplicates;
    private final boolean handleLeftRecordAsDeletions;
    private final boolean mergeOnTransformRecords;

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

        return merge(recType0, recType1, isIgnoreDuplicates, handleLeftRecordAsDeletions, mergeOnTransformRecords,
                unknownable, f.getSourceLocation());
    }

    public static IAType merge(ARecordType recType0, ARecordType recType1) throws AlgebricksException {
        return merge(recType0, recType1, true, false, false, false, DUMMY_LOCATION);
    }

    private static IAType merge(ARecordType recType0, ARecordType recType1, boolean isIgnoreDuplicates,
            boolean handleLeftRecordAsDeletions, boolean mergeOnTransformRecords, boolean unknownable,
            SourceLocation sourceLocation) throws AlgebricksException {

        List<String> resultFieldNames = new ArrayList<>();
        Collections.addAll(resultFieldNames, recType0.getFieldNames());
        Collections.sort(resultFieldNames);

        List<IAType> resultFieldTypes = new ArrayList<>();
        for (String fieldName : resultFieldNames) {
            if (recType0.getFieldType(fieldName).getTypeTag() == ATypeTag.OBJECT) {
                ARecordType nestedType = (ARecordType) recType0.getFieldType(fieldName);
                //Deep Copy prevents altering of input types
                resultFieldTypes.add(nestedType.deepCopy(nestedType));
            } else {
                if (handleLeftRecordAsDeletions) {
                    resultFieldTypes.add(BuiltinType.AMISSING);
                } else {
                    resultFieldTypes.add(recType0.getFieldType(fieldName));
                }
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
                if ((resultFieldType.getTypeTag() != fieldTypes[i].getTypeTag())
                        || (fieldTypes[i].getTypeTag() == ATypeTag.OBJECT && mergeOnTransformRecords)) {
                    // If the ignore duplicates flag is set, we ignore the duplicate fields on the right record
                    if (isIgnoreDuplicates) {
                        if (handleLeftRecordAsDeletions && fieldTypes[i].getTypeTag() != ATypeTag.MISSING) {
                            /** The purpose of adding the field to resultType was to help skip adding fields
                             from the RHS. Remove the MISSING type from LHS as soon as we've skipped all RHS fields. */
                            resultFieldNames.remove(pos);
                            resultFieldTypes.remove(pos);
                        }
                        continue;
                    }
                    // If ignore duplicates flag is not set, we throw a duplicate field exception
                    else {
                        throw new CompilationException(ErrorCode.COMPILATION_DUPLICATE_FIELD_NAME, sourceLocation,
                                LogRedactionUtil.userData(fieldNames[i]));
                    }
                }

                // This is for fields with matching names, matching types, type ARecord, do nested merge
                if (fieldTypes[i].getTypeTag() == ATypeTag.OBJECT) {
                    resultFieldTypes.set(pos, mergedNestedType(fieldNames[i], fieldTypes[i], resultFieldType,
                            isIgnoreDuplicates, handleLeftRecordAsDeletions, mergeOnTransformRecords, sourceLocation));
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

    private static IAType mergedNestedType(String fieldName, IAType fieldType1, IAType fieldType0,
            boolean isIgnoreDuplicates, boolean handleLeftRecordAsDeletions, boolean mergeOnTransformRecords,
            SourceLocation sourceLoc) throws AlgebricksException {
        if (fieldType1.getTypeTag() != ATypeTag.OBJECT || fieldType0.getTypeTag() != ATypeTag.OBJECT) {
            throw new CompilationException(ErrorCode.COMPILATION_DUPLICATE_FIELD_NAME, sourceLoc,
                    LogRedactionUtil.userData(fieldName));
        }

        // If handleLeftRecordAsDeletions, first treat all flat values as missing
        ARecordType resultType = (ARecordType) fieldType0;

        List<String> resultFieldNames = new ArrayList<>();
        Collections.addAll(resultFieldNames, resultType.getFieldNames());
        Collections.sort(resultFieldNames);

        List<IAType> resultFieldTypes = new ArrayList<>();
        for (String fname : resultFieldNames) {
            if (resultType.getFieldType(fname).getTypeTag() == ATypeTag.OBJECT) {
                ARecordType nestedType = (ARecordType) resultType.getFieldType(fname);
                resultFieldTypes.add(nestedType.deepCopy(nestedType));
            } else {
                if (handleLeftRecordAsDeletions) {
                    resultFieldTypes.add(BuiltinType.AMISSING);
                } else {
                    resultFieldTypes.add(resultType.getFieldType(fname));
                }
            }
        }
        ARecordType resultTypeSanitized =
                new ARecordType(resultType.getTypeName(), resultFieldNames.toArray(new String[] {}),
                        resultFieldTypes.toArray(new IAType[] {}), resultType.isOpen());

        ARecordType fieldType1Copy = (ARecordType) fieldType1;

        for (int i = 0; i < fieldType1Copy.getFieldTypes().length; i++) {
            String fname = fieldType1Copy.getFieldNames()[i];
            int pos = resultTypeSanitized.getFieldIndex(fname);
            if (pos >= 0) {
                if ((resultTypeSanitized.getFieldTypes()[pos].getTypeTag() != fieldType1Copy.getFieldTypes()[i]
                        .getTypeTag())
                        || (fieldType1Copy.getFieldTypes()[i].getTypeTag() == ATypeTag.OBJECT
                                && mergeOnTransformRecords)) {
                    if (isIgnoreDuplicates) {
                        continue;
                    } else {
                        throw new CompilationException(ErrorCode.COMPILATION_DUPLICATE_FIELD_NAME, sourceLoc,
                                LogRedactionUtil.userData(fname));
                    }
                }

                // If a sub-record do merge, else ignore and let the values decide what to do
                if (fieldType1Copy.getFieldTypes()[i].getTypeTag() == ATypeTag.OBJECT) {
                    IAType[] oldTypes = resultTypeSanitized.getFieldTypes();
                    oldTypes[pos] = mergedNestedType(fname, fieldType1Copy.getFieldTypes()[i],
                            resultTypeSanitized.getFieldTypes()[pos], isIgnoreDuplicates, handleLeftRecordAsDeletions,
                            mergeOnTransformRecords, sourceLoc);
                    resultTypeSanitized = new ARecordType(resultTypeSanitized.getTypeName(),
                            resultTypeSanitized.getFieldNames(), oldTypes, resultTypeSanitized.isOpen());
                }
            } else {
                IAType[] combinedFieldTypes = ArrayUtils.addAll(resultTypeSanitized.getFieldTypes().clone(),
                        fieldType1Copy.getFieldTypes()[i]);
                resultTypeSanitized = new ARecordType(resultTypeSanitized.getTypeName(),
                        ArrayUtils.addAll(resultTypeSanitized.getFieldNames(), fieldType1Copy.getFieldNames()[i]),
                        combinedFieldTypes, resultTypeSanitized.isOpen());
            }
        }

        return resultTypeSanitized;
    }
}