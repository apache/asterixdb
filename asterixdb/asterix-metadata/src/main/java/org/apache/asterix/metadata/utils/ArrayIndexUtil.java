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
package org.apache.asterix.metadata.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class ArrayIndexUtil {
    /**
     * Similar function to Index's "getSubFieldType", but accounts for array fields as well.
     */
    public static IAType getSubFieldType(ARecordType recordType, List<List<String>> unnestList,
            List<String> projectList) throws AlgebricksException {
        List<String> flattenedFieldName = ArrayIndexUtil.getFlattenedKeyFieldNames(unnestList, projectList);
        List<Boolean> unnestFlags = ArrayIndexUtil.getUnnestFlags(unnestList, projectList);
        IAType subType = recordType.getFieldType(flattenedFieldName.get(0));

        for (int i = 1; i < flattenedFieldName.size(); i++) {
            if (subType == null) {
                return null;

            } else if (subType.getTypeTag().equals(ATypeTag.UNION)) {
                // Support enforced types here.
                subType = ((AUnionType) subType).getActualType();
                if (!subType.getTypeTag().equals(ATypeTag.OBJECT) && !subType.getTypeTag().equals(ATypeTag.ARRAY)
                        && !subType.getTypeTag().equals(ATypeTag.MULTISET)) {
                    throw new AsterixException(ErrorCode.COMPILATION_ERROR,
                            "Field accessor is not defined for values of type " + subType.getTypeTag());
                }
            }

            if (subType.getTypeTag().equals(ATypeTag.OBJECT) && !unnestFlags.get(i - 1)) {
                subType = ((ARecordType) subType).getFieldType(flattenedFieldName.get(i));

            } else if ((subType.getTypeTag().equals(ATypeTag.ARRAY) || subType.getTypeTag().equals(ATypeTag.MULTISET))
                    && unnestFlags.get(i - 1)) {
                subType = TypeComputeUtils.extractListItemType(subType);
                subType = (subType != null) ? ((ARecordType) subType).getFieldType(flattenedFieldName.get(i)) : null;

            } else {
                throw new AsterixException(ErrorCode.COMPILATION_ERROR,
                        unnestFlags.get(i - 1) ? "Object type given, but unnest flag is also raised."
                                : "Array/multiset type given, but unnest flag is lowered.");
            }
        }

        if (subType != null && unnestFlags.get(unnestFlags.size() - 1)) {
            // If the end field is an array, we must extract the list item here as well.
            if (subType instanceof AbstractCollectionType) {
                subType = TypeComputeUtils.extractListItemType(subType);

            } else {
                throw new AsterixException(ErrorCode.COMPILATION_ERROR,
                        "Array type expected for last term, but given: " + subType.getTypeTag());
            }
        }
        return subType;
    }

    /**
     * Given a path of complex types (i.e. lists + records), determine the nullability of the field.
     */
    public static boolean isSubFieldNullable(ARecordType recordType, List<List<String>> unnestList,
            List<String> projectList) throws AlgebricksException {
        List<String> flattenedFieldName = ArrayIndexUtil.getFlattenedKeyFieldNames(unnestList, projectList);
        List<Boolean> unnestFlags = ArrayIndexUtil.getUnnestFlags(unnestList, projectList);
        IAType subType = recordType.getFieldType(flattenedFieldName.get(0));

        for (int i = 1; i < flattenedFieldName.size(); i++) {
            if (subType == null) {
                return true;
            }

            if (subType.getTypeTag().equals(ATypeTag.UNION)) {
                if (NonTaggedFormatUtil.isOptional(subType)) {
                    return true;
                }
                subType = ((AUnionType) subType).getActualType();
                if (subType.getTypeTag() != ATypeTag.OBJECT) {
                    throw new AsterixException(
                            "Field accessor is not defined for values of type " + subType.getTypeTag());
                }
            }

            if (subType instanceof ARecordType) {
                subType = ((ARecordType) subType).getFieldType(flattenedFieldName.get(i));

            } else if (subType instanceof AbstractCollectionType && unnestFlags.get(i - 1)) {
                subType = TypeComputeUtils.extractListItemType(subType);
                subType = (subType != null) ? ((ARecordType) subType).getFieldType(flattenedFieldName.get(i)) : null;

            } else {
                throw CompilationException.create(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        "Illegal field type " + subType.getTypeTag() + " when checking field nullability");
            }
        }
        return subType == null || NonTaggedFormatUtil.isOptional(subType);
    }

    /**
     * Similar function to Index's "getNonNullableOpenFieldType", but accounts for array fields as well.
     */
    public static Pair<IAType, Boolean> getNonNullableOpenFieldType(IAType fieldType, List<List<String>> unnestList,
            List<String> projectList, ARecordType recType) throws AlgebricksException {
        Pair<IAType, Boolean> keyPairType = null;
        IAType subType = recType;
        boolean nullable = false;

        List<String> flattenedFieldName = ArrayIndexUtil.getFlattenedKeyFieldNames(unnestList, projectList);
        List<Boolean> unnestFlags = ArrayIndexUtil.getUnnestFlags(unnestList, projectList);
        for (int i = 0; i < flattenedFieldName.size(); i++) {
            if (subType instanceof AUnionType) {
                nullable = nullable || ((AUnionType) subType).isUnknownableType();
                subType = ((AUnionType) subType).getActualType();
            }
            if (subType instanceof ARecordType) {
                subType = ((ARecordType) subType).getFieldType(flattenedFieldName.get(i));

            } else if ((subType instanceof AOrderedListType || subType instanceof AUnorderedListType)
                    && unnestFlags.get(i - 1)) {
                subType = TypeComputeUtils.extractListItemType(subType);
                if (subType instanceof ARecordType) {
                    subType = ((ARecordType) subType).getFieldType(flattenedFieldName.get(i));

                } else {
                    throw AsterixException.create(ErrorCode.COMPILATION_ILLEGAL_STATE,
                            "Unexpected type " + subType + ", expected record.");
                }

            } else {
                throw AsterixException.create(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        "Unexpected type " + subType + ", expected record, array, or multi-set.");
            }

            if (subType == null) {
                keyPairType = Index.getNonNullableType(fieldType);
                break;
            }
        }

        if (subType != null) {
            IAType keyType = ArrayIndexUtil.getSubFieldType(recType, unnestList, projectList);
            Pair<IAType, Boolean> pair = Index.getNonNullableType(keyType);
            pair.second = pair.second || ArrayIndexUtil.isSubFieldNullable(recType, unnestList, projectList);
            keyPairType = pair;
        }

        keyPairType.second = keyPairType.second || nullable;
        return keyPairType;
    }

    /**
     * @return The concatenation of the unnest list fields and the project field (for use in creating a unique name).
     */
    public static List<String> getFlattenedKeyFieldNames(List<List<String>> unnestList, List<String> projectList) {
        if (unnestList == null) {
            return projectList;

        } else {
            List<String> flattenedKeyNameList = new ArrayList<>();
            for (List<String> unnestField : unnestList) {
                flattenedKeyNameList.addAll(unnestField);
            }
            if (projectList != null) {
                flattenedKeyNameList.addAll(projectList);
            }
            return flattenedKeyNameList;
        }
    }

    /**
     * @return Mapping to the flattened key field names, determine where the UNNESTs occur.
     */
    public static List<Boolean> getUnnestFlags(List<List<String>> unnestList, List<String> projectList) {
        if (unnestList.isEmpty()) {
            // A simple element has no UNNEST flags raised..
            List<Boolean> unnestFlags = new ArrayList<>();
            for (String ignored : projectList) {
                unnestFlags.add(false);
            }
            return unnestFlags;

        } else {
            List<Boolean> unnestFlagsPrefix = new ArrayList<>();
            for (List<String> unnestField : unnestList) {
                for (int i = 0; i < unnestField.size() - 1; i++) {
                    unnestFlagsPrefix.add(false);
                }
                unnestFlagsPrefix.add(true);
            }

            if (projectList == null) {
                // Stop here. The prefix is the flag vector itself.
                return unnestFlagsPrefix;

            } else {
                List<Boolean> unnestFlags = new ArrayList<>(unnestFlagsPrefix);
                for (int i = 0; i < projectList.size(); i++) {
                    unnestFlags.add(false);
                }
                return unnestFlags;
            }
        }
    }

    /**
     * Traverse each distinct record path and invoke the appropriate commands for each scenario. Here, we keep track
     * of the record/list type at each step and give this to each command.
     */
    public static void walkArrayPath(Index index, Index.ArrayIndexElement workingElement, ARecordType baseRecordType,
            List<String> flattenedFieldName, List<Boolean> unnestFlags, TypeTrackerCommandExecutor commandExecutor)
            throws AlgebricksException {
        ArrayPath arrayPath = new ArrayPath(flattenedFieldName, unnestFlags).invoke();
        List<List<String>> fieldNamesPerArray = arrayPath.fieldNamesPerArray;
        List<Boolean> unnestFlagsPerArray = arrayPath.unnestFlagsPerArray;
        boolean requiresOnlyOneUnnest = unnestFlags.stream().filter(f -> f).count() == 1;

        // If we are given no base record type, then we do not need to keep track of the record type. We are solely
        // using this walk for its flags.
        boolean isTrackingType = baseRecordType != null;
        IAType workingType = baseRecordType;

        for (int i = 0; i < fieldNamesPerArray.size(); i++) {
            ARecordType startingStepRecordType =
                    (isTrackingType) ? (ARecordType) workingType : RecordUtil.FULLY_OPEN_RECORD_TYPE;
            if (isTrackingType) {
                if (!workingType.getTypeTag().equals(ATypeTag.OBJECT)) {
                    throw new AsterixException(ErrorCode.COMPILATION_ERROR, "Mismatched record type to depth-"
                            + "indicators. Expected record type, but got: " + workingType.getTypeTag());
                }

                ARecordType intermediateRecordType = startingStepRecordType;
                List<String> fieldName = fieldNamesPerArray.get(i);
                for (String fieldPart : fieldName) {
                    // Determine whether we have an open field or not. Extract the type appropriately.
                    isTrackingType = isTrackingType && intermediateRecordType.doesFieldExist(fieldPart);
                    if (isTrackingType) {
                        workingType =
                                Index.getNonNullableOpenFieldType(index, intermediateRecordType.getFieldType(fieldPart),
                                        Collections.singletonList(fieldPart), intermediateRecordType).first;
                        if (workingType instanceof ARecordType) {
                            // We have an intermediate step, set our record step for the next loop iteration.
                            intermediateRecordType = (ARecordType) workingType;
                        }
                    }
                }
            }

            if (unnestFlagsPerArray.get(i)) {
                if (isTrackingType) {
                    workingType = TypeComputeUtils.extractListItemType(workingType);
                    if (workingType == null) {
                        throw new AsterixException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                "Expected list type inside record: " + startingStepRecordType);
                    }
                }
                boolean isFirstArrayStep = i == 0;
                boolean isLastUnnestInIntermediateStep = i <= fieldNamesPerArray.size() - 1;
                commandExecutor.executeActionOnEachArrayStep(startingStepRecordType, workingType,
                        fieldNamesPerArray.get(i), isFirstArrayStep, isLastUnnestInIntermediateStep);
            }

            if (i == fieldNamesPerArray.size() - 1) {
                boolean isNonArrayStep = !unnestFlagsPerArray.get(i);
                commandExecutor.executeActionOnFinalArrayStep(workingElement, baseRecordType, startingStepRecordType,
                        fieldNamesPerArray.get(i), isNonArrayStep, requiresOnlyOneUnnest);
            }
        }
    }

    /**
     * Traverse each distinct record path and invoke the appropriate commands for each scenario. Here, we keep track
     * of the total number of actions performed and give this to each command.
     */
    public static void walkArrayPath(List<String> flattenedFieldName, List<Boolean> unnestFlags,
            ActionCounterCommandExecutor commandExecutor) throws AlgebricksException {
        ArrayPath arrayPath = new ArrayPath(flattenedFieldName, unnestFlags).invoke();
        List<List<String>> fieldNamesPerArray = arrayPath.fieldNamesPerArray;
        List<Boolean> unnestFlagsPerArray = arrayPath.unnestFlagsPerArray;

        int numberOfActionsPerformed = 0;
        for (int i = 0; i < fieldNamesPerArray.size(); i++) {
            boolean isUnnestFlagRaised = unnestFlagsPerArray.get(i);
            if (i == 0) {
                commandExecutor.executeActionOnFirstArrayStep();
                numberOfActionsPerformed++;
                isUnnestFlagRaised = false;
            }

            if (isUnnestFlagRaised) {
                commandExecutor.executeActionOnIntermediateArrayStep(numberOfActionsPerformed++);
            }

            if (i == fieldNamesPerArray.size() - 1) {
                commandExecutor.executeActionOnFinalArrayStep(numberOfActionsPerformed++);
            }
        }
    }

    public interface ActionCounterCommandExecutor {
        void executeActionOnFirstArrayStep() throws AlgebricksException;

        void executeActionOnIntermediateArrayStep(int numberOfActionsAlreadyPerformed) throws AlgebricksException;

        void executeActionOnFinalArrayStep(int numberOfActionsAlreadyPerformed) throws AlgebricksException;
    }

    public interface TypeTrackerCommandExecutor {
        void executeActionOnEachArrayStep(ARecordType startingStepRecordType, IAType workingType,
                List<String> fieldName, boolean isFirstArrayStep, boolean isLastUnnestInIntermediateStep)
                throws AlgebricksException;

        void executeActionOnFinalArrayStep(Index.ArrayIndexElement workingElement, ARecordType baseRecordType,
                ARecordType startingStepRecordType, List<String> fieldName, boolean isNonArrayStep,
                boolean requiresOnlyOneUnnest) throws AlgebricksException;
    }

    private static class ArrayPath {
        private final List<String> flattenedFieldName;
        private final List<Boolean> unnestFlags;
        private List<List<String>> fieldNamesPerArray;
        private List<Boolean> unnestFlagsPerArray;

        public ArrayPath(List<String> flattenedFieldName, List<Boolean> unnestFlags) {
            this.flattenedFieldName = flattenedFieldName;
            this.unnestFlags = unnestFlags;
        }

        public ArrayPath invoke() {
            fieldNamesPerArray = new ArrayList<>();
            unnestFlagsPerArray = new ArrayList<>();
            List<String> workingRecordPath = new ArrayList<>();
            for (int i = 0; i < unnestFlags.size(); i++) {
                workingRecordPath.add(flattenedFieldName.get(i));

                if (i == unnestFlags.size() - 1 || unnestFlags.get(i)) {
                    unnestFlagsPerArray.add(unnestFlags.get(i));
                    fieldNamesPerArray.add(workingRecordPath);
                    workingRecordPath = new ArrayList<>();
                }
            }
            return this;
        }
    }
}
