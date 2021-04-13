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
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class ArrayIndexUtil {
    /**
     * @deprecated Use the project + unnest scheme instead of array indicators.
     */
    public static IAType getSubFieldInArrayType(ARecordType recordType, List<String> subFieldName,
            List<Integer> arrayDepthIndicators) throws AlgebricksException {
        IAType subType = recordType.getFieldType(subFieldName.get(0));
        for (int i = 1; i < subFieldName.size(); i++) {
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
            if (subType.getTypeTag().equals(ATypeTag.OBJECT) && arrayDepthIndicators.get(i - 1) == 0) {
                subType = ((ARecordType) subType).getFieldType(subFieldName.get(i));
            } else if ((subType.getTypeTag().equals(ATypeTag.ARRAY) || subType.getTypeTag().equals(ATypeTag.MULTISET))
                    && arrayDepthIndicators.get(i - 1) > 0) {
                for (int j = 0; j < arrayDepthIndicators.get(i - 1); j++) {
                    subType = TypeComputeUtils.extractListItemType(subType);
                }
                subType = (subType != null) ? ((ARecordType) subType).getFieldType(subFieldName.get(i)) : null;
            } else {
                throw new AsterixException(ErrorCode.COMPILATION_ERROR,
                        (arrayDepthIndicators.get(i - 1) > 0)
                                ? "Object type given, but array depth indicator is " + "non-zero."
                                : "Array/multiset type given, but array depth indicator is zero.");
            }
        }
        if (subType != null && arrayDepthIndicators.get(arrayDepthIndicators.size() - 1) > 0) {
            // If the end field is an array, we must extract the list item here as well.
            for (int j = 0; j < arrayDepthIndicators.get(arrayDepthIndicators.size() - 1); j++) {
                if (subType instanceof AbstractCollectionType) {
                    subType = TypeComputeUtils.extractListItemType(subType);
                } else {
                    throw new AsterixException(ErrorCode.COMPILATION_ERROR,
                            "Array type expected for last term, but given: "
                                    + ((subType != null) ? subType.getTypeTag() : "null"));
                }
            }
        }
        return subType;
    }

    /**
     * Given a path of complex types (i.e. lists + records), determine the nullability of the field.
     * @deprecated Use the project + unnest scheme instead of array indicators.
     */
    public static boolean isSubFieldNullable(ARecordType recordType, List<String> subFieldName,
            List<Integer> arrayIndicators) throws AlgebricksException {
        IAType subType = recordType.getFieldType(subFieldName.get(0));
        for (int i = 1; i < subFieldName.size(); i++) {
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
                subType = ((ARecordType) subType).getFieldType(subFieldName.get(i));
            } else if (subType instanceof AbstractCollectionType && arrayIndicators.get(i - 1) > 0) {
                for (int j = 0; j < arrayIndicators.get(i - 1); j++) {
                    subType = TypeComputeUtils.extractListItemType(subType);
                }
                subType = (subType != null) ? ((ARecordType) subType).getFieldType(subFieldName.get(i)) : null;
            } else {
                throw CompilationException.create(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        "Illegal field type " + subType.getTypeTag() + " when checking field nullability");
            }
        }
        return subType == null || NonTaggedFormatUtil.isOptional(subType);
    }

    /**
     * Similar function to Index's "getNonNullableOpenFieldType", but accounts for array fields as well.
     * @deprecated Use the project + unnest scheme instead of array indicators.
     */
    public static Pair<IAType, Boolean> getNonNullableOpenFieldType(IAType fieldType, List<String> fieldName,
            ARecordType recType, List<Integer> arrayIndicators) throws AlgebricksException {
        Pair<IAType, Boolean> keyPairType = null;
        IAType subType = recType;
        boolean nullable = false;
        for (int i = 0; i < fieldName.size(); i++) {
            if (subType instanceof AUnionType) {
                nullable = nullable || ((AUnionType) subType).isUnknownableType();
                subType = ((AUnionType) subType).getActualType();
            }
            if (subType instanceof ARecordType) {
                subType = ((ARecordType) subType).getFieldType(fieldName.get(i));
            } else if ((subType instanceof AOrderedListType || subType instanceof AUnorderedListType)
                    && arrayIndicators.get(i - 1) > 0) {
                for (int j = 0; j < arrayIndicators.get(i - 1); j++) {
                    subType = TypeComputeUtils.extractListItemType(subType);
                }
                if (subType instanceof ARecordType) {
                    subType = ((ARecordType) subType).getFieldType(fieldName.get(i));
                } else {
                    throw AsterixException.create(ErrorCode.COMPILATION_ILLEGAL_STATE, "Unexpected type " + fieldType);
                }
            } else {
                throw AsterixException.create(ErrorCode.COMPILATION_ILLEGAL_STATE, "Unexpected type " + fieldType);
            }

            if (subType == null) {
                keyPairType = Index.getNonNullableType(fieldType);
                break;
            }
        }
        if (subType != null) {
            IAType keyType = ArrayIndexUtil.getSubFieldInArrayType(recType, fieldName, arrayIndicators);
            Pair<IAType, Boolean> pair = Index.getNonNullableType(keyType);
            pair.second = pair.second || ArrayIndexUtil.isSubFieldNullable(recType, fieldName, arrayIndicators);
            keyPairType = pair;
        }
        keyPairType.second = keyPairType.second || nullable;
        return keyPairType;
    }

    /**
     * @deprecated Use new unnestList and projectList scheme.
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
     * @deprecated Use new unnestList and projectList scheme.
     */
    public static List<Integer> getArrayDepthIndicator(List<List<String>> unnestList, List<String> projectList) {
        if (unnestList == null) {
            // A simple element has a flat set of depth indicators.
            List<Integer> depthIndicator = new ArrayList<>();
            for (String ignored : projectList) {
                depthIndicator.add(0);
            }
            return depthIndicator;

        } else {
            List<Integer> depthIndicatorPrefix = new ArrayList<>();
            for (List<String> unnestField : unnestList) {
                for (int i = 0; i < unnestField.size() - 1; i++) {
                    depthIndicatorPrefix.add(0);
                }
                depthIndicatorPrefix.add(1);
            }

            if (projectList == null) {
                // Stop here. The prefix is the indicator itself.
                return depthIndicatorPrefix;

            } else {
                List<Integer> depthIndicator = new ArrayList<>(depthIndicatorPrefix);
                for (int i = 0; i < projectList.size(); i++) {
                    depthIndicator.add(0);
                }
                return depthIndicator;
            }
        }
    }

    /**
     * @deprecated Use new unnestList and projectList scheme.
     * @return The record paths and non-zero depth indicators associated each record of fields from an array index.
     */
    public static Pair<List<List<String>>, List<Integer>> unnestComplexRecordPath(List<String> fieldName,
            List<Integer> depthIndicators) {
        List<List<String>> resultantPaths = new ArrayList<>();
        List<Integer> resultantArrayIndicators = new ArrayList<>();
        List<String> workingRecordPath = new ArrayList<>();
        for (int i = 0; i < depthIndicators.size(); i++) {
            workingRecordPath.add(fieldName.get(i));

            if (i == depthIndicators.size() - 1 || depthIndicators.get(i) > 0) {
                resultantArrayIndicators.add(depthIndicators.get(i));
                resultantPaths.add(workingRecordPath);
                workingRecordPath = new ArrayList<>();
            }
        }
        return new Pair<>(resultantPaths, resultantArrayIndicators);
    }

    /**
     * Given the {@code Index}'s representation of an array path (i.e. a concatenation of record paths, with array
     * steps specified in depths corresponding to an index in the aforementioned record path array), traverse each
     * distinct record path and invoke the appropriate commands for each scenario.
     * <p>
     * Here, we keep track of the record/list type at each step and give this to each command.
     */
    public static void walkArrayPath(ARecordType baseRecordType, List<String> flattenedFieldName,
            List<Integer> flattenedDepthIndicators, TypeTrackerCommandExecutor commandExecutor)
            throws AlgebricksException {
        ArrayPath arrayPath = new ArrayPath(flattenedFieldName, flattenedDepthIndicators).invoke();
        List<List<String>> fieldNamesPerArray = arrayPath.fieldNamesPerArray;
        List<Integer> depthOfArraySteps = arrayPath.depthOfArraySteps;

        // If we are given no base record type, then we do not need to keep track of the record type. We are solely 
        // using this walk for its flags.
        boolean isTrackingType = baseRecordType != null;

        IAType workingType = baseRecordType;
        for (int i = 0; i < fieldNamesPerArray.size(); i++) {
            ARecordType startingStepRecordType = null;
            if (isTrackingType) {
                if (!workingType.getTypeTag().equals(ATypeTag.OBJECT)) {
                    throw new AsterixException(ErrorCode.COMPILATION_ERROR, "Mismatched record type to depth-"
                            + "indicators. Expected record type, but got: " + workingType.getTypeTag());
                }
                startingStepRecordType = (ARecordType) workingType;
                workingType = Index.getNonNullableOpenFieldType(
                        startingStepRecordType.getSubFieldType(fieldNamesPerArray.get(i)), fieldNamesPerArray.get(i),
                        startingStepRecordType).first;
            }

            for (int j = 0; j < depthOfArraySteps.get(i); j++) {
                if (isTrackingType) {
                    workingType = TypeComputeUtils.extractListItemType(workingType);
                    if (workingType == null) {
                        throw new AsterixException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                                "Expected list type inside record: " + startingStepRecordType);
                    }
                }
                boolean isFirstArrayStep = i == 0;
                boolean isFirstUnnestInStep = j == 0;
                boolean isLastUnnestInIntermediateStep =
                        j == depthOfArraySteps.get(i) - 1 && i < fieldNamesPerArray.size() - 1;
                commandExecutor.executeActionOnEachArrayStep(startingStepRecordType, workingType,
                        fieldNamesPerArray.get(i), isFirstArrayStep, isFirstUnnestInStep,
                        isLastUnnestInIntermediateStep);
            }

            if (i == fieldNamesPerArray.size() - 1) {
                boolean requiresOnlyOneUnnest = depthOfArraySteps.stream().reduce(0, Integer::sum).equals(1);
                boolean isNonArrayStep = depthOfArraySteps.get(i) == 0;
                commandExecutor.executeActionOnFinalArrayStep(startingStepRecordType, fieldNamesPerArray.get(i),
                        isNonArrayStep, requiresOnlyOneUnnest);
            }
        }
    }

    /**
     * Given the {@code Index}'s representation of an array path (i.e. a concatenation of record paths, with array
     * steps specified in depths corresponding to an index in the aforementioned record path array), traverse each
     * distinct record path and invoke the appropriate commands for each scenario.
     * <p>
     * Here, we keep track of the total number of actions performed and give this to each command.
     */
    public static void walkArrayPath(List<String> flattenedFieldName, List<Integer> flattenedDepthIndicators,
            ActionCounterCommandExecutor commandExecutor) throws AlgebricksException {
        ArrayPath arrayPath = new ArrayPath(flattenedFieldName, flattenedDepthIndicators).invoke();
        List<List<String>> fieldNamesPerArray = arrayPath.fieldNamesPerArray;
        List<Integer> depthOfArraySteps = arrayPath.depthOfArraySteps;

        int numberOfActionsPerformed = 0;
        for (int i = 0; i < fieldNamesPerArray.size(); i++) {
            int unnestLevel = depthOfArraySteps.get(i);
            if (i == 0) {
                commandExecutor.executeActionOnFirstArrayStep();
                numberOfActionsPerformed++;
                unnestLevel--;
            }

            for (int j = 0; j < unnestLevel; j++) {
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
                List<String> fieldName, boolean isFirstArrayStep, boolean isFirstUnnestInStep,
                boolean isLastUnnestInIntermediateStep) throws AlgebricksException;

        void executeActionOnFinalArrayStep(ARecordType startingStepRecordType, List<String> fieldName,
                boolean isNonArrayStep, boolean requiresOnlyOneUnnest) throws AlgebricksException;
    }

    private static class ArrayPath {
        private final List<String> flattenedFieldName;
        private final List<Integer> flattenedDepthIndicators;
        private List<List<String>> fieldNamesPerArray;
        private List<Integer> depthOfArraySteps;

        public ArrayPath(List<String> flattenedFieldName, List<Integer> flattenedDepthIndicators) {
            this.flattenedFieldName = flattenedFieldName;
            this.flattenedDepthIndicators = flattenedDepthIndicators;
        }

        public ArrayPath invoke() {
            fieldNamesPerArray = new ArrayList<>();
            depthOfArraySteps = new ArrayList<>();
            List<String> workingRecordPath = new ArrayList<>();
            for (int i = 0; i < flattenedDepthIndicators.size(); i++) {
                workingRecordPath.add(flattenedFieldName.get(i));

                if (i == flattenedDepthIndicators.size() - 1 || flattenedDepthIndicators.get(i) > 0) {
                    depthOfArraySteps.add(flattenedDepthIndicators.get(i));
                    fieldNamesPerArray.add(workingRecordPath);
                    workingRecordPath = new ArrayList<>();
                }
            }
            return this;
        }
    }
}
