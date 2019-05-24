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

package org.apache.asterix.dataflow.data.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

/**
 * A common facility for resolving conflicting types.
 * It is shared between the <code>ConflictingTypeResolver</code> and <code>SwitchCaseComputer</code>.
 */
public class TypeResolverUtil {

    private TypeResolverUtil() {
    }

    /**
     * Returns a minimally generalized type that conforms to all input types.
     *
     * @param inputTypes,
     *            a list of input types
     * @return a generalized type that conforms to all input types.
     */
    public static IAType resolve(List<IAType> inputTypes) {
        IAType currentType = null;
        for (IAType type : inputTypes) {
            currentType = currentType == null ? type : generalizeTypes(currentType, type);
        }
        return currentType;
    }

    /**
     * Returns a minimally generalized type that conforms to all input types.
     *
     * @param inputTypes,
     *            a list of input types
     * @return a generalized type that conforms to all input types.
     */
    public static IAType resolve(IAType... inputTypes) {
        IAType currentType = null;
        for (IAType type : inputTypes) {
            currentType = currentType == null ? type : generalizeTypes(currentType, type);
        }
        return currentType;
    }

    /**
     * Decides whether a type cast is needed to covert data instances from the input type to the required type.
     *
     * @param reqType,
     *            the required type.
     * @param inputType,
     *            the input type.
     * @return true of a type cast is needed, false otherwise.
     */
    public static boolean needsCast(IAType reqType, IAType inputType) {
        ATypeTag tag = inputType.getTypeTag();
        // Gets the actual input type regardless of MISSING and NULL.
        if (tag == ATypeTag.UNION) {
            tag = ((AUnionType) inputType).getActualType().getTypeTag();
        }
        // Casts are only needed when the original return type is a complex type.
        // (In the runtime, there is already a type tag for scalar types.)
        if (!tag.isDerivedType()) {
            return false;
        }
        return !TypeComputeUtils.getActualType(reqType).equals(TypeComputeUtils.getActualType(inputType));
    }

    // Generalizes two input types.
    private static IAType generalizeTypes(IAType inputLeftType, IAType inputRightType) {
        IAType leftType = inputLeftType;
        IAType rightType = inputRightType;
        ATypeTag leftTypeTag = leftType.getTypeTag();
        ATypeTag rightTypeTag = rightType.getTypeTag();
        boolean unknownable = false;

        // Gets the actual types for UNIONs and mark unknownable to be true.
        if (leftTypeTag == ATypeTag.UNION || rightTypeTag == ATypeTag.UNION) {
            leftType = TypeComputeUtils.getActualType(leftType);
            rightType = TypeComputeUtils.getActualType(rightType);
            leftTypeTag = leftType.getTypeTag();
            rightTypeTag = rightType.getTypeTag();
            unknownable = true;
        }
        if (leftType.equals(rightType)) {
            return unknownable ? AUnionType.createUnknownableType(leftType) : leftType;
        }

        // Deals with the case one input type is null or missing.
        if (leftTypeTag == ATypeTag.MISSING || leftTypeTag == ATypeTag.NULL) {
            return AUnionType.createUnknownableType(rightType);
        }
        if (rightTypeTag == ATypeTag.MISSING || rightTypeTag == ATypeTag.NULL) {
            return AUnionType.createUnknownableType(leftType);
        }

        // If two input types have different type tags (UNION/NULL/MISSING have been excluded), we return ANY here.
        if (leftTypeTag != rightTypeTag) {
            return BuiltinType.ANY;
        }

        // If two input types have the same type tag but are not equal, they can only be complex types.
        IAType generalizedComplexType = generalizeComplexTypes(leftTypeTag, leftType, rightType);
        return unknownable ? AUnionType.createUnknownableType(generalizedComplexType) : generalizedComplexType;
    }

    // Generalizes two complex types, e.g., record, ordered list and unordered list.
    private static IAType generalizeComplexTypes(ATypeTag typeTag, IAType leftType, IAType rightType) {
        switch (typeTag) {
            case OBJECT:
                return generalizeRecordTypes((ARecordType) leftType, (ARecordType) rightType);
            case ARRAY:
                return generalizeOrderedListTypes((AOrderedListType) leftType, (AOrderedListType) rightType);
            case MULTISET:
                return generalizeUnorderedListTypes((AUnorderedListType) leftType, (AUnorderedListType) rightType);
            default:
                return BuiltinType.ANY;
        }
    }

    // Generalizes two record types.
    private static ARecordType generalizeRecordTypes(ARecordType leftType, ARecordType rightType) {
        boolean knowsAdditonalFieldNames = true;
        Set<String> allPossibleAdditionalFieldNames = new HashSet<>();
        if (leftType.isOpen() && !leftType.knowsAllPossibleAdditonalFieldNames()) {
            knowsAdditonalFieldNames = false;
        } else if (leftType.isOpen()) {
            allPossibleAdditionalFieldNames.addAll(leftType.getAllPossibleAdditonalFieldNames());
        }
        if (rightType.isOpen() && !rightType.knowsAllPossibleAdditonalFieldNames()) {
            knowsAdditonalFieldNames = false;
        } else if (rightType.isOpen()) {
            allPossibleAdditionalFieldNames.addAll(rightType.getAllPossibleAdditonalFieldNames());
        }
        boolean canBeClosed = !leftType.isOpen() && !rightType.isOpen();
        List<String> fieldNames = new ArrayList<>();
        List<IAType> fieldTypes = new ArrayList<>();
        boolean leftAllMatched =
                generalizeRecordFields(leftType, rightType, allPossibleAdditionalFieldNames, fieldNames, fieldTypes);
        boolean rightAllMatched =
                generalizeRecordFields(rightType, leftType, allPossibleAdditionalFieldNames, fieldNames, fieldTypes);
        return new ARecordType("generalized-record-type", fieldNames.toArray(new String[fieldNames.size()]),
                fieldTypes.toArray(new IAType[fieldTypes.size()]), !(canBeClosed && leftAllMatched && rightAllMatched),
                knowsAdditonalFieldNames ? allPossibleAdditionalFieldNames : null);
    }

    // Generates closed fields and possible additional fields of a generalized type of two record types.
    private static boolean generalizeRecordFields(ARecordType leftType, ARecordType rightType,
            Set<String> allPossibleAdditionalFieldNames, List<String> fieldNames, List<IAType> fieldTypes) {
        boolean allMatched = true;
        Set<String> existingFieldNames = new HashSet<>(fieldNames);
        for (String fieldName : leftType.getFieldNames()) {
            IAType leftFieldType = leftType.getFieldType(fieldName);
            IAType rightFieldType = rightType.getFieldType(fieldName);
            IAType generalizedFieldType =
                    rightFieldType == null ? null : generalizeTypes(leftFieldType, rightFieldType);
            if (generalizedFieldType == null || generalizedFieldType.equals(BuiltinType.ANY)) {
                allPossibleAdditionalFieldNames.add(fieldName);
                allMatched = false;
            } else if (!existingFieldNames.contains(fieldName)) {
                fieldNames.add(fieldName);
                fieldTypes.add(generalizedFieldType);
            }
        }
        return allMatched;
    }

    // Generalizes two ordered list types.
    private static AOrderedListType generalizeOrderedListTypes(AOrderedListType leftType, AOrderedListType rightType) {
        return new AOrderedListType(processItemType(generalizeTypes(leftType.getItemType(), rightType.getItemType())),
                "generalized-ordered-list");
    }

    // Generalizes two unordered list types.
    private static AUnorderedListType generalizeUnorderedListTypes(AUnorderedListType leftType,
            AUnorderedListType rightType) {
        return new AUnorderedListType(processItemType(generalizeTypes(leftType.getItemType(), rightType.getItemType())),
                "generalized-unordered-list");
    }

    // A special processing for generalized item types in collections:
    // a collection cannot maintain an item type of UNION. In this case, the item type has to be ANY.
    private static IAType processItemType(IAType type) {
        ATypeTag tag = type.getTypeTag();
        return tag == ATypeTag.UNION ? BuiltinType.ANY : type;
    }

}
