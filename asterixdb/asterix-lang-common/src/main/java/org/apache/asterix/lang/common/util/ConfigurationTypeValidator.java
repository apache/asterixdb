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
package org.apache.asterix.lang.common.util;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.object.base.AdmArrayNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.tuple.MutablePair;

class ConfigurationTypeValidator {
    //Error information
    private final Set<String> unknownFieldNames;
    private final Deque<String> path;
    private final MutablePair<ATypeTag, ATypeTag> expectedActualTypePair;
    private ErrorType result;

    public enum ErrorType {
        UNKNOWN_FIELD_NAMES,
        TYPE_MISMATCH,
        MISSING_UNOPTIONAL_FIELD
    }

    protected ConfigurationTypeValidator() {
        unknownFieldNames = new HashSet<>();
        path = new ArrayDeque<>();
        expectedActualTypePair = new MutablePair<>(null, null);
    }

    public void validateType(IAType type, IAdmNode node) throws CompilationException {
        if (!validate(type, node)) {
            throwException();
        }
    }

    private boolean validate(IAType type, IAdmNode node) {
        if (type.getTypeTag().isDerivedType()) {
            return validateDerivedType(type, node);
        } else if (node == null) {
            result = ErrorType.MISSING_UNOPTIONAL_FIELD;
            return false;
        } else if (type.getTypeTag() != node.getType()) {
            setExpectedAndActualType(type.getTypeTag(), node.getType());
            return false;
        }

        return true;
    }

    private boolean validateDerivedType(IAType type, IAdmNode node) {
        final ATypeTag typeTag = type.getTypeTag();
        switch (typeTag) {
            case UNION:
                return validateUnionType(type, node);
            case OBJECT:
                return validateObject(type, node);
            case ARRAY:
                return validateArray(type, node);
            default:
                throw new IllegalStateException("Unsupported derived type: " + typeTag);
        }
    }

    private boolean validateUnionType(IAType type, IAdmNode node) {
        if (node == null || node.getType() == ATypeTag.NULL) {
            return true;
        }
        return validate(((AUnionType) type).getActualType(), node);
    }

    private boolean validateObject(IAType type, IAdmNode node) {
        if (node.getType() != ATypeTag.OBJECT) {
            setExpectedAndActualType(ATypeTag.OBJECT, node.getType());
            return false;
        }

        final ARecordType recordType = (ARecordType) type;
        final AdmObjectNode objectNode = (AdmObjectNode) node;

        final String[] fieldNames = recordType.getFieldNames();

        //Check field names
        final Set<String> definedFieldNames = new HashSet<>(Arrays.asList(fieldNames));
        final Set<String> objectFieldNames = objectNode.getFieldNames();
        if (!definedFieldNames.containsAll(objectFieldNames)) {
            setUnknownFieldNames(definedFieldNames, objectFieldNames);
            return false;
        }

        final IAType[] fieldTypes = recordType.getFieldTypes();

        for (int i = 0; i < fieldTypes.length; i++) {
            if (!validate(fieldTypes[i], objectNode.get(fieldNames[i]))) {
                addToPath(fieldNames[i]);
                return false;
            }
        }
        return true;
    }

    private boolean validateArray(IAType type, IAdmNode node) {
        if (node.getType() != ATypeTag.ARRAY) {
            setExpectedAndActualType(ATypeTag.ARRAY, node.getType());
            return false;
        }

        final IAType itemType = ((AOrderedListType) type).getItemType();
        final AdmArrayNode array = (AdmArrayNode) node;
        for (int i = 0; i < array.size(); i++) {
            if (!validate(itemType, array.get(i))) {
                addToPath(i);
                return false;
            }
        }
        return true;
    }

    private void setUnknownFieldNames(Set<String> definedFieldNames, Set<String> objectFieldNames) {
        unknownFieldNames.addAll(objectFieldNames);
        unknownFieldNames.removeAll(definedFieldNames);
        result = ErrorType.UNKNOWN_FIELD_NAMES;
    }

    private void setExpectedAndActualType(ATypeTag expectedTypeTag, ATypeTag actualTypeTag) {
        expectedActualTypePair.left = expectedTypeTag;
        expectedActualTypePair.right = actualTypeTag;
        result = ErrorType.TYPE_MISMATCH;
    }

    private void addToPath(String fieldName) {
        if (path.isEmpty()) {
            path.push(fieldName);
        } else {
            path.push(fieldName + ".");
        }
    }

    private void addToPath(int arrayIndex) {
        path.push("[" + arrayIndex + "]");
    }

    private void throwException() throws CompilationException {
        final StringBuilder pathBuilder = new StringBuilder();
        while (!path.isEmpty()) {
            pathBuilder.append(path.pop());
        }
        switch (result) {
            case UNKNOWN_FIELD_NAMES:
                if (pathBuilder.length() > 0) {
                    throw new CompilationException(ErrorCode.UNSUPPORTED_WITH_SUBFIELD, unknownFieldNames.toString(),
                            pathBuilder.toString());
                }
                throw new CompilationException(ErrorCode.UNSUPPORTED_WITH_FIELD, unknownFieldNames.toString());
            case TYPE_MISMATCH:
                throw new CompilationException(ErrorCode.WITH_FIELD_MUST_BE_OF_TYPE, pathBuilder.toString(),
                        expectedActualTypePair.left, expectedActualTypePair.right);
            case MISSING_UNOPTIONAL_FIELD:
                throw new CompilationException(ErrorCode.WITH_FIELD_MUST_CONTAIN_SUB_FIELD, pathBuilder.toString());

        }
    }

}
