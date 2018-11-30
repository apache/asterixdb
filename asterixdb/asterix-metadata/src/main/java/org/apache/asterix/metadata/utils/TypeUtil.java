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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Provider utility methods for data types
 */
public class TypeUtil {

    private TypeUtil() {
    }

    /**
     * Merges typed index fields with specified recordType, allowing indexed fields to be optional.
     * I.e. the type { "personId":int32, "name": string, "address" : { "street": string } } with typed indexes
     * on age:int32, address.state:string will be merged into type { "personId":int32, "name": string,
     * "age": int32? "address" : { "street": string, "state": string? } } Used by open indexes to enforce
     * the type of an indexed record
     */
    public static Pair<ARecordType, ARecordType> createEnforcedType(ARecordType recordType, ARecordType metaType,
            List<Index> indexes) throws AlgebricksException {
        IAType enforcedRecordType = recordType;
        ARecordType enforcedMetaType = metaType;
        List<String> subFieldName;
        for (Index index : indexes) {
            if (!index.isSecondaryIndex() || !index.isOverridingKeyFieldTypes()) {
                continue;
            }
            if (index.hasMetaFields()) {
                throw new AlgebricksException("Indexing an open field is only supported on the record part");
            }
            for (int i = 0; i < index.getKeyFieldNames().size(); i++) {
                // keeps track of a record type and a field name in that record type
                Deque<Pair<IAType, String>> nestedTypeStack = new ArrayDeque<>();
                List<String> splits = index.getKeyFieldNames().get(i);
                IAType nestedFieldType = enforcedRecordType;
                boolean openRecords = false;
                String bridgeName = nestedFieldType.getTypeName();
                int j;
                // enforcedRecordType must always be/stay as ARecordType
                validateRecord(enforcedRecordType);
                // build the stack for the enforced type, stack of a mixture of ARecord and AUnion(ARecord) types
                // try to build up to the last record field, e.g. for a.b.c.d.e, build up to and including "d"
                for (j = 1; j < splits.size(); j++) {
                    nestedTypeStack.push(new Pair<>(nestedFieldType, splits.get(j - 1)));
                    bridgeName = nestedFieldType.getTypeName();
                    subFieldName = splits.subList(0, j);
                    nestedFieldType = ((ARecordType) enforcedRecordType).getSubFieldType(subFieldName);
                    if (nestedFieldType == null) {
                        openRecords = true;
                        break;
                    }
                    // nestedFieldType (i.e. nested record field) must be either ARecordType or AUnion(ARecordType)
                    validateNestedRecord(nestedFieldType, subFieldName);
                }
                if (openRecords) {
                    // create the smallest record
                    enforcedRecordType = new ARecordType(splits.get(splits.size() - 2),
                            new String[] { splits.get(splits.size() - 1) },
                            new IAType[] { AUnionType.createUnknownableType(index.getKeyFieldTypes().get(i)) }, true);
                    // create the open part of the nested field
                    for (int k = splits.size() - 3; k > (j - 2); k--) {
                        enforcedRecordType = new ARecordType(splits.get(k), new String[] { splits.get(k + 1) },
                                new IAType[] { AUnionType.createUnknownableType(enforcedRecordType) }, true);
                    }
                    // bridge the gap. Update the parent type to include the new optional field, e.g. c.d.e
                    Pair<IAType, String> gapPair = nestedTypeStack.pop();
                    ARecordType parent = (ARecordType) TypeComputeUtils.getActualType(gapPair.first);

                    // parent type must be "open" to allow inclusion of the non-declared field
                    IAType[] parentFieldTypes = ArrayUtils.addAll(parent.getFieldTypes().clone(),
                            new IAType[] { AUnionType.createUnknownableType(enforcedRecordType) });
                    enforcedRecordType = new ARecordType(bridgeName,
                            ArrayUtils.addAll(parent.getFieldNames(), enforcedRecordType.getTypeName()),
                            parentFieldTypes, true);
                    // make nullable/missable if the original parent was nullable/missable
                    enforcedRecordType = keepUnknown(gapPair.first, (ARecordType) enforcedRecordType);
                } else {
                    // schema is closed all the way to the field. Enforced fields are either null or strongly typed
                    // e.g. nestedFieldType = a.b.c.d
                    ARecordType lastNestedRecord = (ARecordType) TypeComputeUtils.getActualType(nestedFieldType);
                    Map<String, IAType> recordNameTypesMap = TypeUtil.createRecordNameTypeMap(lastNestedRecord);
                    // if a an enforced field already exists and the type is correct
                    IAType enforcedFieldType = recordNameTypesMap.get(splits.get(splits.size() - 1));
                    if (enforcedFieldType != null && enforcedFieldType.getTypeTag() == ATypeTag.UNION
                            && ((AUnionType) enforcedFieldType).isUnknownableType()) {
                        enforcedFieldType = ((AUnionType) enforcedFieldType).getActualType();
                    }
                    if (enforcedFieldType != null && !ATypeHierarchy.canPromote(enforcedFieldType.getTypeTag(),
                            index.getKeyFieldTypes().get(i).getTypeTag())) {
                        throw new AsterixException(ErrorCode.COMPILATION_ERROR,
                                "Cannot enforce field \"" + String.join(".", index.getKeyFieldNames().get(i))
                                        + "\" to have type " + index.getKeyFieldTypes().get(i));
                    }
                    if (enforcedFieldType == null) {
                        recordNameTypesMap.put(splits.get(splits.size() - 1),
                                AUnionType.createUnknownableType(index.getKeyFieldTypes().get(i)));
                    }
                    enforcedRecordType = new ARecordType(lastNestedRecord.getTypeName(),
                            recordNameTypesMap.keySet().toArray(new String[recordNameTypesMap.size()]),
                            recordNameTypesMap.values().toArray(new IAType[recordNameTypesMap.size()]),
                            lastNestedRecord.isOpen());
                    // make nullable/missable if the original nestedFieldType was nullable/missable
                    enforcedRecordType = keepUnknown(nestedFieldType, (ARecordType) enforcedRecordType);
                }

                // Create the enforced type for the nested fields in the schema, from the ground up
                if (!nestedTypeStack.isEmpty()) {
                    while (!nestedTypeStack.isEmpty()) {
                        Pair<IAType, String> nestedType = nestedTypeStack.pop();
                        ARecordType nestedRecType = (ARecordType) TypeComputeUtils.getActualType(nestedType.first);
                        IAType[] nestedRecTypeFieldTypes = nestedRecType.getFieldTypes().clone();
                        nestedRecTypeFieldTypes[nestedRecType.getFieldIndex(nestedType.second)] = enforcedRecordType;
                        enforcedRecordType = new ARecordType(nestedRecType.getTypeName() + "_enforced",
                                nestedRecType.getFieldNames(), nestedRecTypeFieldTypes, nestedRecType.isOpen());
                        // make nullable/missable if the original nestedRecType was nullable/missable
                        enforcedRecordType = keepUnknown(nestedType.first, (ARecordType) enforcedRecordType);
                    }
                }
            }
        }
        // the final enforcedRecordType built must be ARecordType since the original dataset rec. type can't be nullable
        validateRecord(enforcedRecordType);
        return new Pair<>((ARecordType) enforcedRecordType, enforcedMetaType);
    }

    /**
     * Creates a map from name to type for fields in the passed type
     *
     * @param recordType the type to be mapped
     * @return a map mapping all fields to their types
     */
    private static Map<String, IAType> createRecordNameTypeMap(ARecordType recordType) {
        LinkedHashMap<String, IAType> recordNameTypesMap = new LinkedHashMap<>();
        for (int j = 0; j < recordType.getFieldNames().length; j++) {
            recordNameTypesMap.put(recordType.getFieldNames()[j], recordType.getFieldTypes()[j]);
        }
        return recordNameTypesMap;
    }

    /**
     * Maintains the {@code updatedRecordType} as nullable/missable (?) in case it was originally nullable/missable
     * @param originalRecordType the original record type
     * @param updatedRecordType the original record type being enforced/modified with new non-declared fields included
     * @return {@code updatedRecordType}
     */
    private static IAType keepUnknown(IAType originalRecordType, ARecordType updatedRecordType) {
        if (originalRecordType.getTypeTag() == ATypeTag.UNION) {
            return AUnionType.createUnknownableType(updatedRecordType, updatedRecordType.getTypeName());
        }
        return updatedRecordType;
    }

    /**
     * Makes sure the dataset record type being enforced/modified stays as a pure record type
     * @param enforcedDatasetRecordType the dataset record type enforced and modified by adding the extra fields indexed
     */
    private static void validateRecord(IAType enforcedDatasetRecordType) {
        if (enforcedDatasetRecordType.getTypeTag() != ATypeTag.OBJECT) {
            throw new IllegalStateException("The dataset type must be a record type to be able to build an index");
        }
    }

    /**
     * Makes sure the chain of fields accessed and leading to the indexed field are all valid record types.
     * E.g. for CREATE INDEX idx on ds(a.b.c.d: int) validate that a, b and c are all valid record types (?).
     * @param nestedRecordType the nested record field being accessed
     * @param fieldName the name of the nested record field
     * @throws AsterixException when supplying bad fields, e.g. CREATE INDEX i on ds(a.b: int, a.b.c: int) (mostly
     * for non-declared fields)
     */
    private static void validateNestedRecord(IAType nestedRecordType, List<String> fieldName) throws AsterixException {
        IAType actualType = TypeComputeUtils.getActualType(nestedRecordType);
        if (actualType.getTypeTag() != ATypeTag.OBJECT) {
            String fName = String.join(".", fieldName);
            throw new AsterixException(ErrorCode.COMPILATION_ERROR,
                    "Field accessor is not defined for \"" + fName + "\" of type " + actualType.getTypeTag());
        }
    }
}
