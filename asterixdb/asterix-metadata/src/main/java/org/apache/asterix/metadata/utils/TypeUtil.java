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

import org.apache.asterix.metadata.entities.Index;
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
        ARecordType enforcedRecordType = recordType;
        ARecordType enforcedMetaType = metaType;
        for (Index index : indexes) {
            if (!index.isSecondaryIndex() || !index.isEnforcingKeyFileds()) {
                continue;
            }
            if (index.hasMetaFields()) {
                throw new AlgebricksException("Indexing an open field is only supported on the record part");
            }
            for (int i = 0; i < index.getKeyFieldNames().size(); i++) {
                Deque<Pair<ARecordType, String>> nestedTypeStack = new ArrayDeque<>();
                List<String> splits = index.getKeyFieldNames().get(i);
                ARecordType nestedFieldType = enforcedRecordType;
                boolean openRecords = false;
                String bridgeName = nestedFieldType.getTypeName();
                int j;
                // Build the stack for the enforced type
                for (j = 1; j < splits.size(); j++) {
                    nestedTypeStack.push(new Pair<>(nestedFieldType, splits.get(j - 1)));
                    bridgeName = nestedFieldType.getTypeName();
                    nestedFieldType = (ARecordType) enforcedRecordType.getSubFieldType(splits.subList(0, j));
                    if (nestedFieldType == null) {
                        openRecords = true;
                        break;
                    }
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
                    // Bridge the gap
                    Pair<ARecordType, String> gapPair = nestedTypeStack.pop();
                    ARecordType parent = gapPair.first;

                    IAType[] parentFieldTypes = ArrayUtils.addAll(parent.getFieldTypes().clone(),
                            new IAType[] { AUnionType.createUnknownableType(enforcedRecordType) });
                    enforcedRecordType = new ARecordType(bridgeName,
                            ArrayUtils.addAll(parent.getFieldNames(), enforcedRecordType.getTypeName()),
                            parentFieldTypes, true);
                } else {
                    //Schema is closed all the way to the field
                    //enforced fields are either null or strongly typed
                    Map<String, IAType> recordNameTypesMap = TypeUtil.createRecordNameTypeMap(nestedFieldType);
                    // if a an enforced field already exists and the type is correct
                    IAType enforcedFieldType = recordNameTypesMap.get(splits.get(splits.size() - 1));
                    if (enforcedFieldType != null && enforcedFieldType.getTypeTag() == ATypeTag.UNION
                            && ((AUnionType) enforcedFieldType).isUnknownableType()) {
                        enforcedFieldType = ((AUnionType) enforcedFieldType).getActualType();
                    }
                    if (enforcedFieldType != null && !ATypeHierarchy.canPromote(enforcedFieldType.getTypeTag(),
                            index.getKeyFieldTypes().get(i).getTypeTag())) {
                        throw new AlgebricksException("Cannot enforce field " + index.getKeyFieldNames().get(i)
                                + " to have type " + index.getKeyFieldTypes().get(i));
                    }
                    if (enforcedFieldType == null) {
                        recordNameTypesMap.put(splits.get(splits.size() - 1),
                                AUnionType.createUnknownableType(index.getKeyFieldTypes().get(i)));
                    }
                    enforcedRecordType = new ARecordType(nestedFieldType.getTypeName(),
                            recordNameTypesMap.keySet().toArray(new String[recordNameTypesMap.size()]),
                            recordNameTypesMap.values().toArray(new IAType[recordNameTypesMap.size()]),
                            nestedFieldType.isOpen());
                }

                // Create the enforced type for the nested fields in the schema, from the ground up
                if (!nestedTypeStack.isEmpty()) {
                    while (!nestedTypeStack.isEmpty()) {
                        Pair<ARecordType, String> nestedTypePair = nestedTypeStack.pop();
                        ARecordType nestedRecType = nestedTypePair.first;
                        IAType[] nestedRecTypeFieldTypes = nestedRecType.getFieldTypes().clone();
                        nestedRecTypeFieldTypes[nestedRecType.getFieldIndex(nestedTypePair.second)] =
                                enforcedRecordType;
                        enforcedRecordType = new ARecordType(nestedRecType.getTypeName() + "_enforced",
                                nestedRecType.getFieldNames(), nestedRecTypeFieldTypes, nestedRecType.isOpen());
                    }
                }
            }
        }
        return new Pair<>(enforcedRecordType, enforcedMetaType);
    }

    /**
     * Creates a map from name to type for fields in the passed type
     *
     * @param recordType
     *            the type to be mapped
     * @return a map mapping all fields to their types
     */
    public static Map<String, IAType> createRecordNameTypeMap(ARecordType recordType) {
        LinkedHashMap<String, IAType> recordNameTypesMap = new LinkedHashMap<>();
        for (int j = 0; j < recordType.getFieldNames().length; j++) {
            recordNameTypesMap.put(recordType.getFieldNames()[j], recordType.getFieldTypes()[j]);
        }
        return recordNameTypesMap;
    }

}
