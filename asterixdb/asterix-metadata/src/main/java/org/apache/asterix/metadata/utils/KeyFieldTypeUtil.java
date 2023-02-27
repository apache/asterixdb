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
import java.util.Objects;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.LogRedactionUtil;

public class KeyFieldTypeUtil {

    private KeyFieldTypeUtil() {
    }

    /**
     * Get the types of primary key (partitioning key) fields
     *
     * @param dataset,
     *            the dataset to consider.
     * @param recordType,
     *            the main record type.
     * @param metaRecordType
     *            the auxiliary meta record type.
     * @return a list of IATypes, one for each corresponding primary key field.
     * @throws AlgebricksException
     */
    public static List<IAType> getPartitoningKeyTypes(Dataset dataset, ARecordType recordType,
            ARecordType metaRecordType) throws AlgebricksException {
        if (dataset.getDatasetType() != DatasetType.INTERNAL) {
            return null;
        }
        InternalDatasetDetails datasetDetails = (InternalDatasetDetails) dataset.getDatasetDetails();
        return getPartitioningKeyTypes(datasetDetails, recordType, metaRecordType);
    }

    /**
     * Get the types of primary key (partitioning key) fields
     *
     * @param datasetDetails,
     *            contains specific data structures for an internal dataset.
     * @param recordType,
     *            the main record type.
     * @param metaRecordType
     *            the auxiliary meta record type.
     * @return a list of IATypes, one for each corresponding primary key field.
     * @throws AlgebricksException
     */
    public static List<IAType> getPartitioningKeyTypes(InternalDatasetDetails datasetDetails, ARecordType recordType,
            ARecordType metaRecordType) throws AlgebricksException {
        List<Integer> keySourceIndicators = datasetDetails.getKeySourceIndicator();
        List<List<String>> partitioningKeys = datasetDetails.getPartitioningKey();
        return getKeyTypes(recordType, metaRecordType, partitioningKeys, keySourceIndicators);
    }

    /**
     * Get the types of key fields for an index, either primary or secondary.
     *
     * @param recordType,
     *            the main record type.
     * @param metaRecordType,
     *            the auxiliary meta record type.
     * @param keys,
     *            the list of key fields.
     * @param keySourceIndicators,
     *            a list of integers to indicate that each key field is from the main record or the auxiliary meta
     *            record.
     * @return a list of IATypes, one for each corresponding index key field.
     * @throws AlgebricksException
     *
     * @deprecated use {@link #getKeyProjectType(ARecordType, List, SourceLocation)}
     */
    public static List<IAType> getKeyTypes(ARecordType recordType, ARecordType metaRecordType, List<List<String>> keys,
            List<Integer> keySourceIndicators) throws AlgebricksException {
        List<IAType> keyTypes = new ArrayList<>();
        int index = 0;
        for (List<String> partitioningKey : keys) {
            keyTypes.add(chooseSource(keySourceIndicators, index, recordType, metaRecordType)
                    .getSubFieldType(partitioningKey));
            ++index;
        }
        return keyTypes;
    }

    /**
     * Get the types of BTree index key fields
     *
     * @param index,
     *            the index to consider.
     * @param recordType,
     *            the main record type.
     * @param metaRecordType
     *            the auxiliary meta record type.
     * @return a list of IATypes, one for each corresponding index key field.
     * @throws AlgebricksException
     */
    public static List<Pair<IAType, Boolean>> getBTreeIndexKeyTypes(Index index, ARecordType recordType,
            ARecordType metaRecordType) throws AlgebricksException {
        Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
        List<Integer> keySourceIndicators = indexDetails.getKeyFieldSourceIndicators();
        List<Pair<IAType, Boolean>> indexKeyTypes = new ArrayList<>();
        for (int i = 0; i < indexDetails.getKeyFieldNames().size(); i++) {
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(index,
                    indexDetails.getKeyFieldTypes().get(i), indexDetails.getKeyFieldNames().get(i),
                    chooseSource(keySourceIndicators, i, recordType, metaRecordType));
            indexKeyTypes.add(keyPairType);
        }
        return indexKeyTypes;
    }

    /**
     * @see KeyFieldTypeUtil#getBTreeIndexKeyTypes(Index, ARecordType, ARecordType)
     */
    public static List<IAType> getArrayBTreeIndexKeyTypes(Index index, ARecordType recordType,
            ARecordType metaRecordType) throws AlgebricksException {
        Index.ArrayIndexDetails indexDetails = (Index.ArrayIndexDetails) index.getIndexDetails();
        List<IAType> indexKeyTypes = new ArrayList<>();
        for (Index.ArrayIndexElement e : indexDetails.getElementList()) {
            for (int i = 0; i < e.getProjectList().size(); i++) {
                ARecordType sourceType =
                        (e.getSourceIndicator() == Index.RECORD_INDICATOR) ? recordType : metaRecordType;
                Pair<IAType, Boolean> keyPairType = ArrayIndexUtil.getNonNullableOpenFieldType(e.getTypeList().get(i),
                        e.getUnnestList(), e.getProjectList().get(i), sourceType);
                indexKeyTypes.add(keyPairType.first);
            }
        }
        return indexKeyTypes;
    }

    /**
     * Get the types of RTree index key fields
     *
     * @param index,
     *            the index to consider.
     * @param recordType,
     *            the main record type.
     * @param metaRecordType
     *            the auxiliary meta record type.
     * @return a list of IATypes, one for each corresponding index key field.
     * @throws AlgebricksException
     */
    public static List<IAType> getRTreeIndexKeyTypes(Index index, ARecordType recordType, ARecordType metaRecordType)
            throws AlgebricksException {
        Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
        List<Integer> keySourceIndicators = indexDetails.getKeyFieldSourceIndicators();
        List<IAType> indexKeyTypes = new ArrayList<>();
        ARecordType targetRecType = chooseSource(keySourceIndicators, 0, recordType, metaRecordType);
        Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(index,
                indexDetails.getKeyFieldTypes().get(0), indexDetails.getKeyFieldNames().get(0), targetRecType);
        IAType keyType = keyPairType.first;
        IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(keyType.getTypeTag());
        int numKeys = KeyFieldTypeUtil.getNumSecondaryKeys(index, targetRecType, metaRecordType);
        for (int i = 0; i < numKeys; i++) {
            indexKeyTypes.add(nestedKeyType);
        }
        return indexKeyTypes;
    }

    /**
     * Get the number of secondary index keys.
     *
     * @param index,
     *            the index to consider.
     * @param recordType,
     *            the main record type.
     * @param metaRecordType
     *            the auxiliary meta record type.
     * @return the number of secondary index keys.
     * @throws AlgebricksException
     */
    public static int getNumSecondaryKeys(Index index, ARecordType recordType, ARecordType metaRecordType)
            throws AlgebricksException {
        switch (index.getIndexType()) {
            case ARRAY:
                return ((Index.ArrayIndexDetails) index.getIndexDetails()).getElementList().stream()
                        .map(e -> e.getProjectList().size()).reduce(0, Integer::sum);
            case BTREE:
                return ((Index.ValueIndexDetails) index.getIndexDetails()).getKeyFieldNames().size();
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX:
                return ((Index.TextIndexDetails) index.getIndexDetails()).getKeyFieldNames().size();
            case RTREE:
                Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
                List<Integer> keySourceIndicators = indexDetails.getKeyFieldSourceIndicators();
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(index,
                        indexDetails.getKeyFieldTypes().get(0), indexDetails.getKeyFieldNames().get(0),
                        chooseSource(keySourceIndicators, 0, recordType, metaRecordType));
                IAType keyType = keyPairType.first;
                return NonTaggedFormatUtil.getNumDimensions(keyType.getTypeTag()) * 2;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE, index.getIndexType());
        }
    }

    /**
     * Choose between the main record type and the auxiliary record type according to <code>keySourceIndicators</code>.
     *
     * @param keySourceIndicators,
     *            a list of integers, 0 means to choose <code>recordType</code> and 1
     *            means to choose <code>metaRecordType</code>.
     * @param index,
     *            the offset to consider.
     * @param recordType,
     *            the main record type.
     * @param metaRecordType
     *            the auxiliary meta record type.
     * @return the chosen record type.
     */
    public static ARecordType chooseSource(List<Integer> keySourceIndicators, int index, ARecordType recordType,
            ARecordType metaRecordType) {
        return keySourceIndicators.get(index) == 0 ? recordType : metaRecordType;
    }

    /**
     * Returns type after applying UNNEST steps defined by an index element.
     *
     * @return { primeType, nullable, missable } or {@code null} if the path is not found in an open record
     * @throws CompilationException
     *             if path is not found in a closed record
     */
    public static Triple<IAType, Boolean, Boolean> getKeyUnnestType(final ARecordType inputType,
            List<List<String>> unnestPathList, SourceLocation sourceLoc) throws CompilationException {
        if (unnestPathList.isEmpty()) {
            return new Triple<>(inputType, false, false);
        }
        IAType itemType = inputType;
        boolean itemTypeNullable = false, itemTypeMissable = false;
        for (List<String> unnestPath : unnestPathList) {
            // check that the type is a record at this point
            if (itemType.getTypeTag() != ATypeTag.OBJECT) {
                throw new CompilationException(ErrorCode.TYPE_MISMATCH_GENERIC, sourceLoc, ATypeTag.OBJECT,
                        itemType.getTypeTag());
            }
            ARecordType itemRecordType = (ARecordType) itemType;
            Triple<IAType, Boolean, Boolean> fieldTypeResult = getKeyProjectType(itemRecordType, unnestPath, sourceLoc);
            if (fieldTypeResult == null) {
                return null;
            }
            IAType fieldType = fieldTypeResult.first;
            boolean fieldTypeNullable = fieldTypeResult.second;
            boolean fieldTypeMissable = fieldTypeResult.third;
            // check that we've arrived to a collection type
            if (!fieldType.getTypeTag().isListType()) {
                throw new CompilationException(ErrorCode.TYPE_MISMATCH_GENERIC,
                        sourceLoc, ExceptionUtil.toExpectedTypeString(new byte[] {
                                ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG, ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG }),
                        fieldType);
            }
            AbstractCollectionType fieldListType = (AbstractCollectionType) fieldType;
            IAType fieldListItemType = fieldListType.getItemType();
            boolean fieldListItemTypeNullable = false, fieldListItemTypeMissable = false;
            if (fieldListItemType.getTypeTag() == ATypeTag.UNION) {
                AUnionType fieldListItemTypeUnion = (AUnionType) fieldListItemType;
                fieldListItemType = fieldListItemTypeUnion.getActualType();
                fieldListItemTypeNullable = fieldListItemTypeUnion.isNullableType();
                fieldListItemTypeMissable = fieldListItemTypeUnion.isMissableType();
            }
            itemType = fieldListItemType;
            itemTypeNullable = itemTypeNullable || fieldTypeNullable || fieldListItemTypeNullable;
            itemTypeMissable = itemTypeMissable || fieldTypeMissable || fieldListItemTypeMissable;
        }
        return new Triple<>(itemType, itemTypeNullable, itemTypeMissable);
    }

    /**
     * Returns type after applying SELECT steps defined by an index element.
     *
     * @return { primeType, nullable, missable } or {@code null} if the path is not found in an open record
     * @throws CompilationException
     *             if path is not found in a closed record
     */
    public static Triple<IAType, Boolean, Boolean> getKeyProjectType(final ARecordType inputType, List<String> path,
            SourceLocation sourceLoc) throws CompilationException {
        IAType itemType = inputType;
        boolean itemTypeNullable = false, itemTypeMissable = false;
        for (String step : path) {
            // check that the type is a record at this point
            if (itemType.getTypeTag() != ATypeTag.OBJECT) {
                throw new CompilationException(ErrorCode.TYPE_MISMATCH_GENERIC, sourceLoc, ATypeTag.OBJECT,
                        itemType.getTypeTag());
            }
            ARecordType itemRecordType = (ARecordType) itemType;
            IAType fieldType = itemRecordType.getFieldType(step);
            if (fieldType == null) {
                if (itemRecordType.isOpen()) {
                    // open record type and we couldn't find the field -> ok.
                    return null;
                } else {
                    // closed record type and we couldn't find the field -> error.
                    throw new CompilationException(ErrorCode.COMPILATION_FIELD_NOT_FOUND, sourceLoc,
                            LogRedactionUtil.userData(RecordUtil.toFullyQualifiedName(path)));
                }
            }
            if (fieldType.getTypeTag() == ATypeTag.UNION) {
                AUnionType fieldTypeUnion = (AUnionType) fieldType;
                itemType = fieldTypeUnion.getActualType();
                itemTypeNullable = itemTypeNullable || fieldTypeUnion.isNullableType();
                itemTypeMissable = itemTypeMissable || fieldTypeUnion.isMissableType();
            } else {
                itemType = fieldType;
            }
        }
        return new Triple<>(itemType, itemTypeNullable, itemTypeMissable);
    }

    public static IAType makeUnknownableType(IAType primeType, boolean nullable, boolean missable) {
        IAType type = Objects.requireNonNull(primeType);
        if (nullable) {
            type = AUnionType.createNullableType(type);
        }
        if (missable) {
            type = AUnionType.createMissableType(type);
        }
        return type;
    }
}
