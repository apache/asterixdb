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

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;

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
    public static List<IAType> getBTreeIndexKeyTypes(Index index, ARecordType recordType, ARecordType metaRecordType)
            throws AlgebricksException {
        List<Integer> keySourceIndicators = index.getKeyFieldSourceIndicators();
        List<IAType> indexKeyTypes = new ArrayList<>();
        for (int i = 0; i < index.getKeyFieldNames().size(); i++) {
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(i),
                    index.getKeyFieldNames().get(i), chooseSource(keySourceIndicators, i, recordType, metaRecordType));
            indexKeyTypes.add(keyPairType.first);
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
        List<Integer> keySourceIndicators = index.getKeyFieldSourceIndicators();
        List<IAType> indexKeyTypes = new ArrayList<>();
        ARecordType targetRecType = chooseSource(keySourceIndicators, 0, recordType, metaRecordType);
        Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(0),
                index.getKeyFieldNames().get(0), targetRecType);
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
        List<Integer> keySourceIndicators = index.getKeyFieldSourceIndicators();
        switch (index.getIndexType()) {
            case BTREE:
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX:
                return index.getKeyFieldNames().size();
            case RTREE:
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(0),
                        index.getKeyFieldNames().get(0),
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
}
