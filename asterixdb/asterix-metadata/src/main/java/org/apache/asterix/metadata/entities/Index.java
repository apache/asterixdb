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

package org.apache.asterix.metadata.entities;

import java.util.List;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.transactions.IRecoveryManager.ResourceType;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Metadata describing an index.
 */
public class Index implements IMetadataEntity<Index>, Comparable<Index> {

    private static final long serialVersionUID = 1L;
    public static final int RECORD_INDICATOR = 0;

    private final String dataverseName;
    // Enforced to be unique within a dataverse.
    private final String datasetName;
    // Enforced to be unique within a dataverse, dataset combination.
    private final String indexName;
    private final IndexType indexType;
    private final List<List<String>> keyFieldNames;
    private final List<Integer> keyFieldSourceIndicators;
    private final List<IAType> keyFieldTypes;
    private final boolean overrideKeyFieldTypes;
    private final boolean isEnforced;
    private final boolean isPrimaryIndex;
    // Specific to NGRAM indexes.
    private final int gramLength;
    // Type of pending operations with respect to atomic DDL operation
    private int pendingOp;

    public Index(String dataverseName, String datasetName, String indexName, IndexType indexType,
            List<List<String>> keyFieldNames, List<Integer> keyFieldSourceIndicators, List<IAType> keyFieldTypes,
            int gramLength, boolean overrideKeyFieldTypes, boolean isEnforced, boolean isPrimaryIndex, int pendingOp) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.indexName = indexName;
        this.indexType = indexType;
        this.keyFieldNames = keyFieldNames;
        this.keyFieldSourceIndicators = keyFieldSourceIndicators;
        this.keyFieldTypes = keyFieldTypes;
        this.gramLength = gramLength;
        this.overrideKeyFieldTypes = overrideKeyFieldTypes;
        this.isEnforced = isEnforced;
        this.isPrimaryIndex = isPrimaryIndex;
        this.pendingOp = pendingOp;
    }

    public Index(String dataverseName, String datasetName, String indexName, IndexType indexType,
            List<List<String>> keyFieldNames, List<Integer> keyFieldSourceIndicators, List<IAType> keyFieldTypes,
            boolean overrideKeyFieldTypes, boolean isEnforced, boolean isPrimaryIndex, int pendingOp) {
        this(dataverseName, datasetName, indexName, indexType, keyFieldNames, keyFieldSourceIndicators, keyFieldTypes,
                -1, overrideKeyFieldTypes, isEnforced, isPrimaryIndex, pendingOp);
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getIndexName() {
        return indexName;
    }

    public List<List<String>> getKeyFieldNames() {
        return keyFieldNames;
    }

    public List<Integer> getKeyFieldSourceIndicators() {
        return keyFieldSourceIndicators;
    }

    public List<IAType> getKeyFieldTypes() {
        return keyFieldTypes;
    }

    public int getGramLength() {
        return gramLength;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public boolean isPrimaryIndex() {
        return isPrimaryIndex;
    }

    public boolean isOverridingKeyFieldTypes() {
        return overrideKeyFieldTypes;
    }

    public boolean isEnforced() {
        return isEnforced;
    }

    public int getPendingOp() {
        return pendingOp;
    }

    public void setPendingOp(int pendingOp) {
        this.pendingOp = pendingOp;
    }

    public boolean isSecondaryIndex() {
        return !isPrimaryIndex();
    }

    public boolean isPrimaryKeyIndex() {
        // a primary key index has no key field names
        return keyFieldNames.isEmpty();
    }

    public static Pair<IAType, Boolean> getNonNullableType(IAType keyType) {
        boolean nullable = false;
        IAType actualKeyType = keyType;
        if (NonTaggedFormatUtil.isOptional(keyType)) {
            actualKeyType = ((AUnionType) keyType).getActualType();
            nullable = true;
        }
        return new Pair<>(actualKeyType, nullable);
    }

    public static Pair<IAType, Boolean> getNonNullableOpenFieldType(IAType fieldType, List<String> fieldName,
            ARecordType recType) throws AlgebricksException {
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
            } else {
                throw AsterixException.create(ErrorCode.COMPILATION_ILLEGAL_STATE, "Unexpected type " + fieldType);
            }

            if (subType == null) {
                keyPairType = Index.getNonNullableType(fieldType);
                break;
            }
        }
        if (subType != null) {
            keyPairType = Index.getNonNullableKeyFieldType(fieldName, recType);
        }
        keyPairType.second = keyPairType.second || nullable;
        return keyPairType;
    }

    public static Pair<IAType, Boolean> getNonNullableKeyFieldType(List<String> expr, ARecordType recType)
            throws AlgebricksException {
        IAType keyType = Index.keyFieldType(expr, recType);
        Pair<IAType, Boolean> pair = getNonNullableType(keyType);
        pair.second = pair.second || recType.isSubFieldNullable(expr);
        return pair;
    }

    private static IAType keyFieldType(List<String> expr, ARecordType recType) throws AlgebricksException {
        IAType fieldType = recType;
        fieldType = recType.getSubFieldType(expr);
        return fieldType;
    }

    @Override
    public int hashCode() {
        return indexName.hashCode() ^ datasetName.hashCode() ^ dataverseName.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Index)) {
            return false;
        }
        Index otherIndex = (Index) other;
        if (!indexName.equals(otherIndex.getIndexName())) {
            return false;
        }
        if (!datasetName.equals(otherIndex.getDatasetName())) {
            return false;
        }
        if (!dataverseName.equals(otherIndex.getDataverseName())) {
            return false;
        }
        return true;
    }

    @Override
    public Index addToCache(MetadataCache cache) {
        return cache.addIndexIfNotExists(this);
    }

    @Override
    public Index dropFromCache(MetadataCache cache) {
        return cache.dropIndex(this);
    }

    @Override
    public int compareTo(Index otherIndex) {
        /** Gives a primary index first priority. */
        if (isPrimaryIndex && !otherIndex.isPrimaryIndex) {
            return -1;
        }
        if (!isPrimaryIndex && otherIndex.isPrimaryIndex) {
            return 1;
        }

        /** Gives a B-Tree index the second priority. */
        if (indexType == IndexType.BTREE && otherIndex.indexType != IndexType.BTREE) {
            return -1;
        }
        if (indexType != IndexType.BTREE && otherIndex.indexType == IndexType.BTREE) {
            return 1;
        }

        /** Gives a R-Tree index the third priority */
        if (indexType == IndexType.RTREE && otherIndex.indexType != IndexType.RTREE) {
            return -1;
        }
        if (indexType != IndexType.RTREE && otherIndex.indexType == IndexType.RTREE) {
            return 1;
        }

        /** Finally, compares based on names. */
        int result = indexName.compareTo(otherIndex.getIndexName());
        if (result != 0) {
            return result;
        }
        result = datasetName.compareTo(otherIndex.getDatasetName());
        if (result != 0) {
            return result;
        }
        return dataverseName.compareTo(otherIndex.getDataverseName());
    }

    public boolean hasMetaFields() {
        if (keyFieldSourceIndicators != null) {
            for (Integer indicator : keyFieldSourceIndicators) {
                if (indicator.intValue() != 0) {
                    return true;
                }
            }
        }
        return false;
    }

    public byte resourceType() throws CompilationException {
        switch (indexType) {
            case BTREE:
                return ResourceType.LSM_BTREE;
            case RTREE:
                return ResourceType.LSM_RTREE;
            case LENGTH_PARTITIONED_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case SINGLE_PARTITION_WORD_INVIX:
                return ResourceType.LSM_INVERTED_INDEX;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE, indexType);
        }
    }

    @Override
    public String toString() {
        return dataverseName + '.' + datasetName + '.' + indexName;
    }
}
