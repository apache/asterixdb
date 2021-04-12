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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
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

    private static final long serialVersionUID = 3L;
    public static final int RECORD_INDICATOR = 0;
    public static final int META_RECORD_INDICATOR = 1;

    private final DataverseName dataverseName;
    // Enforced to be unique within a dataverse.
    private final String datasetName;
    // Enforced to be unique within a dataverse, dataset combination.
    private final String indexName;
    private final IndexType indexType;
    private final IIndexDetails indexDetails;
    private final boolean isPrimaryIndex;
    private final boolean isEnforced;
    // Type of pending operations with respect to atomic DDL operation
    private int pendingOp;

    public Index(DataverseName dataverseName, String datasetName, String indexName, IndexType indexType,
            IIndexDetails indexDetails, boolean isEnforced, boolean isPrimaryIndex, int pendingOp) {
        boolean categoryOk = (indexType == null && indexDetails == null) || (IndexCategory
                .of(Objects.requireNonNull(indexType)) == ((AbstractIndexDetails) Objects.requireNonNull(indexDetails))
                        .getIndexCategory());
        if (!categoryOk) {
            throw new IllegalArgumentException();
        }
        this.dataverseName = Objects.requireNonNull(dataverseName);
        this.datasetName = Objects.requireNonNull(datasetName);
        this.indexName = Objects.requireNonNull(indexName);
        this.indexType = indexType;
        this.indexDetails = indexDetails;
        this.isPrimaryIndex = isPrimaryIndex;
        this.isEnforced = isEnforced;
        this.pendingOp = pendingOp;
    }

    @Deprecated
    public Index(DataverseName dataverseName, String datasetName, String indexName, IndexType indexType,
            List<List<String>> keyFieldNames, List<Integer> keyFieldSourceIndicators, List<IAType> keyFieldTypes,
            boolean overrideKeyFieldTypes, boolean isEnforced, boolean isPrimaryIndex, int pendingOp) {
        this(dataverseName,
                datasetName, indexName, indexType, createSimpleIndexDetails(indexType, keyFieldNames,
                        keyFieldSourceIndicators, keyFieldTypes, overrideKeyFieldTypes),
                isEnforced, isPrimaryIndex, pendingOp);
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getIndexName() {
        return indexName;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public boolean isPrimaryIndex() {
        return isPrimaryIndex;
    }

    public IIndexDetails getIndexDetails() {
        return indexDetails;
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
        return indexType == IndexType.BTREE && ((ValueIndexDetails) indexDetails).keyFieldNames.isEmpty();
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
        return recType.getSubFieldType(expr);
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

    public byte resourceType() throws CompilationException {
        switch (indexType) {
            case ARRAY:
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
        return dataverseName + "." + datasetName + "." + indexName;
    }

    public enum IndexCategory {
        VALUE,
        TEXT,
        ARRAY;

        public static IndexCategory of(IndexType indexType) {
            switch (indexType) {
                case BTREE:
                case RTREE:
                    return VALUE;
                case SINGLE_PARTITION_WORD_INVIX:
                case SINGLE_PARTITION_NGRAM_INVIX:
                case LENGTH_PARTITIONED_WORD_INVIX:
                case LENGTH_PARTITIONED_NGRAM_INVIX:
                    return TEXT;
                case ARRAY:
                    return ARRAY;
                default:
                    throw new IllegalArgumentException(String.valueOf(indexType));
            }
        }
    }

    public interface IIndexDetails extends Serializable {
        boolean isOverridingKeyFieldTypes();
    }

    static abstract class AbstractIndexDetails implements IIndexDetails {

        private static final long serialVersionUID = 1L;

        abstract IndexCategory getIndexCategory();
    }

    public static final class ValueIndexDetails extends AbstractIndexDetails {

        private static final long serialVersionUID = 1L;

        private final List<List<String>> keyFieldNames;

        private final List<Integer> keyFieldSourceIndicators;

        private final List<IAType> keyFieldTypes;

        private final boolean overrideKeyFieldTypes;

        public ValueIndexDetails(List<List<String>> keyFieldNames, List<Integer> keyFieldSourceIndicators,
                List<IAType> keyFieldTypes, boolean overrideKeyFieldTypes) {
            this.keyFieldNames = keyFieldNames;
            this.keyFieldSourceIndicators = keyFieldSourceIndicators;
            this.keyFieldTypes = keyFieldTypes;
            this.overrideKeyFieldTypes = overrideKeyFieldTypes;
        }

        @Override
        IndexCategory getIndexCategory() {
            return IndexCategory.VALUE;
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

        @Override
        public boolean isOverridingKeyFieldTypes() {
            return overrideKeyFieldTypes;
        }
    }

    public static final class TextIndexDetails extends AbstractIndexDetails {

        private static final long serialVersionUID = 1L;

        private final List<List<String>> keyFieldNames;

        private final List<Integer> keyFieldSourceIndicators;

        private final List<IAType> keyFieldTypes;

        private final boolean overrideKeyFieldTypes;

        // ToDo: to allow index to access the full-text config in another dataverse,
        //   maybe we need to add a new field here fullTextConfigDataverseName for dataverse name of full-text config
        // Specific to FullText indexes.
        private final String fullTextConfigName;

        // Specific to NGRAM indexes.
        private final int gramLength;

        public TextIndexDetails(List<List<String>> keyFieldNames, List<Integer> keyFieldSourceIndicators,
                List<IAType> keyFieldTypes, boolean overrideKeyFieldTypes, int gramLength, String fullTextConfigName) {
            this.keyFieldNames = keyFieldNames;
            this.keyFieldTypes = keyFieldTypes;
            this.keyFieldSourceIndicators = keyFieldSourceIndicators;
            this.overrideKeyFieldTypes = overrideKeyFieldTypes;
            this.gramLength = gramLength;
            this.fullTextConfigName = fullTextConfigName;
        }

        @Override
        IndexCategory getIndexCategory() {
            return IndexCategory.TEXT;
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

        @Override
        public boolean isOverridingKeyFieldTypes() {
            return overrideKeyFieldTypes;
        }

        public int getGramLength() {
            return gramLength;
        }

        public String getFullTextConfigName() {
            return fullTextConfigName;
        }
    }

    public static class ArrayIndexDetails extends AbstractIndexDetails {

        private static final long serialVersionUID = 1L;

        private final List<ArrayIndexElement> elementList;

        private final boolean overrideKeyFieldTypes;

        public ArrayIndexDetails(List<ArrayIndexElement> elementList, boolean overrideKeyFieldTypes) {
            this.elementList = elementList;
            this.overrideKeyFieldTypes = overrideKeyFieldTypes;
        }

        @Override
        IndexCategory getIndexCategory() {
            return IndexCategory.ARRAY;
        }

        public List<ArrayIndexElement> getElementList() {
            return elementList;
        }

        @Override
        public boolean isOverridingKeyFieldTypes() {
            return overrideKeyFieldTypes;
        }
    }

    public static final class ArrayIndexElement implements Serializable {

        private static final long serialVersionUID = 1L;

        private final List<List<String>> unnestList;

        private final List<List<String>> projectList;

        private final List<IAType> typeList;

        private final int sourceIndicator;

        public ArrayIndexElement(List<List<String>> unnestList, List<List<String>> projectList, List<IAType> typeList,
                int sourceIndicator) {
            this.unnestList = unnestList != null ? unnestList : Collections.emptyList();
            this.projectList = projectList;
            this.typeList = typeList;
            this.sourceIndicator = sourceIndicator;
        }

        public List<List<String>> getUnnestList() {
            return unnestList;
        }

        public List<List<String>> getProjectList() {
            return projectList;
        }

        public List<IAType> getTypeList() {
            return typeList;
        }

        public int getSourceIndicator() {
            return sourceIndicator;
        }
    }

    @Deprecated
    private static Index.IIndexDetails createSimpleIndexDetails(IndexType indexType, List<List<String>> keyFieldNames,
            List<Integer> keyFieldSourceIndicators, List<IAType> keyFieldTypes, boolean overrideKeyFieldTypes) {
        if (indexType == null) {
            return null;
        }
        switch (Index.IndexCategory.of(indexType)) {
            case VALUE:
                return new ValueIndexDetails(keyFieldNames, keyFieldSourceIndicators, keyFieldTypes,
                        overrideKeyFieldTypes);
            case TEXT:
                return new TextIndexDetails(keyFieldNames, keyFieldSourceIndicators, keyFieldTypes,
                        overrideKeyFieldTypes, -1, null);
            default:
                throw new IllegalArgumentException(String.valueOf(indexType));
        }
    }
}
