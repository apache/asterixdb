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

import java.io.IOException;
import java.util.List;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * Metadata describing an index.
 */
public class Index implements IMetadataEntity, Comparable<Index> {

    private static final long serialVersionUID = 1L;

    private final String dataverseName;
    // Enforced to be unique within a dataverse.
    private final String datasetName;
    // Enforced to be unique within a dataverse, dataset combination.
    private final String indexName;
    private final IndexType indexType;
    private final List<List<String>> keyFieldNames;
    private final List<IAType> keyFieldTypes;
    private final boolean enforceKeyFields;
    private final boolean isPrimaryIndex;
    // Specific to NGRAM indexes.
    private final int gramLength;
    // Type of pending operations with respect to atomic DDL operation
    private int pendingOp;

    public Index(String dataverseName, String datasetName, String indexName, IndexType indexType,
            List<List<String>> keyFieldNames, List<IAType> keyFieldTypes, int gramLength, boolean enforceKeyFields,
            boolean isPrimaryIndex, int pendingOp) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.indexName = indexName;
        this.indexType = indexType;
        this.keyFieldNames = keyFieldNames;
        this.keyFieldTypes = keyFieldTypes;
        this.gramLength = gramLength;
        this.enforceKeyFields = enforceKeyFields;
        this.isPrimaryIndex = isPrimaryIndex;
        this.pendingOp = pendingOp;
    }

    public Index(String dataverseName, String datasetName, String indexName, IndexType indexType,
            List<List<String>> keyFieldNames, List<IAType> keyFieldTypes, boolean enforceKeyFields,
            boolean isPrimaryIndex, int pendingOp) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.indexName = indexName;
        this.indexType = indexType;
        this.keyFieldNames = keyFieldNames;
        this.keyFieldTypes = keyFieldTypes;
        this.gramLength = -1;
        this.enforceKeyFields = enforceKeyFields;
        this.isPrimaryIndex = isPrimaryIndex;
        this.pendingOp = pendingOp;
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

    public boolean isEnforcingKeyFileds() {
        return enforceKeyFields;
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

    public static Pair<IAType, Boolean> getNonNullableType(IAType keyType) throws AlgebricksException {
        boolean nullable = false;
        if (NonTaggedFormatUtil.isOptional(keyType)) {
            keyType = ((AUnionType) keyType).getNullableType();
            nullable = true;
        }
        return new Pair<IAType, Boolean>(keyType, nullable);
    }

    public static Pair<IAType, Boolean> getNonNullableOpenFieldType(IAType fieldType, List<String> fieldName,
            ARecordType recType) throws AlgebricksException {
        Pair<IAType, Boolean> keyPairType = null;

        try {
            IAType subType = recType;
            for (int i = 0; i < fieldName.size(); i++) {
                subType = ((ARecordType) subType).getFieldType(fieldName.get(i));
                if (subType == null) {
                    keyPairType = Index.getNonNullableType(fieldType);
                    break;
                }
            }
            if (subType != null)
                keyPairType = Index.getNonNullableKeyFieldType(fieldName, recType);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
        return keyPairType;
    }

    public static Pair<IAType, Boolean> getNonNullableKeyFieldType(List<String> expr, ARecordType recType)
            throws AlgebricksException {
        IAType keyType = Index.keyFieldType(expr, recType);
        return getNonNullableType(keyType);
    }

    private static IAType keyFieldType(List<String> expr, ARecordType recType) throws AlgebricksException {
        IAType fieldType = recType;
        try {
            fieldType = recType.getSubFieldType(expr);
        } catch (IOException e) {
            throw new AlgebricksException("Could not find field " + expr + " in the schema.", e);
        }
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
    public Object addToCache(MetadataCache cache) {
        return cache.addIndexIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
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
}
