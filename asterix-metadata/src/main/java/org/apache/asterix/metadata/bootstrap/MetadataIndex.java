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

package org.apache.asterix.metadata.bootstrap;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixRuntimeException;
import org.apache.asterix.common.transactions.DatasetId;
import org.apache.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlBinaryHashFunctionFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.AqlTypeTraitProvider;
import org.apache.asterix.metadata.api.IMetadataIndex;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.io.FileReference;

/**
 * Descriptor for a primary or secondary index on metadata datasets.
 */
public final class MetadataIndex implements IMetadataIndex {
    // Name of dataset that is indexed.
    protected final String datasetName;
    // Name of index. null for primary indexes. non-null for secondary indexes.
    protected final String indexName;
    // Types of key fields.
    protected final IAType[] keyTypes;
    // Names of key fields. Used to compute partitionExprs.
    protected final List<List<String>> keyNames;
    // Field permutation for BTree insert. Auto-created based on numFields.
    protected final int[] fieldPermutation;
    // Key Fields that will be used for the bloom filters in the LSM-btree.
    protected final int[] bloomFilterKeyFields;
    // Type of payload record for primary indexes. null for secondary indexes.
    protected final ARecordType payloadType;
    // Record descriptor of btree tuple. Created in c'tor.
    protected final RecordDescriptor recDesc;
    // Type traits of btree tuple. Created in c'tor.
    protected final ITypeTraits[] typeTraits;
    // Comparator factories for key fields of btree tuple. Created in c'tor.
    protected final IBinaryComparatorFactory[] bcfs;
    // Hash function factories for key fields of btree tuple. Created in c'tor.
    protected final IBinaryHashFunctionFactory[] bhffs;

    protected FileReference file;
    // Identifier of file BufferCache backing this metadata btree index.
    protected int fileId;
    // Resource id of this index for use in transactions.
    protected long resourceId;
    // datasetId
    private final DatasetId datasetId;
    // Flag of primary index
    protected final boolean isPrimaryIndex;
    // PrimaryKeyField indexes used for secondary index operations
    protected final int[] primaryKeyIndexes;

    public MetadataIndex(MetadataIndexImmutableProperties indexImmutableProperties, int numFields, IAType[] keyTypes,
            List<List<String>> keyNames, int numSecondaryIndexKeys, ARecordType payloadType, boolean isPrimaryIndex,
            int[] primaryKeyIndexes) throws AsterixRuntimeException {
        // Sanity checks.
        if (keyTypes.length != keyNames.size()) {
            throw new AsterixRuntimeException("Unequal number of key types and names given.");
        }
        if (keyTypes.length > numFields) {
            throw new AsterixRuntimeException("Number of keys given is greater than total number of fields.");
        }
        // Set simple fields.
        this.datasetName = indexImmutableProperties.getDatasetName();
        this.indexName = indexImmutableProperties.getIndexName();
        this.keyTypes = keyTypes;
        this.keyNames = keyNames;
        this.payloadType = payloadType;
        // Create field permutation.
        fieldPermutation = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldPermutation[i] = i;
        }
        // Create serdes for RecordDescriptor;
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] serdes = new ISerializerDeserializer[numFields];
        for (int i = 0; i < keyTypes.length; i++) {
            serdes[i] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(keyTypes[i]);
        }
        // For primary indexes, add payload field serde.
        if (fieldPermutation.length > keyTypes.length) {
            serdes[numFields - 1] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(payloadType);
        }
        recDesc = new RecordDescriptor(serdes);
        // Create type traits.
        typeTraits = new ITypeTraits[fieldPermutation.length];
        for (int i = 0; i < keyTypes.length; i++) {
            typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyTypes[i]);
        }
        // For primary indexes, add payload field.
        if (fieldPermutation.length > keyTypes.length) {
            typeTraits[fieldPermutation.length - 1] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(payloadType);
        }
        // Create binary comparator factories.
        bcfs = new IBinaryComparatorFactory[keyTypes.length];
        for (int i = 0; i < keyTypes.length; i++) {
            bcfs[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyTypes[i], true);
        }
        // Create binary hash function factories.
        bhffs = new IBinaryHashFunctionFactory[keyTypes.length];
        for (int i = 0; i < keyTypes.length; i++) {
            bhffs[i] = AqlBinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(keyTypes[i]);
        }

        if (isPrimaryIndex) {
            bloomFilterKeyFields = new int[primaryKeyIndexes.length];
            for (int i = 0; i < primaryKeyIndexes.length; ++i) {
                bloomFilterKeyFields[i] = primaryKeyIndexes[i];
            }
        } else {
            bloomFilterKeyFields = new int[numSecondaryIndexKeys];
            for (int i = 0; i < numSecondaryIndexKeys; ++i) {
                bloomFilterKeyFields[i] = i;
            }
        }

        this.datasetId = new DatasetId(indexImmutableProperties.getDatasetId());
        this.isPrimaryIndex = isPrimaryIndex;

        //PrimaryKeyFieldIndexes
        this.primaryKeyIndexes = primaryKeyIndexes;
        this.resourceId = indexImmutableProperties.getResourceId();
    }

    @Override
    public String getIndexedDatasetName() {
        return datasetName;
    }

    @Override
    public int[] getFieldPermutation() {
        return fieldPermutation;
    }

    @Override
    public int[] getBloomFilterKeyFields() {
        return bloomFilterKeyFields;
    }

    @Override
    public int getKeyFieldCount() {
        return keyTypes.length;
    }

    @Override
    public int getFieldCount() {
        return fieldPermutation.length;
    }

    @Override
    public String getDataverseName() {
        return MetadataConstants.METADATA_DATAVERSE_NAME;
    }

    @Override
    public String getNodeGroupName() {
        return MetadataConstants.METADATA_NODEGROUP_NAME;
    }

    @Override
    public List<List<String>> getPartitioningExpr() {
        ArrayList<List<String>> partitioningExpr = new ArrayList<List<String>>();
        for (int i = 0; i < keyNames.size(); i++) {
            partitioningExpr.add(keyNames.get(i));
        }
        return partitioningExpr;
    }

    @Override
    public List<IAType> getPartitioningExprType() {
        return Arrays.asList(keyTypes);
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public ITypeTraits[] getTypeTraits() {
        return typeTraits;
    }

    @Override
    public RecordDescriptor getRecordDescriptor() {
        return recDesc;
    }

    @Override
    public IBinaryComparatorFactory[] getKeyBinaryComparatorFactory() {
        return bcfs;
    }

    @Override
    public IBinaryHashFunctionFactory[] getKeyBinaryHashFunctionFactory() {
        return bhffs;
    }

    @Override
    public String getFileNameRelativePath() {
        return getDataverseName() + File.separator + getIndexedDatasetName() + "_idx_" + getIndexName();
    }

    @Override
    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public int getFileId() {
        return fileId;
    }

    @Override
    public ARecordType getPayloadRecordType() {
        return payloadType;
    }

    @Override
    public void setFile(FileReference file) {
        this.file = file;
    }

    @Override
    public FileReference getFile() {
        return this.file;
    }

    @Override
    public void setResourceID(long resourceID) {
        this.resourceId = resourceID;
    }

    @Override
    public long getResourceID() {
        return resourceId;
    }

    @Override
    public DatasetId getDatasetId() {
        return datasetId;
    }

    @Override
    public boolean isPrimaryIndex() {
        return isPrimaryIndex;
    }

    @Override
    public int[] getPrimaryKeyIndexes() {
        return primaryKeyIndexes;
    }
}