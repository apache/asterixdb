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

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.CorrelatedPrefixMergePolicyFactory;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.AqlTypeTraitProvider;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.bootstrap.MetadataConstants;
import org.apache.asterix.metadata.entities.CompactionPolicy;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

public class DatasetUtils {
    public static IBinaryComparatorFactory[] computeKeysBinaryComparatorFactories(Dataset dataset, ARecordType itemType,
            IBinaryComparatorFactoryProvider comparatorFactoryProvider) throws AlgebricksException {
        List<List<String>> partitioningKeys = getPartitioningKeys(dataset);
        IBinaryComparatorFactory[] bcfs = new IBinaryComparatorFactory[partitioningKeys.size()];
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            // Get comparators for RID fields.
            for (int i = 0; i < partitioningKeys.size(); i++) {
                try {
                    bcfs[i] = IndexingConstants.getComparatorFactory(i);
                } catch (AsterixException e) {
                    throw new AlgebricksException(e);
                }
            }
        } else {
            for (int i = 0; i < partitioningKeys.size(); i++) {
                IAType keyType;
                try {
                    keyType = itemType.getSubFieldType(partitioningKeys.get(i));
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
                bcfs[i] = comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
            }
        }
        return bcfs;
    }

    public static int[] createBloomFilterKeyFields(Dataset dataset) throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("not implemented");
        }
        List<List<String>> partitioningKeys = getPartitioningKeys(dataset);
        int[] bloomFilterKeyFields = new int[partitioningKeys.size()];
        for (int i = 0; i < partitioningKeys.size(); ++i) {
            bloomFilterKeyFields[i] = i;
        }
        return bloomFilterKeyFields;
    }

    public static IBinaryHashFunctionFactory[] computeKeysBinaryHashFunFactories(Dataset dataset, ARecordType itemType,
            IBinaryHashFunctionFactoryProvider hashFunProvider) throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("not implemented");
        }
        List<List<String>> partitioningKeys = getPartitioningKeys(dataset);
        IBinaryHashFunctionFactory[] bhffs = new IBinaryHashFunctionFactory[partitioningKeys.size()];
        for (int i = 0; i < partitioningKeys.size(); i++) {
            IAType keyType;
            try {
                keyType = itemType.getSubFieldType(partitioningKeys.get(i));
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
            bhffs[i] = hashFunProvider.getBinaryHashFunctionFactory(keyType);
        }
        return bhffs;
    }

    public static ITypeTraits[] computeTupleTypeTraits(Dataset dataset, ARecordType itemType)
            throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("not implemented");
        }
        List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        int numKeys = partitioningKeys.size();
        ITypeTraits[] typeTraits = new ITypeTraits[numKeys + 1];
        for (int i = 0; i < numKeys; i++) {
            IAType keyType;
            try {
                keyType = itemType.getSubFieldType(partitioningKeys.get(i));
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
            typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
        }
        typeTraits[numKeys] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(itemType);
        return typeTraits;
    }

    public static List<List<String>> getPartitioningKeys(Dataset dataset) {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return IndexingConstants.getRIDKeys(((ExternalDatasetDetails) dataset.getDatasetDetails()).getProperties());
        }
        return ((InternalDatasetDetails) dataset.getDatasetDetails()).getPartitioningKey();
    }

    public static List<String> getFilterField(Dataset dataset) {
        return (((InternalDatasetDetails) dataset.getDatasetDetails())).getFilterField();
    }

    public static IBinaryComparatorFactory[] computeFilterBinaryComparatorFactories(Dataset dataset,
            ARecordType itemType, IBinaryComparatorFactoryProvider comparatorFactoryProvider)
                    throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return null;
        }
        List<String> filterField = getFilterField(dataset);
        if (filterField == null) {
            return null;
        }
        IBinaryComparatorFactory[] bcfs = new IBinaryComparatorFactory[1];
        IAType type;
        try {
            type = itemType.getSubFieldType(filterField);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
        bcfs[0] = comparatorFactoryProvider.getBinaryComparatorFactory(type, true);
        return bcfs;
    }

    public static ITypeTraits[] computeFilterTypeTraits(Dataset dataset, ARecordType itemType)
            throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return null;
        }
        List<String> filterField = getFilterField(dataset);
        if (filterField == null) {
            return null;
        }
        ITypeTraits[] typeTraits = new ITypeTraits[1];

        IAType type;
        try {
            type = itemType.getSubFieldType(filterField);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
        typeTraits[0] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(type);
        return typeTraits;
    }

    public static int[] createFilterFields(Dataset dataset) throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return null;
        }

        List<String> filterField = getFilterField(dataset);
        if (filterField == null) {
            return null;
        }
        List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        int numKeys = partitioningKeys.size();

        int[] filterFields = new int[1];
        filterFields[0] = numKeys + 1;
        return filterFields;
    }

    public static int[] createBTreeFieldsWhenThereisAFilter(Dataset dataset) throws AlgebricksException {
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            return null;
        }

        List<String> filterField = getFilterField(dataset);
        if (filterField == null) {
            return null;
        }

        List<List<String>> partitioningKeys = getPartitioningKeys(dataset);
        int[] btreeFields = new int[partitioningKeys.size() + 1];
        for (int i = 0; i < btreeFields.length; ++i) {
            btreeFields[i] = i;
        }
        return btreeFields;
    }

    public static int getPositionOfPartitioningKeyField(Dataset dataset, String fieldExpr) {
        List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        for (int i = 0; i < partitioningKeys.size(); i++) {
            if (partitioningKeys.get(i).size() == 1 && partitioningKeys.get(i).get(0).equals(fieldExpr)) {
                return i;
            }
        }
        return -1;
    }

    public static Pair<ILSMMergePolicyFactory, Map<String, String>> getMergePolicyFactory(Dataset dataset,
            MetadataTransactionContext mdTxnCtx) throws AlgebricksException, MetadataException {
        String policyName = dataset.getCompactionPolicy();
        CompactionPolicy compactionPolicy = MetadataManager.INSTANCE.getCompactionPolicy(mdTxnCtx,
                MetadataConstants.METADATA_DATAVERSE_NAME, policyName);
        String compactionPolicyFactoryClassName = compactionPolicy.getClassName();
        ILSMMergePolicyFactory mergePolicyFactory;
        try {
            mergePolicyFactory = (ILSMMergePolicyFactory) Class.forName(compactionPolicyFactoryClassName).newInstance();
            if (mergePolicyFactory.getName().compareTo("correlated-prefix") == 0) {
                ((CorrelatedPrefixMergePolicyFactory) mergePolicyFactory).setDatasetID(dataset.getDatasetId());
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new AlgebricksException(e);
        }
        Map<String, String> properties = dataset.getCompactionPolicyProperties();
        return new Pair<ILSMMergePolicyFactory, Map<String, String>>(mergePolicyFactory, properties);
    }

    @SuppressWarnings("unchecked")
    public static void writePropertyTypeRecord(String name, String value, DataOutput out, ARecordType recordType)
            throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(recordType);
        AMutableString aString = new AMutableString("");
        ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ASTRING);

        // write field 0
        fieldValue.reset();
        aString.setValue(name);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(0, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(1, fieldValue);

        try {
            propertyRecordBuilder.write(out, true);
        } catch (IOException | AsterixException e) {
            throw new HyracksDataException(e);
        }
    }
}
