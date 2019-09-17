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
package org.apache.asterix.test.dataflow;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.external.indexing.FilesIndexDescription;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.metadata.api.IResourceFactoryProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;

public class TestLsmBTreeResourceFactoryProvider implements IResourceFactoryProvider {

    public static final TestLsmBTreeResourceFactoryProvider INSTANCE = new TestLsmBTreeResourceFactoryProvider();

    private TestLsmBTreeResourceFactoryProvider() {
    }

    @Override
    public IResourceFactory getResourceFactory(MetadataProvider mdProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories) throws AlgebricksException {
        int[] filterFields = IndexUtil.getFilterFields(dataset, index, filterTypeTraits);
        int[] btreeFields = IndexUtil.getBtreeFieldsIfFiltered(dataset, index);
        IStorageComponentProvider storageComponentProvider = mdProvider.getStorageComponentProvider();
        ITypeTraits[] typeTraits = getTypeTraits(mdProvider, dataset, index, recordType, metaType);
        IBinaryComparatorFactory[] cmpFactories = getCmpFactories(mdProvider, dataset, index, recordType, metaType);
        int[] bloomFilterFields = getBloomFilterFields(dataset, index);
        double bloomFilterFalsePositiveRate = mdProvider.getStorageProperties().getBloomFilterFalsePositiveRate();
        ILSMOperationTrackerFactory opTrackerFactory = dataset.getIndexOperationTrackerFactory(index);
        if (opTrackerFactory instanceof PrimaryIndexOperationTrackerFactory) {
            opTrackerFactory = new TestPrimaryIndexOperationTrackerFactory(dataset.getDatasetId());
        }
        ILSMIOOperationCallbackFactory ioOpCallbackFactory = dataset.getIoOperationCallbackFactory(index);
        ILSMPageWriteCallbackFactory pageWriteCallbackFactory = dataset.getPageWriteCallbackFactory();
        IStorageManager storageManager = storageComponentProvider.getStorageManager();
        IMetadataPageManagerFactory metadataPageManagerFactory =
                storageComponentProvider.getMetadataPageManagerFactory();
        ILSMIOOperationSchedulerProvider ioSchedulerProvider =
                storageComponentProvider.getIoOperationSchedulerProvider();
        AsterixVirtualBufferCacheProvider vbcProvider = new AsterixVirtualBufferCacheProvider(dataset.getDatasetId());
        return new TestLsmBtreeLocalResourceFactory(storageManager, typeTraits, cmpFactories, filterTypeTraits,
                filterCmpFactories, filterFields, opTrackerFactory, ioOpCallbackFactory, pageWriteCallbackFactory,
                metadataPageManagerFactory, vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties,
                true, bloomFilterFields, bloomFilterFalsePositiveRate, index.isPrimaryIndex(), btreeFields);
    }

    private static ITypeTraits[] getTypeTraits(MetadataProvider metadataProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        ITypeTraits[] primaryTypeTraits = dataset.getPrimaryTypeTraits(metadataProvider, recordType, metaType);
        if (index.isPrimaryIndex()) {
            return primaryTypeTraits;
        } else if (dataset.getDatasetType() == DatasetType.EXTERNAL
                && index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))) {
            return FilesIndexDescription.EXTERNAL_FILE_INDEX_TYPE_TRAITS;
        }
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        int numSecondaryKeys = index.getKeyFieldNames().size();
        ITypeTraitProvider typeTraitProvider = metadataProvider.getStorageComponentProvider().getTypeTraitProvider();
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        for (int i = 0; i < numSecondaryKeys; i++) {
            ARecordType sourceType;
            List<Integer> keySourceIndicators = index.getKeyFieldSourceIndicators();
            if (keySourceIndicators == null || keySourceIndicators.get(i) == 0) {
                sourceType = recordType;
            } else {
                sourceType = metaType;
            }
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(i),
                    index.getKeyFieldNames().get(i), sourceType);
            IAType keyType = keyTypePair.first;
            secondaryTypeTraits[i] = typeTraitProvider.getTypeTrait(keyType);
        }
        // Add serializers and comparators for primary index fields.
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryTypeTraits[numSecondaryKeys + i] = primaryTypeTraits[i];
        }
        return secondaryTypeTraits;
    }

    private static IBinaryComparatorFactory[] getCmpFactories(MetadataProvider metadataProvider, Dataset dataset,
            Index index, ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        IBinaryComparatorFactory[] primaryCmpFactories =
                dataset.getPrimaryComparatorFactories(metadataProvider, recordType, metaType);
        if (index.isPrimaryIndex()) {
            return dataset.getPrimaryComparatorFactories(metadataProvider, recordType, metaType);
        } else if (dataset.getDatasetType() == DatasetType.EXTERNAL
                && index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))) {
            return FilesIndexDescription.FILES_INDEX_COMP_FACTORIES;
        }
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        int numSecondaryKeys = index.getKeyFieldNames().size();
        IBinaryComparatorFactoryProvider cmpFactoryProvider =
                metadataProvider.getStorageComponentProvider().getComparatorFactoryProvider();
        IBinaryComparatorFactory[] secondaryCmpFactories =
                new IBinaryComparatorFactory[numSecondaryKeys + numPrimaryKeys];
        for (int i = 0; i < numSecondaryKeys; i++) {
            ARecordType sourceType;
            List<Integer> keySourceIndicators = index.getKeyFieldSourceIndicators();
            if (keySourceIndicators == null || keySourceIndicators.get(i) == 0) {
                sourceType = recordType;
            } else {
                sourceType = metaType;
            }
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(i),
                    index.getKeyFieldNames().get(i), sourceType);
            IAType keyType = keyTypePair.first;
            secondaryCmpFactories[i] = cmpFactoryProvider.getBinaryComparatorFactory(keyType, true);
        }
        // Add serializers and comparators for primary index fields.
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryCmpFactories[numSecondaryKeys + i] = primaryCmpFactories[i];
        }
        return secondaryCmpFactories;
    }

    private static int[] getBloomFilterFields(Dataset dataset, Index index) throws AlgebricksException {
        // both the Primary index and the Primary Key index have bloom filters
        if (index.isPrimaryIndex() || index.isPrimaryKeyIndex()) {
            return dataset.getPrimaryBloomFilterFields();
        } else if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            if (index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))) {
                return FilesIndexDescription.BLOOM_FILTER_FIELDS;
            } else {
                return new int[] { index.getKeyFieldNames().size() };
            }
        } else if (index.getIndexType() == IndexType.BTREE || index.getIndexType() == IndexType.RTREE) {
            // secondary btrees and rtrees do not have bloom filters
            return null;
        } else {
            // inverted indexes have bloom filters on deleted-key btrees
            int numKeys = index.getKeyFieldNames().size();
            int[] bloomFilterKeyFields = new int[numKeys];
            for (int i = 0; i < numKeys; i++) {
                bloomFilterKeyFields[i] = i;
            }
            return bloomFilterKeyFields;
        }
    }
}
