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
package org.apache.asterix.metadata.declared;

import java.util.List;
import java.util.Map;

import org.apache.asterix.column.ColumnManagerFactory;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.formats.nontagged.NullIntrospector;
import org.apache.asterix.metadata.api.IResourceFactoryProvider;
import org.apache.asterix.metadata.dataset.DatasetFormatInfo;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.dataflow.LSMColumnBTreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;

public class BTreeResourceFactoryProvider implements IResourceFactoryProvider {

    public static final BTreeResourceFactoryProvider INSTANCE = new BTreeResourceFactoryProvider();

    private BTreeResourceFactoryProvider() {
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
        ILSMIOOperationCallbackFactory ioOpCallbackFactory = dataset.getIoOperationCallbackFactory(index);
        ILSMPageWriteCallbackFactory pageWriteCallbackFactory = dataset.getPageWriteCallbackFactory();
        IStorageManager storageManager = storageComponentProvider.getStorageManager();
        IMetadataPageManagerFactory metadataPageManagerFactory =
                storageComponentProvider.getMetadataPageManagerFactory();
        ILSMIOOperationSchedulerProvider ioSchedulerProvider =
                storageComponentProvider.getIoOperationSchedulerProvider();
        boolean hasBloomFilter = bloomFilterFields != null;
        ITypeTraitProvider typeTraitProvider = mdProvider.getDataFormat().getTypeTraitProvider();
        switch (dataset.getDatasetType()) {
            case EXTERNAL:
                return null;
            case INTERNAL:
                AsterixVirtualBufferCacheProvider vbcProvider =
                        new AsterixVirtualBufferCacheProvider(dataset.getDatasetId());

                final ICompressorDecompressorFactory compDecompFactory;
                if (index.isPrimaryIndex()) {
                    //Compress only primary index
                    compDecompFactory = mdProvider.getCompressionManager().getFactory(dataset.getCompressionScheme());
                } else {
                    compDecompFactory = NoOpCompressorDecompressorFactory.INSTANCE;
                }

                boolean isSecondaryNoIncrementalMaintenance = index.getIndexType() == DatasetConfig.IndexType.SAMPLE;

                DatasetFormatInfo datasetFormatInfo = dataset.getDatasetFormatInfo();
                if (!index.isPrimaryIndex() || datasetFormatInfo.getFormat() == DatasetConfig.DatasetFormat.ROW) {
                    return new LSMBTreeLocalResourceFactory(storageManager, typeTraits, cmpFactories, filterTypeTraits,
                            filterCmpFactories, filterFields, opTrackerFactory, ioOpCallbackFactory,
                            pageWriteCallbackFactory, metadataPageManagerFactory, vbcProvider, ioSchedulerProvider,
                            mergePolicyFactory, mergePolicyProperties, true, bloomFilterFields,
                            bloomFilterFalsePositiveRate, index.isPrimaryIndex(), btreeFields, compDecompFactory,
                            hasBloomFilter, typeTraitProvider.getTypeTrait(BuiltinType.ANULL),
                            NullIntrospector.INSTANCE, isSecondaryNoIncrementalMaintenance, dataset.isAtomic());
                } else {
                    //Column
                    List<Integer> keySourceIndicator =
                            ((InternalDatasetDetails) dataset.getDatasetDetails()).getKeySourceIndicator();
                    IColumnManagerFactory columnManagerFactory =
                            new ColumnManagerFactory(recordType, metaType, dataset.getPrimaryKeys(), keySourceIndicator,
                                    mdProvider.getStorageProperties().getBufferCachePageSize(),
                                    datasetFormatInfo.getMaxTupleCount(), datasetFormatInfo.getFreeSpaceTolerance(),
                                    datasetFormatInfo.getMaxLeafNodeSize());
                    return new LSMColumnBTreeLocalResourceFactory(storageManager, typeTraits, cmpFactories,
                            filterTypeTraits, filterCmpFactories, filterFields, opTrackerFactory, ioOpCallbackFactory,
                            pageWriteCallbackFactory, metadataPageManagerFactory, vbcProvider, ioSchedulerProvider,
                            mergePolicyFactory, mergePolicyProperties, bloomFilterFields, bloomFilterFalsePositiveRate,
                            btreeFields, compDecompFactory, typeTraitProvider.getTypeTrait(BuiltinType.ANULL),
                            NullIntrospector.INSTANCE, isSecondaryNoIncrementalMaintenance, columnManagerFactory,
                            dataset.isAtomic());
                }
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_DATASET_TYPE,
                        dataset.getDatasetType().toString());
        }
    }

    private static ITypeTraits[] getTypeTraits(MetadataProvider metadataProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        ITypeTraits[] primaryTypeTraits = dataset.getPrimaryTypeTraits(metadataProvider, recordType, metaType);
        if (index.isPrimaryIndex() || index.getIndexType() == DatasetConfig.IndexType.SAMPLE) {
            return primaryTypeTraits;
        } else if (dataset.getDatasetType() == DatasetType.EXTERNAL
                && index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))) {
            return new ITypeTraits[0];
        }
        Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        int numSecondaryKeys = indexDetails.getKeyFieldNames().size();
        ITypeTraitProvider typeTraitProvider = metadataProvider.getStorageComponentProvider().getTypeTraitProvider();
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        for (int i = 0; i < numSecondaryKeys; i++) {
            ARecordType sourceType;
            List<Integer> keySourceIndicators = indexDetails.getKeyFieldSourceIndicators();
            if (keySourceIndicators == null || keySourceIndicators.get(i) == 0) {
                sourceType = recordType;
            } else {
                sourceType = metaType;
            }
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(index,
                    indexDetails.getKeyFieldTypes().get(i), indexDetails.getKeyFieldNames().get(i), sourceType);
            IAType keyType = keyTypePair.first;
            secondaryTypeTraits[i] = typeTraitProvider.getTypeTrait(keyType);
        }
        // Add serializers and comparators for primary index fields.
        System.arraycopy(primaryTypeTraits, 0, secondaryTypeTraits, numSecondaryKeys, numPrimaryKeys);
        return secondaryTypeTraits;
    }

    private static IBinaryComparatorFactory[] getCmpFactories(MetadataProvider metadataProvider, Dataset dataset,
            Index index, ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        IBinaryComparatorFactory[] primaryCmpFactories =
                dataset.getPrimaryComparatorFactories(metadataProvider, recordType, metaType);
        if (index.isPrimaryIndex() || index.getIndexType() == DatasetConfig.IndexType.SAMPLE) {
            return dataset.getPrimaryComparatorFactories(metadataProvider, recordType, metaType);
        } else if (dataset.getDatasetType() == DatasetType.EXTERNAL
                && index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))) {
            return new IBinaryComparatorFactory[0];
        }
        Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        int numSecondaryKeys = indexDetails.getKeyFieldNames().size();
        IBinaryComparatorFactoryProvider cmpFactoryProvider =
                metadataProvider.getStorageComponentProvider().getComparatorFactoryProvider();
        IBinaryComparatorFactory[] secondaryCmpFactories =
                new IBinaryComparatorFactory[numSecondaryKeys + numPrimaryKeys];
        for (int i = 0; i < numSecondaryKeys; i++) {
            ARecordType sourceType;
            List<Integer> keySourceIndicators = indexDetails.getKeyFieldSourceIndicators();
            if (keySourceIndicators == null || keySourceIndicators.get(i) == 0) {
                sourceType = recordType;
            } else {
                sourceType = metaType;
            }
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(index,
                    indexDetails.getKeyFieldTypes().get(i), indexDetails.getKeyFieldNames().get(i), sourceType);
            IAType keyType = keyTypePair.first;
            secondaryCmpFactories[i] = cmpFactoryProvider.getBinaryComparatorFactory(keyType, true);
        }
        // Add serializers and comparators for primary index fields.
        System.arraycopy(primaryCmpFactories, 0, secondaryCmpFactories, numSecondaryKeys, numPrimaryKeys);
        return secondaryCmpFactories;
    }

    private static int[] getBloomFilterFields(Dataset dataset, Index index) throws AlgebricksException {
        // both the Primary index and the Primary Key index have bloom filters
        if (index.isPrimaryIndex() || index.isPrimaryKeyIndex()) {
            return dataset.getPrimaryBloomFilterFields();
        }
        if (dataset.getDatasetType() == DatasetType.EXTERNAL
                && index.getIndexType() != DatasetConfig.IndexType.SAMPLE) {
            if (index.getIndexName().equals(IndexingConstants.getFilesIndexName(dataset.getDatasetName()))) {
                return new int[0];
            } else {
                Index.ValueIndexDetails indexDetails = ((Index.ValueIndexDetails) index.getIndexDetails());
                return new int[] { indexDetails.getKeyFieldNames().size() };
            }
        }
        switch (index.getIndexType()) {
            case BTREE:
            case RTREE:
                // secondary btrees and rtrees do not have bloom filters
                return null;
            case LENGTH_PARTITIONED_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case SINGLE_PARTITION_WORD_INVIX:
                // inverted indexes have bloom filters on deleted-key btrees
                int numKeys = ((Index.TextIndexDetails) index.getIndexDetails()).getKeyFieldNames().size();
                int[] bloomFilterKeyFields = new int[numKeys];
                for (int i = 0; i < numKeys; i++) {
                    bloomFilterKeyFields[i] = i;
                }
                return bloomFilterKeyFields;
            case SAMPLE:
                return null;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE,
                        String.valueOf(index.getIndexType()));
        }
    }
}
