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

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.api.IResourceFactoryProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.accessors.ShortBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexLocalResourceFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;

public class InvertedIndexResourceFactoryProvider implements IResourceFactoryProvider {
    public static final InvertedIndexResourceFactoryProvider INSTANCE = new InvertedIndexResourceFactoryProvider();

    private InvertedIndexResourceFactoryProvider() {
    }

    @Override
    public IResourceFactory getResourceFactory(MetadataProvider mdProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories) throws AlgebricksException {
        // Get basic info
        List<List<String>> primaryKeys = dataset.getPrimaryKeys();
        List<List<String>> secondaryKeys = index.getKeyFieldNames();
        List<String> filterFieldName = DatasetUtil.getFilterField(dataset);
        int numPrimaryKeys = primaryKeys.size();
        int numSecondaryKeys = secondaryKeys.size();
        // Validate
        if (dataset.getDatasetType() != DatasetType.INTERNAL) {
            throw new CompilationException(ErrorCode.COMPILATION_INDEX_TYPE_NOT_SUPPORTED_FOR_DATASET_TYPE,
                    index.getIndexType().name(), dataset.getDatasetType());
        }
        if (numPrimaryKeys > 1) {
            throw new AsterixException("Cannot create inverted index on dataset with composite primary key.");
        }
        if (numSecondaryKeys > 1) {
            throw new AsterixException("Cannot create composite inverted index on multiple fields.");
        }
        boolean isPartitioned = index.getIndexType() == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                || index.getIndexType() == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX;
        int numTokenKeyPairFields = (!isPartitioned) ? 1 + numPrimaryKeys : 2 + numPrimaryKeys;
        int[] invertedIndexFields = null;
        int[] secondaryFilterFieldsForNonBulkLoadOps = null;
        int[] invertedIndexFieldsForNonBulkLoadOps = null;
        int[] secondaryFilterFields = null;
        if (filterFieldName != null) {
            invertedIndexFields = new int[numTokenKeyPairFields];
            for (int i = 0; i < invertedIndexFields.length; i++) {
                invertedIndexFields[i] = i;
            }
            secondaryFilterFieldsForNonBulkLoadOps = new int[filterFieldName.size()];
            secondaryFilterFieldsForNonBulkLoadOps[0] = numSecondaryKeys + numPrimaryKeys;
            invertedIndexFieldsForNonBulkLoadOps = new int[numSecondaryKeys + numPrimaryKeys];
            for (int i = 0; i < invertedIndexFieldsForNonBulkLoadOps.length; i++) {
                invertedIndexFieldsForNonBulkLoadOps[i] = i;
            }
            secondaryFilterFields = new int[filterFieldName.size()];
            secondaryFilterFields[0] = numTokenKeyPairFields - numPrimaryKeys + numPrimaryKeys;
        }
        IStorageComponentProvider storageComponentProvider = mdProvider.getStorageComponentProvider();
        IStorageManager storageManager = storageComponentProvider.getStorageManager();
        ILSMOperationTrackerFactory opTrackerFactory = dataset.getIndexOperationTrackerFactory(index);
        ILSMIOOperationCallbackFactory ioOpCallbackFactory = dataset.getIoOperationCallbackFactory(index);
        ILSMPageWriteCallbackFactory pageWriteCallbackFactory = dataset.getPageWriteCallbackFactory();
        IMetadataPageManagerFactory metadataPageManagerFactory =
                storageComponentProvider.getMetadataPageManagerFactory();
        AsterixVirtualBufferCacheProvider vbcProvider = new AsterixVirtualBufferCacheProvider(dataset.getDatasetId());
        ILSMIOOperationSchedulerProvider ioSchedulerProvider =
                storageComponentProvider.getIoOperationSchedulerProvider();
        double bloomFilterFalsePositiveRate = mdProvider.getStorageProperties().getBloomFilterFalsePositiveRate();
        ITypeTraits[] typeTraits = getInvListTypeTraits(mdProvider, dataset, recordType, metaType);
        IBinaryComparatorFactory[] cmpFactories =
                getInvListComparatorFactories(mdProvider, dataset, recordType, metaType);
        ITypeTraits[] tokenTypeTraits = getTokenTypeTraits(dataset, index, recordType, metaType);
        IBinaryComparatorFactory[] tokenCmpFactories =
                getTokenComparatorFactories(dataset, index, recordType, metaType);
        IBinaryTokenizerFactory tokenizerFactory = getTokenizerFactory(dataset, index, recordType, metaType);
        return new LSMInvertedIndexLocalResourceFactory(storageManager, typeTraits, cmpFactories, filterTypeTraits,
                filterCmpFactories, secondaryFilterFields, opTrackerFactory, ioOpCallbackFactory,
                pageWriteCallbackFactory, metadataPageManagerFactory, vbcProvider, ioSchedulerProvider,
                mergePolicyFactory, mergePolicyProperties, true, tokenTypeTraits, tokenCmpFactories, tokenizerFactory,
                isPartitioned, invertedIndexFields, secondaryFilterFieldsForNonBulkLoadOps,
                invertedIndexFieldsForNonBulkLoadOps, bloomFilterFalsePositiveRate);
    }

    private static ITypeTraits[] getInvListTypeTraits(MetadataProvider metadataProvider, Dataset dataset,
            ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        ITypeTraits[] primaryTypeTraits = dataset.getPrimaryTypeTraits(metadataProvider, recordType, metaType);
        ITypeTraits[] typeTraits = new ITypeTraits[primaryTypeTraits.length - 1];
        for (int i = 0; i < typeTraits.length; i++) {
            typeTraits[i] = primaryTypeTraits[i];
        }
        return typeTraits;
    }

    private static IBinaryComparatorFactory[] getInvListComparatorFactories(MetadataProvider metadataProvider,
            Dataset dataset, ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        return dataset.getPrimaryComparatorFactories(metadataProvider, recordType, metaType);
    }

    private static ITypeTraits[] getTokenTypeTraits(Dataset dataset, Index index, ARecordType recordType,
            ARecordType metaType) throws AlgebricksException {
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        int numSecondaryKeys = index.getKeyFieldNames().size();
        IndexType indexType = index.getIndexType();
        // Sanity checks.
        if (numPrimaryKeys > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_FOR_DATASET_WITH_COMPOSITE_PRIMARY_INDEX,
                    indexType, RecordUtil.toFullyQualifiedName(dataset.getDataverseName(), dataset.getDatasetName()));
        }
        if (numSecondaryKeys > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_NUM_OF_FIELD, numSecondaryKeys,
                    indexType, 1);
        }
        boolean isPartitioned = indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX;
        ARecordType sourceType;
        List<Integer> keySourceIndicators = index.getKeyFieldSourceIndicators();
        if (keySourceIndicators == null || keySourceIndicators.get(0) == 0) {
            sourceType = recordType;
        } else {
            sourceType = metaType;
        }
        Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(0),
                index.getKeyFieldNames().get(0), sourceType);
        IAType secondaryKeyType = keyTypePair.first;
        int numTokenFields = (!isPartitioned) ? numSecondaryKeys : numSecondaryKeys + 1;
        ITypeTraits[] tokenTypeTraits = new ITypeTraits[numTokenFields];
        tokenTypeTraits[0] = NonTaggedFormatUtil.getTokenTypeTrait(secondaryKeyType);
        if (isPartitioned) {
            // The partitioning field is hardcoded to be a short *without* an Asterix type tag.
            tokenTypeTraits[1] = ShortPointable.TYPE_TRAITS;
        }
        return tokenTypeTraits;
    }

    private static IBinaryComparatorFactory[] getTokenComparatorFactories(Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        int numSecondaryKeys = index.getKeyFieldNames().size();
        IndexType indexType = index.getIndexType();
        // Sanity checks.
        if (numPrimaryKeys > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_FOR_DATASET_WITH_COMPOSITE_PRIMARY_INDEX,
                    indexType, RecordUtil.toFullyQualifiedName(dataset.getDataverseName(), dataset.getDatasetName()));
        }
        if (numSecondaryKeys > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_NUM_OF_FIELD, numSecondaryKeys,
                    indexType, 1);
        }
        boolean isPartitioned = indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX;
        List<Integer> keySourceIndicators = index.getKeyFieldSourceIndicators();
        ARecordType sourceType;
        if (keySourceIndicators == null || keySourceIndicators.get(0) == 0) {
            sourceType = recordType;
        } else {
            sourceType = metaType;
        }
        Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(0),
                index.getKeyFieldNames().get(0), sourceType);
        IAType secondaryKeyType = keyTypePair.first;
        // Comparators and type traits for tokens.
        int numTokenFields = (!isPartitioned) ? numSecondaryKeys : numSecondaryKeys + 1;
        IBinaryComparatorFactory[] tokenComparatorFactories = new IBinaryComparatorFactory[numTokenFields];
        tokenComparatorFactories[0] = NonTaggedFormatUtil.getTokenBinaryComparatorFactory(secondaryKeyType);
        if (isPartitioned) {
            // The partitioning field is hardcoded to be a short *without* an Asterix type tag.
            tokenComparatorFactories[1] = ShortBinaryComparatorFactory.INSTANCE;
        }
        return tokenComparatorFactories;
    }

    private static IBinaryTokenizerFactory getTokenizerFactory(Dataset dataset, Index index, ARecordType recordType,
            ARecordType metaType) throws AlgebricksException {
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        int numSecondaryKeys = index.getKeyFieldNames().size();
        IndexType indexType = index.getIndexType();
        // Sanity checks.
        if (numPrimaryKeys > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_FOR_DATASET_WITH_COMPOSITE_PRIMARY_INDEX,
                    indexType, RecordUtil.toFullyQualifiedName(dataset.getDataverseName(), dataset.getDatasetName()));
        }
        if (numSecondaryKeys > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_NUM_OF_FIELD, numSecondaryKeys,
                    indexType, 1);
        }
        ARecordType sourceType;
        List<Integer> keySourceIndicators = index.getKeyFieldSourceIndicators();
        if (keySourceIndicators == null || keySourceIndicators.get(0) == 0) {
            sourceType = recordType;
        } else {
            sourceType = metaType;
        }
        Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(0),
                index.getKeyFieldNames().get(0), sourceType);
        IAType secondaryKeyType = keyTypePair.first;
        // Set tokenizer factory.
        // TODO: We might want to expose the hashing option at the AQL level,
        // and add the choice to the index metadata.
        return NonTaggedFormatUtil.getBinaryTokenizerFactory(secondaryKeyType.getTypeTag(), indexType,
                index.getGramLength());
    }
}
