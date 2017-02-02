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
import org.apache.asterix.metadata.api.IIndexDataflowHelperFactoryProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.dataflow.PartitionedLSMInvertedIndexDataflowHelperFactory;

public class InvertedIndexDataflowHelperFactoryProvider implements IIndexDataflowHelperFactoryProvider {
    public static final InvertedIndexDataflowHelperFactoryProvider INSTANCE =
            new InvertedIndexDataflowHelperFactoryProvider();

    private InvertedIndexDataflowHelperFactoryProvider() {
    }

    @Override
    public IIndexDataflowHelperFactory getIndexDataflowHelperFactory(MetadataProvider mdProvider, Dataset dataset,
            Index index, ARecordType recordType, ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories) throws AlgebricksException {
        // Get basic info
        List<List<String>> primaryKeys = DatasetUtil.getPartitioningKeys(dataset);
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
        if (!isPartitioned) {
            return new LSMInvertedIndexDataflowHelperFactory(
                    new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), mergePolicyFactory,
                    mergePolicyProperties, dataset.getIndexOperationTrackerFactory(index),
                    storageComponentProvider.getIoOperationSchedulerProvider(),
                    dataset.getIoOperationCallbackFactory(index),
                    mdProvider.getStorageProperties().getBloomFilterFalsePositiveRate(), invertedIndexFields,
                    filterTypeTraits, filterCmpFactories, secondaryFilterFields,
                    secondaryFilterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps,
                    !dataset.getDatasetDetails().isTemp());
        } else {
            return new PartitionedLSMInvertedIndexDataflowHelperFactory(
                    new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), mergePolicyFactory,
                    mergePolicyProperties, dataset.getIndexOperationTrackerFactory(index),
                    storageComponentProvider.getIoOperationSchedulerProvider(),
                    dataset.getIoOperationCallbackFactory(index),
                    mdProvider.getStorageProperties().getBloomFilterFalsePositiveRate(), invertedIndexFields,
                    filterTypeTraits, filterCmpFactories, secondaryFilterFields,
                    secondaryFilterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps,
                    !dataset.getDatasetDetails().isTemp());
        }
    }
}
