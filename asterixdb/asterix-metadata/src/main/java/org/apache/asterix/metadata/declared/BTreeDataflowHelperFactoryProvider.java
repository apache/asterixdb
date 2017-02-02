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

import java.util.Map;

import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.metadata.api.IIndexDataflowHelperFactoryProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.ExternalDatasetsRegistry;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeWithBuddyDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

public class BTreeDataflowHelperFactoryProvider implements IIndexDataflowHelperFactoryProvider {

    public static final BTreeDataflowHelperFactoryProvider INSTANCE = new BTreeDataflowHelperFactoryProvider();

    private BTreeDataflowHelperFactoryProvider() {
    }

    public static String externalFileIndexName(Dataset dataset) {
        return dataset.getDatasetName().concat(IndexingConstants.EXTERNAL_FILE_INDEX_NAME_SUFFIX);
    }

    @Override
    public IIndexDataflowHelperFactory getIndexDataflowHelperFactory(MetadataProvider mdProvider, Dataset dataset,
            Index index, ARecordType recordType, ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories) throws AlgebricksException {
        int[] filterFields = IndexUtil.getFilterFields(dataset, index, filterTypeTraits);
        int[] btreeFields = IndexUtil.getBtreeFieldsIfFiltered(dataset, index);
        IStorageComponentProvider storageComponentProvider = mdProvider.getStorageComponentProvider();
        switch (dataset.getDatasetType()) {
            case EXTERNAL:
                return index.getIndexName().equals(externalFileIndexName(dataset))
                        ? new ExternalBTreeDataflowHelperFactory(mergePolicyFactory, mergePolicyProperties,
                                dataset.getIndexOperationTrackerFactory(index),
                                storageComponentProvider.getIoOperationSchedulerProvider(),
                                dataset.getIoOperationCallbackFactory(index),
                                mdProvider.getStorageProperties().getBloomFilterFalsePositiveRate(),
                                ExternalDatasetsRegistry.INSTANCE.getAndLockDatasetVersion(dataset, mdProvider),
                                !dataset.getDatasetDetails().isTemp())
                        : new ExternalBTreeWithBuddyDataflowHelperFactory(mergePolicyFactory, mergePolicyProperties,
                                dataset.getIndexOperationTrackerFactory(index),
                                storageComponentProvider.getIoOperationSchedulerProvider(),
                                dataset.getIoOperationCallbackFactory(index),
                                mdProvider.getStorageProperties().getBloomFilterFalsePositiveRate(),
                                new int[] { index.getKeyFieldNames().size() },
                                ExternalDatasetsRegistry.INSTANCE.getAndLockDatasetVersion(dataset, mdProvider),
                                !dataset.getDatasetDetails().isTemp());
            case INTERNAL:
                return new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                        mergePolicyFactory, mergePolicyProperties, dataset.getIndexOperationTrackerFactory(index),
                        storageComponentProvider.getIoOperationSchedulerProvider(),
                        dataset.getIoOperationCallbackFactory(index),
                        mdProvider.getStorageProperties().getBloomFilterFalsePositiveRate(), index.isPrimaryIndex(),
                        filterTypeTraits, filterCmpFactories, btreeFields, filterFields,
                        !dataset.getDatasetDetails().isTemp());
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_DATASET_TYPE,
                        dataset.getDatasetType().toString());
        }
    }
}
