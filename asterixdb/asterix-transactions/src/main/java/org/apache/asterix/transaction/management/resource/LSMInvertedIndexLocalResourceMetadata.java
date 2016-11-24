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
package org.apache.asterix.transaction.management.resource;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.context.BaseOperationTracker;
import org.apache.asterix.common.ioopcallbacks.LSMInvertedIndexIOOperationCallbackFactory;
import org.apache.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.Resource;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import org.apache.hyracks.storage.common.file.LocalResource;

public class LSMInvertedIndexLocalResourceMetadata extends Resource {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] invListTypeTraits;
    private final IBinaryComparatorFactory[] invListCmpFactories;
    private final ITypeTraits[] tokenTypeTraits;
    private final IBinaryComparatorFactory[] tokenCmpFactories;
    private final IBinaryTokenizerFactory tokenizerFactory;
    private final boolean isPartitioned;
    private final ILSMMergePolicyFactory mergePolicyFactory;
    private final Map<String, String> mergePolicyProperties;
    private final int[] invertedIndexFields;
    private final int[] filterFieldsForNonBulkLoadOps;
    private final int[] invertedIndexFieldsForNonBulkLoadOps;

    public LSMInvertedIndexLocalResourceMetadata(ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            boolean isPartitioned, int datasetID, int partition, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] invertedIndexFields, int[] filterFields,
            int[] filterFieldsForNonBulkLoadOps, int[] invertedIndexFieldsForNonBulkLoadOps) {
        super(datasetID, partition, filterTypeTraits, filterCmpFactories, filterFields);
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.tokenizerFactory = tokenizerFactory;
        this.isPartitioned = isPartitioned;
        this.mergePolicyFactory = mergePolicyFactory;
        this.mergePolicyProperties = mergePolicyProperties;
        this.invertedIndexFields = invertedIndexFields;
        this.filterFieldsForNonBulkLoadOps = filterFieldsForNonBulkLoadOps;
        this.invertedIndexFieldsForNonBulkLoadOps = invertedIndexFieldsForNonBulkLoadOps;
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider,
            LocalResource resource) throws HyracksDataException {
        IIOManager ioManager = runtimeContextProvider.getIOManager();
        FileReference file = ioManager.getFileRef(resource.getPath(), true);
        List<IODeviceHandle> ioDevices = ioManager.getIODevices();
        int ioDeviceNum = 0;
        for (int i = 0; i < ioDevices.size(); i++) {
            IODeviceHandle device = ioDevices.get(i);
            if (device == file.getDeviceHandle()) {
                ioDeviceNum = i;
                break;
            }
        }
        List<IVirtualBufferCache> virtualBufferCaches = runtimeContextProvider.getDatasetLifecycleManager()
                .getVirtualBufferCaches(datasetId(), ioDeviceNum);
        try {
            if (isPartitioned) {
                return InvertedIndexUtils.createPartitionedLSMInvertedIndex(ioManager, virtualBufferCaches,
                        runtimeContextProvider.getFileMapManager(), invListTypeTraits, invListCmpFactories,
                        tokenTypeTraits, tokenCmpFactories, tokenizerFactory, runtimeContextProvider.getBufferCache(),
                        file.getAbsolutePath(), runtimeContextProvider.getBloomFilterFalsePositiveRate(),
                        mergePolicyFactory.createMergePolicy(mergePolicyProperties,
                                runtimeContextProvider.getDatasetLifecycleManager()),
                        new BaseOperationTracker(datasetId(),
                                runtimeContextProvider.getDatasetLifecycleManager().getDatasetInfo(datasetId())),
                        runtimeContextProvider.getLSMIOScheduler(),
                        LSMInvertedIndexIOOperationCallbackFactory.INSTANCE.createIOOperationCallback(),
                        invertedIndexFields, filterTypeTraits, filterCmpFactories, filterFields,
                        filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps, true);
            } else {
                return InvertedIndexUtils.createLSMInvertedIndex(ioManager, virtualBufferCaches,
                        runtimeContextProvider.getFileMapManager(), invListTypeTraits, invListCmpFactories,
                        tokenTypeTraits, tokenCmpFactories, tokenizerFactory, runtimeContextProvider.getBufferCache(),
                        file.getAbsolutePath(), runtimeContextProvider.getBloomFilterFalsePositiveRate(),
                        mergePolicyFactory.createMergePolicy(mergePolicyProperties,
                                runtimeContextProvider.getDatasetLifecycleManager()),
                        new BaseOperationTracker(datasetId(),
                                runtimeContextProvider.getDatasetLifecycleManager().getDatasetInfo(datasetId())),
                        runtimeContextProvider.getLSMIOScheduler(),
                        LSMInvertedIndexIOOperationCallbackFactory.INSTANCE.createIOOperationCallback(),
                        invertedIndexFields, filterTypeTraits, filterCmpFactories, filterFields,
                        filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps, true);
            }
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
    }
}
