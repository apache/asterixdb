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
package org.apache.hyracks.storage.am.lsm.invertedindex.dataflow;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LsmResource;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class LSMInvertedIndexLocalResource extends LsmResource {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] tokenTypeTraits;
    private final IBinaryComparatorFactory[] tokenCmpFactories;
    private final IBinaryTokenizerFactory tokenizerFactory;
    private final boolean isPartitioned;
    private final int[] invertedIndexFields;
    private final int[] filterFieldsForNonBulkLoadOps;
    private final int[] invertedIndexFieldsForNonBulkLoadOps;
    private final double bloomFilterFalsePositiveRate;

    public LSMInvertedIndexLocalResource(String path, IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            boolean isPartitioned, int[] invertedIndexFields, int[] filterFieldsForNonBulkLoadOps,
            int[] invertedIndexFieldsForNonBulkLoadOps, double bloomFilterFalsePositiveRate) {
        super(path, storageManager, typeTraits, cmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerProvider, ioOpCallbackFactory, metadataPageManagerFactory, vbcProvider, ioSchedulerProvider,
                mergePolicyFactory, mergePolicyProperties, durable);
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.tokenizerFactory = tokenizerFactory;
        this.isPartitioned = isPartitioned;
        this.invertedIndexFields = invertedIndexFields;
        this.filterFieldsForNonBulkLoadOps = filterFieldsForNonBulkLoadOps;
        this.invertedIndexFieldsForNonBulkLoadOps = invertedIndexFieldsForNonBulkLoadOps;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
    }

    @Override
    public ILSMIndex createInstance(INCServiceContext serviceCtx) throws HyracksDataException {
        IIOManager ioManager = serviceCtx.getIoManager();
        FileReference file = ioManager.resolve(path);
        List<IVirtualBufferCache> virtualBufferCaches = vbcProvider.getVirtualBufferCaches(serviceCtx, file);
        IBufferCache bufferCache = storageManager.getBufferCache(serviceCtx);
        ILSMMergePolicy mergePolicy = mergePolicyFactory.createMergePolicy(mergePolicyProperties, serviceCtx);
        ILSMIOOperationScheduler ioScheduler = ioSchedulerProvider.getIoScheduler(serviceCtx);
        ioOpCallbackFactory.initialize(serviceCtx, this);
        if (isPartitioned) {
            return InvertedIndexUtils.createPartitionedLSMInvertedIndex(ioManager, virtualBufferCaches, typeTraits,
                    cmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory, bufferCache,
                    file.getAbsolutePath(), bloomFilterFalsePositiveRate, mergePolicy,
                    opTrackerProvider.getOperationTracker(serviceCtx, this), ioScheduler, ioOpCallbackFactory,
                    invertedIndexFields, filterTypeTraits, filterCmpFactories, filterFields,
                    filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps, durable,
                    metadataPageManagerFactory, serviceCtx.getTracer());
        } else {
            return InvertedIndexUtils.createLSMInvertedIndex(ioManager, virtualBufferCaches, typeTraits, cmpFactories,
                    tokenTypeTraits, tokenCmpFactories, tokenizerFactory, bufferCache, file.getAbsolutePath(),
                    bloomFilterFalsePositiveRate, mergePolicy, opTrackerProvider.getOperationTracker(serviceCtx, this),
                    ioScheduler, ioOpCallbackFactory, invertedIndexFields, filterTypeTraits, filterCmpFactories,
                    filterFields, filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps, durable,
                    metadataPageManagerFactory, serviceCtx.getTracer());
        }
    }
}
