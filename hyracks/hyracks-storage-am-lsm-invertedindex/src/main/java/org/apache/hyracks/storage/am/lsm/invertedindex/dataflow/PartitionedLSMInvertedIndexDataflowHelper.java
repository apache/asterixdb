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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.dataflow.AbstractLSMIndexDataflowHelper;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.PartitionedLSMInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public final class PartitionedLSMInvertedIndexDataflowHelper extends AbstractLSMIndexDataflowHelper {

    private final int[] invertedIndexFields;
    private final int[] filterFieldsForNonBulkLoadOps;
    private final int[] invertedIndexFieldsForNonBulkLoadOps;

    public PartitionedLSMInvertedIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, List<IVirtualBufferCache> virtualBufferCache, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, int[] invertedIndexFields,
            ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            int[] filterFieldsForNonBulkLoadOps, int[] invertedIndexFieldsForNonBulkLoadOps, boolean durable) {
        this(opDesc, ctx, partition, virtualBufferCache, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE, mergePolicy,
                opTrackerFactory, ioScheduler, ioOpCallbackFactory, invertedIndexFields, filterTypeTraits,
                filterCmpFactories, filterFields, filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps,
                durable);
    }

    public PartitionedLSMInvertedIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, List<IVirtualBufferCache> virtualBufferCaches, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerProvider opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            int[] invertedIndexFields, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] filterFields, int[] filterFieldsForNonBulkLoadOps, int[] invertedIndexFieldsForNonBulkLoadOps,
            boolean durable) {
        super(opDesc, ctx, partition, virtualBufferCaches, bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory,
                ioScheduler, ioOpCallbackFactory, filterTypeTraits, filterCmpFactories, filterFields, durable);
        this.invertedIndexFields = invertedIndexFields;
        this.filterFieldsForNonBulkLoadOps = filterFieldsForNonBulkLoadOps;
        this.invertedIndexFieldsForNonBulkLoadOps = invertedIndexFieldsForNonBulkLoadOps;
    }

    @Override
    public IIndex createIndexInstance() throws HyracksDataException {
        IInvertedIndexOperatorDescriptor invIndexOpDesc = (IInvertedIndexOperatorDescriptor) opDesc;
        try {
            IBufferCache diskBufferCache = opDesc.getStorageManager().getBufferCache(ctx);
            IFileMapProvider diskFileMapProvider = opDesc.getStorageManager().getFileMapProvider(ctx);
            PartitionedLSMInvertedIndex invIndex = InvertedIndexUtils.createPartitionedLSMInvertedIndex(
                    virtualBufferCaches, diskFileMapProvider, invIndexOpDesc.getInvListsTypeTraits(),
                    invIndexOpDesc.getInvListsComparatorFactories(), invIndexOpDesc.getTokenTypeTraits(),
                    invIndexOpDesc.getTokenComparatorFactories(), invIndexOpDesc.getTokenizerFactory(),
                    diskBufferCache, file.getFile().getPath(), bloomFilterFalsePositiveRate, mergePolicy,
                    opTrackerFactory.getOperationTracker(ctx), ioScheduler,
                    ioOpCallbackFactory.createIOOperationCallback(), invertedIndexFields, filterTypeTraits,
                    filterCmpFactories, filterFields, filterFieldsForNonBulkLoadOps,
                    invertedIndexFieldsForNonBulkLoadOps, durable);
            return invIndex;
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
    }
}