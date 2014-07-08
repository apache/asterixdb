/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow;

import java.util.List;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.dataflow.AbstractLSMIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.PartitionedLSMInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public final class PartitionedLSMInvertedIndexDataflowHelper extends AbstractLSMIndexDataflowHelper {

    private final int[] invertedIndexFields;
    private final int[] filterFieldsForNonBulkLoadOps;
    private final int[] invertedIndexFieldsForNonBulkLoadOps;

    public PartitionedLSMInvertedIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, List<IVirtualBufferCache> virtualBufferCache, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, int[] invertedIndexFields,
            ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            int[] filterFieldsForNonBulkLoadOps, int[] invertedIndexFieldsForNonBulkLoadOps) {
        this(opDesc, ctx, partition, virtualBufferCache, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE, mergePolicy,
                opTrackerFactory, ioScheduler, ioOpCallbackFactory, invertedIndexFields, filterTypeTraits,
                filterCmpFactories, filterFields, filterFieldsForNonBulkLoadOps, invertedIndexFieldsForNonBulkLoadOps);
    }

    public PartitionedLSMInvertedIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, List<IVirtualBufferCache> virtualBufferCaches, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerProvider opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            int[] invertedIndexFields, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] filterFields, int[] filterFieldsForNonBulkLoadOps, int[] invertedIndexFieldsForNonBulkLoadOps) {
        super(opDesc, ctx, partition, virtualBufferCaches, bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory,
                ioScheduler, ioOpCallbackFactory, filterTypeTraits, filterCmpFactories, filterFields);
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
                    invertedIndexFieldsForNonBulkLoadOps);
            return invIndex;
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
    }
}