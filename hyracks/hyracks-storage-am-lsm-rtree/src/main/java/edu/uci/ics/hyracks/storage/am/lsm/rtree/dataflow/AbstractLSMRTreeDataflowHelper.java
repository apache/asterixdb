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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.dataflow;

import java.util.List;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.dataflow.AbstractLSMIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class AbstractLSMRTreeDataflowHelper extends AbstractLSMIndexDataflowHelper {

    protected final IBinaryComparatorFactory[] btreeComparatorFactories;
    protected final IPrimitiveValueProviderFactory[] valueProviderFactories;
    protected final RTreePolicyType rtreePolicyType;
    protected final ILinearizeComparatorFactory linearizeCmpFactory;

    public AbstractLSMRTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            List<IVirtualBufferCache> virtualBufferCaches, IBinaryComparatorFactory[] btreeComparatorFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerProvider opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILinearizeComparatorFactory linearizeCmpFactory) {
        this(opDesc, ctx, partition, virtualBufferCaches, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE,
                btreeComparatorFactories, valueProviderFactories, rtreePolicyType, mergePolicy, opTrackerFactory,
                ioScheduler, ioOpCallbackFactory, linearizeCmpFactory);
    }

    public AbstractLSMRTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            List<IVirtualBufferCache> virtualBufferCaches, double bloomFilterFalsePositiveRate,
            IBinaryComparatorFactory[] btreeComparatorFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerProvider opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILinearizeComparatorFactory linearizeCmpFactory) {
        super(opDesc, ctx, partition, virtualBufferCaches, bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory,
                ioScheduler, ioOpCallbackFactory);
        this.btreeComparatorFactories = btreeComparatorFactories;
        this.valueProviderFactories = valueProviderFactories;
        this.rtreePolicyType = rtreePolicyType;
        this.linearizeCmpFactory = linearizeCmpFactory;
    }

    @Override
    public ITreeIndex createIndexInstance() throws HyracksDataException {
        AbstractTreeIndexOperatorDescriptor treeOpDesc = (AbstractTreeIndexOperatorDescriptor) opDesc;
        return createLSMTree(virtualBufferCaches, file, opDesc.getStorageManager().getBufferCache(ctx), opDesc
                .getStorageManager().getFileMapProvider(ctx), treeOpDesc.getTreeIndexTypeTraits(),
                treeOpDesc.getTreeIndexComparatorFactories(), btreeComparatorFactories,
                opTrackerFactory.getOperationTracker(ctx), valueProviderFactories, rtreePolicyType, linearizeCmpFactory);

    }

    protected abstract ITreeIndex createLSMTree(List<IVirtualBufferCache> virtualBufferCaches, FileReference file,
            IBufferCache diskBufferCache, IFileMapProvider diskFileMapProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            ILSMOperationTracker opTracker, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, ILinearizeComparatorFactory linearizeCmpFactory)
            throws HyracksDataException;
}
