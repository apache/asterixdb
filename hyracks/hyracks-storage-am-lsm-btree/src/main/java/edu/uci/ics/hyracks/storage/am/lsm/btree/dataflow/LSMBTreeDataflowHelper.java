/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.dataflow.AbstractLSMIndexDataflowHelper;

public class LSMBTreeDataflowHelper extends AbstractLSMIndexDataflowHelper {

    public LSMBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            IVirtualBufferCache virtualBufferCache, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerFactory opTrackerFactory, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackProvider ioOpCallbackProvider) {
        this(opDesc, ctx, partition, virtualBufferCache, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE, mergePolicy,
                opTrackerFactory, ioScheduler, ioOpCallbackProvider);
    }

    public LSMBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            IVirtualBufferCache virtualBufferCache, double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerFactory opTrackerFactory, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackProvider ioOpCallbackProvider) {
        super(opDesc, ctx, partition, virtualBufferCache, bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory,
                ioScheduler, ioOpCallbackProvider);
    }

    @Override
    public ITreeIndex createIndexInstance() throws HyracksDataException {
        AbstractTreeIndexOperatorDescriptor treeOpDesc = (AbstractTreeIndexOperatorDescriptor) opDesc;
        return LSMBTreeUtils.createLSMTree(virtualBufferCache, ctx.getIOManager(), file, opDesc.getStorageManager()
                .getBufferCache(ctx), opDesc.getStorageManager().getFileMapProvider(ctx), treeOpDesc
                .getTreeIndexTypeTraits(), treeOpDesc.getTreeIndexComparatorFactories(), treeOpDesc
                .getTreeIndexBloomFilterKeyFields(), bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory,
                ioScheduler, ioOpCallbackProvider, opDesc.getFileSplitProvider().getFileSplits()[partition]
                        .getIODeviceId());
    }
}
