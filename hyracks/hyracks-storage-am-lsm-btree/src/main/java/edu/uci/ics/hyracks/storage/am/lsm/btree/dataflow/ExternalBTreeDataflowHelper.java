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
package edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow;

import java.util.List;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;

public class ExternalBTreeDataflowHelper extends LSMBTreeDataflowHelper {

    private int version;

    public ExternalBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, boolean needKeyDupCheck, int version) {
        super(opDesc, ctx, partition, null, bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory, ioScheduler,
                ioOpCallbackFactory, false, null, null, null, null);
        this.version = version;
    }

    public ExternalBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            List<IVirtualBufferCache> virtualBufferCaches, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, boolean needKeyDupCheck, int version) {
        this(opDesc, ctx, partition, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE, mergePolicy, opTrackerFactory,
                ioScheduler, ioOpCallbackFactory, needKeyDupCheck, version);
    }

    @Override
    public IIndex getIndexInstance() {
        if (index != null)
            return index;
        synchronized (lcManager) {
            long resourceID;
            try {
                resourceID = getResourceID();
            } catch (HyracksDataException e) {
                return null;
            }
            try {
                index = lcManager.getIndex(resourceID);
            } catch (HyracksDataException e) {
                return null;
            }
        }
        return index;
    }

    @Override
    public ITreeIndex createIndexInstance() throws HyracksDataException {
        AbstractTreeIndexOperatorDescriptor treeOpDesc = (AbstractTreeIndexOperatorDescriptor) opDesc;
        return LSMBTreeUtils.createExternalBTree(file, opDesc.getStorageManager().getBufferCache(ctx), opDesc
                .getStorageManager().getFileMapProvider(ctx), treeOpDesc.getTreeIndexTypeTraits(), treeOpDesc
                .getTreeIndexComparatorFactories(), treeOpDesc.getTreeIndexBloomFilterKeyFields(),
                bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory.getOperationTracker(ctx), ioScheduler,
                ioOpCallbackFactory.createIOOperationCallback(), getVersion());
    }

    public int getVersion() {
        return version;
    }
}
