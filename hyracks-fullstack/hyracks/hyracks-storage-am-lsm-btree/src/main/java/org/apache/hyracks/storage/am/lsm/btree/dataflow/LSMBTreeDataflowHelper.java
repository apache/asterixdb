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

package org.apache.hyracks.storage.am.lsm.btree.dataflow;

import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.util.IndexFileNameUtil;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.dataflow.AbstractLSMIndexDataflowHelper;

public class LSMBTreeDataflowHelper extends AbstractLSMIndexDataflowHelper {

    private final boolean needKeyDupCheck;
    private final int[] btreeFields;

    public LSMBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            List<IVirtualBufferCache> virtualBufferCaches, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, boolean needKeyDupCheck,
            ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories, int[] btreeFields,
            int[] filterFields, boolean durable) throws HyracksDataException {
        this(opDesc, ctx, partition, virtualBufferCaches, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE, mergePolicy,
                opTrackerFactory, ioScheduler, ioOpCallbackFactory, needKeyDupCheck, filterTypeTraits,
                filterCmpFactories, btreeFields, filterFields, durable);
    }

    public LSMBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            List<IVirtualBufferCache> virtualBufferCaches, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerProvider opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            boolean needKeyDupCheck, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] btreeFields, int[] filterFields, boolean durable) throws HyracksDataException {
        super(opDesc, ctx, partition, virtualBufferCaches, bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory,
                ioScheduler, ioOpCallbackFactory, filterTypeTraits, filterCmpFactories, filterFields, durable);
        this.needKeyDupCheck = needKeyDupCheck;
        this.btreeFields = btreeFields;
    }

    @Override
    public ITreeIndex createIndexInstance() throws HyracksDataException {
        AbstractTreeIndexOperatorDescriptor treeOpDesc = (AbstractTreeIndexOperatorDescriptor) opDesc;
        FileReference fileRef = IndexFileNameUtil.getIndexAbsoluteFileRef(treeOpDesc, ctx.getTaskAttemptId()
                .getTaskId().getPartition(), ctx.getIOManager());
        return LSMBTreeUtils.createLSMTree(ctx.getIOManager(), virtualBufferCaches, fileRef, opDesc.getStorageManager()
                .getBufferCache(ctx),
                opDesc.getStorageManager().getFileMapProvider(ctx), treeOpDesc.getTreeIndexTypeTraits(),
                treeOpDesc.getTreeIndexComparatorFactories(), treeOpDesc.getTreeIndexBloomFilterKeyFields(),
                bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory.getOperationTracker(ctx), ioScheduler,
                ioOpCallbackFactory.createIOOperationCallback(), needKeyDupCheck, filterTypeTraits, filterCmpFactories,
                btreeFields, filterFields, durable);
    }
}
