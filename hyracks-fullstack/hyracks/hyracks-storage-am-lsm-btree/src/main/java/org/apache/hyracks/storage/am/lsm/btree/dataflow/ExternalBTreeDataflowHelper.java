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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.util.IndexFileNameUtil;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;

public class ExternalBTreeDataflowHelper extends LSMBTreeDataflowHelper {

    private final int version;

    public ExternalBTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, int version, boolean durable)
            throws HyracksDataException {
        super(opDesc, ctx, partition, null, bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory, ioScheduler,
                ioOpCallbackFactory, false, null, null, null, null, durable);
        this.version = version;
    }

    @Override
    public IIndex getIndexInstance() {
        synchronized (lcManager) {
            if (index == null) {
                try {
                    index = lcManager.get(resourceName);
                } catch (HyracksDataException e) {
                    return null;
                }
            }
        }
        return index;
    }

    @Override
    public ITreeIndex createIndexInstance() throws HyracksDataException {
        AbstractTreeIndexOperatorDescriptor treeOpDesc = (AbstractTreeIndexOperatorDescriptor) opDesc;
        FileReference fileRef = IndexFileNameUtil.getIndexAbsoluteFileRef(treeOpDesc, ctx.getTaskAttemptId()
                .getTaskId().getPartition(), ctx.getIOManager());
        return LSMBTreeUtils.createExternalBTree(ctx.getIOManager(), fileRef, opDesc.getStorageManager().getBufferCache(
                ctx), opDesc
                .getStorageManager().getFileMapProvider(ctx), treeOpDesc.getTreeIndexTypeTraits(), treeOpDesc
                .getTreeIndexComparatorFactories(), treeOpDesc.getTreeIndexBloomFilterKeyFields(),
                bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory.getOperationTracker(ctx), ioScheduler,
                ioOpCallbackFactory.createIOOperationCallback(), getVersion(), durable);
    }

    public int getVersion() {
        return version;
    }
}
