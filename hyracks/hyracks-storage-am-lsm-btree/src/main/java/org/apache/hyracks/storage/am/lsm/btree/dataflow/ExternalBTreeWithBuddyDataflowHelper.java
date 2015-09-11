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
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import org.apache.hyracks.storage.am.lsm.common.dataflow.AbstractLSMIndexDataflowHelper;

public class ExternalBTreeWithBuddyDataflowHelper extends AbstractLSMIndexDataflowHelper {

    private final int[] buddyBtreeFields;
    private final int version;

    public ExternalBTreeWithBuddyDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, ILSMMergePolicy mergePolicy, ILSMOperationTrackerProvider opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            int[] buddyBtreeFields, int version, boolean durable) {
        super(opDesc, ctx, partition, null, mergePolicy, opTrackerFactory, ioScheduler, ioOpCallbackFactory, null,
                null, null, durable);
        this.buddyBtreeFields = buddyBtreeFields;
        this.version = version;
    }

    public ExternalBTreeWithBuddyDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerProvider opTrackerFactory, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, int[] buddyBtreeFields, int version,
            boolean durable) {
        super(opDesc, ctx, partition, null, bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory, ioScheduler,
                ioOpCallbackFactory, null, null, null, durable);
        this.buddyBtreeFields = buddyBtreeFields;
        this.version = version;
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
    protected IIndex createIndexInstance() throws HyracksDataException {
        AbstractTreeIndexOperatorDescriptor treeOpDesc = (AbstractTreeIndexOperatorDescriptor) opDesc;
        return LSMBTreeUtils.createExternalBTreeWithBuddy(file, opDesc.getStorageManager().getBufferCache(ctx), opDesc
                .getStorageManager().getFileMapProvider(ctx), treeOpDesc.getTreeIndexTypeTraits(), treeOpDesc
                .getTreeIndexComparatorFactories(), bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory
                .getOperationTracker(ctx), ioScheduler, ioOpCallbackFactory.createIOOperationCallback(),
                buddyBtreeFields, version, durable);
    }

    public int getTargetVersion() {
        return version;
    }

}
