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
package org.apache.hyracks.storage.am.lsm.btree.impls;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.util.trace.ITracer;

public class ExternalBTreeWithBuddyOpContext extends AbstractLSMIndexOperationContext {
    private MultiComparator bTreeCmp;
    private MultiComparator buddyBTreeCmp;
    private final int targetIndexVersion;
    private LSMBTreeWithBuddyCursorInitialState searchInitialState;

    public ExternalBTreeWithBuddyOpContext(ILSMIndex index, IBinaryComparatorFactory[] btreeCmpFactories,
            IBinaryComparatorFactory[] buddyBtreeCmpFactories, ISearchOperationCallback searchCallback,
            int targetIndexVersion, ILSMHarness lsmHarness, ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, ITreeIndexFrameFactory buddyBtreeLeafFrameFactory,
            ITracer tracer) {
        super(index, null, null, null, searchCallback, null, tracer);
        this.targetIndexVersion = targetIndexVersion;
        this.bTreeCmp = MultiComparator.create(btreeCmpFactories);
        this.buddyBTreeCmp = MultiComparator.create(buddyBtreeCmpFactories);
        searchInitialState = new LSMBTreeWithBuddyCursorInitialState(btreeInteriorFrameFactory, btreeLeafFrameFactory,
                buddyBtreeLeafFrameFactory, lsmHarness, MultiComparator.create(btreeCmpFactories),
                MultiComparator.create(buddyBtreeCmpFactories), NoOpOperationCallback.INSTANCE, null);
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        // Do nothing. this should never be called for disk only indexes
    }

    public MultiComparator getBTreeMultiComparator() {
        return bTreeCmp;
    }

    public MultiComparator getBuddyBTreeMultiComparator() {
        return buddyBTreeCmp;
    }

    // This should never be needed for disk only indexes
    @Override
    public IExtendedModificationOperationCallback getModificationCallback() {
        return null;
    }

    public int getTargetIndexVersion() {
        return targetIndexVersion;
    }

    public LSMBTreeWithBuddyCursorInitialState getSearchInitialState() {
        return searchInitialState;
    }

    @Override
    public void destroy() throws HyracksDataException {
        // No Op
    }
}
