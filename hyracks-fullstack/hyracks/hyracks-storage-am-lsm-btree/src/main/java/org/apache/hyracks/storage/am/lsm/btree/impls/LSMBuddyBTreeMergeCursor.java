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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;

public class LSMBuddyBTreeMergeCursor extends LSMIndexSearchCursor {

    public LSMBuddyBTreeMergeCursor(ILSMIndexOperationContext opCtx, IIndexCursorStats stats) {
        super(opCtx, true, stats);
    }

    @Override
    protected boolean isDeleted(PriorityQueueElement checkElement) throws HyracksDataException {
        return false;
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMBTreeWithBuddyCursorInitialState lsmInitialState = (LSMBTreeWithBuddyCursorInitialState) initialState;
        cmp = lsmInitialState.getBuddyBTreeCmp();
        operationalComponents = lsmInitialState.getOperationalComponents();
        // We intentionally set the lsmHarness to null so that we don't call
        // lsmHarness.endSearch() because we already do that when we merge
        // actual index.
        lsmHarness = null;
        int numBTrees = operationalComponents.size();
        rangeCursors = new IIndexCursor[numBTrees];
        RangePredicate btreePredicate = new RangePredicate(null, null, true, true, cmp, cmp);
        IIndexAccessor[] btreeAccessors = new ITreeIndexAccessor[numBTrees];
        for (int i = 0; i < numBTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            BTree buddyBtree = ((LSMBTreeWithBuddyDiskComponent) component).getBuddyIndex();
            btreeAccessors[i] = buddyBtree.createAccessor(iap);
            rangeCursors[i] = btreeAccessors[i].createSearchCursor(false);
        }
        IndexCursorUtils.open(btreeAccessors, rangeCursors, btreePredicate);
        try {
            setPriorityQueueComparator();
            initPriorityQueue();
        } catch (Throwable th) { // NOSONAR: Must catch all failures
            IndexCursorUtils.close(rangeCursors, th);
            throw HyracksDataException.create(th);
        }
    }
}
