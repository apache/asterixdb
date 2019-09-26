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

package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;

public class LSMInvertedIndexDeletedKeysBTreeMergeCursor extends LSMIndexSearchCursor {

    public LSMInvertedIndexDeletedKeysBTreeMergeCursor(ILSMIndexOperationContext opCtx, IIndexCursorStats stats) {
        super(opCtx, true, stats);
    }

    @Override
    protected boolean isDeleted(PriorityQueueElement checkElement) throws HyracksDataException {
        return false;
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMInvertedIndexRangeSearchCursorInitialState lsmInitialState =
                (LSMInvertedIndexRangeSearchCursorInitialState) initialState;
        cmp = lsmInitialState.getOriginalKeyComparator();
        operationalComponents = lsmInitialState.getOperationalComponents();
        // We intentionally set the lsmHarness to null so that we don't call lsmHarness.endSearch() because we already
        // do that when we merge the inverted indexes.
        lsmHarness = null;
        int numBTrees = operationalComponents.size();
        rangeCursors = new IIndexCursor[numBTrees];

        MultiComparator keyCmp = lsmInitialState.getKeyComparator();
        RangePredicate btreePredicate = new RangePredicate(null, null, true, true, keyCmp, keyCmp);

        IIndexAccessor[] btreeAccessors = new IIndexAccessor[operationalComponents.size()];
        for (int i = 0; i < numBTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            if (component.getType() == LSMComponentType.MEMORY) {
                btreeAccessors[i] = ((LSMInvertedIndexMemoryComponent) component).getBuddyIndex().createAccessor(iap);
            } else {
                btreeAccessors[i] = ((LSMInvertedIndexDiskComponent) component).getBuddyIndex().createAccessor(iap);
            }
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
