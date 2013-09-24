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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;

public class LSMInvertedIndexDeletedKeysBTreeMergeCursor extends LSMIndexSearchCursor {

    public LSMInvertedIndexDeletedKeysBTreeMergeCursor(ILSMIndexOperationContext opCtx) {
        super(opCtx, true);
    }

    @Override
    protected boolean isDeleted(PriorityQueueElement checkElement) throws HyracksDataException, IndexException {
        return false;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException,
            IndexException {
        LSMInvertedIndexRangeSearchCursorInitialState lsmInitialState = (LSMInvertedIndexRangeSearchCursorInitialState) initialState;
        cmp = lsmInitialState.getOriginalKeyComparator();
        operationalComponents = lsmInitialState.getOperationalComponents();
        // We intentionally set the lsmHarness to null so that we don't call lsmHarness.endSearch() because we already do that when we merge the inverted indexes.
        lsmHarness = null;
        int numBTrees = operationalComponents.size();
        rangeCursors = new IIndexCursor[numBTrees];

        MultiComparator keyCmp = lsmInitialState.getKeyComparator();
        RangePredicate btreePredicate = new RangePredicate(null, null, true, true, keyCmp, keyCmp);
        ArrayList<IIndexAccessor> btreeAccessors = lsmInitialState.getDeletedKeysBTreeAccessors();
        for (int i = 0; i < numBTrees; i++) {
            rangeCursors[i] = btreeAccessors.get(i).createSearchCursor();
            btreeAccessors.get(i).search(rangeCursors[i], btreePredicate);
        }
        setPriorityQueueComparator();
        initPriorityQueue();
    }
}
