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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;

public class LSMRTreeDeletedKeysBTreeMergeCursor extends LSMIndexSearchCursor {

    public LSMRTreeDeletedKeysBTreeMergeCursor(ILSMIndexOperationContext opCtx) {
        super(opCtx, true);
    }

    @Override
    protected boolean isDeleted(PriorityQueueElement checkElement) throws HyracksDataException, IndexException {
        return false;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException,
            IndexException {
        LSMRTreeCursorInitialState lsmInitialState = (LSMRTreeCursorInitialState) initialState;
        cmp = lsmInitialState.getBTreeCmp();
        operationalComponents = lsmInitialState.getOperationalComponents();
        // We intentionally set the lsmHarness to null so that we don't call lsmHarness.endSearch() because we already do that when we merge r-trees.
        lsmHarness = null;
        int numBTrees = operationalComponents.size();
        rangeCursors = new IIndexCursor[numBTrees];

        RangePredicate btreePredicate = new RangePredicate(null, null, true, true, cmp, cmp);
        IIndexAccessor[] btreeAccessors = new ITreeIndexAccessor[numBTrees];
        for (int i = 0; i < numBTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getBTreeLeafFrameFactory().createFrame();
            rangeCursors[i] = new BTreeRangeSearchCursor(leafFrame, false);
            BTree btree = (BTree) ((LSMRTreeDiskComponent) component).getBTree();
            btreeAccessors[i] = btree.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            btreeAccessors[i].search(rangeCursors[i], btreePredicate);
        }
        setPriorityQueueComparator();
        initPriorityQueue();
    }
}
