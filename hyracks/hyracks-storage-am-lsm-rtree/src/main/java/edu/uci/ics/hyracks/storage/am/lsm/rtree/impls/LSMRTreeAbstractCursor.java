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

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BloomFilterAwareBTreePointSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public abstract class LSMRTreeAbstractCursor implements ITreeIndexCursor {

    protected boolean open;
    protected RTreeSearchCursor[] rtreeCursors;
    protected ITreeIndexCursor[] btreeCursors;
    protected ITreeIndexAccessor[] rtreeAccessors;
    protected ITreeIndexAccessor[] btreeAccessors;
    private MultiComparator btreeCmp;
    protected int numberOfTrees;
    protected SearchPredicate rtreeSearchPredicate;
    protected RangePredicate btreeRangePredicate;
    protected ITupleReference frameTuple;
    protected boolean includeMutableComponent;
    protected ILSMHarness lsmHarness;
    protected boolean foundNext;
    protected final ILSMIndexOperationContext opCtx;

    protected List<ILSMComponent> operationalComponents;

    public LSMRTreeAbstractCursor(ILSMIndexOperationContext opCtx) {
        super();
        this.opCtx = opCtx;
    }

    public RTreeSearchCursor getCursor(int cursorIndex) {
        return rtreeCursors[cursorIndex];
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMRTreeCursorInitialState lsmInitialState = (LSMRTreeCursorInitialState) initialState;
        btreeCmp = lsmInitialState.getBTreeCmp();

        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();
        numberOfTrees = operationalComponents.size();

        rtreeCursors = new RTreeSearchCursor[numberOfTrees];
        btreeCursors = new ITreeIndexCursor[numberOfTrees];
        rtreeAccessors = new ITreeIndexAccessor[numberOfTrees];
        btreeAccessors = new ITreeIndexAccessor[numberOfTrees];

        includeMutableComponent = false;
        for (int i = 0; i < numberOfTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            RTree rtree;
            BTree btree;
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
                // No need for a bloom filter for the in-memory BTree.
                btreeCursors[i] = new BTreeRangeSearchCursor((IBTreeLeafFrame) lsmInitialState
                        .getBTreeLeafFrameFactory().createFrame(), false);
                rtree = (RTree) ((LSMRTreeMemoryComponent) component).getRTree();
                btree = (BTree) ((LSMRTreeMemoryComponent) component).getBTree();
            } else {
                btreeCursors[i] = new BloomFilterAwareBTreePointSearchCursor((IBTreeLeafFrame) lsmInitialState
                        .getBTreeLeafFrameFactory().createFrame(), false,
                        ((LSMRTreeDiskComponent) operationalComponents.get(i)).getBloomFilter());
                rtree = (RTree) ((LSMRTreeDiskComponent) component).getRTree();
                btree = (BTree) ((LSMRTreeDiskComponent) component).getBTree();
            }
            rtreeCursors[i] = new RTreeSearchCursor((IRTreeInteriorFrame) lsmInitialState
                    .getRTreeInteriorFrameFactory().createFrame(), (IRTreeLeafFrame) lsmInitialState
                    .getRTreeLeafFrameFactory().createFrame());
            rtreeAccessors[i] = rtree.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            btreeAccessors[i] = btree.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        }

        rtreeSearchPredicate = (SearchPredicate) searchPred;
        btreeRangePredicate = new RangePredicate(null, null, true, true, btreeCmp, btreeCmp);

        open = true;
    }

    @Override
    public ICachedPage getPage() {
        // do nothing
        return null;
    }

    @Override
    public void close() throws HyracksDataException {
        if (!open) {
            return;
        }

        try {
            if (rtreeCursors != null && btreeCursors != null) {
                for (int i = 0; i < numberOfTrees; i++) {
                    rtreeCursors[i].close();
                    btreeCursors[i].close();
                }
            }
            rtreeCursors = null;
            btreeCursors = null;
        } finally {
            lsmHarness.endSearch(opCtx);
        }

        open = false;
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        // do nothing
    }

    @Override
    public void setFileId(int fileId) {
        // do nothing
    }

    @Override
    public ITupleReference getTuple() {
        return frameTuple;
    }

    @Override
    public boolean exclusiveLatchNodes() {
        return false;
    }

}