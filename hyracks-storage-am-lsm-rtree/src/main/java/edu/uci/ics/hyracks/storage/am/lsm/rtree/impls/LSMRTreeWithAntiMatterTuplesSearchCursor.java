/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.util.Comparator;
import java.util.PriorityQueue;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;

public class LSMRTreeWithAntiMatterTuplesSearchCursor extends LSMTreeSearchCursor {
    private RTreeSearchCursor memRTreeCursor;
    private BTreeRangeSearchCursor memBTreeCursor;
    private RangePredicate btreeRangePredicate;
    private ITreeIndexAccessor memBTreeAccessor;
    protected PriorityQueueHilbertComparator pqCmp;
    private boolean foundNext;
    private ITupleReference frameTuple;
    private int[] comparatorFields;
    private MultiComparator btreeCmp;

    public void initPriorityQueue() throws HyracksDataException {
        int pqInitSize = (rangeCursors.length > 0) ? rangeCursors.length : 1;
        outputPriorityQueue = new PriorityQueue<PriorityQueueElement>(pqInitSize, pqCmp);
        for (int i = 0; i < rangeCursors.length; i++) {
            PriorityQueueElement element;
            if (rangeCursors[i].hasNext()) {
                rangeCursors[i].next();
                element = new PriorityQueueElement(rangeCursors[i].getTuple(), i);
                outputPriorityQueue.offer(element);
            }
        }
        checkPriorityQueue();
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMRTreeCursorInitialState lsmInitialState = (LSMRTreeCursorInitialState) initialState;
        cmp = lsmInitialState.getHilbertCmp();
        btreeCmp = lsmInitialState.getBTreeCmp();
        int numDiskRTrees = lsmInitialState.getNumberOfTrees();
        rangeCursors = new RTreeSearchCursor[numDiskRTrees];
        for (int i = 0; i < numDiskRTrees; i++) {
            rangeCursors[i] = new RTreeSearchCursor((IRTreeInteriorFrame) lsmInitialState
                    .getRTreeInteriorFrameFactory().createFrame(), (IRTreeLeafFrame) lsmInitialState
                    .getRTreeLeafFrameFactory().createFrame());
        }
        includeMemComponent = lsmInitialState.getIncludeMemComponent();
        if (includeMemComponent) {
            memRTreeCursor = new RTreeSearchCursor((IRTreeInteriorFrame) lsmInitialState.getRTreeInteriorFrameFactory()
                    .createFrame(), (IRTreeLeafFrame) lsmInitialState.getRTreeLeafFrameFactory().createFrame());
            memBTreeCursor = new BTreeRangeSearchCursor((IBTreeLeafFrame) lsmInitialState.getBTreeLeafFrameFactory()
                    .createFrame(), false);
            memBTreeAccessor = lsmInitialState.getBTreeAccessors()[0];
            btreeRangePredicate = new RangePredicate(null, null, true, true, btreeCmp, btreeCmp);
        }
        searcherfRefCount = lsmInitialState.getSearcherRefCount();
        lsmHarness = lsmInitialState.getLSMHarness();
        comparatorFields = lsmInitialState.getComparatorFields();
        setPriorityQueueComparator();
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (includeMemComponent) {
            if (foundNext) {
                return true;
            }
            while (memRTreeCursor.hasNext()) {
                memRTreeCursor.next();
                ITupleReference memRTreeTuple = memRTreeCursor.getTuple();
                if (searchMemBTree(memRTreeTuple)) {
                    foundNext = true;
                    frameTuple = memRTreeTuple;
                    return true;
                }
            }
            while (super.hasNext()) {
                super.next();
                ITupleReference diskRTreeTuple = super.getTuple();
                if (searchMemBTree(diskRTreeTuple)) {
                    foundNext = true;
                    frameTuple = diskRTreeTuple;
                    return true;
                }
            }
        } else {
            return super.hasNext();
        }

        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        if (includeMemComponent) {
            foundNext = false;
        } else {
            super.next();
        }

    }

    @Override
    public ITupleReference getTuple() {
        if (includeMemComponent) {
            return frameTuple;
        } else {
            return super.getTuple();
        }

    }

    @Override
    public void close() throws HyracksDataException {
        if (includeMemComponent) {
            memRTreeCursor.close();
            memBTreeCursor.close();
        }
        super.close();
    }

    public ITreeIndexCursor getMemRTreeCursor() {
        return memRTreeCursor;
    }

    @Override
    protected int compare(MultiComparator cmp, ITupleReference tupleA, ITupleReference tupleB) {
        return cmp.selectiveFieldCompare(tupleA, tupleB, comparatorFields);
    }

    private boolean searchMemBTree(ITupleReference tuple) throws HyracksDataException {
        try {
            btreeRangePredicate.setHighKey(tuple, true);
            btreeRangePredicate.setLowKey(tuple, true);
            memBTreeAccessor.search(memBTreeCursor, btreeRangePredicate);
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
        try {
            if (memBTreeCursor.hasNext()) {
                return false;
            } else {
                return true;
            }
        } finally {
            memBTreeCursor.close();
        }
    }

    @Override
    protected void setPriorityQueueComparator() {
        if (pqCmp == null || cmp != pqCmp.getMultiComparator()) {
            pqCmp = new PriorityQueueHilbertComparator(cmp, comparatorFields);
        }
    }

    public class PriorityQueueHilbertComparator implements Comparator<PriorityQueueElement> {

        private final MultiComparator cmp;
        private final int[] comparatorFields;

        public PriorityQueueHilbertComparator(MultiComparator cmp, int[] comparatorFields) {
            this.cmp = cmp;
            this.comparatorFields = comparatorFields;
        }

        @Override
        public int compare(PriorityQueueElement elementA, PriorityQueueElement elementB) {
            int result = cmp.selectiveFieldCompare(elementA.getTuple(), elementB.getTuple(), comparatorFields);
            if (result != 0) {
                return result;
            }
            if (elementA.getCursorIndex() > elementB.getCursorIndex()) {
                return 1;
            } else {
                return -1;
            }
        }

        public MultiComparator getMultiComparator() {
            return cmp;
        }
    }

}
