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

package edu.uci.ics.hyracks.storage.am.lsm.btree.impls;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMBTreeRangeSearchCursor implements ITreeIndexCursor {
    private BTreeRangeSearchCursor[] rangeCursors;
    private PriorityQueue<PriorityQueueElement> outputPriorityQueue;
    private MultiComparator cmp;
    private PriorityQueueComparator pqCmp;
    private PriorityQueueElement outputElement;
    private PriorityQueueElement reusedElement;
    private boolean needPush;
    private boolean includeMemComponent;
    private AtomicInteger searcherfRefCount;
    private LSMHarness lsmHarness;
    private ISearchOperationCallback searchCallback;

    public LSMBTreeRangeSearchCursor() {
        outputElement = null;
        needPush = false;
    }

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

    public BTreeRangeSearchCursor getCursor(int cursorIndex) {
        return rangeCursors[cursorIndex];
    }

    @Override
    public void reset() {
        outputElement = null;
        needPush = false;
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        checkPriorityQueue();
        return !outputPriorityQueue.isEmpty();
    }

    @Override
    public void next() throws HyracksDataException {
        outputElement = outputPriorityQueue.poll();
        needPush = true;
        if (outputElement == null) {
            throw new HyracksDataException("The outputPriorityQueue is empty");
        }
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        searchCallback = lsmInitialState.getSearchOperationCallback();
        cmp = lsmInitialState.getCmp();
        int numBTrees = lsmInitialState.getNumBTrees();
        rangeCursors = new BTreeRangeSearchCursor[numBTrees];
        for (int i = 0; i < numBTrees; i++) {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getLeafFrameFactory().createFrame();
            rangeCursors[i] = new BTreeRangeSearchCursor(leafFrame, false);
        }
        includeMemComponent = lsmInitialState.getIncludeMemComponent();
        searcherfRefCount = lsmInitialState.getSearcherRefCount();
        lsmHarness = lsmInitialState.getLSMHarness();
        setPriorityQueueComparator();
    }

    private void setPriorityQueueComparator() {
        if (pqCmp == null || cmp != pqCmp.getMultiComparator()) {
            pqCmp = new PriorityQueueComparator(cmp);
        }
    }

    @Override
    public ICachedPage getPage() {
        // do nothing
        return null;
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            outputPriorityQueue.clear();
            for (int i = 0; i < rangeCursors.length; i++) {
                rangeCursors[i].close();
            }
            rangeCursors = null;
        } finally {
            lsmHarness.closeSearchCursor(searcherfRefCount, includeMemComponent);
        }
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
        return (ITupleReference) outputElement.getTuple();
    }

    private void pushIntoPriorityQueue(int cursorIndex) throws HyracksDataException {
        if (rangeCursors[cursorIndex].hasNext()) {
            rangeCursors[cursorIndex].next();
            reusedElement.reset(rangeCursors[cursorIndex].getTuple(), cursorIndex);
            outputPriorityQueue.offer(reusedElement);
        }
    }

    private void checkPriorityQueue() throws HyracksDataException {
        while (!outputPriorityQueue.isEmpty() || needPush == true) {
            if (!outputPriorityQueue.isEmpty()) {
                PriorityQueueElement checkElement = outputPriorityQueue.peek();
                // If there is no previous tuple or the previous tuple can be
                // ignored
                if (outputElement == null) {
                    // Test the tuple is a delete tuple or not
                    if (((LSMBTreeTupleReference) checkElement.getTuple()).isAntimatter() == true) {
                        // If the tuple is a delete tuple then pop it and mark
                        // it "needPush"
                        // Cannot push at this time because the tuple may be
                        // modified if "hasNext" is called
                        outputElement = outputPriorityQueue.poll();
                        needPush = true;
                    } else {
                        break;
                    }
                } else {
                    // Compare the previous tuple and the head tuple in the PQ
                    if (cmp.compare(outputElement.getTuple(), checkElement.getTuple()) == 0) {
                        // If the previous tuple and the head tuple are
                        // identical
                        // then pop the head tuple and push the next tuple from
                        // the tree of head tuple

                        // the head element of PQ is useless now
                        reusedElement = outputPriorityQueue.poll();
                        // int treeNum = reusedElement.getTreeNum();
                        pushIntoPriorityQueue(reusedElement.getCursorIndex());
                    } else {
                        // If the previous tuple and the head tuple are
                        // different
                        // the info of previous tuple is useless
                        if (needPush == true) {
                            reusedElement = outputElement;
                            pushIntoPriorityQueue(outputElement.getCursorIndex());
                            needPush = false;
                        }
                        outputElement = null;
                    }
                }
            } else {
                // the priority queue is empty and needPush
                reusedElement = outputElement;
                pushIntoPriorityQueue(outputElement.getCursorIndex());
                needPush = false;
                outputElement = null;
            }
        }
    }

    @Override
    public boolean exclusiveLatchNodes() {
        return false;
    }

    public class PriorityQueueComparator implements Comparator<PriorityQueueElement> {

        private final MultiComparator cmp;

        public PriorityQueueComparator(MultiComparator cmp) {
            this.cmp = cmp;
        }

        @Override
        public int compare(PriorityQueueElement elementA, PriorityQueueElement elementB) {
            int result = cmp.compare(elementA.getTuple(), elementB.getTuple());
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

    public class PriorityQueueElement {
        private ITupleReference tuple;
        private int cursorIndex;

        public PriorityQueueElement(ITupleReference tuple, int cursorIndex) {
            reset(tuple, cursorIndex);
        }

        public ITupleReference getTuple() {
            return tuple;
        }

        public int getCursorIndex() {
            return cursorIndex;
        }

        public void reset(ITupleReference tuple, int cursorIndex) {
            this.tuple = tuple;
            this.cursorIndex = cursorIndex;
        }
    }
}
