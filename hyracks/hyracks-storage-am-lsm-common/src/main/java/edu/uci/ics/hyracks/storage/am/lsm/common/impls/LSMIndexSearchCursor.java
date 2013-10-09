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

package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public abstract class LSMIndexSearchCursor implements ITreeIndexCursor {
    protected PriorityQueueElement outputElement;
    protected IIndexCursor[] rangeCursors;
    protected PriorityQueue<PriorityQueueElement> outputPriorityQueue;
    protected PriorityQueueComparator pqCmp;
    protected MultiComparator cmp;
    protected boolean needPush;
    protected boolean includeMutableComponent;
    protected ILSMHarness lsmHarness;
    protected final ILSMIndexOperationContext opCtx;
    protected final boolean returnDeletedTuples;

    protected List<ILSMComponent> operationalComponents;

    public LSMIndexSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples) {
        this.opCtx = opCtx;
        this.returnDeletedTuples = returnDeletedTuples;
        outputElement = null;
        needPush = false;
    }

    public ILSMIndexOperationContext getOpCtx() {
        return opCtx;
    }

    public void initPriorityQueue() throws HyracksDataException, IndexException {
        int pqInitSize = (rangeCursors.length > 0) ? rangeCursors.length : 1;
        outputPriorityQueue = new PriorityQueue<PriorityQueueElement>(pqInitSize, pqCmp);
        for (int i = 0; i < rangeCursors.length; i++) {
            pushIntoPriorityQueue(new PriorityQueueElement(i));
        }
    }

    public IIndexCursor getCursor(int cursorIndex) {
        return rangeCursors[cursorIndex];
    }

    @Override
    public void reset() throws HyracksDataException, IndexException {
        outputElement = null;
        needPush = false;

        try {
            if (outputPriorityQueue != null) {
                outputPriorityQueue.clear();
            }

            if (rangeCursors != null) {
                for (int i = 0; i < rangeCursors.length; i++) {
                    rangeCursors[i].reset();
                }
            }
            rangeCursors = null;
        } finally {
            if (lsmHarness != null) {
                lsmHarness.endSearch(opCtx);
            }
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException, IndexException {
        checkPriorityQueue();
        return !outputPriorityQueue.isEmpty();
    }

    @Override
    public void next() throws HyracksDataException {
        outputElement = outputPriorityQueue.poll();
        needPush = true;
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
            if (lsmHarness != null) {
                lsmHarness.endSearch(opCtx);
            }
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
        return outputElement.getTuple();
    }

    protected boolean pushIntoPriorityQueue(PriorityQueueElement e) throws HyracksDataException, IndexException {
        int cursorIndex = e.getCursorIndex();
        if (rangeCursors[cursorIndex].hasNext()) {
            rangeCursors[cursorIndex].next();
            e.reset(rangeCursors[cursorIndex].getTuple());
            outputPriorityQueue.offer(e);
            return true;
        }
        rangeCursors[cursorIndex].close();
        return false;
    }

    protected boolean isDeleted(PriorityQueueElement checkElement) throws HyracksDataException, IndexException {
        return ((ILSMTreeTupleReference) checkElement.getTuple()).isAntimatter();
    }

    protected void checkPriorityQueue() throws HyracksDataException, IndexException {
        while (!outputPriorityQueue.isEmpty() || needPush == true) {
            if (!outputPriorityQueue.isEmpty()) {
                PriorityQueueElement checkElement = outputPriorityQueue.peek();
                // If there is no previous tuple or the previous tuple can be ignored
                if (outputElement == null) {
                    if (isDeleted(checkElement) && !returnDeletedTuples) {
                        // If the key has been deleted then pop it and set needPush to true.
                        // We cannot push immediately because the tuple may be
                        // modified if hasNext() is called
                        outputElement = outputPriorityQueue.poll();
                        needPush = true;
                    } else {
                        break;
                    }
                } else {
                    // Compare the previous tuple and the head tuple in the PQ
                    if (compare(cmp, outputElement.getTuple(), checkElement.getTuple()) == 0) {
                        // If the previous tuple and the head tuple are
                        // identical
                        // then pop the head tuple and push the next tuple from
                        // the tree of head tuple

                        // the head element of PQ is useless now
                        PriorityQueueElement e = outputPriorityQueue.poll();
                        pushIntoPriorityQueue(e);
                    } else {
                        // If the previous tuple and the head tuple are different
                        // the info of previous tuple is useless
                        if (needPush == true) {
                            pushIntoPriorityQueue(outputElement);
                            needPush = false;
                        }
                        outputElement = null;
                    }
                }
            } else {
                // the priority queue is empty and needPush
                pushIntoPriorityQueue(outputElement);
                needPush = false;
                outputElement = null;
            }
        }
    }

    @Override
    public boolean exclusiveLatchNodes() {
        return false;
    }

    public class PriorityQueueElement {
        private ITupleReference tuple;
        private final int cursorIndex;

        public PriorityQueueElement(int cursorIndex) {
            tuple = null;
            this.cursorIndex = cursorIndex;
        }

        public ITupleReference getTuple() {
            return tuple;
        }

        public int getCursorIndex() {
            return cursorIndex;
        }

        public void reset(ITupleReference tuple) {
            this.tuple = tuple;
        }
    }

    public class PriorityQueueComparator implements Comparator<PriorityQueueElement> {

        protected final MultiComparator cmp;

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

    protected void setPriorityQueueComparator() {
        if (pqCmp == null || cmp != pqCmp.getMultiComparator()) {
            pqCmp = new PriorityQueueComparator(cmp);
        }
    }

    protected int compare(MultiComparator cmp, ITupleReference tupleA, ITupleReference tupleB) {
        return cmp.compare(tupleA, tupleB);
    }
}