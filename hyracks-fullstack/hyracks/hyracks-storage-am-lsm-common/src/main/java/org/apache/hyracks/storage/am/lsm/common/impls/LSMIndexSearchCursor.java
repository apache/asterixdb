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

package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public abstract class LSMIndexSearchCursor implements ITreeIndexCursor {
    protected final ILSMIndexOperationContext opCtx;
    protected final boolean returnDeletedTuples;
    protected PriorityQueueElement outputElement;
    protected IIndexCursor[] rangeCursors;
    protected PriorityQueueElement[] pqes;
    protected PriorityQueue<PriorityQueueElement> outputPriorityQueue;
    protected PriorityQueueComparator pqCmp;
    protected MultiComparator cmp;
    protected boolean needPushElementIntoQueue;
    protected boolean includeMutableComponent;
    protected ILSMHarness lsmHarness;

    protected List<ILSMComponent> operationalComponents;

    public LSMIndexSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples) {
        this.opCtx = opCtx;
        this.returnDeletedTuples = returnDeletedTuples;
        outputElement = null;
        needPushElementIntoQueue = false;
    }

    public ILSMIndexOperationContext getOpCtx() {
        return opCtx;
    }

    public void initPriorityQueue() throws HyracksDataException {
        int pqInitSize = (rangeCursors.length > 0) ? rangeCursors.length : 1;
        if (outputPriorityQueue == null) {
            outputPriorityQueue = new PriorityQueue<>(pqInitSize, pqCmp);
            pqes = new PriorityQueueElement[pqInitSize];
            for (int i = 0; i < pqInitSize; i++) {
                pqes[i] = new PriorityQueueElement(i);
            }
            for (int i = 0; i < rangeCursors.length; i++) {
                pushIntoQueueFromCursorAndReplaceThisElement(pqes[i]);
            }
        } else {
            outputPriorityQueue.clear();
            // did size change?
            if (pqInitSize == pqes.length) {
                // size is the same -> re-use
                for (int i = 0; i < rangeCursors.length; i++) {
                    pqes[i].reset(null);
                    pushIntoQueueFromCursorAndReplaceThisElement(pqes[i]);
                }
            } else {
                // size changed (due to flushes, merges, etc) -> re-create
                pqes = new PriorityQueueElement[pqInitSize];
                for (int i = 0; i < rangeCursors.length; i++) {
                    pqes[i] = new PriorityQueueElement(i);
                    pushIntoQueueFromCursorAndReplaceThisElement(pqes[i]);
                }
            }
        }
    }

    public IIndexCursor getCursor(int cursorIndex) {
        return rangeCursors[cursorIndex];
    }

    @Override
    public void reset() throws HyracksDataException {
        outputElement = null;
        needPushElementIntoQueue = false;

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
    public boolean hasNext() throws HyracksDataException {
        checkPriorityQueue();
        return !outputPriorityQueue.isEmpty();
    }

    @Override
    public void next() throws HyracksDataException {
        outputElement = outputPriorityQueue.poll();
        needPushElementIntoQueue = true;
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            if (outputPriorityQueue != null) {
                outputPriorityQueue.clear();
            }
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

    @Override
    public ITupleReference getFilterMinTuple() {
        ILSMComponentFilter filter = operationalComponents.get(outputElement.cursorIndex).getLSMComponentFilter();
        return filter == null ? null : filter.getMinTuple();
    }

    @Override
    public ITupleReference getFilterMaxTuple() {
        ILSMComponentFilter filter = operationalComponents.get(outputElement.cursorIndex).getLSMComponentFilter();
        return filter == null ? null : filter.getMaxTuple();
    }

    protected boolean pushIntoQueueFromCursorAndReplaceThisElement(PriorityQueueElement e) throws HyracksDataException {
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

    protected boolean isDeleted(PriorityQueueElement checkElement) throws HyracksDataException {
        return ((ILSMTreeTupleReference) checkElement.getTuple()).isAntimatter();
    }

    protected void checkPriorityQueue() throws HyracksDataException {
        while (!outputPriorityQueue.isEmpty() || (needPushElementIntoQueue == true)) {
            if (!outputPriorityQueue.isEmpty()) {
                PriorityQueueElement checkElement = outputPriorityQueue.peek();
                // If there is no previous tuple or the previous tuple can be ignored
                if (outputElement == null) {
                    if (isDeleted(checkElement) && !returnDeletedTuples) {
                        // If the key has been deleted then pop it and set needPush to true.
                        // We cannot push immediately because the tuple may be
                        // modified if hasNext() is called
                        outputElement = outputPriorityQueue.poll();
                        needPushElementIntoQueue = true;
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
                        pushIntoQueueFromCursorAndReplaceThisElement(e);
                    } else {
                        // If the previous tuple and the head tuple are different
                        // the info of previous tuple is useless
                        if (needPushElementIntoQueue == true) {
                            pushIntoQueueFromCursorAndReplaceThisElement(outputElement);
                            needPushElementIntoQueue = false;
                        }
                        outputElement = null;
                    }
                }
            } else {
                // the priority queue is empty and needPush
                pushIntoQueueFromCursorAndReplaceThisElement(outputElement);
                needPushElementIntoQueue = false;
                outputElement = null;
            }
        }
    }

    @Override
    public boolean isExclusiveLatchNodes() {
        return false;
    }

    public class PriorityQueueElement {
        private ITupleReference tuple;
        private int cursorIndex;

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

        protected MultiComparator cmp;

        public PriorityQueueComparator(MultiComparator cmp) {
            this.cmp = cmp;
        }

        @Override
        public int compare(PriorityQueueElement elementA, PriorityQueueElement elementB) {
            int result;
            try {
                result = cmp.compare(elementA.getTuple(), elementB.getTuple());
                if (result != 0) {
                    return result;
                }
            } catch (HyracksDataException e) {
                throw new IllegalArgumentException(e);
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

    protected int compare(MultiComparator cmp, ITupleReference tupleA, ITupleReference tupleB)
            throws HyracksDataException {
        return cmp.compare(tupleA, tupleB);
    }

}
