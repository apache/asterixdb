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
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ILSMIndexCursor;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.MultiComparator;

public abstract class LSMIndexSearchCursor extends EnforcedIndexCursor implements ILSMIndexCursor {
    protected static final int SWITCH_COMPONENT_CYCLE = 100;
    protected final ILSMIndexOperationContext opCtx;
    protected final boolean returnDeletedTuples;
    protected PriorityQueueElement outputElement;
    protected final ArrayTupleBuilder[] switchComponentTupleBuilders;
    protected final boolean[] switchRequest;
    protected final PriorityQueueElement[] switchedElements;
    protected IIndexCursor[] rangeCursors;
    protected PriorityQueueElement[] pqes;
    protected PriorityQueue<PriorityQueueElement> outputPriorityQueue;
    protected PriorityQueueComparator pqCmp;
    protected MultiComparator cmp;
    protected boolean needPushElementIntoQueue;
    protected boolean includeMutableComponent;
    protected ILSMHarness lsmHarness;
    protected boolean switchPossible = true;
    protected int hasNextCallCount = 0;

    protected List<ILSMComponent> operationalComponents;
    protected final IIndexAccessParameters iap;

    public LSMIndexSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples, IIndexCursorStats stats) {
        this.opCtx = opCtx;
        this.returnDeletedTuples = returnDeletedTuples;
        outputElement = null;
        needPushElementIntoQueue = false;
        switchComponentTupleBuilders = new ArrayTupleBuilder[opCtx.getIndex().getNumberOfAllMemoryComponents()];
        switchRequest = new boolean[switchComponentTupleBuilders.length];
        switchedElements = new PriorityQueueElement[switchComponentTupleBuilders.length];
        this.iap = IndexAccessParameters.createNoOpParams(stats);
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
    public void doClose() throws HyracksDataException {
        hasNextCallCount = 0;
        switchPossible = true;
        outputElement = null;
        needPushElementIntoQueue = false;
        for (int i = 0; i < switchRequest.length; i++) {
            switchRequest[i] = false;
        }
        try {
            if (outputPriorityQueue != null) {
                outputPriorityQueue.clear();
            }

            if (rangeCursors != null) {
                for (int i = 0; i < rangeCursors.length; i++) {
                    rangeCursors[i].close();
                }
            }
        } finally {
            if (lsmHarness != null) {
                lsmHarness.endSearch(opCtx);
            }
        }
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        hasNextCallCount++;
        checkPriorityQueue();
        return !outputPriorityQueue.isEmpty();
    }

    @Override
    public void doNext() throws HyracksDataException {
        outputElement = outputPriorityQueue.poll();
        needPushElementIntoQueue = true;
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        try {
            if (outputPriorityQueue != null) {
                outputPriorityQueue.clear();
            }
            if (rangeCursors != null) {
                for (int i = 0; i < rangeCursors.length; i++) {
                    if (rangeCursors[i] != null) {
                        rangeCursors[i].destroy();
                    }
                }
                rangeCursors = null;
            }
        } finally {
            if (lsmHarness != null) {
                lsmHarness.endSearch(opCtx);
            }
        }
    }

    @Override
    public ITupleReference doGetTuple() {
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

    protected void pushIntoQueueFromCursorAndReplaceThisElement(PriorityQueueElement e) throws HyracksDataException {
        int cursorIndex = e.getCursorIndex();
        if (rangeCursors[cursorIndex].hasNext()) {
            rangeCursors[cursorIndex].next();
            e.reset(rangeCursors[cursorIndex].getTuple());
            outputPriorityQueue.offer(e);
            return;
        }
        rangeCursors[cursorIndex].close();
        if (cursorIndex == 0) {
            includeMutableComponent = false;
        }
    }

    protected boolean isDeleted(PriorityQueueElement checkElement) throws HyracksDataException {
        return ((ILSMTreeTupleReference) checkElement.getTuple()).isAntimatter();
    }

    protected void checkPriorityQueue() throws HyracksDataException {
        while (!outputPriorityQueue.isEmpty() || needPushElementIntoQueue) {
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

    public static class PriorityQueueElement {
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

    public static class PriorityQueueComparator implements Comparator<PriorityQueueElement> {

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

    @Override
    public boolean getSearchOperationCallbackProceedResult() {
        return false;
    }
}
