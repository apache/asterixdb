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

import java.util.Iterator;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BloomFilterAwareBTreePointSearchCursor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;

public class LSMBTreeRangeSearchCursor extends LSMIndexSearchCursor {
    private final ArrayTupleReference copyTuple;
    private final RangePredicate reusablePred;

    private ISearchOperationCallback searchCallback;
    private RangePredicate predicate;
    private IIndexAccessor memBTreeAccessor;
    private ArrayTupleBuilder tupleBuilder;

    public LSMBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx) {
        super(opCtx);
        this.copyTuple = new ArrayTupleReference();
        this.reusablePred = new RangePredicate(null, null, true, true, null, null);
    }

    @Override
    public boolean hasNext() throws HyracksDataException, IndexException {
        checkPriorityQueue();
        PriorityQueueElement pqHead = outputPriorityQueue.peek();
        if (pqHead == null) {
            // PQ is empty
            return false;
        }

        assert outputElement == null;

        if (searchCallback.proceed(pqHead.getTuple())) {
            // if proceed is successful, then there's no need for doing the "unlatch dance"
            return true;
        }

        if (includeMemComponent) {
            PriorityQueueElement inMemElement = null;
            boolean inMemElementFound = false;

            // scan the PQ for the in-memory component's element
            Iterator<PriorityQueueElement> it = outputPriorityQueue.iterator();
            while (it.hasNext()) {
                inMemElement = it.next();
                if (inMemElement.getCursorIndex() == 0) {
                    inMemElementFound = true;
                    it.remove();
                    break;
                }
            }

            if (!inMemElementFound) {
                // the in-memory cursor is exhausted
                searchCallback.reconcile(pqHead.getTuple());
                return true;
            }

            // copy the in-mem tuple
            if (tupleBuilder == null) {
                tupleBuilder = new ArrayTupleBuilder(cmp.getKeyFieldCount());
            }
            TupleUtils.copyTuple(tupleBuilder, inMemElement.getTuple(), cmp.getKeyFieldCount());
            copyTuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

            // unlatch/unpin
            rangeCursors[0].reset();

            // reconcile
            if (pqHead.getCursorIndex() == 0) {
                searchCallback.reconcile(copyTuple);
            } else {
                searchCallback.reconcile(pqHead.getTuple());
            }

            // retraverse
            reusablePred.setLowKey(copyTuple, true);
            try {
                memBTreeAccessor.search(rangeCursors[0], reusablePred);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }

            if (!pushIntoPriorityQueue(inMemElement)) {
                return !outputPriorityQueue.isEmpty();
            }

            if (pqHead.getCursorIndex() == 0) {
                if (cmp.compare(copyTuple, inMemElement.getTuple()) != 0) {
                    searchCallback.cancel(copyTuple);
                }
            }
            checkPriorityQueue();
        } else {
            searchCallback.reconcile(pqHead.getTuple());
        }

        return true;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        cmp = lsmInitialState.getOriginalKeyComparator();
        includeMemComponent = lsmInitialState.getIncludeMemComponent();
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();
        searchCallback = lsmInitialState.getSearchOperationCallback();
        memBTreeAccessor = lsmInitialState.getMemBTreeAccessor();
        predicate = (RangePredicate) lsmInitialState.getSearchPredicate();
        reusablePred.setLowKeyComparator(cmp);
        reusablePred.setHighKey(predicate.getHighKey(), predicate.isHighKeyInclusive());
        reusablePred.setHighKeyComparator(predicate.getHighKeyComparator());

        int numBTrees = lsmInitialState.getNumBTrees();
        rangeCursors = new BTreeRangeSearchCursor[numBTrees];
        if (lsmInitialState.isPointSearch()) {
            int i = 0;
            if (includeMemComponent) {
                // No need for a bloom filter for the in-memory BTree.
                IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getLeafFrameFactory().createFrame();
                rangeCursors[i] = new BTreeRangeSearchCursor(leafFrame, false);
                ++i;
            }
            for (; i < numBTrees; i++) {
                IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getLeafFrameFactory().createFrame();
                rangeCursors[i] = new BloomFilterAwareBTreePointSearchCursor(leafFrame, false,
                        ((LSMBTreeImmutableComponent) operationalComponents.get(i)).getBloomFilter());
            }
        } else {
            for (int i = 0; i < numBTrees; i++) {
                IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getLeafFrameFactory().createFrame();
                rangeCursors[i] = new BTreeRangeSearchCursor(leafFrame, false);
            }
        }

        setPriorityQueueComparator();
    }
}