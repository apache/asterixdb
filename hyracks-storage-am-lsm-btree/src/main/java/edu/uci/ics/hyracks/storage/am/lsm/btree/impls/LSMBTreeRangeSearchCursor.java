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
import java.util.ListIterator;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;

public class LSMBTreeRangeSearchCursor extends LSMIndexSearchCursor {
    private final ArrayTupleReference copyTuple;
    private final RangePredicate reusablePred;

    private ISearchOperationCallback searchCallback;
    private RangePredicate predicate;
    private IIndexAccessor memBTreeAccessor;
    private ArrayTupleBuilder tupleBuilder;
    private boolean proceed = true;

    public LSMBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx) {
        super(opCtx);
        this.copyTuple = new ArrayTupleReference();
        this.reusablePred = new RangePredicate(null, null, true, true, null, null);
    }

    @Override
    public void reset() throws HyracksDataException, IndexException {
        super.reset();
        proceed = true;
    }

    @Override
    public void next() throws HyracksDataException {
        outputElement = outputPriorityQueue.poll();
        needPush = true;
        proceed = false;
    }

    protected void checkPriorityQueue() throws HyracksDataException, IndexException {
        while (!outputPriorityQueue.isEmpty() || needPush == true) {
            if (!outputPriorityQueue.isEmpty()) {
                PriorityQueueElement checkElement = outputPriorityQueue.peek();
                if (proceed && !searchCallback.proceed(checkElement.getTuple())) {
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
                        if (inMemElementFound) {
                            // copy the in-mem tuple
                            if (tupleBuilder == null) {
                                tupleBuilder = new ArrayTupleBuilder(cmp.getKeyFieldCount());
                            }
                            TupleUtils.copyTuple(tupleBuilder, inMemElement.getTuple(), cmp.getKeyFieldCount());
                            copyTuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

                            // unlatch/unpin
                            rangeCursors[0].reset();

                            // reconcile
                            if (checkElement.getCursorIndex() == 0) {
                                searchCallback.reconcile(copyTuple);
                            } else {
                                searchCallback.reconcile(checkElement.getTuple());
                            }
                            // retraverse
                            reusablePred.setLowKey(copyTuple, true);
                            try {
                                memBTreeAccessor.search(rangeCursors[0], reusablePred);
                            } catch (IndexException e) {
                                throw new HyracksDataException(e);
                            }
                            pushIntoPriorityQueue(inMemElement);
                            if (cmp.compare(copyTuple, inMemElement.getTuple()) != 0) {
                                searchCallback.cancel(copyTuple);
                                continue;
                            }
                        } else {
                            // the in-memory cursor is exhausted
                            searchCallback.reconcile(checkElement.getTuple());
                        }
                    } else {
                        searchCallback.reconcile(checkElement.getTuple());
                    }
                }
                // If there is no previous tuple or the previous tuple can be ignored
                if (outputElement == null) {
                    if (isDeleted(checkElement)) {
                        // If the key has been deleted then pop it and set needPush to true.
                        // We cannot push immediately because the tuple may be
                        // modified if hasNext() is called
                        outputElement = outputPriorityQueue.poll();
                        searchCallback.cancel(checkElement.getTuple());
                        needPush = true;
                        proceed = false;
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
                        proceed = true;
                        outputElement = null;
                    }
                }
            } else {
                // the priority queue is empty and needPush
                pushIntoPriorityQueue(outputElement);
                needPush = false;
                outputElement = null;
                proceed = true;
            }
        }
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException,
            IndexException {
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
        rangeCursors = new IIndexCursor[numBTrees];
        for (int i = 0; i < numBTrees; i++) {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getLeafFrameFactory().createFrame();
            rangeCursors[i] = new BTreeRangeSearchCursor(leafFrame, false);
        }
        setPriorityQueueComparator();

        int cursorIx = 0;
        ListIterator<ILSMComponent> btreesIter = operationalComponents.listIterator();
        if (includeMemComponent) {
            // Open cursor of in-memory BTree at index 0.
            memBTreeAccessor.search(rangeCursors[cursorIx], searchPred);
            // Skip 0 because it is the in-memory BTree.
            ++cursorIx;
            btreesIter.next();
        }

        // Open cursors of on-disk BTrees.
        int numDiskComponents = includeMemComponent ? numBTrees - 1 : numBTrees;
        ITreeIndexAccessor[] diskBTreeAccessors = new ITreeIndexAccessor[numDiskComponents];
        int diskBTreeIx = 0;
        while (btreesIter.hasNext()) {
            BTree diskBTree = (BTree) ((LSMBTreeImmutableComponent) btreesIter.next()).getBTree();
            diskBTreeAccessors[diskBTreeIx] = diskBTree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            diskBTreeAccessors[diskBTreeIx].search(rangeCursors[cursorIx], searchPred);
            cursorIx++;
            diskBTreeIx++;
        }
        initPriorityQueue();
        proceed = true;
    }
}