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

package org.apache.hyracks.storage.am.lsm.btree.impls;

import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class LSMBTreeRangeSearchCursor extends LSMIndexSearchCursor {
    private final ArrayTupleReference copyTuple;
    private final RangePredicate reusablePred;

    private ISearchOperationCallback searchCallback;
    private RangePredicate predicate;
    private BTreeAccessor[] btreeAccessors;
    private ArrayTupleBuilder tupleBuilder;
    private boolean canCallProceed = true;

    public LSMBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, false);
    }

    public LSMBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples) {
        super(opCtx, returnDeletedTuples);
        this.copyTuple = new ArrayTupleReference();
        this.reusablePred = new RangePredicate(null, null, true, true, null, null);
    }

    @Override
    public void reset() throws HyracksDataException {
        super.reset();
        canCallProceed = true;
    }

    @Override
    public void next() throws HyracksDataException {
        outputElement = outputPriorityQueue.poll();
        needPushElementIntoQueue = true;
        canCallProceed = false;
    }

    /**
     * Checks the priority queue and resets and the top element if required.
     * PriorityQueue can hold one element from each cursor.
     * The boolean variable canCallProceedMethod controls whether we can call proceed() method for this element.
     * i.e. it can return this element if proceed() succeeds.
     * If proceed fails, that is most-likely that there is ongoing operations in the in-memory component.
     * After resolving in-memory component issue, it progresses again.
     * Also, in order to not release the same element again, it keeps the previous output and checks it
     * against the current head in the queue.
     */
    @Override
    protected void checkPriorityQueue() throws HyracksDataException {
        while (!outputPriorityQueue.isEmpty() || needPushElementIntoQueue) {
            if (!outputPriorityQueue.isEmpty()) {
                PriorityQueueElement queueHead = outputPriorityQueue.peek();
                if (canCallProceed) {
                    // if there are no memory components. no need to lock at all
                    // since whatever the search reads will never changes
                    if (includeMutableComponent) {
                        if (!searchCallback.proceed(queueHead.getTuple())) {
                            // In case proceed() fails and there is an in-memory component,
                            // we can't simply use this element since there might be a change.
                            PriorityQueueElement mutableElement = removeMutable(outputPriorityQueue);
                            if (mutableElement != null) {
                                // Copies the current queue head
                                if (tupleBuilder == null) {
                                    tupleBuilder = new ArrayTupleBuilder(cmp.getKeyFieldCount());
                                }
                                TupleUtils.copyTuple(tupleBuilder, queueHead.getTuple(), cmp.getKeyFieldCount());
                                copyTuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
                                // Unlatches/unpins the leaf page of the index.
                                rangeCursors[0].reset();
                                // Reconcile.
                                searchCallback.reconcile(copyTuple);
                                // Re-traverses the index.
                                reusablePred.setLowKey(copyTuple, true);
                                btreeAccessors[0].search(rangeCursors[0], reusablePred);
                                //------
                                includeMutableComponent = pushIntoQueueFromCursorAndReplaceThisElement(mutableElement);
                                // now that we have completed the search and we have latches over the pages,
                                // it is safe to complete the operation.. but as per the API of the callback
                                // we only complete if we're producing this tuple
                                // get head again
                                queueHead = outputPriorityQueue.peek();
                                /*
                                 * We need to restart in one of two cases:
                                 * 1. no more elements in the priority queue.
                                 * 2. the key of the head has changed (which means we need to call proceed)
                                 */
                                if (queueHead == null || cmp.compare(copyTuple, queueHead.getTuple()) != 0) {
                                    // cancel since we're not continuing
                                    searchCallback.cancel(copyTuple);
                                    continue;
                                }
                                searchCallback.complete(copyTuple);
                                // it is safe to proceed now
                            } else {
                                // There are no more elements in the memory component.. can safely skip locking for the
                                // remaining operations
                                includeMutableComponent = false;
                            }
                        }
                    }
                }

                // If there is no previous tuple or the previous tuple can be ignored.
                // This check is needed not to release the same tuple again.
                if (outputElement == null) {
                    if (isDeleted(queueHead) && !returnDeletedTuples) {
                        // If the key has been deleted then pop it and set needPush to true.
                        // We cannot push immediately because the tuple may be
                        // modified if hasNext() is called
                        outputElement = outputPriorityQueue.poll();
                        needPushElementIntoQueue = true;
                        canCallProceed = false;
                    } else {
                        break;
                    }
                } else {
                    // Compare the previous tuple and the head tuple in the PQ
                    if (compare(cmp, outputElement.getTuple(), queueHead.getTuple()) == 0) {
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
                        canCallProceed = true;
                        outputElement = null;
                    }
                }
            } else {
                // the priority queue is empty and needPush
                pushIntoQueueFromCursorAndReplaceThisElement(outputElement);
                needPushElementIntoQueue = false;
                outputElement = null;
                canCallProceed = true;
            }
        }

    }

    private PriorityQueueElement removeMutable(PriorityQueue<PriorityQueueElement> outputPriorityQueue) {
        // Scans the PQ for the mutable component's element and delete it
        // since it can be changed.
        // (i.e. we can't ensure that the element is the most current one.)
        Iterator<PriorityQueueElement> it = outputPriorityQueue.iterator();
        while (it.hasNext()) {
            PriorityQueueElement mutableElement = it.next();
            if (mutableElement.getCursorIndex() == 0) {
                it.remove();
                return mutableElement;
            }
        }
        return null;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        cmp = lsmInitialState.getOriginalKeyComparator();
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();
        searchCallback = lsmInitialState.getSearchOperationCallback();
        predicate = (RangePredicate) lsmInitialState.getSearchPredicate();
        reusablePred.setLowKeyComparator(cmp);
        reusablePred.setHighKey(predicate.getHighKey(), predicate.isHighKeyInclusive());
        reusablePred.setHighKeyComparator(predicate.getHighKeyComparator());
        includeMutableComponent = false;

        int numBTrees = operationalComponents.size();
        if (rangeCursors == null || rangeCursors.length != numBTrees) {
            // object creation: should be relatively low
            rangeCursors = new IIndexCursor[numBTrees];
            btreeAccessors = new BTreeAccessor[numBTrees];
        }
        for (int i = 0; i < numBTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            BTree btree;
            if (rangeCursors[i] == null) {
                // create, should be relatively rare
                IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getLeafFrameFactory().createFrame();
                rangeCursors[i] = new BTreeRangeSearchCursor(leafFrame, false);
            } else {
                // re-use
                rangeCursors[i].reset();
            }
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
                btree = ((LSMBTreeMemoryComponent) component).getBTree();
            } else {
                btree = ((LSMBTreeDiskComponent) component).getBTree();
            }

            if (btreeAccessors[i] == null) {
                btreeAccessors[i] = (BTreeAccessor) btree.createAccessor(NoOpOperationCallback.INSTANCE,
                        NoOpOperationCallback.INSTANCE);
            } else {
                // re-use
                btreeAccessors[i].reset(btree, NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            }
            btreeAccessors[i].search(rangeCursors[i], searchPred);
        }
        setPriorityQueueComparator();
        initPriorityQueue();
        canCallProceed = true;
    }
}
