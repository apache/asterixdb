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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.util.TupleUtils;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;

public class LSMBTreeRangeSearchCursor extends LSMIndexSearchCursor {
    private final ArrayTupleReference copyTuple;
    private final RangePredicate reusablePred;

    private ISearchOperationCallback searchCallback;
    private RangePredicate predicate;
    private BTreeAccessor[] btreeAccessors;
    private ArrayTupleBuilder tupleBuilder;
    private boolean proceed = true;

    public LSMBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, false);
    }

    public LSMBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples) {
        super(opCtx, returnDeletedTuples);
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

    @Override
    protected void checkPriorityQueue() throws HyracksDataException, IndexException {
        while (!outputPriorityQueue.isEmpty() || needPush == true) {
            if (!outputPriorityQueue.isEmpty()) {
                PriorityQueueElement checkElement = outputPriorityQueue.peek();
                if (proceed && !searchCallback.proceed(checkElement.getTuple())) {
                    if (includeMutableComponent) {
                        PriorityQueueElement mutableElement = null;
                        boolean mutableElementFound = false;
                        // scan the PQ for the mutable component's element
                        Iterator<PriorityQueueElement> it = outputPriorityQueue.iterator();
                        while (it.hasNext()) {
                            mutableElement = it.next();
                            if (mutableElement.getCursorIndex() == 0) {
                                mutableElementFound = true;
                                it.remove();
                                break;
                            }
                        }
                        if (mutableElementFound) {
                            // copy the in-mem tuple
                            if (tupleBuilder == null) {
                                tupleBuilder = new ArrayTupleBuilder(cmp.getKeyFieldCount());
                            }
                            TupleUtils.copyTuple(tupleBuilder, mutableElement.getTuple(), cmp.getKeyFieldCount());
                            copyTuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

                            // unlatch/unpin
                            rangeCursors[0].reset();

                            // reconcile
                            if (checkElement.getCursorIndex() == 0) {
                                searchCallback.reconcile(copyTuple);
                            } else {
                                searchCallback.reconcile(checkElement.getTuple());
                                searchCallback.complete(checkElement.getTuple());
                            }
                            // retraverse
                            reusablePred.setLowKey(copyTuple, true);
                            btreeAccessors[0].search(rangeCursors[0], reusablePred);
                            boolean isNotExhaustedCursor = pushIntoPriorityQueue(mutableElement);

                            if (checkElement.getCursorIndex() == 0) {
                                if (!isNotExhaustedCursor || cmp.compare(copyTuple, mutableElement.getTuple()) != 0) {
                                    searchCallback.complete(copyTuple);
                                    searchCallback.cancel(copyTuple);
                                    continue;
                                }
                                searchCallback.complete(copyTuple);
                            }
                        } else {
                            // the mutable cursor is exhausted
                            searchCallback.reconcile(checkElement.getTuple());
                        }
                    } else {
                        searchCallback.reconcile(checkElement.getTuple());
                    }
                }
                // If there is no previous tuple or the previous tuple can be ignored
                if (outputElement == null) {
                    if (isDeleted(checkElement) && !returnDeletedTuples) {
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
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred)
            throws HyracksDataException, IndexException {
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
        proceed = true;
    }
}
