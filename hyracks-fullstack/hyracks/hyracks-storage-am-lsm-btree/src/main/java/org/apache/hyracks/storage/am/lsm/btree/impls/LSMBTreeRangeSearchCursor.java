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
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;

public class LSMBTreeRangeSearchCursor extends LSMIndexSearchCursor {
    private final ArrayTupleReference copyTuple;
    private final RangePredicate reusablePred;
    private ISearchOperationCallback searchCallback;
    private BTreeAccessor[] btreeAccessors;
    private boolean[] isMemoryComponent;
    private ArrayTupleBuilder tupleBuilder;
    private boolean canCallProceed = true;
    private boolean resultOfSearchCallbackProceed = false;
    private int tupleFromMemoryComponentCount = 0;

    public LSMBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, false, NoOpIndexCursorStats.INSTANCE);
    }

    public LSMBTreeRangeSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples,
            IIndexCursorStats stats) {
        super(opCtx, returnDeletedTuples, stats);
        this.copyTuple = new ArrayTupleReference();
        this.reusablePred = new RangePredicate(null, null, true, true, null, null);
    }

    @Override
    public void doClose() throws HyracksDataException {
        super.doClose();
        canCallProceed = true;
    }

    @Override
    public void doNext() throws HyracksDataException {
        outputElement = outputPriorityQueue.poll();
        needPushElementIntoQueue = true;
        canCallProceed = false;
        if (outputElement.getCursorIndex() == 0) {
            tupleFromMemoryComponentCount++;
        }
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
        // Every SWITCH_COMPONENT_CYCLE calls, check if memory components need to be swapped with disk components
        // We should do this regardless of the value of includeMutableComponent. This is because if the cursor
        // of the memory component has gone past the end of the in memory component, then the includeMutableComponent
        // will be set to false. Still, when that happens, we want to exit the memory component to allow it to be
        // recycled and used for modifications.
        if (hasNextCallCount >= SWITCH_COMPONENT_CYCLE) {
            replaceMemoryComponentWithDiskComponentIfNeeded();
            hasNextCallCount = 0;
        }
        while (!outputPriorityQueue.isEmpty() || needPushElementIntoQueue) {
            if (!outputPriorityQueue.isEmpty()) {
                PriorityQueueElement queueHead = outputPriorityQueue.peek();
                if (canCallProceed) {
                    if (includeMutableComponent) {
                        resultOfSearchCallbackProceed = searchCallback.proceed(queueHead.getTuple());
                        if (!resultOfSearchCallbackProceed) {
                            // In case proceed() fails and there is an in-memory component,
                            // we can't simply use this element since there might be a change.
                            PriorityQueueElement mutableElement = remove(outputPriorityQueue, 0);
                            if (mutableElement != null) {
                                // Copies the current queue head
                                if (tupleBuilder == null) {
                                    tupleBuilder = new ArrayTupleBuilder(cmp.getKeyFieldCount());
                                }
                                TupleUtils.copyTuple(tupleBuilder, queueHead.getTuple(), cmp.getKeyFieldCount());
                                copyTuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
                                // Unlatches/unpins the leaf page of the index.
                                rangeCursors[0].close();
                                // Reconcile.
                                searchCallback.reconcile(copyTuple);
                                // Re-traverses the index.
                                reusablePred.setLowKey(copyTuple, true);
                                btreeAccessors[0].search(rangeCursors[0], reusablePred);
                                pushIntoQueueFromCursorAndReplaceThisElement(mutableElement);
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
                    } else {
                        // only perform locking for tuples from memory components.
                        // all tuples from disk components have already been committed, and we're safe to proceed
                        resultOfSearchCallbackProceed = true;
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
                        pushOutputElementIntoQueueIfNeeded();
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

    private void pushOutputElementIntoQueueIfNeeded() throws HyracksDataException {
        if (needPushElementIntoQueue) {
            pushIntoQueueFromCursorAndReplaceThisElement(outputElement);
            needPushElementIntoQueue = false;
        }
    }

    private void replaceMemoryComponentWithDiskComponentIfNeeded() throws HyracksDataException {
        int replaceFrom = replaceFrom();
        if (replaceFrom < 0) {
            // no switch is needed, check if we need to re-do the search on the memory component.
            // searches and modifications compete on the pages of the memory component
            // if the cursor on the memory component is not advancing, we re-do the operation in order
            // to release the latches and allow modifications to proceed
            redoMemoryComponentSearchIfNeeded();
            return;
        }
        opCtx.getIndex().getHarness().replaceMemoryComponentsWithDiskComponents(getOpCtx(), replaceFrom);
        // redo the search on the new component
        // switchRequest array has the size = number of memory components. which can be greater
        // than operationalComponents size in certain cases (0 disk component, 1 memory component for example)
        // To avoid index out of bound, we end the loop at the first of the two conditions
        for (int i = replaceFrom; i < switchRequest.length && i < operationalComponents.size(); i++) {
            if (switchRequest[i]) {
                ILSMComponent component = operationalComponents.get(i);
                BTree btree = (BTree) component.getIndex();
                if (i == 0 && component.getType() != LSMComponentType.MEMORY) {
                    includeMutableComponent = false;
                }
                if (switchedElements[i] != null) {
                    copyTuple.reset(switchComponentTupleBuilders[i].getFieldEndOffsets(),
                            switchComponentTupleBuilders[i].getByteArray());
                    reusablePred.setLowKey(copyTuple, true);
                    rangeCursors[i].close();
                    btreeAccessors[i].reset(btree, iap);
                    btreeAccessors[i].search(rangeCursors[i], reusablePred);
                    pushIntoQueueFromCursorAndReplaceThisElement(switchedElements[i]);
                }
            }
            switchRequest[i] = false;
            // any failed switch makes further switches pointless
            switchPossible = switchPossible && operationalComponents.get(i).getType() == LSMComponentType.DISK;
        }
    }

    private int replaceFrom() throws HyracksDataException {
        int replaceFrom = -1;
        if (!switchPossible) {
            return replaceFrom;
        }
        for (int i = 0; i < operationalComponents.size(); i++) {
            ILSMComponent next = operationalComponents.get(i);
            if (next.getType() == LSMComponentType.DISK) {
                if (i == 0) {
                    // if the first component is a disk component, then switch is not possible
                    switchPossible = false;
                }
                break;
            } else if (next.getState() == ComponentState.UNREADABLE_UNWRITABLE) {
                // if the component is UNREADABLE_UNWRITABLE, then it means that the flush has been completed while
                // the search cursor is inside the component, a switch candidate
                if (replaceFrom < 0) {
                    replaceFrom = i;
                }
                // we return the outputElement to the priority queue if it came from this component
                if (outputElement != null && outputElement.getCursorIndex() == i) {
                    pushIntoQueueFromCursorAndReplaceThisElement(outputElement);
                    needPushElementIntoQueue = false;
                    outputElement = null;
                    canCallProceed = true;
                }
                PriorityQueueElement element = remove(outputPriorityQueue, i);
                // if this cursor is still active (has an element)
                // then we copy the search key to restart the operation after
                // replacing the component
                if (element != null) {
                    if (switchComponentTupleBuilders[i] == null) {
                        switchComponentTupleBuilders[i] = new ArrayTupleBuilder(cmp.getKeyFieldCount());
                    }
                    TupleUtils.copyTuple(switchComponentTupleBuilders[i], element.getTuple(), cmp.getKeyFieldCount());
                }
                rangeCursors[i].close();
                switchRequest[i] = true;
                switchedElements[i] = element;
            }
        }
        return replaceFrom;
    }

    private void redoMemoryComponentSearchIfNeeded() throws HyracksDataException {
        if (!includeMutableComponent) {
            return;
        }
        // if the last n records, none were from memory and there are writers inside the component,
        // we need to re-do the search so the cursor doesn't block modifications due to latches over page
        if (tupleFromMemoryComponentCount == 0
                && ((AbstractLSMMemoryComponent) operationalComponents.get(0)).getWriterCount() > 0) {
            // When we reach here, we know that the mutable component element is not the outputElement
            // since if it was the output element, the tupleFromMemoryComponentCount would be at least 1
            PriorityQueueElement mutableElement = remove(outputPriorityQueue, 0);
            if (mutableElement != null) {
                // if the element is null, then there is nothing to do since no latches are held
                if (tupleBuilder == null) {
                    tupleBuilder = new ArrayTupleBuilder(cmp.getKeyFieldCount());
                }
                TupleUtils.copyTuple(tupleBuilder, mutableElement.getTuple(), cmp.getKeyFieldCount());
                copyTuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
                // Unlatches/unpins the leaf page of the index.
                rangeCursors[0].close();
                // Re-traverses the index.
                reusablePred.setLowKey(copyTuple, true);
                btreeAccessors[0].search(rangeCursors[0], reusablePred);
                pushIntoQueueFromCursorAndReplaceThisElement(mutableElement);
            }
        }
        tupleFromMemoryComponentCount = 0;
    }

    private PriorityQueueElement remove(PriorityQueue<PriorityQueueElement> outputPriorityQueue, int cursorIndex) {
        // Scans the PQ for the component's element and delete it
        Iterator<PriorityQueueElement> it = outputPriorityQueue.iterator();
        while (it.hasNext()) {
            PriorityQueueElement e = it.next();
            if (e.getCursorIndex() == cursorIndex) {
                it.remove();
                return e;
            }
        }
        return null;
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        cmp = lsmInitialState.getOriginalKeyComparator();
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();
        searchCallback = lsmInitialState.getSearchOperationCallback();
        RangePredicate predicate = (RangePredicate) lsmInitialState.getSearchPredicate();
        reusablePred.setLowKeyComparator(cmp);
        reusablePred.setHighKey(predicate.getHighKey(), predicate.isHighKeyInclusive());
        reusablePred.setHighKeyComparator(predicate.getHighKeyComparator());
        includeMutableComponent = false;

        int numBTrees = operationalComponents.size();
        if (rangeCursors == null) {
            // object creation: should be relatively low
            rangeCursors = new IIndexCursor[numBTrees];
            btreeAccessors = new BTreeAccessor[numBTrees];
            isMemoryComponent = new boolean[numBTrees];
        } else if (rangeCursors.length != numBTrees) {
            // should destroy first
            Throwable failure = CleanupUtils.destroy(null, btreeAccessors);
            btreeAccessors = null;
            failure = CleanupUtils.destroy(failure, rangeCursors);
            rangeCursors = null;
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
            rangeCursors = new IIndexCursor[numBTrees];
            btreeAccessors = new BTreeAccessor[numBTrees];
            isMemoryComponent = new boolean[numBTrees];
        }
        for (int i = 0; i < numBTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            BTree btree;
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
            }
            btree = (BTree) component.getIndex();
            if (btreeAccessors[i] == null || destroyIncompatible(component, i)) {
                btreeAccessors[i] = btree.createAccessor(iap);
                rangeCursors[i] = btreeAccessors[i].createSearchCursor(false);
            } else {
                // re-use
                btreeAccessors[i].reset(btree, iap);
                rangeCursors[i].close();
            }
            isMemoryComponent[i] = component.getType() == LSMComponentType.MEMORY;
        }
        IndexCursorUtils.open(btreeAccessors, rangeCursors, searchPred);
        try {
            setPriorityQueueComparator();
            initPriorityQueue();
            canCallProceed = true;
        } catch (Throwable th) { // NOSONAR Must catch all
            IndexCursorUtils.close(rangeCursors, th);
            throw HyracksDataException.create(th);
        }
    }

    private boolean destroyIncompatible(ILSMComponent component, int index) throws HyracksDataException {
        // exclusive or. if the component is memory and the previous one at that index was a disk component
        // or vice versa, then we should destroy the cursor and accessor since they need to be recreated
        if (component.getType() == LSMComponentType.MEMORY ^ isMemoryComponent[index]) {
            Throwable failure = CleanupUtils.destroy(null, btreeAccessors[index]);
            btreeAccessors[index] = null;
            failure = CleanupUtils.destroy(failure, rangeCursors[index]);
            rangeCursors[index] = null;
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean getSearchOperationCallbackProceedResult() {
        return resultOfSearchCallbackProceed;
    }

}
