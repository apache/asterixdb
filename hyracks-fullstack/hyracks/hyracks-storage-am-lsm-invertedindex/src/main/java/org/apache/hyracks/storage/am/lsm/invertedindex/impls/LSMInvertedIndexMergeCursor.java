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

package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.List;
import java.util.PriorityQueue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ILSMIndexCursor;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor.PriorityQueueComparator;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor.PriorityQueueElement;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexRangeSearchCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.tuples.TokenKeyPairTuple;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * This cursor is specially designed and optimized for merging inverted index.
 * For simplicity, it assumes all components are disk components, and the cursor is not reused.
 *
 */
public class LSMInvertedIndexMergeCursor extends EnforcedIndexCursor implements ILSMIndexCursor {
    protected final LSMInvertedIndexOpContext opCtx;
    protected PriorityQueueElement outputTokenElement;
    protected OnDiskInvertedIndexRangeSearchCursor[] rangeCursors;
    protected PriorityQueueElement[] tokenQueueElements;
    protected PriorityQueue<PriorityQueueElement> tokenQueue;
    protected PriorityQueueComparator tokenQueueCmp;

    protected PriorityQueueElement outputKeyElement;
    protected PriorityQueueElement[] keyQueueElements;
    protected PriorityQueue<PriorityQueueElement> keyQueue;
    protected PriorityQueueComparator keyQueueCmp;

    protected boolean needPushElementIntoKeyQueue;

    protected ILSMHarness lsmHarness;

    protected MultiComparator tokenCmp;
    protected MultiComparator keyCmp;

    protected List<ILSMComponent> operationalComponents;

    // Assuming the cursor for all deleted-keys indexes are of the same type.
    protected IIndexCursor[] deletedKeysBTreeCursors;
    protected BloomFilter[] bloomFilters;
    protected final long[] hashes = BloomFilter.createHashArray();
    protected IIndexAccessor[] deletedKeysBTreeAccessors;
    protected RangePredicate deletedKeyBTreeSearchPred;

    protected final TokenKeyPairTuple outputTuple;
    protected final IIndexAccessParameters iap;

    public LSMInvertedIndexMergeCursor(ILSMIndexOperationContext opCtx, IIndexCursorStats stats) {
        this.opCtx = (LSMInvertedIndexOpContext) opCtx;
        outputTokenElement = null;
        outputKeyElement = null;
        needPushElementIntoKeyQueue = false;

        IInvertedIndex invertedIndex = (IInvertedIndex) this.opCtx.getIndex();
        this.outputTuple = new TokenKeyPairTuple(invertedIndex.getTokenTypeTraits().length,
                invertedIndex.getInvListTypeTraits().length);

        this.tokenCmp = MultiComparator.create(invertedIndex.getTokenCmpFactories());
        this.keyCmp = MultiComparator.create(invertedIndex.getInvListCmpFactories());
        this.tokenQueueCmp = new PriorityQueueComparator(tokenCmp);
        this.keyQueueCmp = new PriorityQueueComparator(keyCmp);
        this.iap = IndexAccessParameters.createNoOpParams(stats);
    }

    public LSMInvertedIndexOpContext getOpCtx() {
        return opCtx;
    }

    @Override
    public void doOpen(ICursorInitialState initState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMInvertedIndexRangeSearchCursorInitialState lsmInitState =
                (LSMInvertedIndexRangeSearchCursorInitialState) initState;
        List<ILSMComponent> components = lsmInitState.getOperationalComponents();
        int numComponents = lsmInitState.getNumComponents();
        rangeCursors = new OnDiskInvertedIndexRangeSearchCursor[numComponents];
        for (int i = 0; i < numComponents; i++) {
            IInvertedIndexAccessor invIndexAccessor =
                    (IInvertedIndexAccessor) components.get(i).getIndex().createAccessor(iap);
            rangeCursors[i] = (OnDiskInvertedIndexRangeSearchCursor) invIndexAccessor.createRangeSearchCursor();
            invIndexAccessor.rangeSearch(rangeCursors[i], lsmInitState.getSearchPredicate());
        }
        lsmHarness = lsmInitState.getLSMHarness();
        operationalComponents = lsmInitState.getOperationalComponents();
        deletedKeysBTreeAccessors = new IIndexAccessor[numComponents];
        bloomFilters = new BloomFilter[numComponents];
        deletedKeysBTreeCursors = new IIndexCursor[numComponents];
        for (int i = 0; i < numComponents; i++) {
            ILSMComponent component = operationalComponents.get(i);
            if (component.getType() == LSMComponentType.MEMORY) {
                // No need for a bloom filter for the in-memory BTree.
                deletedKeysBTreeAccessors[i] = ((LSMInvertedIndexMemoryComponent) component).getBuddyIndex()
                        .createAccessor(NoOpIndexAccessParameters.INSTANCE);
                bloomFilters[i] = null;
            } else {
                deletedKeysBTreeAccessors[i] = ((LSMInvertedIndexDiskComponent) component).getBuddyIndex()
                        .createAccessor(NoOpIndexAccessParameters.INSTANCE);
                bloomFilters[i] = ((LSMInvertedIndexDiskComponent) component).getBloomFilter();
            }
            deletedKeysBTreeCursors[i] = deletedKeysBTreeAccessors[i].createSearchCursor(false);
        }
        deletedKeyBTreeSearchPred = new RangePredicate(null, null, true, true, keyCmp, keyCmp);
        initPriorityQueues();
    }

    private void initPriorityQueues() throws HyracksDataException {
        int pqInitSize = (rangeCursors.length > 0) ? rangeCursors.length : 1;
        tokenQueue = new PriorityQueue<>(pqInitSize, tokenQueueCmp);
        keyQueue = new PriorityQueue<>(pqInitSize, keyQueueCmp);
        tokenQueueElements = new PriorityQueueElement[pqInitSize];
        keyQueueElements = new PriorityQueueElement[pqInitSize];
        for (int i = 0; i < pqInitSize; i++) {
            tokenQueueElements[i] = new PriorityQueueElement(i);
            keyQueueElements[i] = new PriorityQueueElement(i);
        }
        for (int i = 0; i < rangeCursors.length; i++) {
            if (rangeCursors[i].hasNext()) {
                rangeCursors[i].next();
                tokenQueueElements[i].reset(rangeCursors[i].getTuple());
                tokenQueue.offer(tokenQueueElements[i]);
            } else {
                rangeCursors[i].close();
            }
        }
        searchNextToken();
    }

    private void searchNextToken() throws HyracksDataException {
        if (tokenQueue.isEmpty()) {
            return;
        }
        if (!keyQueue.isEmpty()) {
            throw new IllegalStateException("Illegal call of initializing key queue");
        }
        outputTokenElement = tokenQueue.poll();
        initPushIntoKeyQueue(outputTokenElement);
        ITupleReference tokenTuple = getTokenTuple(outputTokenElement);
        outputTuple.setTokenTuple(tokenTuple);
        // pop all same tokens
        while (!tokenQueue.isEmpty()) {
            PriorityQueueElement tokenElement = tokenQueue.peek();
            if (TupleUtils.equalTuples(tokenTuple, getTokenTuple(tokenElement), tokenCmp.getKeyFieldCount())) {
                initPushIntoKeyQueue(tokenElement);
                tokenQueue.poll();
            } else {
                break;
            }
        }
    }

    private ITupleReference getKeyTuple(PriorityQueueElement tokenElement) {
        return ((TokenKeyPairTuple) tokenElement.getTuple()).getKeyTuple();
    }

    private ITupleReference getTokenTuple(PriorityQueueElement tokenElement) {
        return ((TokenKeyPairTuple) tokenElement.getTuple()).getTokenTuple();
    }

    private void initPushIntoKeyQueue(PriorityQueueElement tokenElement) {
        PriorityQueueElement keyElement = keyQueueElements[tokenElement.getCursorIndex()];
        keyElement.reset(getKeyTuple(tokenElement));
        keyQueue.add(keyElement);
    }

    private void pushIntoKeyQueueAndReplace(PriorityQueueElement keyElement) throws HyracksDataException {
        int cursorIndex = keyElement.getCursorIndex();
        if (rangeCursors[cursorIndex].hasNext()) {
            rangeCursors[cursorIndex].next();
            TokenKeyPairTuple tuple = (TokenKeyPairTuple) rangeCursors[cursorIndex].getTuple();
            if (tuple.isNewToken()) {
                // if this element is a new token, then the current inverted list has exuasted
                PriorityQueueElement tokenElement = tokenQueueElements[cursorIndex];
                tokenElement.reset(tuple);
                tokenQueue.offer(tokenElement);
            } else {
                keyElement.reset(tuple.getKeyTuple());
                keyQueue.offer(keyElement);
            }
        } else {
            rangeCursors[cursorIndex].close();
        }
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        checkPriorityQueue();
        return !keyQueue.isEmpty();
    }

    @Override
    public void doNext() throws HyracksDataException {
        outputKeyElement = keyQueue.poll();
        outputTuple.setKeyTuple(outputKeyElement.getTuple());
        needPushElementIntoKeyQueue = true;
    }

    @Override
    public ITupleReference doGetTuple() {
        return outputTuple;
    }

    protected void checkPriorityQueue() throws HyracksDataException {
        checkKeyQueue();
        if (keyQueue.isEmpty()) {
            // if key queue is empty, we search the next token and check again
            searchNextToken();
            checkKeyQueue();
        }
    }

    protected void checkKeyQueue() throws HyracksDataException {
        while (!keyQueue.isEmpty() || needPushElementIntoKeyQueue) {
            if (!keyQueue.isEmpty()) {
                PriorityQueueElement checkElement = keyQueue.peek();
                // If there is no previous tuple or the previous tuple can be ignored
                if (outputKeyElement == null) {
                    if (isDeleted(checkElement)) {
                        // If the key has been deleted then pop it and set needPush to true.
                        // We cannot push immediately because the tuple may be
                        // modified if hasNext() is called
                        outputKeyElement = checkElement;
                        needPushElementIntoKeyQueue = true;
                    } else {
                        // we have found the next record
                        return;
                    }
                } else {
                    // Compare the previous tuple and the head tuple in the PQ
                    if (keyCmp.compare(outputKeyElement.getTuple(), checkElement.getTuple()) == 0) {
                        // If the previous tuple and the head tuple are
                        // identical
                        // then pop the head tuple and push the next tuple from
                        // the tree of head tuple

                        // the head element of PQ is useless now
                        PriorityQueueElement e = keyQueue.poll();
                        pushIntoKeyQueueAndReplace(e);
                    } else {
                        // If the previous tuple and the head tuple are different
                        // the info of previous tuple is useless
                        if (needPushElementIntoKeyQueue) {
                            pushIntoKeyQueueAndReplace(outputKeyElement);
                            needPushElementIntoKeyQueue = false;
                        }
                        outputKeyElement = null;
                    }
                }
            } else {
                // the priority queue is empty and needPush
                // NOSONAR: outputKeyElement is not null when needPushElementIntoKeyQueue = true
                pushIntoKeyQueueAndReplace(outputKeyElement);
                needPushElementIntoKeyQueue = false;
                outputKeyElement = null;
            }
        }
    }

    /**
     * Check deleted-keys BTrees whether they contain the key in the checkElement's tuple.
     */
    protected boolean isDeleted(PriorityQueueElement keyElement) throws HyracksDataException {
        ITupleReference keyTuple = keyElement.getTuple();
        int end = keyElement.getCursorIndex();
        for (int i = 0; i < end; i++) {
            if (bloomFilters[i] != null && !bloomFilters[i].contains(keyTuple, hashes)) {
                continue;
            }
            deletedKeysBTreeCursors[i].close();
            deletedKeysBTreeAccessors[i].search(deletedKeysBTreeCursors[i], deletedKeyBTreeSearchPred);
            try {
                if (deletedKeysBTreeCursors[i].hasNext()) {
                    return true;
                }
            } finally {
                deletedKeysBTreeCursors[i].close();
            }
        }
        return false;
    }

    @Override
    public void doClose() throws HyracksDataException {
        outputTokenElement = null;
        outputKeyElement = null;
        needPushElementIntoKeyQueue = false;
        try {
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
    public void doDestroy() throws HyracksDataException {
        try {
            if (tokenQueue != null) {
                tokenQueue.clear();
            }
            if (keyQueue != null) {
                keyQueue.clear();
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
    public ITupleReference getFilterMinTuple() {
        return null;
    }

    @Override
    public ITupleReference getFilterMaxTuple() {
        return null;
    }

    @Override
    public boolean getSearchOperationCallbackProceedResult() {
        return false;
    }

}
