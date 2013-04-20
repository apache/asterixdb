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

import java.util.ListIterator;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BloomFilterAwareBTreePointSearchCursor;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMBTreePointSearchCursor implements ITreeIndexCursor {

    private IIndexCursor[] rangeCursors;
    private final ILSMIndexOperationContext opCtx;
    private ISearchOperationCallback searchCallback;
    private RangePredicate predicate;
    private IIndexAccessor memBTreeAccessor;
    private boolean includeMemComponent;
    private int numBTrees;
    private IIndexAccessor[] bTreeAccessors;
    private ILSMHarness lsmHarness;
    private boolean nextHasBeenCalled;
    private boolean foundTuple;
    private ITupleReference frameTuple;

    public LSMBTreePointSearchCursor(ILSMIndexOperationContext opCtx) {
        this.opCtx = opCtx;
    }

    @Override
    public boolean hasNext() throws HyracksDataException, IndexException {
        if (nextHasBeenCalled) {
            return false;
        } else if (foundTuple) {
            return true;
        }
        boolean reconciled = false;
        for (int i = 0; i < numBTrees; ++i) {
            bTreeAccessors[i].search(rangeCursors[i], predicate);
            if (rangeCursors[i].hasNext()) {
                rangeCursors[i].next();
                // We use the predicate's to lock the key instead of the tuple that we get from cursor to avoid copying the tuple when we do the "unlatch dance"
                if (reconciled || searchCallback.proceed(predicate.getLowKey())) {
                    // if proceed is successful, then there's no need for doing the "unlatch dance"
                    if (((ILSMTreeTupleReference) rangeCursors[i].getTuple()).isAntimatter()) {
                        searchCallback.cancel(predicate.getLowKey());
                        rangeCursors[i].close();
                        return false;
                    } else {
                        frameTuple = rangeCursors[i].getTuple();
                        foundTuple = true;
                        return true;
                    }
                }
                if (i == 0 && includeMemComponent) {
                    // unlatch/unpin
                    rangeCursors[i].reset();
                    searchCallback.reconcile(predicate.getLowKey());
                    reconciled = true;

                    // retraverse
                    memBTreeAccessor.search(rangeCursors[i], predicate);
                    searchCallback.complete(predicate.getLowKey());
                    if (rangeCursors[i].hasNext()) {
                        rangeCursors[i].next();
                        if (((ILSMTreeTupleReference) rangeCursors[i].getTuple()).isAntimatter()) {
                            searchCallback.cancel(predicate.getLowKey());
                            rangeCursors[i].close();
                            return false;
                        } else {
                            frameTuple = rangeCursors[i].getTuple();
                            foundTuple = true;
                            return true;
                        }
                    } else {
                        rangeCursors[i].close();
                    }
                } else {
                    frameTuple = rangeCursors[i].getTuple();
                    searchCallback.reconcile(frameTuple);
                    searchCallback.complete(frameTuple);
                    foundTuple = true;
                    return true;
                }
            } else {
                rangeCursors[i].close();
            }
        }
        return false;
    }

    @Override
    public void reset() throws HyracksDataException, IndexException {
        if (rangeCursors != null) {
            for (int i = 0; i < rangeCursors.length; ++i) {
                rangeCursors[i].reset();
            }
        }
        rangeCursors = null;
        nextHasBeenCalled = false;
        foundTuple = false;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        includeMemComponent = lsmInitialState.getIncludeMemComponent();
        lsmHarness = lsmInitialState.getLSMHarness();
        searchCallback = lsmInitialState.getSearchOperationCallback();
        memBTreeAccessor = lsmInitialState.getMemBTreeAccessor();
        predicate = (RangePredicate) lsmInitialState.getSearchPredicate();

        numBTrees = lsmInitialState.getNumBTrees();
        rangeCursors = new IIndexCursor[numBTrees];
        int i = 0;
        if (includeMemComponent) {
            // No need for a bloom filter for the in-memory BTree.
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getLeafFrameFactory().createFrame();
            rangeCursors[i] = new BTreeRangeSearchCursor(leafFrame, false);
            ++i;
        }
        for (; i < numBTrees; ++i) {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getLeafFrameFactory().createFrame();
            rangeCursors[i] = new BloomFilterAwareBTreePointSearchCursor(leafFrame, false,
                    ((LSMBTreeImmutableComponent) lsmInitialState.getOperationalComponents().get(i)).getBloomFilter());
        }

        bTreeAccessors = new IIndexAccessor[numBTrees];
        int cursorIx = 0;
        ListIterator<ILSMComponent> btreesIter = lsmInitialState.getOperationalComponents().listIterator();
        if (includeMemComponent) {
            bTreeAccessors[cursorIx] = memBTreeAccessor;
            ++cursorIx;
            btreesIter.next();
        }

        while (btreesIter.hasNext()) {
            BTree diskBTree = ((LSMBTreeImmutableComponent) btreesIter.next()).getBTree();
            bTreeAccessors[cursorIx] = diskBTree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            cursorIx++;
        }
        nextHasBeenCalled = false;
        foundTuple = false;
    }

    @Override
    public void next() throws HyracksDataException {
        nextHasBeenCalled = true;
    }

    @Override
    public void close() throws HyracksDataException {
        if (lsmHarness != null) {
            try {
                for (int i = 0; i < rangeCursors.length; i++) {
                    rangeCursors[i].close();
                }
                rangeCursors = null;
            } finally {
                lsmHarness.endSearch(opCtx);
            }
        }
        nextHasBeenCalled = false;
        foundTuple = false;
    }

    @Override
    public ITupleReference getTuple() {
        return frameTuple;
    }

    @Override
    public ICachedPage getPage() {
        // do nothing
        return null;
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
    public boolean exclusiveLatchNodes() {
        return false;
    }
}