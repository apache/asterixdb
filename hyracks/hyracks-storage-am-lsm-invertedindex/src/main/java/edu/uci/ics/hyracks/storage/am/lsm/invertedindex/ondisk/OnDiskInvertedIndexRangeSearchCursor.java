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
package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.tuples.ConcatenatingTupleReference;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;

/**
 * Scans a range of tokens, returning tuples containing a token and an inverted-list element.
 */
public class OnDiskInvertedIndexRangeSearchCursor implements IIndexCursor {

    private final BTree btree;
    private final IIndexAccessor btreeAccessor;
    private final IInvertedIndex invIndex;
    private final IIndexOperationContext opCtx;
    private final IInvertedListCursor invListCursor;
    private boolean unpinNeeded;
    
    private final IIndexCursor btreeCursor;
    private RangePredicate btreePred;

    private final PermutingTupleReference tokenTuple;
    private ConcatenatingTupleReference concatTuple;

    public OnDiskInvertedIndexRangeSearchCursor(IInvertedIndex invIndex, IIndexOperationContext opCtx) {
        this.btree = ((OnDiskInvertedIndex) invIndex).getBTree();
        this.btreeAccessor = btree.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        this.invIndex = invIndex;
        this.opCtx = opCtx;
        // Project away non-token fields of the BTree tuples.
        int[] fieldPermutation = new int[invIndex.getTokenTypeTraits().length];
        for (int i = 0; i < invIndex.getTokenTypeTraits().length; i++) {
            fieldPermutation[i] = i;
        }
        tokenTuple = new PermutingTupleReference(fieldPermutation);
        btreeCursor = btreeAccessor.createSearchCursor();
        concatTuple = new ConcatenatingTupleReference(2);
        invListCursor = invIndex.createInvertedListCursor();
        unpinNeeded = false;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException, IndexException {
        this.btreePred = (RangePredicate) searchPred;
        try {
            btreeAccessor.search(btreeCursor, btreePred);
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }        
        invListCursor.pinPages();
        unpinNeeded = true;
    }

    @Override
    public boolean hasNext() throws HyracksDataException, IndexException {
        if (invListCursor.hasNext()) {
            return true;
        }
        if (unpinNeeded) {
            invListCursor.unpinPages();
            unpinNeeded = false;
        }
        if (!btreeCursor.hasNext()) {
            return false;
        }
        btreeCursor.next();
        tokenTuple.reset(btreeCursor.getTuple());
        try {
            invIndex.openInvertedListCursor(invListCursor, tokenTuple, opCtx);
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
        invListCursor.pinPages();
        invListCursor.hasNext();
        unpinNeeded = true;
        concatTuple.reset();
        concatTuple.addTuple(tokenTuple);
        return true;
    }

    @Override
    public void next() throws HyracksDataException {
        invListCursor.next();
        if (concatTuple.hasMaxTuples()) {
            concatTuple.removeLastTuple();
        }
        concatTuple.addTuple(invListCursor.getTuple());
    }

    @Override
    public void close() throws HyracksDataException {
        if (unpinNeeded) {
            invListCursor.unpinPages();
            unpinNeeded = false;
        }
        btreeCursor.close();
    }

    @Override
    public void reset() throws HyracksDataException, IndexException {
        if (unpinNeeded) {
            invListCursor.unpinPages();
            unpinNeeded = false;
        }
        btreeCursor.close();
    }

    @Override
    public ITupleReference getTuple() {
        return concatTuple;
    }
}
