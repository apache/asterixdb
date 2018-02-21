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
package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.tuples.ConcatenatingTupleReference;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInPlaceInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.InvertedListCursor;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;

/**
 * Scans a range of tokens, returning tuples containing a token and an inverted-list element.
 */
public class OnDiskInvertedIndexRangeSearchCursor extends EnforcedIndexCursor {

    private final BTree btree;
    private final IIndexAccessor btreeAccessor;
    private final IInPlaceInvertedIndex invIndex;
    private final IIndexOperationContext opCtx;
    private final InvertedListCursor invListRangeSearchCursor;
    private boolean isInvListCursorOpen;

    private final IIndexCursor btreeCursor;
    private RangePredicate btreePred;

    private final PermutingTupleReference tokenTuple;
    private ConcatenatingTupleReference concatTuple;

    public OnDiskInvertedIndexRangeSearchCursor(IInPlaceInvertedIndex invIndex, IIndexOperationContext opCtx)
            throws HyracksDataException {
        this.btree = ((OnDiskInvertedIndex) invIndex).getBTree();
        this.btreeAccessor = btree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        this.invIndex = invIndex;
        this.opCtx = opCtx;
        // Project away non-token fields of the BTree tuples.
        int[] fieldPermutation = new int[invIndex.getTokenTypeTraits().length];
        for (int i = 0; i < invIndex.getTokenTypeTraits().length; i++) {
            fieldPermutation[i] = i;
        }
        tokenTuple = new PermutingTupleReference(fieldPermutation);
        btreeCursor = btreeAccessor.createSearchCursor(false);
        concatTuple = new ConcatenatingTupleReference(2);
        invListRangeSearchCursor = invIndex.createInvertedListRangeSearchCursor();
        isInvListCursorOpen = false;
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        this.btreePred = (RangePredicate) searchPred;
        btreeAccessor.search(btreeCursor, btreePred);
        openInvListRangeSearchCursor();
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        // No more results possible
        if (!isInvListCursorOpen) {
            return false;
        }
        if (invListRangeSearchCursor.hasNext()) {
            return true;
        }
        // The current inverted-list-range-search cursor is exhausted.
        try {
            invListRangeSearchCursor.unloadPages();
        } finally {
            invListRangeSearchCursor.close();
        }
        isInvListCursorOpen = false;
        openInvListRangeSearchCursor();
        return isInvListCursorOpen;
    }

    @Override
    public void doNext() throws HyracksDataException {
        invListRangeSearchCursor.next();
        if (concatTuple.hasMaxTuples()) {
            concatTuple.removeLastTuple();
        }
        concatTuple.addTuple(invListRangeSearchCursor.getTuple());
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        try {
            if (isInvListCursorOpen) {
                try {
                    invListRangeSearchCursor.unloadPages();
                } finally {
                    isInvListCursorOpen = false;
                    invListRangeSearchCursor.destroy();
                }
            }
        } finally {
            btreeCursor.destroy();
        }
    }

    @Override
    public void doClose() throws HyracksDataException {
        try {
            if (isInvListCursorOpen) {
                try {
                    invListRangeSearchCursor.unloadPages();
                } finally {
                    invListRangeSearchCursor.close();
                }
                isInvListCursorOpen = false;
            }
        } finally {
            btreeCursor.close();
        }
    }

    @Override
    public ITupleReference doGetTuple() {
        return concatTuple;
    }

    // Opens an inverted-list-scan cursor for the given tuple.
    private void openInvListRangeSearchCursor() throws HyracksDataException {
        if (btreeCursor.hasNext()) {
            btreeCursor.next();
            tokenTuple.reset(btreeCursor.getTuple());
            invIndex.openInvertedListCursor(invListRangeSearchCursor, tokenTuple, opCtx);
            invListRangeSearchCursor.prepareLoadPages();
            invListRangeSearchCursor.loadPages();
            concatTuple.reset();
            concatTuple.addTuple(tokenTuple);
            isInvListCursorOpen = true;
        } else {
            isInvListCursorOpen = false;
        }
    }
}
