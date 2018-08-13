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

package org.apache.hyracks.storage.am.lsm.invertedindex.inmemory;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.InvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexSearchCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.TOccurrenceSearcher;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class InMemoryInvertedIndexAccessor implements IInvertedIndexAccessor {
    protected final IHyracksTaskContext ctx;
    protected IInvertedIndexSearcher searcher;
    protected IIndexOperationContext opCtx;
    protected InMemoryInvertedIndex index;
    protected BTreeAccessor btreeAccessor;
    private boolean destroyed = false;

    public InMemoryInvertedIndexAccessor(InMemoryInvertedIndex index, IIndexOperationContext opCtx,
            IHyracksTaskContext ctx) throws HyracksDataException {
        this.ctx = ctx;
        this.opCtx = opCtx;
        this.index = index;
        // Searcher will be initialized when conducting an actual search.
        this.searcher = null;
        this.btreeAccessor = index.getBTree().createAccessor(NoOpIndexAccessParameters.INSTANCE);
    }

    @Override
    public void insert(ITupleReference tuple) throws HyracksDataException {
        opCtx.setOperation(IndexOperation.INSERT);
        index.insert(tuple, btreeAccessor, opCtx);
    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException {
        opCtx.setOperation(IndexOperation.DELETE);
        index.delete(tuple, btreeAccessor, opCtx);
    }

    @Override
    public IIndexCursor createSearchCursor(boolean exclusive) throws HyracksDataException {
        if (searcher == null) {
            searcher = createSearcher();
        }
        return new OnDiskInvertedIndexSearchCursor(searcher);
    }

    @Override
    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
        if (searcher == null) {
            searcher = createSearcher();
        }
        searcher.search(cursor, (InvertedIndexSearchPredicate) searchPred, opCtx);
    }

    @Override
    public InvertedListCursor createInvertedListCursor() {
        return index.createInvertedListCursor(ctx);
    }

    @Override
    public void openInvertedListCursor(InvertedListCursor listCursor, ITupleReference searchKey)
            throws HyracksDataException {
        index.openInvertedListCursor(listCursor, searchKey, opCtx);
    }

    @Override
    public IIndexCursor createRangeSearchCursor() {
        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) index.getBTree().getLeafFrameFactory().createFrame();
        return new BTreeRangeSearchCursor(leafFrame, false);
    }

    @Override
    public void rangeSearch(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
        btreeAccessor.search(cursor, searchPred);
    }

    public BTreeAccessor getBTreeAccessor() {
        return btreeAccessor;
    }

    @Override
    public void update(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException("Update not supported by in-memory inverted index.");
    }

    @Override
    public void upsert(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException("Upsert not supported by in-memory inverted index.");
    }

    protected IInvertedIndexSearcher createSearcher() throws HyracksDataException {
        if (ctx != null) {
            return new TOccurrenceSearcher(index, ctx);
        }
        return null;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (destroyed) {
            return;
        }
        destroyed = true;
        doDestroy();
    }

    private void doDestroy() throws HyracksDataException {
        try {
            btreeAccessor.destroy();
        } finally {
            opCtx.destroy();
        }
    }
}
