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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearcher;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex.DefaultHyracksCommonContext;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndexSearchCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndexSearchPredicate;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.TOccurrenceSearcher;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;

public class InMemoryBtreeInvertedIndexAccessor implements IIndexAccessor {
    // TODO: This ctx needs to go away.
    protected final IHyracksCommonContext hyracksCtx = new DefaultHyracksCommonContext();
    protected final IInvertedIndexSearcher searcher;
    protected IIndexOpContext opCtx;
    protected InMemoryBtreeInvertedIndex memoryBtreeInvertedIndex;
    protected BTreeAccessor btreeAccessor;

    public InMemoryBtreeInvertedIndexAccessor(InMemoryBtreeInvertedIndex memoryBtreeInvertedIndex,
            IIndexOpContext opCtx, IBinaryTokenizer tokenizer) {
        this.opCtx = opCtx;
        this.memoryBtreeInvertedIndex = memoryBtreeInvertedIndex;
        this.searcher = new TOccurrenceSearcher(hyracksCtx, memoryBtreeInvertedIndex);
        // TODO: Ignore opcallbacks for now.
        this.btreeAccessor = (BTreeAccessor) memoryBtreeInvertedIndex.getBTree().createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
    }

    public void insert(ITupleReference tuple) throws HyracksDataException, IndexException {
        opCtx.reset(IndexOp.INSERT);
        memoryBtreeInvertedIndex.insert(tuple, btreeAccessor, opCtx);
    }

    @Override
    public IIndexCursor createSearchCursor() {
        return new InvertedIndexSearchCursor(searcher);
    }

    @Override
    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException, IndexException {
        searcher.search((InvertedIndexSearchCursor) cursor, (InvertedIndexSearchPredicate) searchPred, opCtx);
    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("Delete not supported by in-memory inverted index.");
    }

    @Override
    public void update(ITupleReference tuple) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("Update not supported by in-memory inverted index.");
    }

    @Override
    public void upsert(ITupleReference tuple) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("Upsert not supported by in-memory inverted index.");
    }
}
