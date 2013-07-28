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
package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexNonExistentKeyException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class InMemoryInvertedIndex implements IInvertedIndex {

    protected final BTree btree;
    protected final ITypeTraits[] tokenTypeTraits;
    protected final IBinaryComparatorFactory[] tokenCmpFactories;
    protected final ITypeTraits[] invListTypeTraits;
    protected final IBinaryComparatorFactory[] invListCmpFactories;
    protected final IBinaryTokenizerFactory tokenizerFactory;

    protected final ITypeTraits[] btreeTypeTraits;
    protected final IBinaryComparatorFactory[] btreeCmpFactories;

    public InMemoryInvertedIndex(IBufferCache virtualBufferCache, IFreePageManager virtualFreePageManager,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory, FileReference btreeFileRef) throws BTreeException {
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenizerFactory = tokenizerFactory;
        // BTree tuples: <tokens, inverted-list elements>.
        int numBTreeFields = tokenTypeTraits.length + invListTypeTraits.length;
        btreeTypeTraits = new ITypeTraits[numBTreeFields];
        btreeCmpFactories = new IBinaryComparatorFactory[numBTreeFields];
        for (int i = 0; i < tokenTypeTraits.length; i++) {
            btreeTypeTraits[i] = tokenTypeTraits[i];
            btreeCmpFactories[i] = tokenCmpFactories[i];
        }
        for (int i = 0; i < invListTypeTraits.length; i++) {
            btreeTypeTraits[tokenTypeTraits.length + i] = invListTypeTraits[i];
            btreeCmpFactories[tokenTypeTraits.length + i] = invListCmpFactories[i];
        }
        this.btree = BTreeUtils.createBTree(virtualBufferCache, virtualFreePageManager,
                ((IVirtualBufferCache) virtualBufferCache).getFileMapProvider(), btreeTypeTraits, btreeCmpFactories,
                BTreeLeafFrameType.REGULAR_NSM, btreeFileRef);
    }

    @Override
    public void create() throws HyracksDataException {
        btree.create();
    }

    @Override
    public void activate() throws HyracksDataException {
        btree.activate();
    }

    @Override
    public void clear() throws HyracksDataException {
        btree.clear();
    }

    @Override
    public void deactivate() throws HyracksDataException {
        btree.deactivate();
    }

    @Override
    public void destroy() throws HyracksDataException {
        btree.destroy();
    }

    @Override
    public void validate() throws HyracksDataException {
        btree.validate();
    }

    public void insert(ITupleReference tuple, BTreeAccessor btreeAccessor, IIndexOperationContext ictx)
            throws HyracksDataException, IndexException {
        InMemoryInvertedIndexOpContext ctx = (InMemoryInvertedIndexOpContext) ictx;
        ctx.tupleIter.reset(tuple);
        while (ctx.tupleIter.hasNext()) {
            ctx.tupleIter.next();
            ITupleReference insertTuple = ctx.tupleIter.getTuple();
            try {
                btreeAccessor.insert(insertTuple);
            } catch (TreeIndexDuplicateKeyException e) {
                // This exception may be caused by duplicate tokens in the same insert "document".
                // We ignore such duplicate tokens in all inverted-index implementations, hence
                // we can safely ignore this exception.
            }
        }
    }

    public void delete(ITupleReference tuple, BTreeAccessor btreeAccessor, IIndexOperationContext ictx)
            throws HyracksDataException, IndexException {
        InMemoryInvertedIndexOpContext ctx = (InMemoryInvertedIndexOpContext) ictx;
        ctx.tupleIter.reset(tuple);
        while (ctx.tupleIter.hasNext()) {
            ctx.tupleIter.next();
            ITupleReference deleteTuple = ctx.tupleIter.getTuple();
            try {
                btreeAccessor.delete(deleteTuple);
            } catch (TreeIndexNonExistentKeyException e) {
                // Ignore this exception, since a document may have duplicate tokens.
            }
        }
    }

    @Override
    public long getMemoryAllocationSize() {
        IBufferCache virtualBufferCache = btree.getBufferCache();
        return virtualBufferCache.getNumPages() * virtualBufferCache.getPageSize();
    }

    @Override
    public IInvertedListCursor createInvertedListCursor() {
        return new InMemoryInvertedListCursor(invListTypeTraits.length, tokenTypeTraits.length);
    }

    @Override
    public void openInvertedListCursor(IInvertedListCursor listCursor, ITupleReference searchKey,
            IIndexOperationContext ictx) throws HyracksDataException, IndexException {
        InMemoryInvertedIndexOpContext ctx = (InMemoryInvertedIndexOpContext) ictx;
        ctx.setOperation(IndexOperation.SEARCH);
        InMemoryInvertedListCursor inMemListCursor = (InMemoryInvertedListCursor) listCursor;
        inMemListCursor.prepare(ctx.btreeAccessor, ctx.btreePred, ctx.tokenFieldsCmp, ctx.btreeCmp);
        inMemListCursor.reset(searchKey);
    }

    @Override
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) throws HyracksDataException {
        return new InMemoryInvertedIndexAccessor(this, new InMemoryInvertedIndexOpContext(btree, tokenCmpFactories,
                tokenizerFactory));
    }

    @Override
    public IBufferCache getBufferCache() {
        return btree.getBufferCache();
    }

    public BTree getBTree() {
        return btree;
    }

    @Override
    public IBinaryComparatorFactory[] getInvListCmpFactories() {
        return invListCmpFactories;
    }

    @Override
    public ITypeTraits[] getInvListTypeTraits() {
        return invListTypeTraits;
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws IndexException {
        throw new UnsupportedOperationException("Bulk load not supported by in-memory inverted index.");
    }

    @Override
    public ITypeTraits[] getTokenTypeTraits() {
        return tokenTypeTraits;
    }

    @Override
    public IBinaryComparatorFactory[] getTokenCmpFactories() {
        return tokenCmpFactories;
    }
}
