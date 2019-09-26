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
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInPlaceInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.InvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;

public class InMemoryInvertedIndex implements IInPlaceInvertedIndex {

    protected final BTree btree;
    protected final ITypeTraits[] tokenTypeTraits;
    protected final IBinaryComparatorFactory[] tokenCmpFactories;
    protected final ITypeTraits[] invListTypeTraits;
    protected final IBinaryComparatorFactory[] invListCmpFactories;
    protected final IBinaryTokenizerFactory tokenizerFactory;

    protected final ITypeTraits[] btreeTypeTraits;
    protected final IBinaryComparatorFactory[] btreeCmpFactories;

    public InMemoryInvertedIndex(IBufferCache virtualBufferCache, IPageManager virtualFreePageManager,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory, FileReference btreeFileRef) throws HyracksDataException {
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
        this.btree = BTreeUtils.createBTree(virtualBufferCache, virtualFreePageManager, btreeTypeTraits,
                btreeCmpFactories, BTreeLeafFrameType.REGULAR_NSM, btreeFileRef, false);
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
            throws HyracksDataException {
        InMemoryInvertedIndexOpContext ctx = (InMemoryInvertedIndexOpContext) ictx;
        ctx.getTupleIter().reset(tuple);
        while (ctx.getTupleIter().hasNext()) {
            ctx.getTupleIter().next();
            ITupleReference insertTuple = ctx.getTupleIter().getTuple();
            try {
                btreeAccessor.insert(insertTuple);
            } catch (HyracksDataException e) {
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    // This exception may be caused by duplicate tokens in the same insert "document".
                    // We ignore such duplicate tokens in all inverted-index implementations, hence
                    // we can safely ignore this exception.
                    throw e;
                }
            }
        }
    }

    public void delete(ITupleReference tuple, BTreeAccessor btreeAccessor, IIndexOperationContext ictx)
            throws HyracksDataException {
        InMemoryInvertedIndexOpContext ctx = (InMemoryInvertedIndexOpContext) ictx;
        ctx.getTupleIter().reset(tuple);
        while (ctx.getTupleIter().hasNext()) {
            ctx.getTupleIter().next();
            ITupleReference deleteTuple = ctx.getTupleIter().getTuple();
            try {
                btreeAccessor.delete(deleteTuple);
            } catch (HyracksDataException e) {
                if (e.getErrorCode() != ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                    // Ignore this exception, since a document may have duplicate tokens.
                    throw e;
                }
            }
        }
    }

    @Override
    public long getMemoryAllocationSize() {
        IBufferCache virtualBufferCache = btree.getBufferCache();
        return (long) virtualBufferCache.getPageBudget() * virtualBufferCache.getPageSize();
    }

    @Override
    public InvertedListCursor createInvertedListCursor(IHyracksTaskContext ctx) {
        return new InMemoryInvertedListCursor(invListTypeTraits.length, tokenTypeTraits.length);
    }

    @Override
    public InvertedListCursor createInvertedListRangeSearchCursor(IIndexCursorStats stats) {
        // An in-memory index does not have a separate inverted list.
        // Therefore, a different range-search cursor for an inverted list is not required.
        return createInvertedListCursor(null);
    }

    @Override
    public void openInvertedListCursor(InvertedListCursor listCursor, ITupleReference searchKey,
            IIndexOperationContext ictx) throws HyracksDataException {
        InMemoryInvertedIndexOpContext ctx = (InMemoryInvertedIndexOpContext) ictx;
        ctx.setOperation(IndexOperation.SEARCH);
        InMemoryInvertedListCursor inMemListCursor = (InMemoryInvertedListCursor) listCursor;
        inMemListCursor.prepare(ctx.getBtreeAccessor(), ctx.getBtreePred(), ctx.getTokenFieldsCmp(), ctx.getBtreeCmp());
        inMemListCursor.reset(searchKey);
        // Makes the cursor state to OPENED
        inMemListCursor.open(null, null);
    }

    @Override
    public InMemoryInvertedIndexAccessor createAccessor(IIndexAccessParameters iap) throws HyracksDataException {
        return new InMemoryInvertedIndexAccessor(this,
                new InMemoryInvertedIndexOpContext(btree, tokenCmpFactories, tokenizerFactory),
                (IHyracksTaskContext) iap.getParameters().get(HyracksConstants.HYRACKS_TASK_CONTEXT));
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
            boolean checkIfEmptyIndex, IPageWriteCallback callback) throws HyracksDataException {
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

    @Override
    public int getNumOfFilterFields() {
        return 0;
    }

    @Override
    public void purge() throws HyracksDataException {
        btree.purge();
    }

}
