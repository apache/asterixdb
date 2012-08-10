/*
 * Copyright 2009-2012 by The Regents of the University of California
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

import java.io.File;
import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IToken;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class InMemoryBtreeInvertedIndex implements IInvertedIndex {

    private final BTree btree;
    private final FileReference memBTreeFile = new FileReference(new File("memBTree"));
    private final ITypeTraits[] tokenTypeTraits;
    private final IBinaryComparatorFactory[] tokenCmpFactories;
    private final ITypeTraits[] invListTypeTraits;
    private final IBinaryComparatorFactory[] invListCmpFactories;
    private final IBinaryTokenizer tokenizer;

    private final ITypeTraits[] btreeTypeTraits;
    private final IBinaryComparatorFactory[] btreeCmpFactories;

    public InMemoryBtreeInvertedIndex(IBufferCache memBufferCache, IFreePageManager memFreePageManager,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizer tokenizer)
            throws BTreeException {
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenizer = tokenizer;
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
        this.btree = BTreeUtils.createBTree(memBufferCache,
                ((InMemoryBufferCache) memBufferCache).getFileMapProvider(), btreeTypeTraits, btreeCmpFactories,
                BTreeLeafFrameType.REGULAR_NSM, memBTreeFile);
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

    public boolean insert(ITupleReference tuple, BTreeAccessor btreeAccessor, IIndexOpContext ictx)
            throws HyracksDataException, IndexException {
        InMemoryBtreeInvertedIndexOpContext ctx = (InMemoryBtreeInvertedIndexOpContext) ictx;
        // TODO: We can possibly avoid copying the data into a new tuple here.
        tokenizer.reset(tuple.getFieldData(0), tuple.getFieldStart(0), tuple.getFieldLength(0));
        while (tokenizer.hasNext()) {
            tokenizer.next();
            IToken token = tokenizer.getToken();
            ctx.btreeTupleBuilder.reset();
            // Add token field.
            try {
                token.serializeToken(ctx.btreeTupleBuilder.getDataOutput());
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            ctx.btreeTupleBuilder.addFieldEndOffset();
            // Add inverted-list element fields.
            for (int i = 0; i < invListTypeTraits.length; i++) {
                ctx.btreeTupleBuilder.addField(tuple.getFieldData(i + 1), tuple.getFieldStart(i + 1),
                        tuple.getFieldLength(i + 1));
            }
            // Reset tuple reference for insert operation.
            ctx.btreeTupleReference.reset(ctx.btreeTupleBuilder.getFieldEndOffsets(),
                    ctx.btreeTupleBuilder.getByteArray());
            try {
                btreeAccessor.insert(ctx.btreeTupleReference);
            } catch (BTreeDuplicateKeyException e) {
                // This exception may be caused by duplicate tokens in the same insert "document".
                // We ignore such duplicate tokens in all inverted-index implementations, hence
                // we can safely ignore this exception.
            }
        }
        return true;
    }

    @Override
    public long getInMemorySize() {
        InMemoryBufferCache memBufferCache = (InMemoryBufferCache) btree.getBufferCache();
        return memBufferCache.getNumPages() * memBufferCache.getPageSize();
    }

    @Override
    public IInvertedListCursor createInvertedListCursor() {
        return new InMemoryBtreeInvertedListCursor(invListTypeTraits.length, tokenTypeTraits.length);
    }

    @Override
    public void openInvertedListCursor(IInvertedListCursor listCursor, ITupleReference tupleReference, IIndexOpContext ictx)
            throws HyracksDataException, IndexException {
        InMemoryBtreeInvertedIndexOpContext ctx = (InMemoryBtreeInvertedIndexOpContext) ictx;
        InMemoryBtreeInvertedListCursor inMemListCursor = (InMemoryBtreeInvertedListCursor) listCursor;
        inMemListCursor.prepare(ctx.btreeAccessor, ctx.btreePred, ctx.tokenFieldsCmp, ctx.btreeCmp);
        
        
        inMemListCursor.reset(tupleReference);
    }

    @Override
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new InMemoryBtreeInvertedIndexAccessor(this, new InMemoryBtreeInvertedIndexOpContext(
                btree, tokenCmpFactories), tokenizer);
    }

    @Override
    public IBufferCache getBufferCache() {
        return null;
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.INVERTED;
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
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput) throws IndexException {
        throw new UnsupportedOperationException("Bulk load not supported by in-memory inverted index.");
    }
}
