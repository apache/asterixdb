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

package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearcher;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

/**
 * An inverted index consists of two files: 1. a file storing (paginated)
 * inverted lists 2. a BTree-file mapping from tokens to inverted lists.
 * Implemented features: bulk loading and searching (based on T-Occurrence) Not
 * implemented features: updates (insert/update/delete) Limitations: a query
 * cannot exceed the size of a Hyracks frame.
 */
public class InvertedIndex implements IIndex {
    private final IHyracksCommonContext ctx = new DefaultHyracksCommonContext();

    private BTree btree;
    private int rootPageId = 0;
    private IBufferCache bufferCache;
    private int fileId;
    private final ITypeTraits[] invListTypeTraits;
    private final IBinaryComparatorFactory[] invListCmpFactories;
    private final IInvertedListBuilder invListBuilder;
    private final int numTokenFields;
    private final int numInvListKeys;

    public InvertedIndex(IBufferCache bufferCache, BTree btree, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, IInvertedListBuilder invListBuilder) {
        this.bufferCache = bufferCache;
        this.btree = btree;
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.invListBuilder = invListBuilder;
        this.numTokenFields = btree.getComparatorFactories().length;
        this.numInvListKeys = invListCmpFactories.length;
    }

    @Override
    public void open(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public void create(int indexFileId) throws HyracksDataException {
    }

    @Override
    public void close() {
        this.fileId = -1;
    }

    public boolean openCursor(ITreeIndexCursor btreeCursor, RangePredicate btreePred, ITreeIndexAccessor btreeAccessor,
            IInvertedListCursor invListCursor) throws HyracksDataException, IndexException {
        btreeAccessor.search(btreeCursor, btreePred);
        boolean ret = false;
        try {
            if (btreeCursor.hasNext()) {
                btreeCursor.next();
                ITupleReference frameTuple = btreeCursor.getTuple();
                // Hardcoded mapping of btree fields
                int startPageId = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(1),
                        frameTuple.getFieldStart(1));
                int endPageId = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(2),
                        frameTuple.getFieldStart(2));
                int startOff = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(3),
                        frameTuple.getFieldStart(3));
                int numElements = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(4),
                        frameTuple.getFieldStart(4));
                invListCursor.reset(startPageId, endPageId, startOff, numElements);
                ret = true;
            } else {
                invListCursor.reset(0, 0, 0, 0);
            }
        } finally {
            btreeCursor.close();
            btreeCursor.reset();
        }
        return ret;
    }

    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws TreeIndexException, HyracksDataException {
        InvertedIndexBulkLoadContext ctx = new InvertedIndexBulkLoadContext(fillFactor);
        ctx.init(rootPageId, fileId);
        return ctx;
    }

    /**
     * Assumptions:
     * The first btree.getMultiComparator().getKeyFieldCount() fields in tuple
     * are btree keys (e.g., a string token).
     * The next invListCmp.getKeyFieldCount() fields in tuple are keys of the
     * inverted list (e.g., primary key).
     * Key fields of inverted list are fixed size.
     */
    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        InvertedIndexBulkLoadContext ctx = (InvertedIndexBulkLoadContext) ictx;
        boolean firstElement = ctx.lastTupleBuilder.getSize() == 0;
        boolean startNewList = firstElement;
        if (!firstElement) {
            // If the current and the last token don't match, we start a new list.
            ctx.lastTuple.reset(ctx.lastTupleBuilder.getFieldEndOffsets(), ctx.lastTupleBuilder.getByteArray());
            startNewList = ctx.tokenCmp.compare(tuple, ctx.lastTuple) != 0;
        }
        if (startNewList) {
            if (!firstElement) {
                // Create entry in btree for last inverted list.
                createAndInsertBTreeTuple(ctx);
            }
            if (!invListBuilder.startNewList(tuple, numTokenFields)) {
                ctx.pinNextPage();
                invListBuilder.setTargetBuffer(ctx.currentPage.getBuffer().array(), 0);
                if (!invListBuilder.startNewList(tuple, numTokenFields)) {
                    throw new IllegalStateException("Failed to create first inverted list.");
                }
            }
            ctx.currentInvListStartPageId = ctx.currentPageId;
            ctx.currentInvListStartOffset = invListBuilder.getPos();
        } else {
            if (ctx.invListCmp.compare(tuple, ctx.lastTuple, numTokenFields) == 0) {
                // Duplicate inverted-list element.
                return;
            }
        }

        // Append to current inverted list.
        if (!invListBuilder.appendElement(tuple, numTokenFields, numInvListKeys)) {
            ctx.pinNextPage();
            invListBuilder.setTargetBuffer(ctx.currentPage.getBuffer().array(), 0);
            if (!invListBuilder.appendElement(tuple, numTokenFields, numInvListKeys)) {
                throw new IllegalStateException(
                        "Failed to append element to inverted list after switching to a new page.");
            }
        }

        // Remember last tuple by creating a copy.
        // TODO: This portion can be optimized by only copying the token when it changes, and using the last appended inverted-list element as a reference.
        ctx.lastTupleBuilder.reset();
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            ctx.lastTupleBuilder.addField(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
        }
    }

    private void createAndInsertBTreeTuple(InvertedIndexBulkLoadContext ctx) throws HyracksDataException {
        // Build tuple.        
        ctx.btreeTupleBuilder.reset();
        ctx.btreeTupleBuilder.addField(ctx.lastTuple.getFieldData(0), ctx.lastTuple.getFieldStart(0),
                ctx.lastTuple.getFieldLength(0));
        ctx.btreeTupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, ctx.currentInvListStartPageId);
        ctx.btreeTupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, ctx.currentPageId);
        ctx.btreeTupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, ctx.currentInvListStartOffset);
        ctx.btreeTupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, invListBuilder.getListSize());
        // Reset tuple reference and add it.
        ctx.btreeTupleReference.reset(ctx.btreeTupleBuilder.getFieldEndOffsets(), ctx.btreeTupleBuilder.getByteArray());
        btree.bulkLoadAddTuple(ctx.btreeTupleReference, ctx.btreeBulkLoadCtx);
    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
        // Create entry in btree for last inverted list.
        InvertedIndexBulkLoadContext ctx = (InvertedIndexBulkLoadContext) ictx;
        if (ctx.lastTuple.getFieldData(0) != null) {
            createAndInsertBTreeTuple(ctx);
        }
        btree.endBulkLoad(ctx.btreeBulkLoadCtx);
        ctx.deinit();
    }

    public final class InvertedIndexBulkLoadContext implements IIndexBulkLoadContext {
        private final ArrayTupleBuilder btreeTupleBuilder;
        private final ArrayTupleReference btreeTupleReference;
        private final float btreeFillFactor;
        private IIndexBulkLoadContext btreeBulkLoadCtx;

        private int currentInvListStartPageId;
        private int currentInvListStartOffset;
        private final ArrayTupleBuilder lastTupleBuilder;
        private final ArrayTupleReference lastTuple;

        private int currentPageId;
        private ICachedPage currentPage;
        private final MultiComparator tokenCmp;
        private final MultiComparator invListCmp;

        public InvertedIndexBulkLoadContext(float btreeFillFactor) {
            this.tokenCmp = MultiComparator.create(btree.getComparatorFactories());
            this.invListCmp = MultiComparator.create(invListCmpFactories);
            this.btreeTupleBuilder = new ArrayTupleBuilder(btree.getFieldCount());
            this.btreeTupleReference = new ArrayTupleReference();
            this.btreeFillFactor = btreeFillFactor;
            this.lastTupleBuilder = new ArrayTupleBuilder(numTokenFields + numInvListKeys);
            this.lastTuple = new ArrayTupleReference();
        }

        public void init(int startPageId, int fileId) throws HyracksDataException, TreeIndexException {
            btreeBulkLoadCtx = btree.beginBulkLoad(btreeFillFactor);
            currentPageId = startPageId;
            currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), true);
            currentPage.acquireWriteLatch();
            invListBuilder.setTargetBuffer(currentPage.getBuffer().array(), 0);
        }

        public void deinit() throws HyracksDataException {
            if (currentPage != null) {
                currentPage.releaseWriteLatch();
                bufferCache.unpin(currentPage);
            }
        }

        public void pinNextPage() throws HyracksDataException {
            currentPage.releaseWriteLatch();
            bufferCache.unpin(currentPage);
            currentPageId++;
            currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), true);
            currentPage.acquireWriteLatch();
        }
    }

    @Override
    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public int getInvListsFileId() {
        return fileId;
    }

    public IBinaryComparatorFactory[] getInvListElementCmpFactories() {
        return invListCmpFactories;
    }

    public ITypeTraits[] getTypeTraits() {
        return invListTypeTraits;
    }

    public BTree getBTree() {
        return btree;
    }

    public class InvertedIndexAccessor implements IIndexAccessor {
        private final IInvertedIndexSearcher searcher;

        public InvertedIndexAccessor(InvertedIndex index) {
            this.searcher = new TOccurrenceSearcher(ctx, index);
        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException, IndexException {
            // TODO Auto-generated method stub
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException, IndexException {
            // TODO Auto-generated method stub
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException, IndexException {
            // TODO Auto-generated method stub
        }

        @Override
        public void upsert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            // TODO Auto-generated method stub
        }

        @Override
        public IIndexCursor createSearchCursor() {
            return new InvertedIndexSearchCursor(searcher);
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException,
                IndexException {
            searcher.search((InvertedIndexSearchCursor) cursor, (InvertedIndexSearchPredicate) searchPred);
        }

        public IInvertedIndexSearcher getSearcher() {
            return searcher;
        }
    }

    @Override
    public IIndexAccessor createAccessor() {
        return new InvertedIndexAccessor(this);
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.INVERTED;
    }

    // This is just a dummy hyracks context for allocating frames for temporary
    // results during inverted index searches.
    // TODO: In the future we should use the real HyracksTaskContext to track
    // frame usage.
    private class DefaultHyracksCommonContext implements IHyracksCommonContext {
        private final int FRAME_SIZE = 32768;

        @Override
        public int getFrameSize() {
            return FRAME_SIZE;
        }

        @Override
        public IIOManager getIOManager() {
            return null;
        }

        @Override
        public ByteBuffer allocateFrame() {
            return ByteBuffer.allocate(FRAME_SIZE);
        }
    }
}
