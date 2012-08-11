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

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

/**
 * An inverted index consists of two files: 1. a file storing (paginated)
 * inverted lists 2. a BTree-file mapping from tokens to inverted lists.
 * Implemented features: bulk loading and searching (based on T-Occurrence) Not
 * implemented features: updates (insert/update/delete) Limitations: a query
 * cannot exceed the size of a Hyracks frame.
 */
public class InvertedIndex implements IInvertedIndex {
    private final IHyracksCommonContext ctx = new DefaultHyracksCommonContext();

    // Schema of BTree tuples.
    public final int TOKEN_FIELD = 0;
    public final int INVLIST_START_PAGE_ID_FIELD = 1;
    public final int INVLIST_END_PAGE_ID_FIELD = 2;
    public final int INVLIST_START_OFF_FIELD = 3;
    public final int INVLIST_NUM_ELEMENTS_FIELD = 4;
    
    private BTree btree;
    private int rootPageId = 0;
    private IBufferCache bufferCache;
    private IFileMapProvider fileMapProvider;
    private int fileId = -1;
    private final ITypeTraits[] invListTypeTraits;
    private final IBinaryComparatorFactory[] invListCmpFactories;
    private final IInvertedListBuilder invListBuilder;
    private final int numTokenFields;
    private final int numInvListKeys;
    private final FileReference invListsFile;
    
    private boolean isOpen = false;

    public InvertedIndex(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            IInvertedListBuilder invListBuilder, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, FileReference btreeFile, FileReference invListsFile)
            throws IndexException {
        this.bufferCache = bufferCache;
        this.fileMapProvider = fileMapProvider;
        this.invListBuilder = invListBuilder;
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.btree = BTreeUtils.createBTree(bufferCache, fileMapProvider,
                InvertedIndexUtils.getBTreeTypeTraits(tokenTypeTraits), tokenCmpFactories,
                BTreeLeafFrameType.REGULAR_NSM, btreeFile);
        this.numTokenFields = btree.getComparatorFactories().length;
        this.numInvListKeys = invListCmpFactories.length;
        this.invListsFile = invListsFile;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        if (isOpen) {
            throw new HyracksDataException("Failed to create since index is already open.");
        }
        btree.create();

        boolean fileIsMapped = false;
        synchronized (fileMapProvider) {
            fileIsMapped = fileMapProvider.isMapped(invListsFile);
            if (!fileIsMapped) {
                bufferCache.createFile(invListsFile);
            }
            fileId = fileMapProvider.lookupFileId(invListsFile);
            try {
                // Also creates the file if it doesn't exist yet.
                bufferCache.openFile(fileId);
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }
        bufferCache.closeFile(fileId);
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        if (isOpen) {
            return;
        }

        btree.activate();
        boolean fileIsMapped = false;
        synchronized (fileMapProvider) {
            fileIsMapped = fileMapProvider.isMapped(invListsFile);
            if (!fileIsMapped) {
                bufferCache.createFile(invListsFile);
            }
            fileId = fileMapProvider.lookupFileId(invListsFile);
            try {
                // Also creates the file if it doesn't exist yet.
                bufferCache.openFile(fileId);
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }

        isOpen = true;
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        if (!isOpen) {
            return;
        }

        btree.deactivate();
        bufferCache.closeFile(fileId);

        isOpen = false;
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        if (isOpen) {
            throw new HyracksDataException("Failed to destroy since index is already open.");
        }

        btree.destroy();
        invListsFile.getFile().delete();
        if (fileId == -1) {
            return;
        }

        bufferCache.deleteFile(fileId, false);
        fileId = -1;
    }

    @Override
    public synchronized void clear() throws HyracksDataException {
        if (!isOpen) {
            throw new HyracksDataException("Failed to clear since index is not open.");
        }
        btree.clear();
        bufferCache.closeFile(fileId);
        bufferCache.deleteFile(fileId, false);
        invListsFile.getFile().delete();

        boolean fileIsMapped = false;
        synchronized (fileMapProvider) {
            fileIsMapped = fileMapProvider.isMapped(invListsFile);
            if (!fileIsMapped) {
                bufferCache.createFile(invListsFile);
            }
            fileId = fileMapProvider.lookupFileId(invListsFile);
            try {
                // Also creates the file if it doesn't exist yet.
                bufferCache.openFile(fileId);
            } catch (HyracksDataException e) {
                // Revert state of buffer cache since file failed to open.
                if (!fileIsMapped) {
                    bufferCache.deleteFile(fileId, false);
                }
                throw e;
            }
        }
    }

    @Override
    public void openInvertedListCursor(IInvertedListCursor listCursor, ITupleReference searchKey, IIndexOpContext ictx)
            throws HyracksDataException, IndexException {
        InvertedIndexOpContext ctx = (InvertedIndexOpContext) ictx;
        ctx.btreePred.setLowKeyComparator(ctx.searchCmp);
        ctx.btreePred.setHighKeyComparator(ctx.searchCmp);
        ctx.btreePred.setLowKey(searchKey, true);
        ctx.btreePred.setHighKey(searchKey, true);
        ctx.btreeAccessor.search(ctx.btreeCursor, ctx.btreePred);
        try {
            if (ctx.btreeCursor.hasNext()) {
                ctx.btreeCursor.next();
                ITupleReference frameTuple = ctx.btreeCursor.getTuple();
                int startPageId = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(INVLIST_START_PAGE_ID_FIELD),
                        frameTuple.getFieldStart(INVLIST_START_PAGE_ID_FIELD));
                int endPageId = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(INVLIST_END_PAGE_ID_FIELD),
                        frameTuple.getFieldStart(INVLIST_END_PAGE_ID_FIELD));
                int startOff = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(INVLIST_START_OFF_FIELD),
                        frameTuple.getFieldStart(INVLIST_START_OFF_FIELD));
                int numElements = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(INVLIST_NUM_ELEMENTS_FIELD),
                        frameTuple.getFieldStart(INVLIST_NUM_ELEMENTS_FIELD));
                listCursor.reset(startPageId, endPageId, startOff, numElements);
            } else {
                listCursor.reset(0, 0, 0, 0);
            }
        } finally {
            ctx.btreeCursor.close();
            ctx.btreeCursor.reset();
        }
    }
    
    public final class InvertedIndexBulkLoader implements IIndexBulkLoader {
        private final ArrayTupleBuilder btreeTupleBuilder;
        private final ArrayTupleReference btreeTupleReference;
        private final IIndexBulkLoader btreeBulkloader;

        private int currentInvListStartPageId;
        private int currentInvListStartOffset;
        private final ArrayTupleBuilder lastTupleBuilder;
        private final ArrayTupleReference lastTuple;

        private int currentPageId;
        private ICachedPage currentPage;
        private final MultiComparator tokenCmp;
        private final MultiComparator invListCmp;

        public InvertedIndexBulkLoader(float btreeFillFactor, boolean verifyInput, int startPageId, int fileId)
                throws IndexException, HyracksDataException {
            this.tokenCmp = MultiComparator.create(btree.getComparatorFactories());
            this.invListCmp = MultiComparator.create(invListCmpFactories);
            this.btreeTupleBuilder = new ArrayTupleBuilder(btree.getFieldCount());
            this.btreeTupleReference = new ArrayTupleReference();
            this.lastTupleBuilder = new ArrayTupleBuilder(numTokenFields + numInvListKeys);
            this.lastTuple = new ArrayTupleReference();
            this.btreeBulkloader = btree.createBulkLoader(btreeFillFactor, verifyInput);
            currentPageId = startPageId;
            currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), true);
            currentPage.acquireWriteLatch();
            invListBuilder.setTargetBuffer(currentPage.getBuffer().array(), 0);
        }

        public void pinNextPage() throws HyracksDataException {
            currentPage.releaseWriteLatch();
            bufferCache.unpin(currentPage);
            currentPageId++;
            currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), true);
            currentPage.acquireWriteLatch();
        }

        private void createAndInsertBTreeTuple() throws IndexException, HyracksDataException {
            // Build tuple.        
            btreeTupleBuilder.reset();
            btreeTupleBuilder.addField(lastTuple.getFieldData(0), lastTuple.getFieldStart(0),
                    lastTuple.getFieldLength(0));
            // TODO: Boxing integers here. Fix it.
            btreeTupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, currentInvListStartPageId);
            btreeTupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, currentPageId);
            btreeTupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, currentInvListStartOffset);
            btreeTupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, invListBuilder.getListSize());
            // Reset tuple reference and add it.
            btreeTupleReference.reset(btreeTupleBuilder.getFieldEndOffsets(), btreeTupleBuilder.getByteArray());
            btreeBulkloader.add(btreeTupleReference);
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
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            boolean firstElement = lastTupleBuilder.getSize() == 0;
            boolean startNewList = firstElement;
            if (!firstElement) {
                // If the current and the last token don't match, we start a new list.
                lastTuple.reset(lastTupleBuilder.getFieldEndOffsets(), lastTupleBuilder.getByteArray());
                startNewList = tokenCmp.compare(tuple, lastTuple) != 0;
            }
            if (startNewList) {
                if (!firstElement) {
                    // Create entry in btree for last inverted list.
                    createAndInsertBTreeTuple();
                }
                if (!invListBuilder.startNewList(tuple, numTokenFields)) {
                    pinNextPage();
                    invListBuilder.setTargetBuffer(currentPage.getBuffer().array(), 0);
                    if (!invListBuilder.startNewList(tuple, numTokenFields)) {
                        throw new IllegalStateException("Failed to create first inverted list.");
                    }
                }
                currentInvListStartPageId = currentPageId;
                currentInvListStartOffset = invListBuilder.getPos();
            } else {
                if (invListCmp.compare(tuple, lastTuple, numTokenFields) == 0) {
                    // Duplicate inverted-list element.
                    return;
                }
            }

            // Append to current inverted list.
            if (!invListBuilder.appendElement(tuple, numTokenFields, numInvListKeys)) {
                pinNextPage();
                invListBuilder.setTargetBuffer(currentPage.getBuffer().array(), 0);
                if (!invListBuilder.appendElement(tuple, numTokenFields, numInvListKeys)) {
                    throw new IllegalStateException(
                            "Failed to append element to inverted list after switching to a new page.");
                }
            }

            // Remember last tuple by creating a copy.
            // TODO: This portion can be optimized by only copying the token when it changes, and using the last appended inverted-list element as a reference.
            lastTupleBuilder.reset();
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                lastTupleBuilder.addField(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            }
        }

        @Override
        public void end() throws IndexException, HyracksDataException {
            createAndInsertBTreeTuple();
            btreeBulkloader.end();

            if (currentPage != null) {
                currentPage.releaseWriteLatch();
                bufferCache.unpin(currentPage);
            }
        }
    }

    @Override
    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public int getInvListsFileId() {
        return fileId;
    }

    public IBinaryComparatorFactory[] getInvListCmpFactories() {
        return invListCmpFactories;
    }

    public ITypeTraits[] getInvListTypeTraits() {
        return invListTypeTraits;
    }

    public BTree getBTree() {
        return btree;
    }

    public class InvertedIndexAccessor implements IIndexAccessor {
        private final IInvertedIndexSearcher searcher;
        private final IIndexOpContext opCtx = new InvertedIndexOpContext(btree);

        public InvertedIndexAccessor(InvertedIndex index) {
            this.searcher = new TOccurrenceSearcher(ctx, index);
        }

        @Override
        public IIndexCursor createSearchCursor() {
            return new InvertedIndexSearchCursor(searcher);
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException,
                IndexException {
            searcher.search((InvertedIndexSearchCursor) cursor, (InvertedIndexSearchPredicate) searchPred, opCtx);
        }

        public IInvertedIndexSearcher getSearcher() {
            return searcher;
        }
        
        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException, IndexException {
            throw new UnsupportedOperationException("Insert not supported by inverted index.");
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException, IndexException {
            throw new UnsupportedOperationException("Update not supported by inverted index.");
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException, IndexException {
            throw new UnsupportedOperationException("Delete not supported by inverted index.");
        }

        @Override
        public void upsert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            throw new UnsupportedOperationException("Upsert not supported by inverted index.");
        }
    }

    @Override
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
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
    public static class DefaultHyracksCommonContext implements IHyracksCommonContext {
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

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput) throws IndexException {
        try {
            return new InvertedIndexBulkLoader(fillFactor, verifyInput, rootPageId, fileId);
        } catch (HyracksDataException e) {
            throw new InvertedIndexException(e);
        }
    }

    @Override
    public void validate() throws HyracksDataException {
        throw new UnsupportedOperationException("Validation not implemented for Inverted Indexes.");
    }

    @Override
    public long getInMemorySize() {
        return 0;
    }

    @Override
    public IInvertedListCursor createInvertedListCursor() {
        return new FixedSizeElementInvertedListCursor(bufferCache, fileId, invListTypeTraits);
    }
}
