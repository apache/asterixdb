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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.util.TupleUtils;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.api.UnsortedInputException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.exceptions.InvertedIndexException;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.TOccurrenceSearcher;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageQueue;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

/**
 * An inverted index consists of two files: 1. a file storing (paginated)
 * inverted lists 2. a BTree-file mapping from tokens to inverted lists.
 * Implemented features: bulk loading and searching (based on T-Occurrence) Not
 * implemented features: updates (insert/update/delete) Limitations: a query
 * cannot exceed the size of a Hyracks frame.
 */
public class OnDiskInvertedIndex implements IInvertedIndex {
    protected final IHyracksCommonContext ctx = new DefaultHyracksCommonContext();

    // Schema of BTree tuples, set in constructor.
    protected final int invListStartPageIdField;
    protected final int invListEndPageIdField;
    protected final int invListStartOffField;
    protected final int invListNumElementsField;

    // Type traits to be appended to the token type trait which finally form the BTree field type traits.
    protected static final ITypeTraits[] btreeValueTypeTraits = new ITypeTraits[4];
    static {
        // startPageId
        btreeValueTypeTraits[0] = IntegerPointable.TYPE_TRAITS;
        // endPageId
        btreeValueTypeTraits[1] = IntegerPointable.TYPE_TRAITS;
        // startOff
        btreeValueTypeTraits[2] = IntegerPointable.TYPE_TRAITS;
        // numElements
        btreeValueTypeTraits[3] = IntegerPointable.TYPE_TRAITS;
    }

    protected BTree btree;
    protected int rootPageId = 0;
    protected IBufferCache bufferCache;
    protected IFileMapProvider fileMapProvider;
    protected int fileId = -1;
    protected final ITypeTraits[] invListTypeTraits;
    protected final IBinaryComparatorFactory[] invListCmpFactories;
    protected final ITypeTraits[] tokenTypeTraits;
    protected final IBinaryComparatorFactory[] tokenCmpFactories;
    protected final IInvertedListBuilder invListBuilder;
    protected final int numTokenFields;
    protected final int numInvListKeys;
    protected final FileReference invListsFile;
    // Last page id of inverted-lists file (inclusive). Set during bulk load.
    protected int invListsMaxPageId = -1;
    protected boolean isOpen = false;
    protected boolean wasOpen = false;

    public OnDiskInvertedIndex(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            IInvertedListBuilder invListBuilder, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, FileReference btreeFile, FileReference invListsFile)
            throws IndexException {
        this.bufferCache = bufferCache;
        this.fileMapProvider = fileMapProvider;
        this.invListBuilder = invListBuilder;
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.btree = BTreeUtils.createBTree(bufferCache, fileMapProvider, getBTreeTypeTraits(tokenTypeTraits),
                tokenCmpFactories, BTreeLeafFrameType.REGULAR_NSM, btreeFile);
        this.numTokenFields = btree.getComparatorFactories().length;
        this.numInvListKeys = invListCmpFactories.length;
        this.invListsFile = invListsFile;
        this.invListStartPageIdField = numTokenFields;
        this.invListEndPageIdField = numTokenFields + 1;
        this.invListStartOffField = numTokenFields + 2;
        this.invListNumElementsField = numTokenFields + 3;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        create(false);
    }

    public synchronized void create(boolean appendOnly) throws HyracksDataException {
        if (isOpen) {
            throw new HyracksDataException("Failed to create since index is already open.");
        }
        if (!appendOnly) {
            btree.create();
        }
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
        activate(false);
    }

    public synchronized void activate(boolean appendOnly) throws HyracksDataException {
        if (isOpen) {
            throw new HyracksDataException("Failed to activate the index since it is already activated.");
        }
        if (!appendOnly) {
            btree.activate();
        }
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
        wasOpen = true;
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        if (!isOpen && wasOpen) {
            throw new HyracksDataException("Failed to deactivate the index since it is already deactivated.");
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
        invListsFile.delete();
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
    public IInvertedListCursor createInvertedListCursor() {
        return new FixedSizeElementInvertedListCursor(bufferCache, fileId, invListTypeTraits);
    }

    @Override
    public void openInvertedListCursor(IInvertedListCursor listCursor, ITupleReference searchKey,
            IIndexOperationContext ictx) throws HyracksDataException, IndexException {
        OnDiskInvertedIndexOpContext ctx = (OnDiskInvertedIndexOpContext) ictx;
        ctx.btreePred.setLowKeyComparator(ctx.searchCmp);
        ctx.btreePred.setHighKeyComparator(ctx.searchCmp);
        ctx.btreePred.setLowKey(searchKey, true);
        ctx.btreePred.setHighKey(searchKey, true);
        ctx.btreeAccessor.search(ctx.btreeCursor, ctx.btreePred);
        try {
            if (ctx.btreeCursor.hasNext()) {
                ctx.btreeCursor.next();
                resetInvertedListCursor(ctx.btreeCursor.getTuple(), listCursor);
            } else {
                listCursor.reset(0, 0, 0, 0);
            }
        } finally {
            ctx.btreeCursor.close();
            ctx.btreeCursor.reset();
        }
    }

    public void resetInvertedListCursor(ITupleReference btreeTuple, IInvertedListCursor listCursor) {
        int startPageId = IntegerPointable.getInteger(btreeTuple.getFieldData(invListStartPageIdField), btreeTuple.getFieldStart(invListStartPageIdField));
        int endPageId = IntegerPointable.getInteger(btreeTuple.getFieldData(invListEndPageIdField), btreeTuple.getFieldStart(invListEndPageIdField));
        int startOff = IntegerPointable.getInteger(btreeTuple.getFieldData(invListStartOffField), btreeTuple.getFieldStart(invListStartOffField));
        int numElements = IntegerPointable.getInteger(btreeTuple.getFieldData(invListNumElementsField), btreeTuple.getFieldStart(invListNumElementsField));
        listCursor.reset(startPageId, endPageId, startOff, numElements);
    }

    public final class OnDiskInvertedIndexBulkLoader implements IIndexBulkLoader {
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

        private final boolean verifyInput;
        private final MultiComparator allCmp;

        private IFIFOPageQueue queue;

        public OnDiskInvertedIndexBulkLoader(float btreeFillFactor, boolean verifyInput, long numElementsHint,
                boolean checkIfEmptyIndex, int startPageId, boolean appendOnly) throws IndexException,
                HyracksDataException {
            this.verifyInput = verifyInput;
            this.tokenCmp = MultiComparator.create(btree.getComparatorFactories());
            this.invListCmp = MultiComparator.create(invListCmpFactories);
            if (verifyInput) {
                allCmp = MultiComparator.create(btree.getComparatorFactories(), invListCmpFactories);
            } else {
                allCmp = null;
            }
            this.btreeTupleBuilder = new ArrayTupleBuilder(btree.getFieldCount());
            this.btreeTupleReference = new ArrayTupleReference();
            this.lastTupleBuilder = new ArrayTupleBuilder(numTokenFields + numInvListKeys);
            this.lastTuple = new ArrayTupleReference();
            if (appendOnly) {
                create(appendOnly);
                activate(appendOnly);
            }
            this.btreeBulkloader = btree.createBulkLoader(btreeFillFactor, verifyInput, numElementsHint,
                    checkIfEmptyIndex, appendOnly);
            currentPageId = startPageId;
            currentPage = bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
            invListBuilder.setTargetBuffer(currentPage.getBuffer().array(), 0);
            queue = bufferCache.createFIFOQueue();
        }

        public void pinNextPage() throws HyracksDataException {
            queue.put(currentPage);
            currentPageId++;
            currentPage = bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
        }

        private void createAndInsertBTreeTuple() throws IndexException, HyracksDataException {
            // Build tuple.
            btreeTupleBuilder.reset();
            DataOutput output = btreeTupleBuilder.getDataOutput();
            // Add key fields.
            lastTuple.reset(lastTupleBuilder.getFieldEndOffsets(), lastTupleBuilder.getByteArray());
            for (int i = 0; i < numTokenFields; i++) {
                btreeTupleBuilder.addField(lastTuple.getFieldData(i), lastTuple.getFieldStart(i),
                        lastTuple.getFieldLength(i));
            }
            // Add inverted-list 'pointer' value fields.
            try {
                output.writeInt(currentInvListStartPageId);
                btreeTupleBuilder.addFieldEndOffset();
                output.writeInt(currentPageId);
                btreeTupleBuilder.addFieldEndOffset();
                output.writeInt(currentInvListStartOffset);
                btreeTupleBuilder.addFieldEndOffset();
                output.writeInt(invListBuilder.getListSize());
                btreeTupleBuilder.addFieldEndOffset();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            // Reset tuple reference and add it into the BTree load.
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

            if (verifyInput && lastTupleBuilder.getSize() != 0) {
                if (allCmp.compare(tuple, lastTuple) <= 0) {
                    throw new UnsortedInputException(
                            "Input stream given to OnDiskInvertedIndex bulk load is not sorted.");
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
            // The last tuple builder is empty if add() was never called.
            if (lastTupleBuilder.getSize() != 0) {
                createAndInsertBTreeTuple();
            }
            btreeBulkloader.end();

            if (currentPage != null) {
                queue.put(currentPage);
            }
            invListsMaxPageId = currentPageId;
            bufferCache.finishQueue();
        }

        @Override
        public void abort() throws HyracksDataException {
            if(btreeBulkloader != null){
                btreeBulkloader.abort();
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

    public int getInvListsMaxPageId() {
        return invListsMaxPageId;
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

    public FileReference getInvListsFile() {
        return invListsFile;
    }

    public class OnDiskInvertedIndexAccessor implements IInvertedIndexAccessor {
        private final OnDiskInvertedIndex index;
        private final IInvertedIndexSearcher searcher;
        private final IIndexOperationContext opCtx = new OnDiskInvertedIndexOpContext(btree);

        public OnDiskInvertedIndexAccessor(OnDiskInvertedIndex index) throws HyracksDataException {
            this.index = index;
            this.searcher = new TOccurrenceSearcher(ctx, index);
        }

        // Let subclasses initialize.
        protected OnDiskInvertedIndexAccessor(OnDiskInvertedIndex index, IInvertedIndexSearcher searcher) {
            this.index = index;
            this.searcher = searcher;
        }

        @Override
        public IIndexCursor createSearchCursor(boolean exclusive) {
            return new OnDiskInvertedIndexSearchCursor(searcher, index.getInvListTypeTraits().length);
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException,
                IndexException {
            searcher.search((OnDiskInvertedIndexSearchCursor) cursor, (InvertedIndexSearchPredicate) searchPred, opCtx);
        }

        @Override
        public IInvertedListCursor createInvertedListCursor() {
            return index.createInvertedListCursor();
        }

        @Override
        public void openInvertedListCursor(IInvertedListCursor listCursor, ITupleReference searchKey)
                throws HyracksDataException, IndexException {
            index.openInvertedListCursor(listCursor, searchKey, opCtx);
        }

        @Override
        public IIndexCursor createRangeSearchCursor() {
            return new OnDiskInvertedIndexRangeSearchCursor(index, opCtx);
        }

        @Override
        public void rangeSearch(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException,
                IndexException {
            OnDiskInvertedIndexRangeSearchCursor rangeSearchCursor = (OnDiskInvertedIndexRangeSearchCursor) cursor;
            rangeSearchCursor.open(null, searchPred);
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
            ISearchOperationCallback searchCallback) throws HyracksDataException {
        return new OnDiskInvertedIndexAccessor(this);
    }

    // This is just a dummy hyracks context for allocating frames for temporary
    // results during inverted index searches.
    // TODO: In the future we should use the real HyracksTaskContext to track
    // frame usage.
    public static class DefaultHyracksCommonContext implements IHyracksCommonContext {
        private final int FRAME_SIZE = 32768;

        @Override
        public int getInitialFrameSize() {
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

        @Override public ByteBuffer allocateFrame(int bytes) throws HyracksDataException {
            return ByteBuffer.allocate(bytes);
        }

        @Override public ByteBuffer reallocateFrame(ByteBuffer bytes, int newSizeInBytes, boolean copyOldData) throws HyracksDataException {
            throw new HyracksDataException("TODO");
        }

        @Override
        public void deallocateFrames(int bytes) {
            // TODO Auto-generated method stub

        }
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws IndexException {
        try {
            return new OnDiskInvertedIndexBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex,
                    rootPageId, false);
        } catch (HyracksDataException e) {
            throw new InvertedIndexException(e);
        }
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, boolean appendOnly) throws IndexException {
        try {
            return new OnDiskInvertedIndexBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex,
                    rootPageId, appendOnly);

        } catch (HyracksDataException e) {
            throw new InvertedIndexException(e);
        }

    }

    @Override
    public void validate() throws HyracksDataException {
        btree.validate();
        // Scan the btree and validate the order of elements in each inverted-list.
        IIndexAccessor btreeAccessor = btree.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        IIndexCursor btreeCursor = btreeAccessor.createSearchCursor(false);
        MultiComparator btreeCmp = MultiComparator.create(btree.getComparatorFactories());
        RangePredicate rangePred = new RangePredicate(null, null, true, true, btreeCmp, btreeCmp);
        int[] fieldPermutation = new int[tokenTypeTraits.length];
        for (int i = 0; i < tokenTypeTraits.length; i++) {
            fieldPermutation[i] = i;
        }
        PermutingTupleReference tokenTuple = new PermutingTupleReference(fieldPermutation);

        IInvertedIndexAccessor invIndexAccessor = (IInvertedIndexAccessor) createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        IInvertedListCursor invListCursor = invIndexAccessor.createInvertedListCursor();
        MultiComparator invListCmp = MultiComparator.create(invListCmpFactories);

        try {
            // Search key for finding an inverted-list in the actual index.
            ArrayTupleBuilder prevBuilder = new ArrayTupleBuilder(invListTypeTraits.length);
            ArrayTupleReference prevTuple = new ArrayTupleReference();
            btreeAccessor.search(btreeCursor, rangePred);
            while (btreeCursor.hasNext()) {
                btreeCursor.next();
                tokenTuple.reset(btreeCursor.getTuple());
                // Validate inverted list by checking that the elements are totally ordered.
                invIndexAccessor.openInvertedListCursor(invListCursor, tokenTuple);
                invListCursor.pinPages();
                try {
                    if (invListCursor.hasNext()) {
                        invListCursor.next();
                        ITupleReference invListElement = invListCursor.getTuple();
                        // Initialize prev tuple.
                        TupleUtils.copyTuple(prevBuilder, invListElement, invListElement.getFieldCount());
                        prevTuple.reset(prevBuilder.getFieldEndOffsets(), prevBuilder.getByteArray());
                    }
                    while (invListCursor.hasNext()) {
                        invListCursor.next();
                        ITupleReference invListElement = invListCursor.getTuple();
                        // Compare with previous element.
                        if (invListCmp.compare(invListElement, prevTuple) <= 0) {
                            throw new HyracksDataException("Index validation failed.");
                        }
                        // Set new prevTuple.
                        TupleUtils.copyTuple(prevBuilder, invListElement, invListElement.getFieldCount());
                        prevTuple.reset(prevBuilder.getFieldEndOffsets(), prevBuilder.getByteArray());
                    }
                } finally {
                    invListCursor.unpinPages();
                }
            }
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        } finally {
            btreeCursor.close();
        }
    }

    @Override
    public long getMemoryAllocationSize() {
        return 0;
    }

    protected static ITypeTraits[] getBTreeTypeTraits(ITypeTraits[] tokenTypeTraits) {
        ITypeTraits[] btreeTypeTraits = new ITypeTraits[tokenTypeTraits.length + btreeValueTypeTraits.length];
        // Set key type traits.
        for (int i = 0; i < tokenTypeTraits.length; i++) {
            btreeTypeTraits[i] = tokenTypeTraits[i];
        }
        // Set value type traits.
        for (int i = 0; i < btreeValueTypeTraits.length; i++) {
            btreeTypeTraits[i + tokenTypeTraits.length] = btreeValueTypeTraits[i];
        }
        return btreeTypeTraits;
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
    public boolean hasMemoryComponents() {
        return true;
    }

}
