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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.DiskBTree;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManagerFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInPlaceInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.InvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexSearchCursorInitialState;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.TOccurrenceSearcher;
import org.apache.hyracks.storage.am.lsm.invertedindex.tuples.TokenKeyPairTuple;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.PageWriteFailureCallback;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * An inverted index consists of two files: 1. a file storing (paginated)
 * inverted lists 2. a BTree-file mapping from tokens to inverted lists.
 * Implemented features: bulk loading and searching (based on T-Occurrence) Not
 * implemented features: updates (insert/update/delete) Limitations: a query
 * cannot exceed the size of a Hyracks frame.
 */
public class OnDiskInvertedIndex implements IInPlaceInvertedIndex {
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

    protected DiskBTree btree;
    protected int rootPageId = 0;
    protected IBufferCache bufferCache;
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

    public OnDiskInvertedIndex(IBufferCache bufferCache, IInvertedListBuilder invListBuilder,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories, FileReference btreeFile,
            FileReference invListsFile, IPageManagerFactory pageManagerFactory) throws HyracksDataException {
        this.bufferCache = bufferCache;
        this.invListBuilder = invListBuilder;
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.btree = BTreeUtils.createDiskBTree(bufferCache, getBTreeTypeTraits(tokenTypeTraits), tokenCmpFactories,
                BTreeLeafFrameType.REGULAR_NSM, btreeFile, pageManagerFactory.createPageManager(bufferCache), false);
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
        if (isOpen) {
            throw new HyracksDataException("Failed to create since index is already open.");
        }
        btree.create();
        fileId = bufferCache.createFile(invListsFile);
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        if (isOpen) {
            throw new HyracksDataException("Failed to activate the index since it is already activated.");
        }
        btree.activate();
        if (fileId >= 0) {
            bufferCache.openFile(fileId);
        } else {
            fileId = bufferCache.openFile(invListsFile);
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
        bufferCache.deleteFile(invListsFile);
    }

    @Override
    public synchronized void clear() throws HyracksDataException {
        if (!isOpen) {
            throw new HyracksDataException("Failed to clear since index is not open.");
        }
        btree.clear();
        bufferCache.closeFile(fileId);
        bufferCache.deleteFile(fileId);
        fileId = bufferCache.createFile(invListsFile);
        bufferCache.openFile(fileId);
    }

    @Override
    public InvertedListCursor createInvertedListCursor(IHyracksTaskContext ctx) throws HyracksDataException {
        return new FixedSizeElementInvertedListCursor(bufferCache, fileId, invListTypeTraits, ctx,
                NoOpIndexCursorStats.INSTANCE);
    }

    @Override
    public InvertedListCursor createInvertedListRangeSearchCursor(IIndexCursorStats stats) throws HyracksDataException {
        return new FixedSizeElementInvertedListScanCursor(bufferCache, fileId, invListTypeTraits, stats);
    }

    @Override
    public void openInvertedListCursor(InvertedListCursor listCursor, ITupleReference searchKey,
            IIndexOperationContext ictx) throws HyracksDataException {
        OnDiskInvertedIndexOpContext ctx = (OnDiskInvertedIndexOpContext) ictx;
        ctx.getBtreePred().setLowKeyComparator(ctx.getSearchCmp());
        ctx.getBtreePred().setHighKeyComparator(ctx.getSearchCmp());
        ctx.getBtreePred().setLowKey(searchKey, true);
        ctx.getBtreePred().setHighKey(searchKey, true);
        ctx.getBtreeAccessor().search(ctx.getBtreeCursor(), ctx.getBtreePred());
        try {
            if (ctx.getBtreeCursor().hasNext()) {
                ctx.getBtreeCursor().next();
                openInvertedListCursor(ctx.getBtreeCursor().getTuple(), listCursor, ctx);
            } else {
                LSMInvertedIndexSearchCursorInitialState initState = ctx.getCursorInitialState();
                initState.setInvertedListInfo(0, 0, 0, 0);
                listCursor.open(initState, null);
            }
        } finally {
            ctx.getBtreeCursor().close();
        }
    }

    public void openInvertedListCursor(ITupleReference btreeTuple, InvertedListCursor listCursor,
            OnDiskInvertedIndexOpContext opCtx) throws HyracksDataException {
        int startPageId = IntegerPointable.getInteger(btreeTuple.getFieldData(invListStartPageIdField),
                btreeTuple.getFieldStart(invListStartPageIdField));
        int endPageId = IntegerPointable.getInteger(btreeTuple.getFieldData(invListEndPageIdField),
                btreeTuple.getFieldStart(invListEndPageIdField));
        int startOff = IntegerPointable.getInteger(btreeTuple.getFieldData(invListStartOffField),
                btreeTuple.getFieldStart(invListStartOffField));
        int numElements = IntegerPointable.getInteger(btreeTuple.getFieldData(invListNumElementsField),
                btreeTuple.getFieldStart(invListNumElementsField));
        LSMInvertedIndexSearchCursorInitialState initState = opCtx.getCursorInitialState();
        initState.setInvertedListInfo(startPageId, endPageId, startOff, numElements);
        listCursor.open(initState, null);
    }

    public abstract class AbstractOnDiskInvertedIndexBulkLoader extends PageWriteFailureCallback
            implements IIndexBulkLoader {
        protected final ArrayTupleBuilder btreeTupleBuilder;
        protected final ArrayTupleReference btreeTupleReference;
        protected final IIndexBulkLoader btreeBulkloader;

        protected int currentInvListStartPageId;
        protected int currentInvListStartOffset;
        protected final ArrayTupleBuilder lastTupleBuilder;
        protected final ArrayTupleReference lastTuple;

        protected int currentPageId;
        protected ICachedPage currentPage;
        protected final MultiComparator invListCmp;

        protected final boolean verifyInput;
        protected final MultiComparator allCmp;

        protected final IFIFOPageWriter queue;

        public AbstractOnDiskInvertedIndexBulkLoader(float btreeFillFactor, boolean verifyInput, long numElementsHint,
                boolean checkIfEmptyIndex, int startPageId, IPageWriteCallback callback) throws HyracksDataException {
            this.verifyInput = verifyInput;
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
            this.btreeBulkloader = btree.createBulkLoader(btreeFillFactor, verifyInput, numElementsHint,
                    checkIfEmptyIndex, NoOpPageWriteCallback.INSTANCE);
            currentPageId = startPageId;
            currentPage = bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
            invListBuilder.setTargetBuffer(currentPage.getBuffer().array(), 0);
            queue = bufferCache.createFIFOWriter(callback, this);
        }

        protected void pinNextPage() throws HyracksDataException {
            queue.write(currentPage);
            currentPageId++;
            currentPage = bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
        }

        protected void insertBTreeTuple() throws HyracksDataException {
            // Build tuple.
            DataOutput output = btreeTupleBuilder.getDataOutput();
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
                throw HyracksDataException.create(e);
            }
            // Reset tuple reference and add it into the BTree load.
            btreeTupleReference.reset(btreeTupleBuilder.getFieldEndOffsets(), btreeTupleBuilder.getByteArray());
            btreeBulkloader.add(btreeTupleReference);
            btreeTupleBuilder.reset();
        }

        protected void startNewList(ITupleReference tokenTuple) throws HyracksDataException {
            if (!invListBuilder.startNewList(tokenTuple, numTokenFields)) {
                pinNextPage();
                invListBuilder.setTargetBuffer(currentPage.getBuffer().array(), 0);
                if (!invListBuilder.startNewList(tokenTuple, numTokenFields)) {
                    throw new IllegalStateException("Failed to create first inverted list.");
                }
            }
            currentInvListStartPageId = currentPageId;
            currentInvListStartOffset = invListBuilder.getPos();
        }

        protected void appendInvertedList(ITupleReference keyTuple, int startField) throws HyracksDataException {
            if (!invListBuilder.appendElement(keyTuple, startField, numInvListKeys)) {
                pinNextPage();
                invListBuilder.setTargetBuffer(currentPage.getBuffer().array(), 0);
                if (!invListBuilder.appendElement(keyTuple, startField, numInvListKeys)) {
                    throw new IllegalStateException(
                            "Failed to append element to inverted list after switching to a new page.");
                }
            }
        }

        protected void verifyTuple(ITupleReference tuple) throws HyracksDataException {
            if (lastTupleBuilder.getSize() > 0 && allCmp.compare(tuple, lastTuple) <= 0) {
                HyracksDataException.create(ErrorCode.UNSORTED_LOAD_INPUT);
            }
        }

        protected void saveLastTuple(ITupleReference tuple) throws HyracksDataException {
            lastTupleBuilder.reset();
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                lastTupleBuilder.addField(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            }
            lastTuple.reset(lastTupleBuilder.getFieldEndOffsets(), lastTupleBuilder.getByteArray());
        }

        protected void copyTokenToBTreeTuple(ITupleReference tokenTuple) throws HyracksDataException {
            for (int i = 0; i < numTokenFields; i++) {
                btreeTupleBuilder.addField(tokenTuple.getFieldData(i), tokenTuple.getFieldStart(i),
                        tokenTuple.getFieldLength(i));
            }
        }

        @Override
        public void end() throws HyracksDataException {
            if (btreeTupleBuilder.getSize() != 0) {
                insertBTreeTuple();
            }

            btreeBulkloader.end();

            if (currentPage != null) {
                queue.write(currentPage);
            }
            invListsMaxPageId = currentPageId;
            if (hasFailed()) {
                throw HyracksDataException.create(getFailure());
            }
        }

        @Override
        public void abort() throws HyracksDataException {
            if (btreeBulkloader != null) {
                btreeBulkloader.abort();
            }
        }

        @Override
        public void force() throws HyracksDataException {
            btreeBulkloader.force();
            bufferCache.force(fileId, false);
        }

    }

    public class OnDiskInvertedIndexMergeBulkLoader extends AbstractOnDiskInvertedIndexBulkLoader {

        public OnDiskInvertedIndexMergeBulkLoader(float btreeFillFactor, boolean verifyInput, long numElementsHint,
                boolean checkIfEmptyIndex, int startPageId, IPageWriteCallback callback) throws HyracksDataException {
            super(btreeFillFactor, verifyInput, numElementsHint, checkIfEmptyIndex, startPageId, callback);
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException {
            TokenKeyPairTuple pairTuple = (TokenKeyPairTuple) tuple;
            ITupleReference tokenTuple = pairTuple.getTokenTuple();
            ITupleReference keyTuple = pairTuple.getKeyTuple();
            boolean startNewList = pairTuple.isNewToken();
            if (startNewList) {
                if (btreeTupleBuilder.getSize() > 0) {
                    insertBTreeTuple();
                }
                startNewList(tokenTuple);
                copyTokenToBTreeTuple(tokenTuple);
            }
            appendInvertedList(keyTuple, 0);
            if (verifyInput) {
                verifyTuple(tuple);
                saveLastTuple(tuple);
            }
        }
    }

    public class OnDiskInvertedIndexBulkLoader extends AbstractOnDiskInvertedIndexBulkLoader {

        public OnDiskInvertedIndexBulkLoader(float btreeFillFactor, boolean verifyInput, long numElementsHint,
                boolean checkIfEmptyIndex, int startPageId, IPageWriteCallback callback) throws HyracksDataException {
            super(btreeFillFactor, verifyInput, numElementsHint, checkIfEmptyIndex, startPageId, callback);
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException {
            boolean firstElement = btreeTupleBuilder.getSize() == 0;
            boolean startNewList = firstElement;
            if (!firstElement) {
                // If the current and the last token don't match, we start a new list.
                startNewList = !TupleUtils.equalTuples(tuple, lastTuple, numTokenFields);
            }
            if (startNewList) {
                if (!firstElement) {
                    // Create entry in btree for last inverted list.
                    insertBTreeTuple();
                }
                startNewList(tuple);
                copyTokenToBTreeTuple(tuple);
            } else {
                if (invListCmp.compare(tuple, lastTuple, numTokenFields) == 0) {
                    // Duplicate inverted-list element.
                    return;
                }
            }
            appendInvertedList(tuple, numTokenFields);
            if (verifyInput) {
                verifyTuple(tuple);
            }

            saveLastTuple(tuple);
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

    @Override
    public IBinaryComparatorFactory[] getInvListCmpFactories() {
        return invListCmpFactories;
    }

    @Override
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
        protected final OnDiskInvertedIndex index;
        protected final IIndexOperationContext opCtx;
        protected final IHyracksTaskContext ctx;
        protected IInvertedIndexSearcher searcher;
        private boolean destroyed = false;
        protected final IIndexAccessParameters iap;

        public OnDiskInvertedIndexAccessor(OnDiskInvertedIndex index, IIndexAccessParameters iap)
                throws HyracksDataException {
            this.index = index;
            this.ctx = (IHyracksTaskContext) iap.getParameters().get(HyracksConstants.HYRACKS_TASK_CONTEXT);
            this.opCtx = new OnDiskInvertedIndexOpContext(btree);
            this.iap = iap;
        }

        @Override
        public IIndexCursor createSearchCursor(boolean exclusive) throws HyracksDataException {
            if (searcher == null) {
                searcher = new TOccurrenceSearcher(index, ctx);
            }
            return new OnDiskInvertedIndexSearchCursor(searcher);
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
            if (searcher == null) {
                searcher = new TOccurrenceSearcher(index, ctx);
            }
            searcher.search(cursor, (InvertedIndexSearchPredicate) searchPred, opCtx);
        }

        @Override
        public InvertedListCursor createInvertedListCursor() throws HyracksDataException {
            return index.createInvertedListCursor(ctx);
        }

        @Override
        public void openInvertedListCursor(InvertedListCursor listCursor, ITupleReference searchKey)
                throws HyracksDataException {
            index.openInvertedListCursor(listCursor, searchKey, opCtx);
        }

        @Override
        public IIndexCursor createRangeSearchCursor() throws HyracksDataException {
            return new OnDiskInvertedIndexRangeSearchCursor(index, opCtx, (IIndexCursorStats) iap.getParameters()
                    .getOrDefault(HyracksConstants.INDEX_CURSOR_STATS, NoOpIndexCursorStats.INSTANCE));
        }

        @Override
        public void rangeSearch(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
            OnDiskInvertedIndexRangeSearchCursor rangeSearchCursor = (OnDiskInvertedIndexRangeSearchCursor) cursor;
            rangeSearchCursor.open(null, searchPred);
        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException {
            throw new UnsupportedOperationException("Insert not supported by inverted index.");
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException {
            throw new UnsupportedOperationException("Update not supported by inverted index.");
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException {
            throw new UnsupportedOperationException("Delete not supported by inverted index.");
        }

        @Override
        public void upsert(ITupleReference tuple) throws HyracksDataException {
            throw new UnsupportedOperationException("Upsert not supported by inverted index.");
        }

        @Override
        public void destroy() throws HyracksDataException {
            if (destroyed) {
                return;
            }
            destroyed = true;
            opCtx.destroy();
        }
    }

    @Override
    public OnDiskInvertedIndexAccessor createAccessor(IIndexAccessParameters iap) throws HyracksDataException {
        return new OnDiskInvertedIndexAccessor(this, iap);
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, IPageWriteCallback callback) throws HyracksDataException {
        return new OnDiskInvertedIndexBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex,
                rootPageId, callback);
    }

    public IIndexBulkLoader createMergeBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, IPageWriteCallback callback) throws HyracksDataException {
        return new OnDiskInvertedIndexMergeBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex,
                rootPageId, callback);
    }

    @Override
    public void validate() throws HyracksDataException {
        btree.validate();
        // Scan the btree and validate the order of elements in each inverted-list.
        IIndexAccessor btreeAccessor = btree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        try {
            MultiComparator btreeCmp = MultiComparator.create(btree.getComparatorFactories());
            RangePredicate rangePred = new RangePredicate(null, null, true, true, btreeCmp, btreeCmp);
            IIndexCursor btreeCursor = btreeAccessor.createSearchCursor(false);
            try {
                btreeAccessor.search(btreeCursor, rangePred);
                try {
                    doValidate(btreeCursor);
                } finally {
                    btreeCursor.close();
                }
            } finally {
                btreeCursor.destroy();
            }
        } finally {
            btreeAccessor.destroy();
        }
    }

    private void doValidate(IIndexCursor btreeCursor) throws HyracksDataException {
        int[] fieldPermutation = new int[tokenTypeTraits.length];
        for (int i = 0; i < tokenTypeTraits.length; i++) {
            fieldPermutation[i] = i;
        }
        PermutingTupleReference tokenTuple = new PermutingTupleReference(fieldPermutation);
        IIndexOperationContext opCtx = new OnDiskInvertedIndexOpContext(btree);
        // Search key for finding an inverted-list in the actual index.
        ArrayTupleBuilder prevBuilder = new ArrayTupleBuilder(invListTypeTraits.length);
        ArrayTupleReference prevTuple = new ArrayTupleReference();
        IInvertedIndexAccessor invIndexAccessor = createAccessor(NoOpIndexAccessParameters.INSTANCE);
        try {
            InvertedListCursor invListCursor = createInvertedListRangeSearchCursor(NoOpIndexCursorStats.INSTANCE);
            MultiComparator invListCmp = MultiComparator.create(invListCmpFactories);
            while (btreeCursor.hasNext()) {
                btreeCursor.next();
                tokenTuple.reset(btreeCursor.getTuple());
                // Validate inverted list by checking that the elements are totally ordered.
                openInvertedListCursor(invListCursor, tokenTuple, opCtx);
                invListCursor.prepareLoadPages();
                invListCursor.loadPages();
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
                        validateWithPrevious(invListCmp, invListElement, prevTuple);
                        // Set new prevTuple.
                        TupleUtils.copyTuple(prevBuilder, invListElement, invListElement.getFieldCount());
                        prevTuple.reset(prevBuilder.getFieldEndOffsets(), prevBuilder.getByteArray());
                    }
                } finally {
                    invListCursor.unloadPages();
                    invListCursor.close();
                }
            }
        } finally {
            invIndexAccessor.destroy();
        }
    }

    private void validateWithPrevious(MultiComparator invListCmp, ITupleReference invListElement,
            ArrayTupleReference prevTuple) throws HyracksDataException {
        if (invListCmp.compare(invListElement, prevTuple) <= 0) {
            throw new HyracksDataException("Index validation failed.");
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
    public int getNumOfFilterFields() {
        return 0;
    }

    @Override
    public synchronized void purge() throws HyracksDataException {
        if (isOpen) {
            throw HyracksDataException.create(ErrorCode.CANNOT_PURGE_ACTIVE_INDEX);
        }
        btree.purge();
        bufferCache.purgeHandle(fileId);
        fileId = -1;
    }

}
