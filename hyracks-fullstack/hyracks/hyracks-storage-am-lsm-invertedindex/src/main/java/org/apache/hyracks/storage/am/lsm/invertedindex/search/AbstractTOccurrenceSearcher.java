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

package org.apache.hyracks.storage.am.lsm.invertedindex.search;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppenderAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.buffermanager.BufferManagerBackedVSizeFrame;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInPlaceInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IObjectFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.InvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeFrameTupleAccessor;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.TokenizerInfo.TokenizerType;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.ObjectCache;
import org.apache.hyracks.storage.common.MultiComparator;

public abstract class AbstractTOccurrenceSearcher implements IInvertedIndexSearcher {
    protected static final RecordDescriptor QUERY_TOKEN_REC_DESC =
            new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer() });

    protected final int OBJECT_CACHE_INIT_SIZE = 10;
    protected final int OBJECT_CACHE_EXPAND_SIZE = 10;

    protected final IHyracksTaskContext ctx;

    protected final InvertedListMerger invListMerger;
    // Final search result is needed because multiple merge() calls can happen.
    // We can't just use one of intermediate results as the final search result.
    protected final InvertedIndexFinalSearchResult finalSearchResult;
    protected final IInPlaceInvertedIndex invIndex;
    protected final MultiComparator invListCmp;

    protected final ArrayTupleBuilder queryTokenBuilder = new ArrayTupleBuilder(QUERY_TOKEN_REC_DESC.getFieldCount());
    protected final IFrame queryTokenFrame;
    protected final FrameTupleAppenderAccessor queryTokenAppender;
    protected final FrameTupleReference searchKey = new FrameTupleReference();

    protected int occurrenceThreshold;

    protected final IObjectFactory<InvertedListCursor> invListCursorFactory;
    protected final ObjectCache<InvertedListCursor> invListCursorCache;

    protected final ISimpleFrameBufferManager bufferManager;
    protected boolean isFinishedSearch;

    // For a single inverted list case
    protected InvertedListCursor singleInvListCursor;
    protected boolean isSingleInvertedList;

    // To read the final search result
    protected ByteBuffer searchResultBuffer;
    protected int searchResultTupleIndex = 0;
    protected final IFrameTupleAccessor searchResultFta;
    protected FixedSizeTupleReference searchResultTuple;

    public AbstractTOccurrenceSearcher(IInPlaceInvertedIndex invIndex, IHyracksTaskContext ctx)
            throws HyracksDataException {
        this.invIndex = invIndex;
        this.ctx = ctx;
        if (ctx == null) {
            throw HyracksDataException.create(ErrorCode.CANNOT_CONTINUE_TEXT_SEARCH_HYRACKS_TASK_IS_NULL);
        }
        this.bufferManager = TaskUtil.get(HyracksConstants.INVERTED_INDEX_SEARCH_FRAME_MANAGER, ctx);
        if (bufferManager == null) {
            throw HyracksDataException.create(ErrorCode.CANNOT_CONTINUE_TEXT_SEARCH_BUFFER_MANAGER_IS_NULL);
        }
        this.finalSearchResult =
                new InvertedIndexFinalSearchResult(invIndex.getInvListTypeTraits(), ctx, bufferManager);
        this.invListMerger = new InvertedListMerger(ctx, invIndex, bufferManager);
        this.invListCmp = MultiComparator.create(invIndex.getInvListCmpFactories());
        this.invListCursorFactory = new InvertedListCursorFactory(invIndex, ctx);
        this.invListCursorCache =
                new ObjectCache<>(invListCursorFactory, OBJECT_CACHE_INIT_SIZE, OBJECT_CACHE_EXPAND_SIZE);
        this.queryTokenFrame = new BufferManagerBackedVSizeFrame(ctx, bufferManager);
        if (queryTokenFrame.getBuffer() == null) {
            throw HyracksDataException.create(ErrorCode.NOT_ENOUGH_BUDGET_FOR_TEXTSEARCH,
                    this.getClass().getSimpleName());
        }
        this.queryTokenAppender = new FrameTupleAppenderAccessor(QUERY_TOKEN_REC_DESC);
        this.queryTokenAppender.reset(queryTokenFrame, true);
        this.isSingleInvertedList = false;
        this.searchResultTuple = new FixedSizeTupleReference(invIndex.getInvListTypeTraits());
        this.searchResultFta =
                new FixedSizeFrameTupleAccessor(ctx.getInitialFrameSize(), invIndex.getInvListTypeTraits());
    }

    protected void tokenizeQuery(InvertedIndexSearchPredicate searchPred) throws HyracksDataException {
        ITupleReference queryTuple = searchPred.getQueryTuple();
        int queryFieldIndex = searchPred.getQueryFieldIndex();
        IBinaryTokenizer queryTokenizer = searchPred.getQueryTokenizer();
        // Is this a full-text query?
        // Then, the last argument is conjunctive or disjunctive search option, not a query text.
        // Thus, we need to remove the last argument.
        boolean isFullTextSearchQuery = searchPred.getIsFullTextSearchQuery();
        // Get the type of query tokenizer.
        TokenizerType queryTokenizerType = queryTokenizer.getTokenizerType();
        int tokenCountInOneField = 0;

        queryTokenAppender.reset(queryTokenFrame, true);
        queryTokenizer.reset(queryTuple.getFieldData(queryFieldIndex), queryTuple.getFieldStart(queryFieldIndex),
                queryTuple.getFieldLength(queryFieldIndex));

        while (queryTokenizer.hasNext()) {
            queryTokenizer.next();
            queryTokenBuilder.reset();
            tokenCountInOneField++;
            try {
                IToken token = queryTokenizer.getToken();
                // For the full-text search, we don't support a phrase search yet.
                // So, each field should have only one token.
                // If it's a list, it can have multiple keywords in it. But, each keyword should not be a phrase.
                if (isFullTextSearchQuery) {
                    if (queryTokenizerType == TokenizerType.STRING && tokenCountInOneField > 1) {
                        throw HyracksDataException.create(ErrorCode.FULLTEXT_PHRASE_FOUND);
                    } else if (queryTokenizerType == TokenizerType.LIST) {
                        for (int j = 1; j < token.getTokenLength(); j++) {
                            if (DelimitedUTF8StringBinaryTokenizer
                                    .isSeparator((char) token.getData()[token.getStartOffset() + j])) {
                                throw HyracksDataException.create(ErrorCode.FULLTEXT_PHRASE_FOUND);
                            }
                        }
                    }
                }

                token.serializeToken(queryTokenBuilder.getFieldData());
                queryTokenBuilder.addFieldEndOffset();
                // WARNING: assuming one frame is big enough to hold all tokens
                queryTokenAppender.append(queryTokenBuilder.getFieldEndOffsets(), queryTokenBuilder.getByteArray(), 0,
                        queryTokenBuilder.getSize());
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    public int getOccurrenceThreshold() {
        return occurrenceThreshold;
    }

    public void printNewResults(int maxResultBufIdx, List<ByteBuffer> buffer) {
        StringBuffer strBuffer = new StringBuffer();
        FixedSizeFrameTupleAccessor resultFrameTupleAcc = finalSearchResult.getAccessor();
        for (int i = 0; i <= maxResultBufIdx; i++) {
            ByteBuffer testBuf = buffer.get(i);
            resultFrameTupleAcc.reset(testBuf);
            for (int j = 0; j < resultFrameTupleAcc.getTupleCount(); j++) {
                strBuffer.append(IntegerPointable.getInteger(resultFrameTupleAcc.getBuffer().array(),
                        resultFrameTupleAcc.getFieldStartOffset(j, 0)) + ",");
                strBuffer.append(IntegerPointable.getInteger(resultFrameTupleAcc.getBuffer().array(),
                        resultFrameTupleAcc.getFieldStartOffset(j, 1)) + " ");
            }
        }
        System.out.println(strBuffer.toString());
    }

    /**
     * Checks whether underlying the inverted list cursor or final search result has a tuple to return.
     */
    @Override
    public boolean hasNext() throws HyracksDataException {
        do {
            boolean moreToRead = hasMoreElement();
            if (moreToRead) {
                return true;
            }
            // Current cursor or buffer is exhausted. Unbinds the inverted list cursor or
            // cleans the output buffer of the final search result.
            resetResultSource();
            // Search is done? Then, there's nothing left.
            if (isFinishedSearch) {
                return false;
            }
            // Otherwise, resume the search process.
            continueSearch();
        } while (true);
    }

    @Override
    public void next() throws HyracksDataException {
        // Case 1: fetching a tuple from an inverted list cursor
        if (isSingleInvertedList) {
            singleInvListCursor.next();
        } else {
            // Case 2: fetching a tuple from the output frame of a final search result
            searchResultTuple.reset(searchResultFta.getBuffer().array(),
                    searchResultFta.getTupleStartOffset(searchResultTupleIndex));
            searchResultTupleIndex++;
        }
    }

    private boolean hasMoreElement() throws HyracksDataException {
        // Case #1: single inverted list cursor
        if (isSingleInvertedList) {
            return singleInvListCursor.hasNext();
        }
        // Case #2: ouput buffer from a final search result
        return searchResultTupleIndex < searchResultFta.getTupleCount();
    }

    private void resetResultSource() throws HyracksDataException {
        if (isSingleInvertedList) {
            isSingleInvertedList = false;
            try {
                singleInvListCursor.unloadPages();
            } finally {
                singleInvListCursor.close();
            }
            singleInvListCursor = null;
        } else {
            finalSearchResult.resetBuffer();
            searchResultTupleIndex = 0;
        }
    }

    public void destroy() throws HyracksDataException {
        // To ensure to release the buffer of the query token frame.
        ((BufferManagerBackedVSizeFrame) queryTokenFrame).destroy();

        // Releases the frames of the cursor.
        if (singleInvListCursor != null) {
            try {
                singleInvListCursor.unloadPages();
            } finally {
                singleInvListCursor.close();
            }
        }
        // Releases the frame of the final search result.
        finalSearchResult.close();

        // Releases the frames of the two intermediate search result.
        invListMerger.close();
    }

    @Override
    public ITupleReference getTuple() {
        if (isSingleInvertedList) {
            return singleInvListCursor.getTuple();
        }
        return searchResultTuple;
    }

    /**
     * Prepares the search process. This mainly allocates/clears the buffer frames of the each component.
     */
    protected void prepareSearch() throws HyracksDataException {
        finalSearchResult.prepareIOBuffer();
        invListMerger.prepareMerge();
        ((BufferManagerBackedVSizeFrame) queryTokenFrame).acquireFrame();
        isFinishedSearch = false;
        isSingleInvertedList = false;
        searchResultFta.reset(finalSearchResult.getNextFrame());
        searchResultTupleIndex = 0;
        singleInvListCursor = null;
    }

}
