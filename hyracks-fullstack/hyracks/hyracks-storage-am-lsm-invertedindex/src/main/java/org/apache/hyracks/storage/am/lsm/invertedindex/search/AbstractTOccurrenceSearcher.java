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
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppenderAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IObjectFactory;
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

    protected final IHyracksCommonContext ctx;

    protected final InvertedListMerger invListMerger;
    protected final SearchResult searchResult;
    protected final IInvertedIndex invIndex;
    protected final MultiComparator invListCmp;

    protected final ArrayTupleBuilder queryTokenBuilder = new ArrayTupleBuilder(QUERY_TOKEN_REC_DESC.getFieldCount());
    protected final IFrame queryTokenFrame;
    protected final FrameTupleAppenderAccessor queryTokenAppender;
    protected final FrameTupleReference searchKey = new FrameTupleReference();

    protected int occurrenceThreshold;

    protected final IObjectFactory<IInvertedListCursor> invListCursorFactory;
    protected final ObjectCache<IInvertedListCursor> invListCursorCache;

    public AbstractTOccurrenceSearcher(IHyracksCommonContext ctx, IInvertedIndex invIndex) throws HyracksDataException {
        this.ctx = ctx;
        this.invListMerger = new InvertedListMerger(ctx, invIndex);
        this.searchResult = new SearchResult(invIndex.getInvListTypeTraits(), ctx);
        this.invIndex = invIndex;
        this.invListCmp = MultiComparator.create(invIndex.getInvListCmpFactories());
        this.invListCursorFactory = new InvertedListCursorFactory(invIndex);
        this.invListCursorCache = new ObjectCache<>(invListCursorFactory, OBJECT_CACHE_INIT_SIZE,
                OBJECT_CACHE_EXPAND_SIZE);
        this.queryTokenFrame = new VSizeFrame(ctx);
        this.queryTokenAppender = new FrameTupleAppenderAccessor(QUERY_TOKEN_REC_DESC);
        this.queryTokenAppender.reset(queryTokenFrame, true);
    }

    @Override
    public void reset() {
        searchResult.clear();
        invListMerger.reset();
    }

    protected void tokenizeQuery(InvertedIndexSearchPredicate searchPred) throws HyracksDataException {
        ITupleReference queryTuple = searchPred.getQueryTuple();
        int queryFieldIndex = searchPred.getQueryFieldIndex();
        IBinaryTokenizer queryTokenizer = searchPred.getQueryTokenizer();
        // Is this a full-text query?
        // Then, the last argument is conjuctive or disjunctive search option, not a query text.
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
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public IFrameTupleAccessor createResultFrameTupleAccessor() {
        return new FixedSizeFrameTupleAccessor(ctx.getInitialFrameSize(), searchResult.getTypeTraits());
    }

    @Override
    public ITupleReference createResultFrameTupleReference() {
        return new FixedSizeTupleReference(searchResult.getTypeTraits());
    }

    @Override
    public List<ByteBuffer> getResultBuffers() {
        return searchResult.getBuffers();
    }

    @Override
    public int getNumValidResultBuffers() {
        return searchResult.getCurrentBufferIndex() + 1;
    }

    public int getOccurrenceThreshold() {
        return occurrenceThreshold;
    }

    public void printNewResults(int maxResultBufIdx, List<ByteBuffer> buffer) {
        StringBuffer strBuffer = new StringBuffer();
        FixedSizeFrameTupleAccessor resultFrameTupleAcc = searchResult.getAccessor();
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
}
