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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IObjectFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.exceptions.OccurrenceThresholdPanicException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeFrameTupleAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.ObjectCache;

public abstract class AbstractTOccurrenceSearcher implements IInvertedIndexSearcher {
    protected static final RecordDescriptor QUERY_TOKEN_REC_DESC = new RecordDescriptor(
            new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });

    protected final int OBJECT_CACHE_INIT_SIZE = 10;
    protected final int OBJECT_CACHE_EXPAND_SIZE = 10;

    protected final IHyracksCommonContext ctx;

    protected final InvertedListMerger invListMerger;
    protected final SearchResult searchResult;
    protected final IInvertedIndex invIndex;
    protected final MultiComparator invListCmp;

    protected final ArrayTupleBuilder queryTokenBuilder = new ArrayTupleBuilder(QUERY_TOKEN_REC_DESC.getFieldCount());
    protected final ByteBuffer queryTokenFrame;
    protected final FrameTupleAppender queryTokenAppender;
    protected final FrameTupleAccessor queryTokenAccessor;
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
        this.invListCursorCache = new ObjectCache<IInvertedListCursor>(invListCursorFactory, OBJECT_CACHE_INIT_SIZE,
                OBJECT_CACHE_EXPAND_SIZE);
        this.queryTokenFrame = ctx.allocateFrame();
        this.queryTokenAppender = new FrameTupleAppender(ctx.getFrameSize());
        this.queryTokenAccessor = new FrameTupleAccessor(ctx.getFrameSize(), QUERY_TOKEN_REC_DESC);
        this.queryTokenAccessor.reset(queryTokenFrame);
    }

    public void reset() {
        searchResult.clear();
        invListMerger.reset();
    }

    protected void tokenizeQuery(InvertedIndexSearchPredicate searchPred) throws HyracksDataException,
            OccurrenceThresholdPanicException {
        ITupleReference queryTuple = searchPred.getQueryTuple();
        int queryFieldIndex = searchPred.getQueryFieldIndex();
        IBinaryTokenizer queryTokenizer = searchPred.getQueryTokenizer();

        queryTokenAppender.reset(queryTokenFrame, true);
        queryTokenizer.reset(queryTuple.getFieldData(queryFieldIndex), queryTuple.getFieldStart(queryFieldIndex),
                queryTuple.getFieldLength(queryFieldIndex));

        while (queryTokenizer.hasNext()) {
            queryTokenizer.next();
            queryTokenBuilder.reset();
            try {
                IToken token = queryTokenizer.getToken();
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

    public IFrameTupleAccessor createResultFrameTupleAccessor() {
        return new FixedSizeFrameTupleAccessor(ctx.getFrameSize(), searchResult.getTypeTraits());
    }

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
                strBuffer.append(IntegerSerializerDeserializer.getInt(resultFrameTupleAcc.getBuffer().array(),
                        resultFrameTupleAcc.getFieldStartOffset(j, 0)) + ",");
                strBuffer.append(IntegerSerializerDeserializer.getInt(resultFrameTupleAcc.getBuffer().array(),
                        resultFrameTupleAcc.getFieldStartOffset(j, 1)) + " ");
            }
        }
        System.out.println(strBuffer.toString());
    }
}
