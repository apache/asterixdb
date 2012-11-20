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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.exceptions.OccurrenceThresholdPanicException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeFrameTupleAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexSearchCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;

public class TOccurrenceSearcher implements IInvertedIndexSearcher {

    protected final IHyracksCommonContext ctx;

    protected final InvertedListMerger invListMerger;
    protected final SearchResult searchResult;

    protected RecordDescriptor queryTokenRecDesc = new RecordDescriptor(
            new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });
    protected ArrayTupleBuilder queryTokenBuilder = new ArrayTupleBuilder(queryTokenRecDesc.getFieldCount());
    protected DataOutput queryTokenDos = queryTokenBuilder.getDataOutput();
    protected FrameTupleAppender queryTokenAppender;
    protected ByteBuffer queryTokenFrame;
    protected final FrameTupleReference searchKey = new FrameTupleReference();

    protected final IInvertedIndex invIndex;
    protected final MultiComparator invListCmp;
    protected int occurrenceThreshold;

    protected final int cursorCacheSize = 10;
    protected ArrayList<IInvertedListCursor> invListCursorCache = new ArrayList<IInvertedListCursor>(cursorCacheSize);
    protected ArrayList<IInvertedListCursor> invListCursors = new ArrayList<IInvertedListCursor>(cursorCacheSize);

    public TOccurrenceSearcher(IHyracksCommonContext ctx, IInvertedIndex invIndex) {
        this.ctx = ctx;
        this.invListMerger = new InvertedListMerger(ctx, invIndex);
        this.searchResult = new SearchResult(invIndex.getInvListTypeTraits(), ctx);
        this.invIndex = invIndex;
        this.invListCmp = MultiComparator.create(invIndex.getInvListCmpFactories());

        // Pre-create cursor objects.
        for (int i = 0; i < cursorCacheSize; i++) {
            invListCursorCache.add(invIndex.createInvertedListCursor());
        }

        queryTokenAppender = new FrameTupleAppender(ctx.getFrameSize());
        queryTokenFrame = ctx.allocateFrame();
    }

    public void reset() {
        searchResult.clear();
        invListMerger.reset();
    }

    public void search(OnDiskInvertedIndexSearchCursor resultCursor, InvertedIndexSearchPredicate searchPred,
            IIndexOperationContext ictx) throws HyracksDataException, IndexException {
        ITupleReference queryTuple = searchPred.getQueryTuple();
        int queryFieldIndex = searchPred.getQueryFieldIndex();
        IInvertedIndexSearchModifier searchModifier = searchPred.getSearchModifier();
        IBinaryTokenizer queryTokenizer = searchPred.getQueryTokenizer();

        queryTokenAppender.reset(queryTokenFrame, true);
        queryTokenizer.reset(queryTuple.getFieldData(queryFieldIndex), queryTuple.getFieldStart(queryFieldIndex),
                queryTuple.getFieldLength(queryFieldIndex));

        while (queryTokenizer.hasNext()) {
            queryTokenizer.next();
            queryTokenBuilder.reset();
            try {
                IToken token = queryTokenizer.getToken();
                token.serializeToken(queryTokenDos);
                queryTokenBuilder.addFieldEndOffset();
                // WARNING: assuming one frame is big enough to hold all tokens
                queryTokenAppender.append(queryTokenBuilder.getFieldEndOffsets(), queryTokenBuilder.getByteArray(), 0,
                        queryTokenBuilder.getSize());
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        FrameTupleAccessor queryTokenAccessor = new FrameTupleAccessor(ctx.getFrameSize(), queryTokenRecDesc);
        queryTokenAccessor.reset(queryTokenFrame);
        int numQueryTokens = queryTokenAccessor.getTupleCount();

        // Expand cursor cache if necessary.
        if (numQueryTokens > invListCursorCache.size()) {
            int diff = numQueryTokens - invListCursorCache.size();
            for (int i = 0; i < diff; i++) {
                invListCursorCache.add(invIndex.createInvertedListCursor());
            }
        }

        invListCursors.clear();
        for (int i = 0; i < numQueryTokens; i++) {
            searchKey.reset(queryTokenAccessor, i);
            invIndex.openInvertedListCursor(invListCursorCache.get(i), searchKey, ictx);
            invListCursors.add(invListCursorCache.get(i));
        }

        occurrenceThreshold = searchModifier.getOccurrenceThreshold(numQueryTokens);
        if (occurrenceThreshold <= 0) {
            throw new OccurrenceThresholdPanicException("Merge threshold is <= 0. Failing Search.");
        }
        int numPrefixLists = searchModifier.getNumPrefixLists(occurrenceThreshold, invListCursors.size());

        searchResult.reset();
        invListMerger.merge(invListCursors, occurrenceThreshold, numPrefixLists, searchResult);
        resultCursor.open(null, searchPred);
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
