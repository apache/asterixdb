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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.fuzzyjoin.tokenizer.IBinaryTokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.IToken;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.TreeIndexOp;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexResultCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearcher;

public class SimpleConjunctiveSearcher implements IInvertedIndexSearcher {

    private final int numKeyFields;
    private final int numValueFields;

    private final IBinaryComparator[] keyCmps;
    private final IBinaryComparator[] valueCmps;

    private final BTree btree;
    private final IHyracksStageletContext ctx;
    private final ArrayTupleBuilder resultTupleBuilder;
    private final FrameTupleAppender resultTupleAppender;
    private final FrameTupleAccessor resultFrameAccessor;

    private List<ByteBuffer> newResultBuffers = new ArrayList<ByteBuffer>();
    private List<ByteBuffer> prevResultBuffers = new ArrayList<ByteBuffer>();
    private List<ByteBuffer> swap = null;
    private final ListResultCursor resultCursor = new ListResultCursor();
    private int maxResultBufIdx = 0;

    private final IBTreeLeafFrame leafFrame;
    private final IBTreeInteriorFrame interiorFrame;
    private final IBTreeCursor btreeCursor;
    private final FrameTupleReference searchKey = new FrameTupleReference();
    private final RangePredicate pred = new RangePredicate(true, null, null, true, true, null, null);

    private final IBinaryTokenizer queryTokenizer;

    public SimpleConjunctiveSearcher(IHyracksStageletContext ctx, BTree btree, RecordDescriptor btreeRecDesc,
            IBinaryTokenizer queryTokenizer, int numKeyFields, int numValueFields) {
        this.ctx = ctx;
        this.btree = btree;
        this.queryTokenizer = queryTokenizer;
        this.numKeyFields = numKeyFields;
        this.numValueFields = numValueFields;

        leafFrame = btree.getLeafFrameFactory().getFrame();
        interiorFrame = btree.getInteriorFrameFactory().getFrame();
        btreeCursor = new RangeSearchCursor(leafFrame);
        resultTupleAppender = new FrameTupleAppender(ctx.getFrameSize());
        resultTupleBuilder = new ArrayTupleBuilder(numValueFields);
        newResultBuffers.add(ctx.allocateFrame());
        prevResultBuffers.add(ctx.allocateFrame());

        MultiComparator btreeCmp = btree.getMultiComparator();

        keyCmps = new IBinaryComparator[numKeyFields];
        for (int i = 0; i < numKeyFields; i++) {
            keyCmps[i] = btreeCmp.getComparators()[i];
        }

        valueCmps = new IBinaryComparator[numValueFields];
        for (int i = 0; i < numValueFields; i++) {
            valueCmps[i] = btreeCmp.getComparators()[numKeyFields + i];
        }

        MultiComparator searchCmp = new MultiComparator(btreeCmp.getTypeTraits(), keyCmps);
        pred.setLowKeyComparator(searchCmp);
        pred.setHighKeyComparator(searchCmp);
        pred.setLowKey(searchKey, true);
        pred.setHighKey(searchKey, true);

        ISerializerDeserializer[] valueSerde = new ISerializerDeserializer[numValueFields];
        for (int i = 0; i < numValueFields; i++) {
            valueSerde[i] = btreeRecDesc.getFields()[numKeyFields + i];

        }
        RecordDescriptor valueRecDesc = new RecordDescriptor(valueSerde);
        resultFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), valueRecDesc);
    }

    public void search(ITupleReference queryTuple, int queryFieldIndex) throws Exception {

        // parse query, TODO: this parsing is too simple
        RecordDescriptor queryTokenRecDesc = new RecordDescriptor(
                new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });

        ArrayTupleBuilder queryTokenBuilder = new ArrayTupleBuilder(queryTokenRecDesc.getFields().length);
        DataOutput queryTokenDos = queryTokenBuilder.getDataOutput();
        FrameTupleAppender queryTokenAppender = new FrameTupleAppender(ctx.getFrameSize());
        ByteBuffer queryTokenFrame = ctx.allocateFrame();
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
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }

            // WARNING: assuming one frame is enough to hold all tokens
            queryTokenAppender.append(queryTokenBuilder.getFieldEndOffsets(), queryTokenBuilder.getByteArray(), 0,
                    queryTokenBuilder.getSize());
        }

        FrameTupleAccessor queryTokenAccessor = new FrameTupleAccessor(ctx.getFrameSize(), queryTokenRecDesc);
        queryTokenAccessor.reset(queryTokenFrame);
        int numQueryTokens = queryTokenAccessor.getTupleCount();

        maxResultBufIdx = 0;

        BTreeOpContext opCtx = btree.createOpContext(TreeIndexOp.TI_SEARCH, leafFrame, interiorFrame, null);

        resultTupleAppender.reset(newResultBuffers.get(0), true);
        try {
            // append first inverted list to temporary results
            searchKey.reset(queryTokenAccessor, 0);
            btree.search(btreeCursor, pred, opCtx);
            while (btreeCursor.hasNext()) {
                btreeCursor.next();
                maxResultBufIdx = appendTupleToNewResults(btreeCursor, maxResultBufIdx);
            }
            btreeCursor.close();
            btreeCursor.reset();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        resultFrameAccessor.reset(newResultBuffers.get(0));

        // intersect temporary results with remaining inverted lists
        for (int i = 1; i < numQueryTokens; i++) {
            swap = prevResultBuffers;
            prevResultBuffers = newResultBuffers;
            newResultBuffers = swap;
            try {
                searchKey.reset(queryTokenAccessor, i);
                btree.search(btreeCursor, pred, opCtx);
                maxResultBufIdx = intersectList(btreeCursor, prevResultBuffers, maxResultBufIdx, newResultBuffers);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
            btreeCursor.close();
            btreeCursor.reset();
        }
    }

    private int appendTupleToNewResults(IBTreeCursor btreeCursor, int newBufIdx) throws IOException {
        ByteBuffer newCurrentBuffer = newResultBuffers.get(newBufIdx);

        ITupleReference tuple = btreeCursor.getTuple();
        resultTupleBuilder.reset();
        DataOutput dos = resultTupleBuilder.getDataOutput();
        for (int i = 0; i < numValueFields; i++) {
            int fIdx = numKeyFields + i;
            dos.write(tuple.getFieldData(fIdx), tuple.getFieldStart(fIdx), tuple.getFieldLength(fIdx));
            resultTupleBuilder.addFieldEndOffset();
        }

        if (!resultTupleAppender.append(resultTupleBuilder.getFieldEndOffsets(), resultTupleBuilder.getByteArray(), 0,
                resultTupleBuilder.getSize())) {
            newBufIdx++;
            if (newBufIdx >= newResultBuffers.size()) {
                newResultBuffers.add(ctx.allocateFrame());
            }
            newCurrentBuffer = newResultBuffers.get(newBufIdx);
            resultTupleAppender.reset(newCurrentBuffer, true);
            if (!resultTupleAppender.append(resultTupleBuilder.getFieldEndOffsets(), resultTupleBuilder.getByteArray(),
                    0, resultTupleBuilder.getSize())) {
                throw new IllegalStateException();
            }
        }

        return newBufIdx;
    }

    private int intersectList(IBTreeCursor btreeCursor, List<ByteBuffer> prevResultBuffers, int maxPrevBufIdx,
            List<ByteBuffer> newResultBuffers) throws IOException, Exception {

        int newBufIdx = 0;
        ByteBuffer newCurrentBuffer = newResultBuffers.get(0);

        int prevBufIdx = 0;
        ByteBuffer prevCurrentBuffer = prevResultBuffers.get(0);

        resultTupleBuilder.reset();
        resultTupleAppender.reset(newCurrentBuffer, true);
        resultFrameAccessor.reset(prevCurrentBuffer);

        // WARNING: not very efficient but good enough for the first cut
        boolean advanceCursor = true;
        boolean advancePrevResult = false;
        int resultTidx = 0;

        while ((!advanceCursor || btreeCursor.hasNext()) && prevBufIdx <= maxPrevBufIdx
                && resultTidx < resultFrameAccessor.getTupleCount()) {

            if (advanceCursor)
                btreeCursor.next();
            ITupleReference tuple = btreeCursor.getTuple();

            int cmp = 0;
            for (int i = 0; i < valueCmps.length; i++) {
                int tupleFidx = numKeyFields + i;
                cmp = valueCmps[i].compare(tuple.getFieldData(tupleFidx), tuple.getFieldStart(tupleFidx),
                        tuple.getFieldLength(tupleFidx), resultFrameAccessor.getBuffer().array(),
                        resultFrameAccessor.getTupleStartOffset(resultTidx) + resultFrameAccessor.getFieldSlotsLength()
                                + resultFrameAccessor.getFieldStartOffset(resultTidx, i),
                        resultFrameAccessor.getFieldLength(resultTidx, i));
                if (cmp != 0)
                    break;
            }

            // match found
            if (cmp == 0) {
                newBufIdx = appendTupleToNewResults(btreeCursor, newBufIdx);

                advanceCursor = true;
                advancePrevResult = true;
            } else {
                if (cmp < 0) {
                    advanceCursor = true;
                    advancePrevResult = false;
                } else {
                    advanceCursor = false;
                    advancePrevResult = true;
                }
            }

            if (advancePrevResult) {
                resultTidx++;
                if (resultTidx >= resultFrameAccessor.getTupleCount()) {
                    prevBufIdx++;
                    if (prevBufIdx <= maxPrevBufIdx) {
                        prevCurrentBuffer = prevResultBuffers.get(prevBufIdx);
                        resultFrameAccessor.reset(prevCurrentBuffer);
                        resultTidx = 0;
                    }
                }
            }
        }

        return newBufIdx;
    }

    @Override
    public IInvertedIndexResultCursor getResultCursor() {
        resultCursor.setResults(newResultBuffers, maxResultBufIdx + 1);
        return resultCursor;
    }
}
