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

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexResultCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearcher;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IToken;

public class TOccurrenceSearcher implements IInvertedIndexSearcher {

    protected final IHyracksTaskContext ctx;
    protected final FixedSizeFrameTupleAppender resultFrameTupleApp;
    protected final FixedSizeFrameTupleAccessor resultFrameTupleAcc;
    protected final FixedSizeTupleReference resultTuple;
    protected final int invListKeyLength;
    protected int currentNumResults;

    protected List<ByteBuffer> newResultBuffers = new ArrayList<ByteBuffer>();
    protected List<ByteBuffer> prevResultBuffers = new ArrayList<ByteBuffer>();
    protected List<ByteBuffer> swap = null;
    protected int maxResultBufIdx = 0;

    protected final ITreeIndexFrame leafFrame;
    protected final ITreeIndexFrame interiorFrame;
    protected final ITreeIndexCursor btreeCursor;
    protected final FrameTupleReference searchKey = new FrameTupleReference();
    protected final RangePredicate btreePred = new RangePredicate(true, null, null, true, true, null, null);
    protected final ITreeIndexAccessor btreeAccessor;

    protected RecordDescriptor queryTokenRecDesc = new RecordDescriptor(
            new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });
    protected ArrayTupleBuilder queryTokenBuilder = new ArrayTupleBuilder(queryTokenRecDesc.getFields().length);
    protected DataOutput queryTokenDos = queryTokenBuilder.getDataOutput();
    protected FrameTupleAppender queryTokenAppender;
    protected ByteBuffer queryTokenFrame;

    protected final InvertedIndex invIndex;
    protected final IBinaryTokenizer queryTokenizer;
    protected final ITypeTraits[] invListFieldsWithCount;
    protected int occurrenceThreshold;

    protected final int cursorCacheSize = 10;
    protected List<IInvertedListCursor> invListCursorCache = new ArrayList<IInvertedListCursor>(cursorCacheSize);
    protected List<IInvertedListCursor> invListCursors = new ArrayList<IInvertedListCursor>(cursorCacheSize);

    public TOccurrenceSearcher(IHyracksTaskContext ctx, InvertedIndex invIndex, IBinaryTokenizer queryTokenizer) {
        this.ctx = ctx;
        this.invIndex = invIndex;
        this.queryTokenizer = queryTokenizer;

        leafFrame = invIndex.getBTree().getLeafFrameFactory().createFrame();
        interiorFrame = invIndex.getBTree().getInteriorFrameFactory().createFrame();

        btreeCursor = new BTreeRangeSearchCursor((IBTreeLeafFrame) leafFrame, false);
        ITypeTraits[] invListFields = invIndex.getTypeTraits();
        invListFieldsWithCount = new ITypeTraits[invListFields.length + 1];
        int tmp = 0;
        for (int i = 0; i < invListFields.length; i++) {
            invListFieldsWithCount[i] = invListFields[i];
            tmp += invListFields[i].getFixedLength();
        }
        // using an integer for counting occurrences
        invListFieldsWithCount[invListFields.length] = IntegerPointable.TYPE_TRAITS;
        invListKeyLength = tmp;

        resultFrameTupleApp = new FixedSizeFrameTupleAppender(ctx.getFrameSize(), invListFieldsWithCount);
        resultFrameTupleAcc = new FixedSizeFrameTupleAccessor(ctx.getFrameSize(), invListFieldsWithCount);
        resultTuple = new FixedSizeTupleReference(invListFieldsWithCount);
        newResultBuffers.add(ctx.allocateFrame());
        prevResultBuffers.add(ctx.allocateFrame());

        MultiComparator searchCmp = invIndex.getBTree().getMultiComparator();
        btreePred.setLowKeyComparator(searchCmp);
        btreePred.setHighKeyComparator(searchCmp);
        btreePred.setLowKey(searchKey, true);
        btreePred.setHighKey(searchKey, true);

        // pre-create cursor objects
        for (int i = 0; i < cursorCacheSize; i++) {
            invListCursorCache.add(new FixedSizeElementInvertedListCursor(invIndex.getBufferCache(), invIndex
                    .getInvListsFileId(), invIndex.getTypeTraits()));
        }

        queryTokenAppender = new FrameTupleAppender(ctx.getFrameSize());
        queryTokenFrame = ctx.allocateFrame();

        btreeAccessor = invIndex.getBTree().createAccessor();
        currentNumResults = 0;
    }

    public void reset() {
        for (ByteBuffer b : newResultBuffers) {
            resultFrameTupleApp.reset(b, true);
        }
        for (ByteBuffer b : prevResultBuffers) {
            resultFrameTupleApp.reset(b, true);
        }
        currentNumResults = 0;
    }

    public void search(IInvertedIndexResultCursor resultCursor, ITupleReference queryTuple, int queryField,
            IInvertedIndexSearchModifier searchModifier) throws Exception {

        queryTokenAppender.reset(queryTokenFrame, true);
        queryTokenizer.reset(queryTuple.getFieldData(queryField), queryTuple.getFieldStart(queryField),
                queryTuple.getFieldLength(queryField));

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

        // expand cursor cache if necessary
        if (numQueryTokens > invListCursorCache.size()) {
            int diff = numQueryTokens - invListCursorCache.size();
            for (int i = 0; i < diff; i++) {
                invListCursorCache.add(new FixedSizeElementInvertedListCursor(invIndex.getBufferCache(), invIndex
                        .getInvListsFileId(), invIndex.getTypeTraits()));
            }
        }

        invListCursors.clear();
        for (int i = 0; i < numQueryTokens; i++) {
            searchKey.reset(queryTokenAccessor, i);
            invIndex.openCursor(btreeCursor, btreePred, btreeAccessor, invListCursorCache.get(i));
            invListCursors.add(invListCursorCache.get(i));
        }

        occurrenceThreshold = searchModifier.getOccurrenceThreshold(invListCursors);

        // TODO: deal with panic cases properly
        if (occurrenceThreshold <= 0) {
            throw new OccurrenceThresholdPanicException("Merge Threshold is <= 0. Failing Search.");
        }

        int numPrefixLists = searchModifier.getPrefixLists(invListCursors);
        maxResultBufIdx = mergePrefixLists(numPrefixLists, numQueryTokens);
        maxResultBufIdx = mergeSuffixLists(numPrefixLists, numQueryTokens, maxResultBufIdx);

        resultCursor.reset(this);
    }

    protected int mergePrefixLists(int numPrefixTokens, int numQueryTokens) throws IOException {
        int maxPrevBufIdx = 0;
        for (int i = 0; i < numPrefixTokens; i++) {
            swap = prevResultBuffers;
            prevResultBuffers = newResultBuffers;
            newResultBuffers = swap;
            currentNumResults = 0;

            invListCursors.get(i).pinPagesSync();
            maxPrevBufIdx = mergePrefixList(invListCursors.get(i), prevResultBuffers, maxPrevBufIdx, newResultBuffers);
            invListCursors.get(i).unpinPages();
        }
        return maxPrevBufIdx;
    }

    protected int mergeSuffixLists(int numPrefixTokens, int numQueryTokens, int maxPrevBufIdx) throws IOException {
        for (int i = numPrefixTokens; i < numQueryTokens; i++) {
            swap = prevResultBuffers;
            prevResultBuffers = newResultBuffers;
            newResultBuffers = swap;

            invListCursors.get(i).pinPagesSync();
            int numInvListElements = invListCursors.get(i).getNumElements();
            // should we binary search the next list or should we sort-merge it?
            if (currentNumResults * Math.log(numInvListElements) < currentNumResults + numInvListElements) {
                maxPrevBufIdx = mergeSuffixListProbe(invListCursors.get(i), prevResultBuffers, maxPrevBufIdx,
                        newResultBuffers, i, numQueryTokens);
            } else {
                maxPrevBufIdx = mergeSuffixListScan(invListCursors.get(i), prevResultBuffers, maxPrevBufIdx,
                        newResultBuffers, i, numQueryTokens);
            }
            invListCursors.get(i).unpinPages();
        }
        return maxPrevBufIdx;
    }

    protected int mergeSuffixListProbe(IInvertedListCursor invListCursor, List<ByteBuffer> prevResultBuffers,
            int maxPrevBufIdx, List<ByteBuffer> newResultBuffers, int invListIx, int numQueryTokens) throws IOException {

        int newBufIdx = 0;
        ByteBuffer newCurrentBuffer = newResultBuffers.get(0);

        int prevBufIdx = 0;
        ByteBuffer prevCurrentBuffer = prevResultBuffers.get(0);

        int resultTidx = 0;

        currentNumResults = 0;

        MultiComparator invListCmp = invIndex.getInvListElementCmp();

        resultFrameTupleAcc.reset(prevCurrentBuffer);
        resultFrameTupleApp.reset(newCurrentBuffer, true);

        while (resultTidx < resultFrameTupleAcc.getTupleCount()) {

            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));
            int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                    resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));

            if (invListCursor.containsKey(resultTuple, invListCmp)) {
                count++;
                newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
            } else {
                if (count + numQueryTokens - invListIx > occurrenceThreshold) {
                    newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
                }
            }

            resultTidx++;
            if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                prevBufIdx++;
                if (prevBufIdx <= maxPrevBufIdx) {
                    prevCurrentBuffer = prevResultBuffers.get(prevBufIdx);
                    resultFrameTupleAcc.reset(prevCurrentBuffer);
                    resultTidx = 0;
                }
            }
        }

        return newBufIdx;
    }

    protected int mergeSuffixListScan(IInvertedListCursor invListCursor, List<ByteBuffer> prevResultBuffers,
            int maxPrevBufIdx, List<ByteBuffer> newResultBuffers, int invListIx, int numQueryTokens) throws IOException {
        int newBufIdx = 0;
        ByteBuffer newCurrentBuffer = newResultBuffers.get(0);

        int prevBufIdx = 0;
        ByteBuffer prevCurrentBuffer = prevResultBuffers.get(0);

        boolean advanceCursor = true;
        boolean advancePrevResult = false;
        int resultTidx = 0;

        MultiComparator invListCmp = invIndex.getInvListElementCmp();

        resultFrameTupleAcc.reset(prevCurrentBuffer);
        resultFrameTupleApp.reset(newCurrentBuffer, true);

        int invListTidx = 0;
        int invListNumTuples = invListCursor.getNumElements();

        if (invListCursor.hasNext())
            invListCursor.next();

        while (invListTidx < invListNumTuples && resultTidx < resultFrameTupleAcc.getTupleCount()) {

            ITupleReference invListTuple = invListCursor.getTuple();

            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));

            int cmp = invListCmp.compare(invListTuple, resultTuple);
            if (cmp == 0) {
                int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                        resultTuple.getFieldStart(resultTuple.getFieldCount() - 1)) + 1;
                newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
                advanceCursor = true;
                advancePrevResult = true;
            } else {
                if (cmp < 0) {
                    advanceCursor = true;
                    advancePrevResult = false;
                } else {
                    int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                            resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));
                    if (count + numQueryTokens - invListIx > occurrenceThreshold) {
                        newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
                    }
                    advanceCursor = false;
                    advancePrevResult = true;
                }
            }

            if (advancePrevResult) {
                resultTidx++;
                if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                    prevBufIdx++;
                    if (prevBufIdx <= maxPrevBufIdx) {
                        prevCurrentBuffer = prevResultBuffers.get(prevBufIdx);
                        resultFrameTupleAcc.reset(prevCurrentBuffer);
                        resultTidx = 0;
                    }
                }
            }

            if (advanceCursor) {
                invListTidx++;
                invListCursor.next();
            }
        }

        // append remaining elements from previous result set
        while (resultTidx < resultFrameTupleAcc.getTupleCount()) {

            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));

            int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                    resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));
            if (count + numQueryTokens - invListIx > occurrenceThreshold) {
                newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
            }

            resultTidx++;
            if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                prevBufIdx++;
                if (prevBufIdx <= maxPrevBufIdx) {
                    prevCurrentBuffer = prevResultBuffers.get(prevBufIdx);
                    resultFrameTupleAcc.reset(prevCurrentBuffer);
                    resultTidx = 0;
                }
            }
        }

        return newBufIdx;
    }

    protected int mergePrefixList(IInvertedListCursor invListCursor, List<ByteBuffer> prevResultBuffers,
            int maxPrevBufIdx, List<ByteBuffer> newResultBuffers) throws IOException {
        int newBufIdx = 0;
        ByteBuffer newCurrentBuffer = newResultBuffers.get(0);

        int prevBufIdx = 0;
        ByteBuffer prevCurrentBuffer = prevResultBuffers.get(0);

        boolean advanceCursor = true;
        boolean advancePrevResult = false;
        int resultTidx = 0;

        MultiComparator invListCmp = invIndex.getInvListElementCmp();

        resultFrameTupleAcc.reset(prevCurrentBuffer);
        resultFrameTupleApp.reset(newCurrentBuffer, true);

        int invListTidx = 0;
        int invListNumTuples = invListCursor.getNumElements();

        if (invListCursor.hasNext())
            invListCursor.next();

        while (invListTidx < invListNumTuples && resultTidx < resultFrameTupleAcc.getTupleCount()) {

            ITupleReference invListTuple = invListCursor.getTuple();
            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));

            int cmp = invListCmp.compare(invListTuple, resultTuple);
            if (cmp == 0) {
                int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                        resultTuple.getFieldStart(resultTuple.getFieldCount() - 1)) + 1;
                newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
                advanceCursor = true;
                advancePrevResult = true;
            } else {
                if (cmp < 0) {
                    int count = 1;
                    newBufIdx = appendTupleToNewResults(invListTuple, count, newBufIdx);
                    advanceCursor = true;
                    advancePrevResult = false;
                } else {
                    int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                            resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));
                    newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);
                    advanceCursor = false;
                    advancePrevResult = true;
                }
            }

            if (advancePrevResult) {
                resultTidx++;
                if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                    prevBufIdx++;
                    if (prevBufIdx <= maxPrevBufIdx) {
                        prevCurrentBuffer = prevResultBuffers.get(prevBufIdx);
                        resultFrameTupleAcc.reset(prevCurrentBuffer);
                        resultTidx = 0;
                    }
                }
            }

            if (advanceCursor) {
                invListTidx++;
                invListCursor.next();
            }
        }

        // append remaining new elements from inverted list
        while (invListTidx < invListNumTuples) {
            ITupleReference invListTuple = invListCursor.getTuple();
            newBufIdx = appendTupleToNewResults(invListTuple, 1, newBufIdx);
            invListTidx++;
            invListCursor.next();
        }

        // append remaining elements from previous result set
        while (resultTidx < resultFrameTupleAcc.getTupleCount()) {

            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));

            int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                    resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));
            newBufIdx = appendTupleToNewResults(resultTuple, count, newBufIdx);

            resultTidx++;
            if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                prevBufIdx++;
                if (prevBufIdx <= maxPrevBufIdx) {
                    prevCurrentBuffer = prevResultBuffers.get(prevBufIdx);
                    resultFrameTupleAcc.reset(prevCurrentBuffer);
                    resultTidx = 0;
                }
            }
        }

        return newBufIdx;
    }

    protected int appendTupleToNewResults(ITupleReference tuple, int newCount, int newBufIdx) throws IOException {
        ByteBuffer newCurrentBuffer = newResultBuffers.get(newBufIdx);

        if (!resultFrameTupleApp.hasSpace()) {
            newBufIdx++;
            if (newBufIdx >= newResultBuffers.size()) {
                newResultBuffers.add(ctx.allocateFrame());
            }
            newCurrentBuffer = newResultBuffers.get(newBufIdx);
            resultFrameTupleApp.reset(newCurrentBuffer, true);
        }

        // append key
        if (!resultFrameTupleApp.append(tuple.getFieldData(0), tuple.getFieldStart(0), invListKeyLength)) {
            throw new IllegalStateException();
        }

        // append new count
        if (!resultFrameTupleApp.append(newCount)) {
            throw new IllegalStateException();
        }

        resultFrameTupleApp.incrementTupleCount(1);

        currentNumResults++;

        return newBufIdx;
    }

    public IFrameTupleAccessor createResultFrameTupleAccessor() {
        return new FixedSizeFrameTupleAccessor(ctx.getFrameSize(), invListFieldsWithCount);
    }

    public ITupleReference createResultTupleReference() {
        return new FixedSizeTupleReference(invListFieldsWithCount);
    }

    @Override
    public List<ByteBuffer> getResultBuffers() {
        return newResultBuffers;
    }

    @Override
    public int getNumValidResultBuffers() {
        return maxResultBufIdx + 1;
    }

    public int getOccurrenceThreshold() {
        return occurrenceThreshold;
    }

    public void printNewResults(int maxResultBufIdx) {
        StringBuffer strBuffer = new StringBuffer();
        for (int i = 0; i <= maxResultBufIdx; i++) {
            ByteBuffer testBuf = newResultBuffers.get(i);
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
