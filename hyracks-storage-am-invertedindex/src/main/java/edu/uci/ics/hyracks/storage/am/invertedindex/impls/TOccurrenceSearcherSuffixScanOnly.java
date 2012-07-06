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
import java.util.List;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;

public class TOccurrenceSearcherSuffixScanOnly extends TOccurrenceSearcher {

	protected final MultiComparator invListCmp;
	
    public TOccurrenceSearcherSuffixScanOnly(IHyracksTaskContext ctx, InvertedIndex invIndex) {
        super(ctx, invIndex);
        this.invListCmp = MultiComparator.create(invIndex.getInvListElementCmpFactories());
    }

    protected int mergeSuffixLists(int numPrefixTokens, int numQueryTokens, int maxPrevBufIdx) throws HyracksDataException {
        for (int i = numPrefixTokens; i < numQueryTokens; i++) {
            swap = prevResultBuffers;
            prevResultBuffers = newResultBuffers;
            newResultBuffers = swap;
            currentNumResults = 0;

            invListCursors.get(i).pinPagesSync();
            maxPrevBufIdx = mergeSuffixListScan(invListCursors.get(i), prevResultBuffers, maxPrevBufIdx,
                    newResultBuffers, i, numQueryTokens);
            invListCursors.get(i).unpinPages();
        }
        return maxPrevBufIdx;
    }

    protected int mergeSuffixListScan(IInvertedListCursor invListCursor, List<ByteBuffer> prevResultBuffers,
            int maxPrevBufIdx, List<ByteBuffer> newResultBuffers, int invListIx, int numQueryTokens) {

        int newBufIdx = 0;
        ByteBuffer newCurrentBuffer = newResultBuffers.get(0);

        int prevBufIdx = 0;
        ByteBuffer prevCurrentBuffer = prevResultBuffers.get(0);

        boolean advanceCursor = true;
        boolean advancePrevResult = false;
        int resultTidx = 0;

        resultFrameTupleAcc.reset(prevCurrentBuffer);
        resultFrameTupleApp.reset(newCurrentBuffer, true);

        while (invListCursor.hasNext() && resultTidx < resultFrameTupleAcc.getTupleCount()) {

            if (advanceCursor)
                invListCursor.next();

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
        }

        // append remaining elements from previous result set
        while (resultTidx < resultFrameTupleAcc.getTupleCount()) {

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
}
