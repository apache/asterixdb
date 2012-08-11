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

import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;

public class TOccurrenceSearcherSuffixProbeOnly extends TOccurrenceSearcher {

	protected final MultiComparator invListCmp;
	
    public TOccurrenceSearcherSuffixProbeOnly(IHyracksTaskContext ctx, OnDiskInvertedIndex invIndex) {
        super(ctx, invIndex);
        this.invListCmp = MultiComparator.create(invIndex.getInvListCmpFactories());
    }

    protected int mergeSuffixLists(int numPrefixTokens, int numQueryTokens, int maxPrevBufIdx) throws HyracksDataException {
        for (int i = numPrefixTokens; i < numQueryTokens; i++) {
            swap = prevResultBuffers;
            prevResultBuffers = newResultBuffers;
            newResultBuffers = swap;
            currentNumResults = 0;

            invListCursors.get(i).pinPages();
            maxPrevBufIdx = mergeSuffixListProbe(invListCursors.get(i), prevResultBuffers, maxPrevBufIdx,
                    newResultBuffers, i, numQueryTokens);
            invListCursors.get(i).unpinPages();
        }
        return maxPrevBufIdx;
    }

    protected int mergeSuffixListProbe(IInvertedListCursor invListCursor, List<ByteBuffer> prevResultBuffers,
            int maxPrevBufIdx, List<ByteBuffer> newResultBuffers, int invListIx, int numQueryTokens) throws HyracksDataException {

        int newBufIdx = 0;
        ByteBuffer newCurrentBuffer = newResultBuffers.get(0);

        int prevBufIdx = 0;
        ByteBuffer prevCurrentBuffer = prevResultBuffers.get(0);

        int resultTidx = 0;

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
}
