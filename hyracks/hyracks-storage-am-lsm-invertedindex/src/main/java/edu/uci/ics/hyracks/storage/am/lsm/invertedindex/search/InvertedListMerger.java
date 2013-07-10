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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeFrameTupleAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeTupleReference;

// TODO: The merge procedure is rather confusing regarding cursor positions, hasNext() calls etc.
// Needs an overhaul some time.
public class InvertedListMerger {

    protected final MultiComparator invListCmp;
    protected SearchResult prevSearchResult;
    protected SearchResult newSearchResult;

    public InvertedListMerger(IHyracksCommonContext ctx, IInvertedIndex invIndex) throws HyracksDataException {
        this.invListCmp = MultiComparator.createIgnoreFieldLength(invIndex.getInvListCmpFactories());
        this.prevSearchResult = new SearchResult(invIndex.getInvListTypeTraits(), ctx);
        this.newSearchResult = new SearchResult(prevSearchResult);
    }

    public void merge(ArrayList<IInvertedListCursor> invListCursors, int occurrenceThreshold, int numPrefixLists,
            SearchResult searchResult) throws HyracksDataException, IndexException {
        Collections.sort(invListCursors);
        int numInvLists = invListCursors.size();
        SearchResult result = null;
        for (int i = 0; i < numInvLists; i++) {
            SearchResult swapTemp = prevSearchResult;
            prevSearchResult = newSearchResult;
            newSearchResult = swapTemp;
            newSearchResult.reset();
            if (i + 1 != numInvLists) {
                // Use temporary search results when not merging last list.
                result = newSearchResult;
            } else {
                // When merging the last list, append results to the final search result.
                result = searchResult;
            }
            IInvertedListCursor invListCursor = invListCursors.get(i);
            invListCursor.pinPages();
            if (i < numPrefixLists) {
                // Merge prefix list.
                mergePrefixList(invListCursor, prevSearchResult, result);
            } else {
                // Merge suffix list.
                int numInvListElements = invListCursor.size();
                int currentNumResults = prevSearchResult.getNumResults();
                // Should we binary search the next list or should we sort-merge it?
                if (currentNumResults * Math.log(numInvListElements) < currentNumResults + numInvListElements) {
                    mergeSuffixListProbe(invListCursor, prevSearchResult, result, i, numInvLists, occurrenceThreshold);
                } else {
                    mergeSuffixListScan(invListCursor, prevSearchResult, result, i, numInvLists, occurrenceThreshold);
                }
            }
            invListCursor.unpinPages();
        }
    }

    protected void mergeSuffixListProbe(IInvertedListCursor invListCursor, SearchResult prevSearchResult,
            SearchResult newSearchResult, int invListIx, int numInvLists, int occurrenceThreshold)
            throws HyracksDataException, IndexException {

        int prevBufIdx = 0;
        int maxPrevBufIdx = prevSearchResult.getCurrentBufferIndex();
        ByteBuffer prevCurrentBuffer = prevSearchResult.getBuffers().get(0);

        FixedSizeFrameTupleAccessor resultFrameTupleAcc = prevSearchResult.getAccessor();
        FixedSizeTupleReference resultTuple = prevSearchResult.getTuple();

        int resultTidx = 0;

        resultFrameTupleAcc.reset(prevCurrentBuffer);

        while (resultTidx < resultFrameTupleAcc.getTupleCount()) {

            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));
            int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                    resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));

            if (invListCursor.containsKey(resultTuple, invListCmp)) {
                count++;
                newSearchResult.append(resultTuple, count);
            } else {
                if (count + numInvLists - invListIx > occurrenceThreshold) {
                    newSearchResult.append(resultTuple, count);
                }
            }

            resultTidx++;
            if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                prevBufIdx++;
                if (prevBufIdx <= maxPrevBufIdx) {
                    prevCurrentBuffer = prevSearchResult.getBuffers().get(prevBufIdx);
                    resultFrameTupleAcc.reset(prevCurrentBuffer);
                    resultTidx = 0;
                }
            }
        }
    }

    protected void mergeSuffixListScan(IInvertedListCursor invListCursor, SearchResult prevSearchResult,
            SearchResult newSearchResult, int invListIx, int numInvLists, int occurrenceThreshold)
            throws HyracksDataException, IndexException {

        int prevBufIdx = 0;
        int maxPrevBufIdx = prevSearchResult.getCurrentBufferIndex();
        ByteBuffer prevCurrentBuffer = prevSearchResult.getBuffers().get(0);

        FixedSizeFrameTupleAccessor resultFrameTupleAcc = prevSearchResult.getAccessor();
        FixedSizeTupleReference resultTuple = prevSearchResult.getTuple();

        boolean advanceCursor = true;
        boolean advancePrevResult = false;
        int resultTidx = 0;

        resultFrameTupleAcc.reset(prevCurrentBuffer);

        int invListTidx = 0;
        int invListNumTuples = invListCursor.size();

        if (invListCursor.hasNext())
            invListCursor.next();

        while (invListTidx < invListNumTuples && resultTidx < resultFrameTupleAcc.getTupleCount()) {

            ITupleReference invListTuple = invListCursor.getTuple();

            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));

            int cmp = invListCmp.compare(invListTuple, resultTuple);
            if (cmp == 0) {
                int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                        resultTuple.getFieldStart(resultTuple.getFieldCount() - 1)) + 1;
                newSearchResult.append(resultTuple, count);
                advanceCursor = true;
                advancePrevResult = true;
            } else {
                if (cmp < 0) {
                    advanceCursor = true;
                    advancePrevResult = false;
                } else {
                    int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                            resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));
                    if (count + numInvLists - invListIx > occurrenceThreshold) {
                        newSearchResult.append(resultTuple, count);
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
                        prevCurrentBuffer = prevSearchResult.getBuffers().get(prevBufIdx);
                        resultFrameTupleAcc.reset(prevCurrentBuffer);
                        resultTidx = 0;
                    }
                }
            }

            if (advanceCursor) {
                invListTidx++;
                if (invListCursor.hasNext()) {
                    invListCursor.next();
                }
            }
        }

        // append remaining elements from previous result set
        while (resultTidx < resultFrameTupleAcc.getTupleCount()) {

            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));

            int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                    resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));
            if (count + numInvLists - invListIx > occurrenceThreshold) {
                newSearchResult.append(resultTuple, count);
            }

            resultTidx++;
            if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                prevBufIdx++;
                if (prevBufIdx <= maxPrevBufIdx) {
                    prevCurrentBuffer = prevSearchResult.getBuffers().get(prevBufIdx);
                    resultFrameTupleAcc.reset(prevCurrentBuffer);
                    resultTidx = 0;
                }
            }
        }
    }

    protected void mergePrefixList(IInvertedListCursor invListCursor, SearchResult prevSearchResult,
            SearchResult newSearchResult) throws HyracksDataException, IndexException {

        int prevBufIdx = 0;
        int maxPrevBufIdx = prevSearchResult.getCurrentBufferIndex();
        ByteBuffer prevCurrentBuffer = prevSearchResult.getBuffers().get(0);

        FixedSizeFrameTupleAccessor resultFrameTupleAcc = prevSearchResult.getAccessor();
        FixedSizeTupleReference resultTuple = prevSearchResult.getTuple();

        boolean advanceCursor = true;
        boolean advancePrevResult = false;
        int resultTidx = 0;

        resultFrameTupleAcc.reset(prevCurrentBuffer);

        int invListTidx = 0;
        int invListNumTuples = invListCursor.size();

        if (invListCursor.hasNext())
            invListCursor.next();

        while (invListTidx < invListNumTuples && resultTidx < resultFrameTupleAcc.getTupleCount()) {

            ITupleReference invListTuple = invListCursor.getTuple();
            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));

            int cmp = invListCmp.compare(invListTuple, resultTuple);
            if (cmp == 0) {
                int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                        resultTuple.getFieldStart(resultTuple.getFieldCount() - 1)) + 1;
                newSearchResult.append(resultTuple, count);
                advanceCursor = true;
                advancePrevResult = true;
            } else {
                if (cmp < 0) {
                    int count = 1;
                    newSearchResult.append(invListTuple, count);
                    advanceCursor = true;
                    advancePrevResult = false;
                } else {
                    int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                            resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));
                    newSearchResult.append(resultTuple, count);
                    advanceCursor = false;
                    advancePrevResult = true;
                }
            }

            if (advancePrevResult) {
                resultTidx++;
                if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                    prevBufIdx++;
                    if (prevBufIdx <= maxPrevBufIdx) {
                        prevCurrentBuffer = prevSearchResult.getBuffers().get(prevBufIdx);
                        resultFrameTupleAcc.reset(prevCurrentBuffer);
                        resultTidx = 0;
                    }
                }
            }

            if (advanceCursor) {
                invListTidx++;
                if (invListCursor.hasNext()) {
                    invListCursor.next();
                }
            }
        }

        // append remaining new elements from inverted list
        while (invListTidx < invListNumTuples) {
            ITupleReference invListTuple = invListCursor.getTuple();
            newSearchResult.append(invListTuple, 1);
            invListTidx++;
            if (invListCursor.hasNext()) {
                invListCursor.next();
            }
        }

        // append remaining elements from previous result set
        while (resultTidx < resultFrameTupleAcc.getTupleCount()) {

            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));

            int count = IntegerSerializerDeserializer.getInt(resultTuple.getFieldData(0),
                    resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));
            newSearchResult.append(resultTuple, count);

            resultTidx++;
            if (resultTidx >= resultFrameTupleAcc.getTupleCount()) {
                prevBufIdx++;
                if (prevBufIdx <= maxPrevBufIdx) {
                    prevCurrentBuffer = prevSearchResult.getBuffers().get(prevBufIdx);
                    resultFrameTupleAcc.reset(prevCurrentBuffer);
                    resultTidx = 0;
                }
            }
        }
    }

    public SearchResult createSearchResult() throws HyracksDataException {
        return new SearchResult(prevSearchResult);
    }

    public void reset() {
        prevSearchResult.clear();
        newSearchResult.clear();
    }
}
