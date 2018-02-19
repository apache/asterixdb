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
import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.tuples.ConcatenatingTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInPlaceInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IPartitionedInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.InvertedListCursor;
import org.apache.hyracks.storage.common.IIndexCursor;

/**
 * Conducts T-Occurrence searches on inverted lists in one or more partition.
 */
public class PartitionedTOccurrenceSearcher extends AbstractTOccurrenceSearcher {

    protected final ArrayTupleBuilder lowerBoundTupleBuilder = new ArrayTupleBuilder(1);
    protected final ArrayTupleReference lowerBoundTuple = new ArrayTupleReference();
    protected final ArrayTupleBuilder upperBoundTupleBuilder = new ArrayTupleBuilder(1);
    protected final ArrayTupleReference upperBoundTuple = new ArrayTupleReference();
    protected final ConcatenatingTupleReference fullLowSearchKey = new ConcatenatingTupleReference(2);
    protected final ConcatenatingTupleReference fullHighSearchKey = new ConcatenatingTupleReference(2);
    protected final InvertedListPartitions partitions = new InvertedListPartitions();

    // To keep the current state of this search
    protected int curPartIdx;
    protected int endPartIdx;
    protected int numPrefixLists;
    protected boolean isFinalPartIdx;
    protected boolean needToReadNewPart;
    List<InvertedListCursor>[] partitionCursors;
    IInvertedIndexSearchModifier searchModifier;

    public PartitionedTOccurrenceSearcher(IInPlaceInvertedIndex invIndex, IHyracksTaskContext ctx)
            throws HyracksDataException {
        super(invIndex, ctx);
        initHelperTuples();
        curPartIdx = 0;
        endPartIdx = 0;
        isFinalPartIdx = false;
        isFinishedSearch = false;
        needToReadNewPart = true;
    }

    private void initHelperTuples() {
        try {
            lowerBoundTupleBuilder.reset();
            // Write dummy value.
            lowerBoundTupleBuilder.getDataOutput().writeShort(Short.MIN_VALUE);
            lowerBoundTupleBuilder.addFieldEndOffset();
            lowerBoundTuple.reset(lowerBoundTupleBuilder.getFieldEndOffsets(), lowerBoundTupleBuilder.getByteArray());
            // Only needed for setting the number of fields in searchKey.
            searchKey.reset(queryTokenAppender, 0);
            fullLowSearchKey.reset();
            fullLowSearchKey.addTuple(searchKey);
            fullLowSearchKey.addTuple(lowerBoundTuple);

            upperBoundTupleBuilder.reset();
            // Write dummy value.
            upperBoundTupleBuilder.getDataOutput().writeShort(Short.MAX_VALUE);
            upperBoundTupleBuilder.addFieldEndOffset();
            upperBoundTuple.reset(upperBoundTupleBuilder.getFieldEndOffsets(), upperBoundTupleBuilder.getByteArray());
            // Only needed for setting the number of fields in searchKey.
            searchKey.reset(queryTokenAppender, 0);
            fullHighSearchKey.reset();
            fullHighSearchKey.addTuple(searchKey);
            fullHighSearchKey.addTuple(upperBoundTuple);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void search(IIndexCursor resultCursor, InvertedIndexSearchPredicate searchPred, IIndexOperationContext ictx)
            throws HyracksDataException {
        prepareSearch();
        IPartitionedInvertedIndex partInvIndex = (IPartitionedInvertedIndex) invIndex;
        finalSearchResult.reset();
        if (partInvIndex.isEmpty()) {
            isFinishedSearch = true;
            resultCursor.open(null, searchPred);
            return;
        }
        tokenizeQuery(searchPred);
        short numQueryTokens = (short) queryTokenAppender.getTupleCount();

        searchModifier = searchPred.getSearchModifier();
        short numTokensLowerBound = searchModifier.getNumTokensLowerBound(numQueryTokens);
        short numTokensUpperBound = searchModifier.getNumTokensUpperBound(numQueryTokens);
        occurrenceThreshold = searchModifier.getOccurrenceThreshold(numQueryTokens);
        if (occurrenceThreshold <= 0) {
            throw HyracksDataException.create(ErrorCode.OCCURRENCE_THRESHOLD_PANIC_EXCEPTION);
        }

        short maxCountPossible = numQueryTokens;
        invListCursorCache.reset();
        partitions.reset(numTokensLowerBound, numTokensUpperBound);
        for (int i = 0; i < numQueryTokens; i++) {
            searchKey.reset(queryTokenAppender, i);
            if (!partInvIndex.openInvertedListPartitionCursors(this, ictx, numTokensLowerBound, numTokensUpperBound,
                    partitions)) {
                maxCountPossible--;
                // No results possible.
                if (maxCountPossible < occurrenceThreshold) {
                    // Closes all opened cursors.
                    closeCursorsInPartitions(partitions);
                    isFinishedSearch = true;
                    resultCursor.open(null, searchPred);
                    return;
                }
            }
        }

        partitionCursors = partitions.getPartitions();
        short start = partitions.getMinValidPartitionIndex();
        short end = partitions.getMaxValidPartitionIndex();

        // Process the partitions one-by-one.
        endPartIdx = end;
        for (int i = start; i <= end; i++) {
            // Prune partition because no element in it can satisfy the occurrence threshold.
            if (partitionCursors[i] == null) {
                continue;
            }
            // Prune partition because no element in it can satisfy the occurrence threshold.
            // An opened cursor should be closed.
            if (partitionCursors[i].size() < occurrenceThreshold) {
                for (InvertedListCursor cursor : partitionCursors[i]) {
                    cursor.close();
                }
                continue;
            }
            // Merge inverted lists of current partition.
            numPrefixLists = searchModifier.getNumPrefixLists(occurrenceThreshold, partitionCursors[i].size());
            invListMerger.reset();
            curPartIdx = i;
            isFinalPartIdx = i == end ? true : false;

            // If the number of inverted list cursor is one, then we don't need to go through the merge process
            // for this partition.
            if (partitionCursors[i].size() == 1) {
                singleInvListCursor = partitionCursors[i].get(0);
                singleInvListCursor.prepareLoadPages();
                singleInvListCursor.loadPages();
                isSingleInvertedList = true;
                needToReadNewPart = true;
            } else {
                singleInvListCursor = null;
                isSingleInvertedList = false;
                needToReadNewPart = invListMerger.merge(partitionCursors[i], occurrenceThreshold, numPrefixLists,
                        finalSearchResult);
                searchResultBuffer = finalSearchResult.getNextFrame();
                searchResultTupleIndex = 0;
                searchResultFta.reset(searchResultBuffer);
            }

            // By now, some output was generated by the merger or only one cursor in this partition is associated.
            // So, we open the cursor that will fetch these result. If it's the final partition, the outside of
            // this for loop will handle opening of the result cursor for a single inverted list cursor case.
            if (needToReadNewPart && isFinalPartIdx) {
                invListMerger.close();
                finalSearchResult.finalizeWrite();
                isFinishedSearch = true;
            }
            resultCursor.open(null, searchPred);
            return;
        }

        // The control reaches here if the above loop doesn't have any valid cursor.
        isFinishedSearch = true;
        needToReadNewPart = true;
        resultCursor.open(null, searchPred);
        return;
    }

    /**
     * Continues a search process in case of the following two cases:
     * #1. If it was paused because the output buffer of the final result was full.
     * #2. All tuples from a single inverted list has been read.
     *
     * @return true only if all processing for the final list for the final partition is done.
     *         false otherwise.
     * @throws HyracksDataException
     */
    @Override
    public boolean continueSearch() throws HyracksDataException {
        if (isFinishedSearch) {
            return true;
        }

        // Case #1 only - output buffer was full
        if (!needToReadNewPart) {
            needToReadNewPart = invListMerger.continueMerge();
            searchResultBuffer = finalSearchResult.getNextFrame();
            searchResultTupleIndex = 0;
            searchResultFta.reset(searchResultBuffer);
            // Final calculation done?
            if (needToReadNewPart && isFinalPartIdx) {
                isFinishedSearch = true;
                invListMerger.close();
                finalSearchResult.finalizeWrite();
                return true;
            }
            return false;
        }

        // Finished one partition for the both cases #1 and #2. So, moves to the next partition.
        curPartIdx++;
        if (curPartIdx <= endPartIdx) {
            boolean suitablePartFound = false;
            for (int i = curPartIdx; i <= endPartIdx; i++) {
                // Prune partition because no element in it can satisfy the occurrence threshold.
                if (partitionCursors[i] == null) {
                    continue;
                }
                // Prune partition because no element in it can satisfy the occurrence threshold.
                // An opened cursor should be closed.
                if (partitionCursors[i].size() < occurrenceThreshold) {
                    for (InvertedListCursor cursor : partitionCursors[i]) {
                        cursor.close();
                    }
                    continue;
                }
                suitablePartFound = true;
                curPartIdx = i;
                break;
            }

            // If no partition is availble to explore, we stop here.
            if (!suitablePartFound) {
                isFinishedSearch = true;
                invListMerger.close();
                finalSearchResult.finalizeWrite();
                return true;
            }

            // Merge inverted lists of current partition.
            numPrefixLists = searchModifier.getNumPrefixLists(occurrenceThreshold, partitionCursors[curPartIdx].size());
            invListMerger.reset();
            finalSearchResult.resetBuffer();
            isFinalPartIdx = curPartIdx == endPartIdx ? true : false;

            // If the number of inverted list cursor is one, then we don't need to go through the merge process.
            if (partitionCursors[curPartIdx].size() == 1) {
                singleInvListCursor = partitionCursors[curPartIdx].get(0);
                singleInvListCursor.prepareLoadPages();
                singleInvListCursor.loadPages();
                isSingleInvertedList = true;
                needToReadNewPart = true;
            } else {
                singleInvListCursor = null;
                isSingleInvertedList = false;
                needToReadNewPart = invListMerger.merge(partitionCursors[curPartIdx], occurrenceThreshold,
                        numPrefixLists, finalSearchResult);
                searchResultBuffer = finalSearchResult.getNextFrame();
                searchResultTupleIndex = 0;
                searchResultFta.reset(searchResultBuffer);
            }

            // Finished processing one partition
            if (needToReadNewPart && isFinalPartIdx) {
                invListMerger.close();
                finalSearchResult.finalizeWrite();
                isFinishedSearch = true;
                return true;
            }

        } else {
            isFinishedSearch = true;
        }

        return false;
    }

    private void closeCursorsInPartitions(InvertedListPartitions parts) throws HyracksDataException {
        List<InvertedListCursor>[] partCursors = parts.getPartitions();
        short start = parts.getMinValidPartitionIndex();
        short end = parts.getMaxValidPartitionIndex();
        for (int i = start; i <= end; i++) {
            if (partCursors[i] == null) {
                continue;
            }
            for (InvertedListCursor cursor : partCursors[i]) {
                cursor.close();
            }
        }
    }

    public void setNumTokensBoundsInSearchKeys(short numTokensLowerBound, short numTokensUpperBound) {
        ShortPointable.setShort(lowerBoundTuple.getFieldData(0), lowerBoundTuple.getFieldStart(0), numTokensLowerBound);
        ShortPointable.setShort(upperBoundTuple.getFieldData(0), upperBoundTuple.getFieldStart(0), numTokensUpperBound);
    }

    public ITupleReference getPrefixSearchKey() {
        return searchKey;
    }

    public ITupleReference getFullLowSearchKey() {
        return fullLowSearchKey;
    }

    public ITupleReference getFullHighSearchKey() {
        return fullHighSearchKey;
    }

    public InvertedListCursor getCachedInvertedListCursor() throws HyracksDataException {
        return invListCursorCache.getNext();
    }

}
