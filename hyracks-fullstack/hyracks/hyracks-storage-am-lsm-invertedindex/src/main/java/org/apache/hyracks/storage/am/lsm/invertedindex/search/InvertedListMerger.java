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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.InvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeFrameTupleAccessor;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.FixedSizeTupleReference;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * Conducts the merging operation among all inverted list cursors and generates the final result.
 */
public class InvertedListMerger {

    // current merge process type
    public enum processType {
        PREFIX_LIST,
        SUFFIX_LIST_PROBE,
        SUFFIX_LIST_SCAN,
        NONE
    }

    protected final MultiComparator invListCmp;
    protected InvertedIndexSearchResult prevSearchResult;
    protected InvertedIndexSearchResult newSearchResult;
    protected InvertedIndexFinalSearchResult finalSearchResult;

    // To Keep the status of this merge process since we only calculate one frame at a time in case of the final result
    protected InvertedListCursor finalInvListCursor;
    protected int occurrenceThreshold;
    protected int numInvertedLists;
    protected int invListIdx;
    protected int numPrevResult;
    protected int prevBufIdx;
    protected int maxPrevBufIdx;
    protected int numExpectedPages;
    protected ByteBuffer prevCurrentBuffer;
    protected FixedSizeFrameTupleAccessor resultFrameTupleAcc;
    protected FixedSizeTupleReference resultTuple;
    protected boolean advanceCursor;
    protected boolean advancePrevResult;
    protected int resultTidx;
    protected int invListTidx;
    protected int invListTupleCount;
    protected ITupleReference invListTuple;
    protected int prevResultFrameTupleCount;

    // Entire calculation done?
    protected boolean isProcessingFinished;
    // Dealing with the final list?
    protected boolean isProcessingFinalList;
    // Dealing with the final partition?
    protected boolean isProcessingFinalPartition;
    // Variable Initialization done?
    protected boolean listVisited;
    protected processType currentProcessType = processType.NONE;

    public InvertedListMerger(IHyracksTaskContext ctx, IInvertedIndex invIndex, ISimpleFrameBufferManager bufferManager)
            throws HyracksDataException {
        this.invListCmp = MultiComparator.create(invIndex.getInvListCmpFactories());
        this.prevSearchResult = new InvertedIndexSearchResult(invIndex.getInvListTypeTraits(), ctx, bufferManager);
        this.newSearchResult = new InvertedIndexSearchResult(invIndex.getInvListTypeTraits(), ctx, bufferManager);
    }

    /**
     * Generates the result based on the given occurrenceThreshold. For the prefix lists, we do merge.
     * For the suffix lists, we either conduct a binary search or a scan for each record ID.
     *
     * @return true only if all processing for the final list for a partition is done.
     *         false otherwise.
     * @throws HyracksDataException
     */
    public boolean merge(List<InvertedListCursor> invListCursors, int occurrenceThreshold, int numPrefixLists,
            InvertedIndexFinalSearchResult finalSearchResult) throws HyracksDataException {
        Collections.sort(invListCursors);
        int numInvLists = invListCursors.size();
        InvertedIndexSearchResult result = null;
        prevSearchResult.reset();
        newSearchResult.reset();
        boolean isFinalList = false;
        // This will be only set to true when the processing the last list in a partition is done.
        boolean doneMerge = false;
        this.occurrenceThreshold = occurrenceThreshold;
        for (int i = 0; i < numInvLists; i++) {
            // Sets the previous search result and the new (current) search result that will be written.
            InvertedIndexSearchResult swapTemp = prevSearchResult;
            prevSearchResult = newSearchResult;
            newSearchResult = swapTemp;
            newSearchResult.reset();
            if (i + 1 != numInvLists) {
                // Use a temporary intermediate search result when not merging last list.
                result = newSearchResult;
            } else {
                // When merging the last list, appends results to the final search result. This is needed since
                // this merge() can be called multiple times in case of partitioned T-occurrrence searches.
                // So, we need to keep a separate search result.
                result = finalSearchResult;
                isFinalList = true;
            }
            InvertedListCursor invListCursor = invListCursors.get(i);
            // Track whether an exception is occurred.
            boolean finishedTryBlock = false;
            try {
                // Loads the inverted list (at least some part of it).
                invListCursor.prepareLoadPages();
                invListCursor.loadPages();
                if (i < numPrefixLists) {
                    // Merges a prefix list.
                    doneMerge = mergePrefixList(invListCursor, prevSearchResult, result, isFinalList);
                } else {
                    // Merge suffix list.
                    int numInvListElements = invListCursor.size();
                    int currentNumResults = prevSearchResult.getNumResults();
                    // Should we binary search the next list or should we sort-merge it?
                    if (currentNumResults * Math.log(numInvListElements) < currentNumResults + numInvListElements) {
                        doneMerge = mergeSuffixListProbe(invListCursor, prevSearchResult, result, i, numInvLists,
                                occurrenceThreshold, isFinalList);
                    } else {
                        doneMerge = mergeSuffixListScan(invListCursor, prevSearchResult, result, i, numInvLists,
                                occurrenceThreshold, isFinalList);
                    }
                }
                finishedTryBlock = true;
            } finally {
                // An intermediate inverted list should always be closed.
                // The final inverted list should be closed only when traversing the list is done.
                // If an exception was occurred, just close the cursor.
                if (!isFinalList || (isFinalList && doneMerge) || !finishedTryBlock) {
                    try {
                        invListCursor.unloadPages();
                    } finally {
                        invListCursor.close();
                    }
                }
            }

            // Needs to return the calculation result for the final list only.
            // Otherwise, the process needs to be continued until this method traverses the final inverted list
            // and either generates some output in the output buffer or finishes traversing it.
            if (isFinalList) {
                return doneMerge;
            }
        }

        // Control does not reach here.
        return true;
    }

    /**
     * Continues the merge process on a final list if it has been paused because
     * the output buffer of the final result was full.
     *
     * @return true only if all processing for the final list for a partition is done.
     *         false otherwise (still more to proceed).
     */
    public boolean continueMerge() throws HyracksDataException {
        boolean doneMerge;
        switch (currentProcessType) {
            case PREFIX_LIST:
                doneMerge =
                        mergePrefixList(finalInvListCursor, prevSearchResult, finalSearchResult, isProcessingFinalList);
                break;
            case SUFFIX_LIST_PROBE:
                doneMerge = mergeSuffixListProbe(finalInvListCursor, prevSearchResult, finalSearchResult, invListIdx,
                        numInvertedLists, occurrenceThreshold, isProcessingFinalList);
                break;
            case SUFFIX_LIST_SCAN:
                doneMerge = mergeSuffixListScan(finalInvListCursor, prevSearchResult, finalSearchResult, invListIdx,
                        numInvertedLists, occurrenceThreshold, isProcessingFinalList);
                break;
            default:
                throw HyracksDataException.create(ErrorCode.UNDEFINED_INVERTED_LIST_MERGE_TYPE);
        }

        if (doneMerge) {
            // Final calculation is done.
            try {
                finalInvListCursor.unloadPages();
            } finally {
                finalInvListCursor.close();
            }
        }

        return doneMerge;
    }

    /**
     * Merges the given suffix list to the previous result using the given inverted list cursor. When traversing the
     * inverted list cursor, a binary search will be conducted for each tuple in the previous search result.
     *
     * @return true only if all processing for the final list for a partition is done.
     *         false otherwise.
     */
    protected boolean mergeSuffixListProbe(InvertedListCursor invListCursor, InvertedIndexSearchResult prevSearchResult,
            InvertedIndexSearchResult newSearchResult, int invListIx, int numInvLists, int occurrenceThreshold,
            boolean isFinalList) throws HyracksDataException {
        if (isProcessingFinished) {
            return true;
        }

        initMergingOneList(invListCursor, prevSearchResult, newSearchResult, isFinalList, invListIx, numInvLists,
                occurrenceThreshold, processType.SUFFIX_LIST_PROBE);

        while (resultTidx < prevResultFrameTupleCount) {
            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));
            int count = getCount(resultTuple);
            if (invListCursor.containsKey(resultTuple, invListCmp)) {
                // Found the same tuple again on the current list. Increases the count by one.
                count++;
                if (!newSearchResult.append(resultTuple, count)) {
                    // For a final result, needs to pause when a frame becomes full to let the caller consume the frame.
                    // SearchResult.append() should only return false for this case.
                    return false;
                }
            } else if (count + numInvLists - invListIx > occurrenceThreshold) {
                // This tuple only exists on the previous result. However, it can be a part of the answer
                // if it will be found again in the remaining lists.
                if (!newSearchResult.append(resultTuple, count)) {
                    // For a final result, needs to pause when a frame becomes full to let the caller consume the frame.
                    // SearchResult.append() should only return false for this case.
                    return false;
                }
            }
            resultTidx++;
            checkPrevResultAndFetchNextFrame(prevSearchResult);
        }

        return finishMergingOneList(isFinalList, prevSearchResult, newSearchResult, invListCursor);
    }

    /**
     * Merges the given suffix list to the previous result using the given inverted list cursor. When traversing the
     * inverted list cursor, a scan will be conducted like the mergePrefixList() method.
     *
     * @return true only if all processing for the final list for a partition is done.
     *         false otherwise.
     */
    protected boolean mergeSuffixListScan(InvertedListCursor invListCursor, InvertedIndexSearchResult prevSearchResult,
            InvertedIndexSearchResult newSearchResult, int invListIx, int numInvLists, int occurrenceThreshold,
            boolean isFinalList) throws HyracksDataException {
        if (isProcessingFinished) {
            return true;
        }

        // When generating the final result, we need to initialize the variable only once.
        initMergingOneList(invListCursor, prevSearchResult, newSearchResult, isFinalList, invListIx, numInvLists,
                occurrenceThreshold, processType.SUFFIX_LIST_SCAN);

        int cmp;
        int count;
        while (invListTidx < invListTupleCount && resultTidx < prevResultFrameTupleCount) {
            invListTuple = invListCursor.getTuple();
            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));
            cmp = invListCmp.compare(invListTuple, resultTuple);
            if (cmp == 0) {
                // Found the same tuple again on the current list. Increases the count by one.
                // Also, both the result and the cursor advances.
                count = getCount(resultTuple) + 1;
                if (!newSearchResult.append(resultTuple, count)) {
                    // For a final result, needs to pause when a frame becomes full to let the caller consume the frame.
                    // SearchResult.append() should only return false for this case.
                    return false;
                }
                advanceCursor = true;
                advancePrevResult = true;
            } else if (cmp < 0) {
                // Found a new tuple on the current list. Based on prefix/suffix algorithm,
                // this tuple can be ignored since it can't be a part of the answer.
                advanceCursor = true;
                advancePrevResult = false;
            } else {
                // This tuple only exists on the previous result. However, it can be a part of the answer
                // if it will be found again in the remaining lists.
                count = getCount(resultTuple);
                if (count + numInvLists - invListIx > occurrenceThreshold) {
                    if (!newSearchResult.append(resultTuple, count)) {
                        // For a final result, needs to pause when a frame becomes full to let the caller
                        // consume the frame. SearchResult.append() should only return false for this case.
                        return false;
                    }
                }
                advanceCursor = false;
                advancePrevResult = true;
            }
            // Gets the next tuple from the previous result and the list cursor.
            advancePrevResultAndList(advancePrevResult, advanceCursor, prevSearchResult, invListCursor);
        }

        // append remaining elements from previous result set
        // These remaining elements can be a part of the answer if they will be found again in the remaining lists.
        while (resultTidx < prevResultFrameTupleCount) {
            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));
            count = getCount(resultTuple);
            if (count + numInvLists - invListIx > occurrenceThreshold) {
                if (!newSearchResult.append(resultTuple, count)) {
                    // For a final result, needs to pause when a frame becomes full to let the caller
                    // consume the frame. SearchResult.append() should only return false for this case.
                    return false;
                }
            }
            resultTidx++;
            checkPrevResultAndFetchNextFrame(prevSearchResult);
        }

        return finishMergingOneList(isFinalList, prevSearchResult, newSearchResult, invListCursor);
    }

    /**
     * Merges the prefix lists one by one. It reads the previous search result and one inverted list,
     * then generates a new result by applying UNIONALL operation on these two. This method returns true
     * only if all processing for the given final list is done. Otherwise, it returns false.
     */
    protected boolean mergePrefixList(InvertedListCursor invListCursor, InvertedIndexSearchResult prevSearchResult,
            InvertedIndexSearchResult newSearchResult, boolean isFinalList) throws HyracksDataException {
        if (isProcessingFinished) {
            return true;
        }

        // Assigns necessary variables and fetches a tuple from the inverted list cursor.
        initMergingOneList(invListCursor, prevSearchResult, newSearchResult, isFinalList, processType.PREFIX_LIST);

        int cmp;
        int count;
        // Traverses the inverted list and the previous result at the same time.
        while (invListTidx < invListTupleCount && resultTidx < prevResultFrameTupleCount) {
            invListTuple = invListCursor.getTuple();
            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));
            cmp = invListCmp.compare(invListTuple, resultTuple);
            // Found the same tuple again on the current list: count + 1. Both the result and the cursor advances.
            if (cmp == 0) {
                count = getCount(resultTuple) + 1;
                if (!newSearchResult.append(resultTuple, count)) {
                    // For a final result, needs to pause when a frame becomes full to let the caller
                    // consume the frame. SearchResult.append() should only return false for this case.
                    return false;
                }
                advanceCursor = true;
                advancePrevResult = true;
            } else if (cmp < 0) {
                // Found a new tuple on the current list. Sets its count to 1. Only advances the cursor.
                count = 1;
                if (!newSearchResult.append(invListTuple, count)) {
                    // For a final result, needs to pause when a frame becomes full to let the caller
                    // consume the frame. SearchResult.append() should only return false for this case.
                    return false;
                }
                advanceCursor = true;
                advancePrevResult = false;
                //
            } else {
                // This tuple only exists on the previous result. Maintains its count. Only advances the result.
                count = getCount(resultTuple);
                if (!newSearchResult.append(resultTuple, count)) {
                    // For a final result, needs to pause when a frame becomes full to let the caller
                    // consume the frame. SearchResult.append() should only return false for this case.
                    return false;
                }
                advanceCursor = false;
                advancePrevResult = true;
                //
            }
            // Gets the next tuple from the previous result and the list cursor.
            advancePrevResultAndList(advancePrevResult, advanceCursor, prevSearchResult, invListCursor);
        }

        // append remaining new elements from inverted list
        //
        while (invListTidx < invListTupleCount) {
            invListTuple = invListCursor.getTuple();
            if (!newSearchResult.append(invListTuple, 1)) {
                // For a final result, needs to pause when a frame becomes full to let the caller
                // consume the frame. SearchResult.append() should only return false for this case.
                return false;
            }
            invListTidx++;
            if (invListCursor.hasNext()) {
                invListCursor.next();
            }
        }

        // append remaining elements from previous result set
        while (resultTidx < prevResultFrameTupleCount) {
            resultTuple.reset(prevCurrentBuffer.array(), resultFrameTupleAcc.getTupleStartOffset(resultTidx));
            count = getCount(resultTuple);
            if (!newSearchResult.append(resultTuple, count)) {
                // For a final result, needs to pause when a frame becomes full to let the caller
                // consume the frame. SearchResult.append() should only return false for this case.
                return false;
            }
            resultTidx++;
            checkPrevResultAndFetchNextFrame(prevSearchResult);
        }

        return finishMergingOneList(isFinalList, prevSearchResult, newSearchResult, invListCursor);
    }

    /**
     * Initializes necessary information for each merging operation (prefix_list) for a list.
     */
    protected void initMergingOneList(InvertedListCursor invListCursor, InvertedIndexSearchResult prevSearchResult,
            InvertedIndexSearchResult newSearchResult, boolean isFinalList, processType mergeOpType)
            throws HyracksDataException {
        initMergingOneList(invListCursor, prevSearchResult, newSearchResult, isFinalList, 0, 0, 0, mergeOpType);
    }

    /**
     * Initializes necessary information for each merging operation (suffix_list_probe or suffix_list_scan) for a list.
     */
    protected void initMergingOneList(InvertedListCursor invListCursor, InvertedIndexSearchResult prevSearchResult,
            InvertedIndexSearchResult newSearchResult, boolean isFinalList, int invListIx, int numInvLists,
            int occurrenceThreshold, processType mergeOpType) throws HyracksDataException {
        // Each inverted list will be visited only once except the final inverted list.
        // When generating the final result, the given inverted list can be visited multiple times
        // since we only generate one frame at a time. So, we need to initialize the following variables only once.
        if (!listVisited) {
            resetVariable();
            prevBufIdx = 0;
            maxPrevBufIdx = prevSearchResult.getCurrentBufferIndex();
            // Gets the maximum possible number of expected result pages (in case no common element at all).
            numPrevResult = prevSearchResult.getNumResults();
            invListTupleCount = invListCursor.size();
            numExpectedPages = newSearchResult.getExpectedNumPages(numPrevResult + invListTupleCount);
            newSearchResult.prepareWrite(numExpectedPages);
            prevSearchResult.prepareResultRead();
            prevCurrentBuffer = prevSearchResult.getNextFrame();
            resultFrameTupleAcc = prevSearchResult.getAccessor();
            resultTuple = prevSearchResult.getTuple();
            advanceCursor = true;
            advancePrevResult = false;
            resultTidx = 0;
            resultFrameTupleAcc.reset(prevCurrentBuffer);
            invListTidx = 0;
            numInvertedLists = numInvLists;
            invListIdx = invListIx;
            prevResultFrameTupleCount = prevCurrentBuffer == null ? 0 : resultFrameTupleAcc.getTupleCount();

            // Additional variables to keep the current status of the given merge process
            if (isFinalList) {
                finalInvListCursor = invListCursor;
                finalSearchResult = (InvertedIndexFinalSearchResult) newSearchResult;
                currentProcessType = mergeOpType;
                this.occurrenceThreshold = occurrenceThreshold;
                isProcessingFinalList = true;
                listVisited = true;
            }

            if (invListCursor.hasNext()) {
                invListCursor.next();
            }
        }
    }

    /**
     * Finishes the merging operation of one list.
     *
     * @return true only if this merging operation is for a final list
     *         false otherwise
     */
    protected boolean finishMergingOneList(boolean isFinalList, InvertedIndexSearchResult prevSearchResult,
            InvertedIndexSearchResult newSearchResult, InvertedListCursor invListCursor) throws HyracksDataException {
        prevSearchResult.closeResultRead(false);
        invListCursor.close();
        // Final search result can be called multiple times for partitioned occurrence searcher case
        // so that it can be written multiple times. So, should not finalize the write here.
        // The caller of merge() should ensure that.
        if (!isFinalList) {
            newSearchResult.finalizeWrite();
            return false;
        } else {
            // Final list? then, the calculation is done.
            isProcessingFinished = true;
            return true;
        }
    }

    /**
     * Fetches the next previous result frame if the current frame has been consumed.
     * Also fetches next element from the inverted list cursor.
     */
    protected void advancePrevResultAndList(boolean advancePrevResult, boolean advanceCursor,
            InvertedIndexSearchResult prevSearchResult, InvertedListCursor invListCursor) throws HyracksDataException {
        if (advancePrevResult) {
            resultTidx++;
            checkPrevResultAndFetchNextFrame(prevSearchResult);
        }

        if (advanceCursor) {
            invListTidx++;
            if (invListCursor.hasNext()) {
                invListCursor.next();
            }
        }
    }

    /**
     * Fetches the next page of the previous result if possible.
     */
    protected void checkPrevResultAndFetchNextFrame(InvertedIndexSearchResult prevSearchResult)
            throws HyracksDataException {
        if (resultTidx >= prevResultFrameTupleCount) {
            prevBufIdx++;
            if (prevBufIdx <= maxPrevBufIdx) {
                prevCurrentBuffer = prevSearchResult.getNextFrame();
                resultFrameTupleAcc.reset(prevCurrentBuffer);
                prevResultFrameTupleCount = resultFrameTupleAcc.getTupleCount();
                resultTidx = 0;
            }
        }
    }

    /**
     * Gets the count of the given tuple in the previous search result.
     */
    protected int getCount(FixedSizeTupleReference resultTuple) {
        return IntegerPointable.getInteger(resultTuple.getFieldData(0),
                resultTuple.getFieldStart(resultTuple.getFieldCount() - 1));
    }

    public void reset() throws HyracksDataException {
        prevSearchResult.reset();
        newSearchResult.reset();
        resetVariable();
    }

    /**
     * Prepares the merge process. This mainly allocates buffers for the two intermediate search results.
     */
    public void prepareMerge() throws HyracksDataException {
        prevSearchResult.prepareIOBuffer();
        newSearchResult.prepareIOBuffer();
        resetVariable();
    }

    /**
     * Cleans every stuff.
     */
    public void close() throws HyracksDataException {
        try {
            prevSearchResult.close();
            newSearchResult.close();
        } finally {
            if (finalInvListCursor != null) {
                finalInvListCursor.close();
            }
        }
    }

    // Resets the variables.
    private void resetVariable() {
        prevBufIdx = 0;
        maxPrevBufIdx = 0;
        numPrevResult = 0;
        invListTupleCount = 0;
        numExpectedPages = 0;
        prevCurrentBuffer = null;
        resultFrameTupleAcc = null;
        resultTuple = null;
        advanceCursor = false;
        advancePrevResult = false;
        resultTidx = 0;
        invListTidx = 0;
        prevResultFrameTupleCount = 0;
        finalInvListCursor = null;
        finalSearchResult = null;
        currentProcessType = processType.NONE;
        isProcessingFinalList = false;
        isProcessingFinished = false;
        listVisited = false;
        occurrenceThreshold = 0;
    }

}
