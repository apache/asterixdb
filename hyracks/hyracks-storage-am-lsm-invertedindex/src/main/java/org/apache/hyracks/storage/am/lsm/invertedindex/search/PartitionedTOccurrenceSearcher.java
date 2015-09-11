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
import java.util.ArrayList;

import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.tuples.ConcatenatingTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IPartitionedInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.exceptions.OccurrenceThresholdPanicException;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexSearchCursor;

public class PartitionedTOccurrenceSearcher extends AbstractTOccurrenceSearcher {

    protected final ArrayTupleBuilder lowerBoundTupleBuilder = new ArrayTupleBuilder(1);
    protected final ArrayTupleReference lowerBoundTuple = new ArrayTupleReference();
    protected final ArrayTupleBuilder upperBoundTupleBuilder = new ArrayTupleBuilder(1);
    protected final ArrayTupleReference upperBoundTuple = new ArrayTupleReference();
    protected final ConcatenatingTupleReference fullLowSearchKey = new ConcatenatingTupleReference(2);
    protected final ConcatenatingTupleReference fullHighSearchKey = new ConcatenatingTupleReference(2);

    // Inverted list cursors ordered by token. Used to read relevant inverted-list partitions of one token one after
    // the other for better I/O performance (because the partitions of one inverted list are stored contiguously in a file).
    // The above implies that we currently require holding all inverted list for a query in memory.
    protected final ArrayList<IInvertedListCursor> cursorsOrderedByTokens = new ArrayList<IInvertedListCursor>();
    protected final InvertedListPartitions partitions = new InvertedListPartitions();

    public PartitionedTOccurrenceSearcher(IHyracksCommonContext ctx, IInvertedIndex invIndex)
            throws HyracksDataException {
        super(ctx, invIndex);
        initHelperTuples();
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

    public void search(OnDiskInvertedIndexSearchCursor resultCursor, InvertedIndexSearchPredicate searchPred,
            IIndexOperationContext ictx) throws HyracksDataException, IndexException {
        IPartitionedInvertedIndex partInvIndex = (IPartitionedInvertedIndex) invIndex;
        searchResult.reset();
        if (partInvIndex.isEmpty()) {
            return;
        }

        tokenizeQuery(searchPred);
        short numQueryTokens = (short) queryTokenAppender.getTupleCount();

        IInvertedIndexSearchModifier searchModifier = searchPred.getSearchModifier();
        short numTokensLowerBound = searchModifier.getNumTokensLowerBound(numQueryTokens);
        short numTokensUpperBound = searchModifier.getNumTokensUpperBound(numQueryTokens);

        occurrenceThreshold = searchModifier.getOccurrenceThreshold(numQueryTokens);
        if (occurrenceThreshold <= 0) {
            throw new OccurrenceThresholdPanicException("Merge Threshold is <= 0. Failing Search.");
        }

        short maxCountPossible = numQueryTokens;
        invListCursorCache.reset();
        partitions.reset(numTokensLowerBound, numTokensUpperBound);
        cursorsOrderedByTokens.clear();
        for (int i = 0; i < numQueryTokens; i++) {
            searchKey.reset(queryTokenAppender, i);
            if (!partInvIndex.openInvertedListPartitionCursors(this, ictx, numTokensLowerBound, numTokensUpperBound,
                    partitions, cursorsOrderedByTokens)) {
                maxCountPossible--;
                // No results possible.
                if (maxCountPossible < occurrenceThreshold) {
                    return;
                }
            }
        }

        ArrayList<IInvertedListCursor>[] partitionCursors = partitions.getPartitions();
        short start = partitions.getMinValidPartitionIndex();
        short end = partitions.getMaxValidPartitionIndex();

        // Typically, we only enter this case for disk-based inverted indexes. 
        // TODO: This behavior could potentially lead to a deadlock if we cannot pin 
        // all inverted lists in memory, and are forced to wait for a page to get evicted
        // (other concurrent searchers may be in the same situation).
        // We should detect such cases, then unpin all pages, and then keep retrying to pin until we succeed.
        // This will require a different "tryPin()" mechanism in the BufferCache that will return false
        // if we'd have to wait for a page to get evicted.
        if (!cursorsOrderedByTokens.isEmpty()) {
            for (int i = start; i <= end; i++) {
                if (partitionCursors[i] == null) {
                    continue;
                }
                // Prune partition because no element in it can satisfy the occurrence threshold.
                if (partitionCursors[i].size() < occurrenceThreshold) {
                    cursorsOrderedByTokens.removeAll(partitionCursors[i]);
                }
            }
            // Pin all the cursors in the order of tokens.
            int numCursors = cursorsOrderedByTokens.size();
            for (int i = 0; i < numCursors; i++) {
                cursorsOrderedByTokens.get(i).pinPages();
            }
        }

        // Process the partitions one-by-one.
        for (int i = start; i <= end; i++) {
            if (partitionCursors[i] == null) {
                continue;
            }
            // Prune partition because no element in it can satisfy the occurrence threshold.
            if (partitionCursors[i].size() < occurrenceThreshold) {
                continue;
            }
            // Merge inverted lists of current partition.
            int numPrefixLists = searchModifier.getNumPrefixLists(occurrenceThreshold, partitionCursors[i].size());
            invListMerger.reset();
            invListMerger.merge(partitionCursors[i], occurrenceThreshold, numPrefixLists, searchResult);
        }

        resultCursor.open(null, searchPred);
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

    public IInvertedListCursor getCachedInvertedListCursor() {
        return invListCursorCache.getNext();
    }
}
