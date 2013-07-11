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
import java.util.ArrayList;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.tuples.ConcatenatingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IPartitionedInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.exceptions.OccurrenceThresholdPanicException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexSearchCursor;

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
            searchKey.reset(queryTokenAccessor, 0);
            fullLowSearchKey.reset();
            fullLowSearchKey.addTuple(searchKey);
            fullLowSearchKey.addTuple(lowerBoundTuple);

            upperBoundTupleBuilder.reset();
            // Write dummy value.
            upperBoundTupleBuilder.getDataOutput().writeShort(Short.MAX_VALUE);
            upperBoundTupleBuilder.addFieldEndOffset();
            upperBoundTuple.reset(upperBoundTupleBuilder.getFieldEndOffsets(), upperBoundTupleBuilder.getByteArray());
            // Only needed for setting the number of fields in searchKey.
            searchKey.reset(queryTokenAccessor, 0);
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
        short numQueryTokens = (short) queryTokenAccessor.getTupleCount();

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
            searchKey.reset(queryTokenAccessor, i);
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
        ShortSerializerDeserializer.putShort(numTokensLowerBound, lowerBoundTuple.getFieldData(0),
                lowerBoundTuple.getFieldStart(0));
        ShortSerializerDeserializer.putShort(numTokensUpperBound, upperBoundTuple.getFieldData(0),
                upperBoundTuple.getFieldStart(0));
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
