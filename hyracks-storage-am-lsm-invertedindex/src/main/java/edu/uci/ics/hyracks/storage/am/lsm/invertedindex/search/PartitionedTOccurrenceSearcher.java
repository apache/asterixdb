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

import java.io.IOException;
import java.util.ArrayList;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.tuples.ConcatenatingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IObjectFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.exceptions.OccurrenceThresholdPanicException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexSearchCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.PartitionedOnDiskInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.ObjectCache;

public class PartitionedTOccurrenceSearcher extends AbstractTOccurrenceSearcher {

    protected final ArrayTupleBuilder lowerBoundTupleBuilder = new ArrayTupleBuilder(1);
    protected final ArrayTupleReference lowerBoundTuple = new ArrayTupleReference();
    protected final ArrayTupleBuilder upperBoundTupleBuilder = new ArrayTupleBuilder(1);
    protected final ArrayTupleReference upperBoundTuple = new ArrayTupleReference();
    protected final ConcatenatingTupleReference partLowSearchKey = new ConcatenatingTupleReference(2);
    protected final ConcatenatingTupleReference partHighSearchKey = new ConcatenatingTupleReference(2);

    protected final IObjectFactory<ArrayList<IInvertedListCursor>> arrayListFactory;
    protected final ObjectCache<ArrayList<IInvertedListCursor>> arrayListCache;

    protected final InvertedListPartitions partitions;

    public PartitionedTOccurrenceSearcher(IHyracksCommonContext ctx, IInvertedIndex invIndex) {
        super(ctx, invIndex);
        this.arrayListFactory = new ArrayListFactory<IInvertedListCursor>();
        this.arrayListCache = new ObjectCache<ArrayList<IInvertedListCursor>>(arrayListFactory, 10, 10);
        OnDiskInvertedIndex partInvIndex = (OnDiskInvertedIndex) invIndex;
        this.partitions = new InvertedListPartitions(partInvIndex, invListCursorCache, arrayListCache);
    }

    public void search(OnDiskInvertedIndexSearchCursor resultCursor, InvertedIndexSearchPredicate searchPred,
            IIndexOperationContext ictx) throws HyracksDataException, IndexException {
        tokenizeQuery(searchPred);
        int numQueryTokens = queryTokenAccessor.getTupleCount();

        IInvertedIndexSearchModifier searchModifier = searchPred.getSearchModifier();
        int numTokensLowerBound = searchModifier.getNumTokensLowerBound(numQueryTokens);
        int numTokensUpperBound = searchModifier.getNumTokensUpperBound(numQueryTokens);
        ITupleReference lowSearchKey = null;
        ITupleReference highSearchKey = null;
        try {
            if (numTokensLowerBound >= 0) {
                lowerBoundTupleBuilder.reset();
                lowerBoundTupleBuilder.getDataOutput().writeInt(numTokensLowerBound);
                lowerBoundTupleBuilder.addFieldEndOffset();
                lowerBoundTuple.reset(lowerBoundTupleBuilder.getFieldEndOffsets(),
                        lowerBoundTupleBuilder.getByteArray());
                // Only needed for setting the number of fields in searchKey.
                searchKey.reset(queryTokenAccessor, 0);
                partLowSearchKey.reset();
                partLowSearchKey.addTuple(searchKey);
                partLowSearchKey.addTuple(lowerBoundTuple);
                lowSearchKey = partLowSearchKey;
            } else {
                lowSearchKey = searchKey;
            }
            if (numTokensUpperBound >= 0) {
                upperBoundTupleBuilder.reset();
                upperBoundTupleBuilder.getDataOutput().writeInt(numTokensUpperBound);
                upperBoundTupleBuilder.addFieldEndOffset();
                upperBoundTuple.reset(upperBoundTupleBuilder.getFieldEndOffsets(),
                        upperBoundTupleBuilder.getByteArray());
                // Only needed for setting the number of fields in searchKey.
                searchKey.reset(queryTokenAccessor, 0);
                partHighSearchKey.reset();
                partHighSearchKey.addTuple(searchKey);
                partHighSearchKey.addTuple(upperBoundTuple);
                highSearchKey = partHighSearchKey;
            } else {
                highSearchKey = searchKey;
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }

        PartitionedOnDiskInvertedIndex partInvIndex = (PartitionedOnDiskInvertedIndex) invIndex;
        partitions.reset(numTokensLowerBound, numTokensUpperBound);
        for (int i = 0; i < numQueryTokens; i++) {
            searchKey.reset(queryTokenAccessor, i);
            partInvIndex.openInvertedListPartitionCursors(partitions, lowSearchKey, highSearchKey, ictx);
        }

        occurrenceThreshold = searchModifier.getOccurrenceThreshold(numQueryTokens);
        if (occurrenceThreshold <= 0) {
            throw new OccurrenceThresholdPanicException("Merge Threshold is <= 0. Failing Search.");
        }

        // Process the partitions one-by-one.
        ArrayList<IInvertedListCursor>[] partitionCursors = partitions.getPartitions();
        int start = partitions.getMinValidPartitionIndex();
        int end = partitions.getMaxValidPartitionIndex();
        searchResult.reset();
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
}
