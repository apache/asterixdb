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

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.exceptions.OccurrenceThresholdPanicException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexSearchCursor;

public class TOccurrenceSearcher extends AbstractTOccurrenceSearcher {

    protected final ArrayList<IInvertedListCursor> invListCursors = new ArrayList<IInvertedListCursor>();

    public TOccurrenceSearcher(IHyracksCommonContext ctx, IInvertedIndex invIndex) throws HyracksDataException {
        super(ctx, invIndex);
    }

    public void search(OnDiskInvertedIndexSearchCursor resultCursor, InvertedIndexSearchPredicate searchPred,
            IIndexOperationContext ictx) throws HyracksDataException, IndexException {
        tokenizeQuery(searchPred);
        int numQueryTokens = queryTokenAccessor.getTupleCount();

        invListCursors.clear();
        invListCursorCache.reset();
        for (int i = 0; i < numQueryTokens; i++) {
            searchKey.reset(queryTokenAccessor, i);
            IInvertedListCursor invListCursor = invListCursorCache.getNext();
            invIndex.openInvertedListCursor(invListCursor, searchKey, ictx);
            invListCursors.add(invListCursor);
        }

        IInvertedIndexSearchModifier searchModifier = searchPred.getSearchModifier();
        occurrenceThreshold = searchModifier.getOccurrenceThreshold(numQueryTokens);
        if (occurrenceThreshold <= 0) {
            throw new OccurrenceThresholdPanicException("Merge threshold is <= 0. Failing Search.");
        }
        int numPrefixLists = searchModifier.getNumPrefixLists(occurrenceThreshold, invListCursors.size());

        searchResult.reset();
        invListMerger.merge(invListCursors, occurrenceThreshold, numPrefixLists, searchResult);
        resultCursor.open(null, searchPred);
    }
}
