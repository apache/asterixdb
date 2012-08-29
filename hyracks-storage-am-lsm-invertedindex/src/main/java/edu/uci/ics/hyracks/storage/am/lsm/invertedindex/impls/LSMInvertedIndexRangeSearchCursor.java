/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;

public class LSMInvertedIndexRangeSearchCursor extends LSMTreeSearchCursor {

    // Assuming the cursor for all deleted-keys indexes are of the same type.
    protected IIndexCursor deletedKeysBTreeCursor;
    protected ArrayList<IIndexAccessor> deletedKeysBTreeAccessors;
    protected PermutingTupleReference keysOnlyTuple;
    protected RangePredicate keySearchPred;
    
    @Override
    public void open(ICursorInitialState initState, ISearchPredicate searchPred) throws IndexException,
            HyracksDataException {
        LSMInvertedIndexRangeSearchCursorInitialState lsmInitState = (LSMInvertedIndexRangeSearchCursorInitialState) initState;
        cmp = lsmInitState.getOriginalKeyComparator();
        int numComponents = lsmInitState.getNumComponents();
        rangeCursors = new IIndexCursor[numComponents];
        for (int i = 0; i < numComponents; i++) {
            IInvertedIndexAccessor invIndexAccessor = (IInvertedIndexAccessor) lsmInitState.getIndexAccessors().get(i);
            rangeCursors[i] = invIndexAccessor.createRangeSearchCursor();
            invIndexAccessor.rangeSearch(rangeCursors[i], lsmInitState.getSearchPredicate());

        }
        
        // For searching the deleted-keys BTrees.
        this.keysOnlyTuple = lsmInitState.getKeysOnlyTuple();
        deletedKeysBTreeAccessors = lsmInitState.getDeletedKeysBTreeAccessors();
        if (!deletedKeysBTreeAccessors.isEmpty()) {
            deletedKeysBTreeCursor = deletedKeysBTreeAccessors.get(0).createSearchCursor();
        }
        MultiComparator keyCmp = lsmInitState.getKeyComparator();
        keySearchPred = new RangePredicate(keysOnlyTuple, keysOnlyTuple, true, true, keyCmp, keyCmp);
        
        searcherRefCount = lsmInitState.getSearcherRefCount();
        lsmHarness = lsmInitState.getLSMHarness();
        includeMemComponent = lsmInitState.getIncludeMemComponent();
        setPriorityQueueComparator();
        initPriorityQueue();
    }
    
    /**
     * Check deleted-keys BTrees whether they contain the key in the checkElement's tuple.
     */
    @Override
    protected boolean isDeleted(PriorityQueueElement checkElement) throws HyracksDataException {
        keysOnlyTuple.reset(checkElement.getTuple());
        int end = checkElement.getCursorIndex();
        for (int i = 0; i < end; i++) {
            deletedKeysBTreeCursor.reset();       
            try {
                deletedKeysBTreeAccessors.get(i).search(deletedKeysBTreeCursor, keySearchPred);
                if (deletedKeysBTreeCursor.hasNext()) {
                    return true;
                }
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            } finally {
                deletedKeysBTreeCursor.close();
            }
        }
        return false;
    }
}
