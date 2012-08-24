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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;

/**
 * Searches the components one-by-one, completely consuming a cursor before moving on to the next one.
 * Therefore, the are no guarantees about sort order of the results.
 */
public class LSMInvertedIndexSearchCursor implements IIndexCursor {

    private IIndexAccessor currentAccessor;
    private IIndexCursor currentCursor;
    private int accessorIndex = -1;
    private boolean tupleConsumed = true;
    private LSMHarness harness;
    private boolean includeMemComponent;
    private AtomicInteger searcherRefCount;
    private List<IIndexAccessor> indexAccessors;
    private ISearchPredicate searchPred;
    
    // Assuming the cursor for all deleted-keys indexes are of the same type.
    protected IIndexCursor deletedKeysBTreeCursor;
    protected List<IIndexAccessor> deletedKeysBTreeAccessors;
    protected RangePredicate keySearchPred;

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMInvertedIndexSearchCursorInitialState lsmInitState = (LSMInvertedIndexSearchCursorInitialState) initialState;
        harness = lsmInitState.getLSMHarness();
        includeMemComponent = lsmInitState.getIncludeMemComponent();
        searcherRefCount = lsmInitState.getSearcherRefCount();
        indexAccessors = lsmInitState.getIndexAccessors();
        accessorIndex = 0;
        this.searchPred = searchPred;
        
        // For searching the deleted-keys BTrees.
        deletedKeysBTreeAccessors = lsmInitState.getDeletedKeysBTreeAccessors();
        deletedKeysBTreeCursor = deletedKeysBTreeAccessors.get(0).createSearchCursor();        
        MultiComparator keyCmp = lsmInitState.getKeyComparator();
        keySearchPred = new RangePredicate(null, null, true, true, keyCmp, keyCmp);
    }

    protected boolean isDeleted(ITupleReference key) throws HyracksDataException {
        keySearchPred.setLowKey(key, true);
        keySearchPred.setHighKey(key, true);
        for (int i = 0; i <= accessorIndex; i++) {
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
    
    // Move to the next tuple that has not been deleted.
    private boolean nextValidTuple() throws HyracksDataException {
        while (currentCursor.hasNext()) {
            currentCursor.next();
            if (!isDeleted(currentCursor.getTuple())) { 
                tupleConsumed = false;
                return true;
            }
        }
        return false;
    }
    
    @Override
    public boolean hasNext() throws HyracksDataException {
        if (!tupleConsumed) {
            return true;
        }
        if (currentCursor != null) {
            if (nextValidTuple()) {
                return true;
            }
            currentCursor.close();
            accessorIndex++;
        }
        while (accessorIndex < indexAccessors.size()) {
            // Current cursor has been exhausted, switch to next accessor/cursor.
            currentAccessor = indexAccessors.get(accessorIndex);
            currentCursor = currentAccessor.createSearchCursor();
            try {
                currentAccessor.search(currentCursor, searchPred);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
            if (nextValidTuple()) {
                return true;
            }
            // Close as we go to release resources.
            currentCursor.close();
            accessorIndex++;
        }
        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        // Mark the tuple as consumed, so hasNext() can move on.
        tupleConsumed = true;                
    }

    @Override
    public void close() throws HyracksDataException {
        reset();
        accessorIndex = -1;
        harness.closeSearchCursor(searcherRefCount, includeMemComponent);
    }

    @Override
    public void reset() throws HyracksDataException {
        if (currentCursor != null) {
            currentCursor.close();
            currentCursor = null;
        }
        accessorIndex = 0;
    }

    @Override
    public ITupleReference getTuple() {
        return currentCursor.getTuple();
    }
}
