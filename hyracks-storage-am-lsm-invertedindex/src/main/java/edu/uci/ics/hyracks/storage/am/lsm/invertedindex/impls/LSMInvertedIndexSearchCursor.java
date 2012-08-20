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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;

/**
 * Searches the components one-by-one, completely consuming a cursor before moving on to the next one.
 * Therefore, the are no guarantees about sort order of the results.
 */
public class LSMInvertedIndexSearchCursor implements IIndexCursor {

    private final List<IIndexCursor> indexCursors = new ArrayList<IIndexCursor>();
    private int cursorIndex = -1;
    private LSMHarness harness;
    private boolean includeMemComponent;
    private AtomicInteger searcherRefCount;
    private List<IIndexAccessor> indexAccessors;
    private ISearchPredicate searchPred;

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMInvertedIndexCursorInitialState lsmInitialState = (LSMInvertedIndexCursorInitialState) initialState;
        harness = lsmInitialState.getLSMHarness();
        includeMemComponent = lsmInitialState.getIncludeMemComponent();
        searcherRefCount = lsmInitialState.getSearcherRefCount();
        indexAccessors = lsmInitialState.getIndexAccessors();
        indexCursors.clear();
        cursorIndex = 0;
        this.searchPred = searchPred;
        while (cursorIndex < indexAccessors.size()) {
            // Open cursors and perform search lazily as each component is passed over
            IIndexAccessor currentAccessor = indexAccessors.get(cursorIndex);
            IIndexCursor currentCursor = currentAccessor.createSearchCursor();
            try {
                currentAccessor.search(currentCursor, searchPred);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
            indexCursors.add(currentCursor);
            if (currentCursor.hasNext()) {
                break;
            }
            // Close as we go to release any resources.
            currentCursor.close();
            cursorIndex++;
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (cursorIndex >= indexAccessors.size()) {
            return false;
        }
        IIndexCursor currentCursor = indexCursors.get(cursorIndex);
        if (currentCursor.hasNext()) {
            return true;
        }
        currentCursor.close();
        cursorIndex++;
        while (cursorIndex < indexAccessors.size()) {
            IIndexAccessor currentAccessor = indexAccessors.get(cursorIndex);
            currentCursor = currentAccessor.createSearchCursor();
            try {
                currentAccessor.search(currentCursor, searchPred);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
            indexCursors.add(currentCursor);
            if (currentCursor.hasNext()) {
                return true;
            }
            currentCursor.close();
            cursorIndex++;
        }
        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        IIndexCursor currentCursor = indexCursors.get(cursorIndex);
        if (currentCursor.hasNext()) {
            currentCursor.next();
        } else {
            currentCursor.close();
            cursorIndex++;
            while (cursorIndex < indexAccessors.size()) {
                IIndexAccessor currentAccessor = indexAccessors.get(cursorIndex);
                currentCursor = currentAccessor.createSearchCursor();
                try {
                    currentAccessor.search(currentCursor, searchPred);
                } catch (IndexException e) {
                    throw new HyracksDataException(e);
                }
                indexCursors.add(currentCursor);
                if (currentCursor.hasNext()) {
                    currentCursor.next();
                    break;
                } else {
                    cursorIndex++;
                }
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        cursorIndex = -1;
        for (int i = 0; i < indexCursors.size(); i++) {
            indexCursors.get(i).close();
        }
        indexCursors.clear();
        harness.closeSearchCursor(searcherRefCount, includeMemComponent);
    }

    @Override
    public void reset() throws HyracksDataException {
        cursorIndex = 0;
        for (int i = 0; i < indexCursors.size(); i++) {
            indexCursors.get(i).reset();
        }
    }

    @Override
    public ITupleReference getTuple() {
        if (cursorIndex < indexCursors.size()) {
            return indexCursors.get(cursorIndex).getTuple();
        }
        return null;
    }

}
