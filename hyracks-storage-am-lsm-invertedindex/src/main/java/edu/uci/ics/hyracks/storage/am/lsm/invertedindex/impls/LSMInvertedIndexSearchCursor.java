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

public class LSMInvertedIndexSearchCursor implements IIndexCursor {

    private int cursorIndex = -1;
    private LSMHarness harness;
    private boolean includeMemComponent;
    private AtomicInteger searcherRefCount;
    private List<IIndexAccessor> indexAccessors;
    private List<IIndexCursor> indexCursors;// = new ArrayList<IIndexCursor>();
    private ISearchPredicate searchPred;

    public LSMInvertedIndexSearchCursor() {
        indexCursors = new ArrayList<IIndexCursor>();
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMInvertedIndexCursorInitialState lsmInitialState = (LSMInvertedIndexCursorInitialState) initialState;
        harness = lsmInitialState.getLSMHarness();
        includeMemComponent = lsmInitialState.getIncludeMemComponent();
        searcherRefCount = lsmInitialState.getSearcherRefCount();
        indexAccessors = lsmInitialState.getIndexAccessors();
        indexCursors.clear();
        //        indexCursors = new ArrayList<IIndexCursor>(indexAccessors.size());
        cursorIndex = 0;
        this.searchPred = searchPred;

        IIndexAccessor currentAccessor;
        IIndexCursor currentCursor;
        while (cursorIndex < indexAccessors.size()) {
            // Open cursors and perform search lazily as each component is passed over
            currentAccessor = indexAccessors.get(cursorIndex);
            currentCursor = currentAccessor.createSearchCursor();
            try {
                currentAccessor.search(currentCursor, searchPred);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
            indexCursors.add(currentCursor);

            if (currentCursor.hasNext()) {
                break;
            }

            // Close as we go to release any resources
            currentCursor.close();
            cursorIndex++;
        }

    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        IIndexAccessor currentAccessor;
        IIndexCursor currentCursor;

        if (cursorIndex >= indexAccessors.size()) {
            return false;
        }

        currentCursor = indexCursors.get(cursorIndex);
        if (currentCursor.hasNext()) {
            return true;
        } else {
            currentCursor.close();
            cursorIndex++;
            while (cursorIndex < indexAccessors.size()) {
                currentAccessor = indexAccessors.get(cursorIndex);
                currentCursor = currentAccessor.createSearchCursor();
                try {
                    currentAccessor.search(currentCursor, searchPred);
                } catch (IndexException e) {
                    throw new HyracksDataException(e);
                }
                indexCursors.add(currentCursor);

                if (currentCursor.hasNext()) {
                    return true;
                } else {
                    currentCursor.close();
                    cursorIndex++;
                }
            }
        }

        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        IIndexAccessor currentAccessor;
        IIndexCursor currentCursor = indexCursors.get(cursorIndex);

        if (currentCursor.hasNext()) {
            currentCursor.next();
        } else {
            currentCursor.close();
            cursorIndex++;
            while (cursorIndex < indexAccessors.size()) {
                currentAccessor = indexAccessors.get(cursorIndex);
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
        } else {
            return null;
        }
    }

}
