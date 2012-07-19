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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

public class LSMRTreeSearchCursor extends LSMRTreeAbstractCursor implements ITreeIndexCursor {

    private int currentCursror;

    public LSMRTreeSearchCursor() {
        currentCursror = 0;
    }

    @Override
    public void reset() throws HyracksDataException {
        if (!open) {
            return;
        }

        currentCursror = 0;
        foundNext = false;
        try {
            for (int i = 0; i < numberOfTrees; i++) {
                rtreeCursors[i].close();
                btreeCursors[i].close();
            }
            rtreeCursors = null;
            btreeCursors = null;
        } finally {
            lsmHarness.closeSearchCursor(searcherRefCount, includeMemRTree);
        }
    }

    private void searchNextCursor() throws HyracksDataException {
        if (currentCursror < numberOfTrees) {
            rtreeCursors[currentCursror].reset();
            try {
                diskRTreeAccessors[currentCursror].search(rtreeCursors[currentCursror], rtreeSearchPredicate);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (foundNext) {
            return true;
        }
        while (currentCursror < numberOfTrees) {
            while (rtreeCursors[currentCursror].hasNext()) {
                rtreeCursors[currentCursror].next();
                ITupleReference currentTuple = rtreeCursors[currentCursror].getTuple();

                boolean killerTupleFound = false;
                for (int i = 0; i <= currentCursror; i++) {
                    try {
                        btreeCursors[i].reset();
                        btreeRangePredicate.setHighKey(currentTuple, true);
                        btreeRangePredicate.setLowKey(currentTuple, true);
                        diskBTreeAccessors[i].search(btreeCursors[i], btreeRangePredicate);
                    } catch (IndexException e) {
                        throw new HyracksDataException(e);
                    }
                    try {
                        if (btreeCursors[i].hasNext()) {
                            killerTupleFound = true;
                            break;
                        }
                    } finally {
                        btreeCursors[i].close();
                    }
                }
                if (!killerTupleFound) {
                    frameTuple = currentTuple;
                    foundNext = true;
                    return true;
                }
            }
            rtreeCursors[currentCursror].close();
            currentCursror++;
            searchNextCursor();
        }
        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        foundNext = false;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        super.open(initialState, searchPred);
        searchNextCursor();
    }

}