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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;

public class LSMRTreeSearchCursor extends LSMRTreeAbstractCursor {

    private int currentCursor;

    public LSMRTreeSearchCursor(ILSMIndexOperationContext opCtx) {
        super(opCtx);
        currentCursor = 0;
    }

    @Override
    public void close() throws HyracksDataException {
        super.close();
        currentCursor = 0;
    }

    @Override
    public void reset() throws HyracksDataException {
        if (!open) {
            return;
        }

        currentCursor = 0;
        foundNext = false;
        try {
            for (int i = 0; i < numberOfTrees; i++) {
                rtreeCursors[i].close();
                btreeCursors[i].close();
            }
            rtreeCursors = null;
            btreeCursors = null;
        } finally {
            lsmHarness.endSearch(opCtx);
        }
    }

    private void searchNextCursor() throws HyracksDataException {
        if (currentCursor < numberOfTrees) {
            rtreeCursors[currentCursor].reset();
            try {
                rtreeAccessors[currentCursor].search(rtreeCursors[currentCursor], rtreeSearchPredicate);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException, IndexException {
        if (foundNext) {
            return true;
        }
        while (currentCursor < numberOfTrees) {
            while (rtreeCursors[currentCursor].hasNext()) {
                rtreeCursors[currentCursor].next();
                ITupleReference currentTuple = rtreeCursors[currentCursor].getTuple();

                boolean killerTupleFound = false;
                for (int i = 0; i < currentCursor; i++) {
                    btreeCursors[i].reset();
                    btreeRangePredicate.setHighKey(currentTuple, true);
                    btreeRangePredicate.setLowKey(currentTuple, true);
                    btreeAccessors[i].search(btreeCursors[i], btreeRangePredicate);
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
            rtreeCursors[currentCursor].close();
            currentCursor++;
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