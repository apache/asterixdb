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
package org.apache.hyracks.storage.am.lsm.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;

public class LSMBTreeWithBuddySearchCursor extends LSMBTreeWithBuddyAbstractCursor{
	private int currentCursor;
    private PermutingTupleReference buddyBTreeTuple;
    
    public LSMBTreeWithBuddySearchCursor(ILSMIndexOperationContext opCtx, int[] buddyBTreeFields) {
        super(opCtx);
        currentCursor = 0;
        this.buddyBTreeTuple = new PermutingTupleReference(buddyBTreeFields);
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
                btreeCursors[i].close();
                buddyBtreeCursors[i].close();
            }
            btreeCursors = null;
            buddyBtreeCursors = null;
        } finally {
            lsmHarness.endSearch(opCtx);
        }
    }
    
    private void searchNextCursor() throws HyracksDataException {
        if (currentCursor < numberOfTrees) {
            try {
            	btreeCursors[currentCursor].reset();
                btreeAccessors[currentCursor].search(btreeCursors[currentCursor], btreeRangePredicate);
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
            while (btreeCursors[currentCursor].hasNext()) {
                btreeCursors[currentCursor].next();
                ITupleReference currentTuple = btreeCursors[currentCursor].getTuple();
                buddyBTreeTuple.reset(btreeCursors[currentCursor].getTuple());
                boolean killerTupleFound = false;
                for (int i = 0; i < currentCursor; i++) {
                    buddyBtreeCursors[i].reset();
                    buddyBtreeRangePredicate.setHighKey(buddyBTreeTuple, true);
                    buddyBtreeRangePredicate.setLowKey(buddyBTreeTuple, true);
                    buddyBtreeAccessors[i].search(buddyBtreeCursors[i], buddyBtreeRangePredicate);
                    try {
                        if (buddyBtreeCursors[i].hasNext()) {
                            killerTupleFound = true;
                            break;
                        }
                    } finally {
                        buddyBtreeCursors[i].close();
                    }
                }
                if (!killerTupleFound) {
                    frameTuple = currentTuple;
                    foundNext = true;
                    return true;
                }
            }
            btreeCursors[currentCursor].close();
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
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException, IndexException {
        super.open(initialState, searchPred);
        searchNextCursor();
    }
}