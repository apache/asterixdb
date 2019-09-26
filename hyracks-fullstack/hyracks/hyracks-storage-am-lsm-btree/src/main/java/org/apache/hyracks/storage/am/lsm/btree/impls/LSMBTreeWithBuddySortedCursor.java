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
import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;

public class LSMBTreeWithBuddySortedCursor extends LSMBTreeWithBuddyAbstractCursor {
    // TODO: This class can be removed and instead use a search cursor that uses
    // a logic similar
    // to the one in LSMRTreeWithAntiMatterTuplesSearchCursor
    // currently, this cursor is only used when doing merge operations.
    private boolean[] depletedBtreeCursors;
    private int foundIn = -1;
    private PermutingTupleReference buddyBtreeTuple;

    public LSMBTreeWithBuddySortedCursor(ILSMIndexOperationContext opCtx, int[] buddyBTreeFields,
            IIndexCursorStats stats) throws HyracksDataException {
        super(opCtx, stats);
        this.buddyBtreeTuple = new PermutingTupleReference(buddyBTreeFields);
        close();
    }

    public ILSMIndexOperationContext getOpCtx() {
        return opCtx;
    }

    @Override
    public void doClose() throws HyracksDataException {
        depletedBtreeCursors = new boolean[numberOfTrees];
        foundNext = false;
        try {
            for (int i = 0; i < numberOfTrees; i++) {
                btreeCursors[i].close();
                btreeAccessors[i].search(btreeCursors[i], btreeRangePredicate);
                if (btreeCursors[i].hasNext()) {
                    btreeCursors[i].next();
                } else {
                    depletedBtreeCursors[i] = true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException("error while reseting the btrees of the lsm btree with buddy btree", e);
        } finally {
            if (open) {
                lsmHarness.endSearch(opCtx);
            }
        }
    }

    @Override
    public ITupleReference getFilterMinTuple() {
        ILSMComponentFilter filter = getFilter();
        return filter == null ? null : filter.getMinTuple();
    }

    @Override
    public ITupleReference getFilterMaxTuple() {
        ILSMComponentFilter filter = getFilter();
        return filter == null ? null : filter.getMaxTuple();
    }

    private ILSMComponentFilter getFilter() {
        if (foundIn < 0) {
            return null;
        }
        return operationalComponents.get(foundIn).getLSMComponentFilter();
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        while (!foundNext) {
            frameTuple = null;

            if (foundIn != -1) {
                if (btreeCursors[foundIn].hasNext()) {
                    btreeCursors[foundIn].next();
                } else {
                    depletedBtreeCursors[foundIn] = true;
                }
            }

            foundIn = -1;
            for (int i = 0; i < numberOfTrees; i++) {
                if (depletedBtreeCursors[i]) {
                    continue;
                }

                if (frameTuple == null) {
                    frameTuple = btreeCursors[i].getTuple();
                    foundIn = i;
                    continue;
                }

                if (btreeCmp.compare(frameTuple, btreeCursors[i].getTuple()) > 0) {
                    frameTuple = btreeCursors[i].getTuple();
                    foundIn = i;
                }
            }

            if (foundIn == -1) {
                return false;
            }

            boolean killed = false;
            buddyBtreeTuple.reset(frameTuple);
            for (int i = 0; i < foundIn; i++) {
                buddyBtreeCursors[i].close();
                buddyBtreeRangePredicate.setHighKey(buddyBtreeTuple, true);
                btreeRangePredicate.setLowKey(buddyBtreeTuple, true);
                btreeAccessors[i].search(btreeCursors[i], btreeRangePredicate);
                try {
                    if (btreeCursors[i].hasNext()) {
                        killed = true;
                        break;
                    }
                } finally {
                    btreeCursors[i].close();
                }
            }
            if (!killed) {
                foundNext = true;
            }
        }

        return true;
    }

    @Override
    public void doNext() throws HyracksDataException {
        foundNext = false;
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        super.doOpen(initialState, searchPred);
        depletedBtreeCursors = new boolean[numberOfTrees];
        foundNext = false;
        for (int i = 0; i < numberOfTrees; i++) {
            btreeCursors[i].close();
        }
        IndexCursorUtils.open(btreeAccessors, btreeCursors, btreeRangePredicate);
        try {
            for (int i = 0; i < numberOfTrees; i++) {
                if (btreeCursors[i].hasNext()) {
                    btreeCursors[i].next();
                } else {
                    depletedBtreeCursors[i] = true;
                }
            }
        } catch (Throwable th) { // NOSONAR Must catch all failures to close before throwing
            for (int i = 0; i < numberOfTrees; i++) {
                IndexCursorUtils.close(btreeCursors[i], th);
            }
            throw HyracksDataException.create(th);
        }
    }
}
