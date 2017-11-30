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

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class LSMBTreePointSearchCursor implements ITreeIndexCursor {

    private BTreeRangeSearchCursor[] rangeCursors;
    private final ILSMIndexOperationContext opCtx;
    private ISearchOperationCallback searchCallback;
    private RangePredicate predicate;
    private boolean includeMutableComponent;
    private int numBTrees;
    private BTreeAccessor[] btreeAccessors;
    private BloomFilter[] bloomFilters;
    private ILSMHarness lsmHarness;
    private boolean nextHasBeenCalled;
    private boolean foundTuple;
    private int foundIn = -1;
    private ITupleReference frameTuple;
    private List<ILSMComponent> operationalComponents;

    private final long[] hashes = BloomFilter.createHashArray();

    public LSMBTreePointSearchCursor(ILSMIndexOperationContext opCtx) {
        this.opCtx = opCtx;
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (nextHasBeenCalled) {
            return false;
        } else if (foundTuple) {
            return true;
        }
        boolean reconciled = false;
        for (int i = 0; i < numBTrees; ++i) {
            if (bloomFilters[i] != null && !bloomFilters[i].contains(predicate.getLowKey(), hashes)) {
                continue;
            }
            btreeAccessors[i].search(rangeCursors[i], predicate);
            if (rangeCursors[i].hasNext()) {
                rangeCursors[i].next();
                // We use the predicate's to lock the key instead of the tuple that we get from cursor
                // to avoid copying the tuple when we do the "unlatch dance".
                if (reconciled || searchCallback.proceed(predicate.getLowKey())) {
                    // if proceed is successful, then there's no need for doing the "unlatch dance"
                    if (((ILSMTreeTupleReference) rangeCursors[i].getTuple()).isAntimatter()) {
                        if (reconciled) {
                            searchCallback.cancel(predicate.getLowKey());
                        }
                        rangeCursors[i].close();
                        return false;
                    } else {
                        frameTuple = rangeCursors[i].getTuple();
                        foundTuple = true;
                        foundIn = i;
                        return true;
                    }
                }
                if (i == 0 && includeMutableComponent) {
                    // unlatch/unpin
                    rangeCursors[i].reset();
                    searchCallback.reconcile(predicate.getLowKey());
                    reconciled = true;

                    // retraverse
                    btreeAccessors[0].search(rangeCursors[i], predicate);
                    if (rangeCursors[i].hasNext()) {
                        rangeCursors[i].next();
                        if (((ILSMTreeTupleReference) rangeCursors[i].getTuple()).isAntimatter()) {
                            searchCallback.cancel(predicate.getLowKey());
                            rangeCursors[i].close();
                            return false;
                        } else {
                            frameTuple = rangeCursors[i].getTuple();
                            foundTuple = true;
                            searchCallback.complete(predicate.getLowKey());
                            foundIn = i;
                            return true;
                        }
                    } else {
                        searchCallback.cancel(predicate.getLowKey());
                        rangeCursors[i].close();
                    }
                } else {
                    frameTuple = rangeCursors[i].getTuple();
                    searchCallback.reconcile(frameTuple);
                    searchCallback.complete(frameTuple);
                    foundTuple = true;
                    foundIn = i;
                    return true;
                }
            } else {
                rangeCursors[i].close();
            }
        }
        return false;
    }

    @Override
    public void reset() throws HyracksDataException {
        try {
            if (rangeCursors != null) {
                for (int i = 0; i < rangeCursors.length; ++i) {
                    rangeCursors[i].reset();
                }
            }
            nextHasBeenCalled = false;
            foundTuple = false;
        } finally {
            if (lsmHarness != null) {
                lsmHarness.endSearch(opCtx);
            }
        }
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();
        searchCallback = lsmInitialState.getSearchOperationCallback();
        predicate = (RangePredicate) lsmInitialState.getSearchPredicate();
        numBTrees = operationalComponents.size();
        if (rangeCursors == null || rangeCursors.length < numBTrees) {
            // object creation: should be relatively low
            rangeCursors = new BTreeRangeSearchCursor[numBTrees];
            btreeAccessors = new BTreeAccessor[numBTrees];
            bloomFilters = new BloomFilter[numBTrees];
        }
        includeMutableComponent = false;

        for (int i = 0; i < numBTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            BTree btree;
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
                if (rangeCursors[i] == null) {
                    // create a new one
                    IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getLeafFrameFactory().createFrame();
                    rangeCursors[i] = new BTreeRangeSearchCursor(leafFrame, false);
                } else {
                    // reset
                    rangeCursors[i].reset();
                }
                btree = ((LSMBTreeMemoryComponent) component).getIndex();
                // no bloom filter for in-memory BTree
                bloomFilters[i] = null;
            } else {
                if (rangeCursors[i] != null) {
                    // can re-use cursor
                    rangeCursors[i].reset();
                } else {
                    // create new cursor <should be relatively rare>
                    IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getLeafFrameFactory().createFrame();
                    rangeCursors[i] = new BTreeRangeSearchCursor(leafFrame, false);
                }
                btree = ((LSMBTreeWithBloomFilterDiskComponent) component).getIndex();
                bloomFilters[i] = ((LSMBTreeWithBloomFilterDiskComponent) component).getBloomFilter();
            }
            if (btreeAccessors[i] == null) {
                btreeAccessors[i] = btree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            } else {
                // re-use
                btreeAccessors[i].reset(btree, NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            }
        }
        nextHasBeenCalled = false;
        foundTuple = false;
    }

    @Override
    public void next() throws HyracksDataException {
        nextHasBeenCalled = true;
    }

    @Override
    public void close() throws HyracksDataException {
        if (lsmHarness != null) {
            try {
                closeCursors();
                rangeCursors = null;
            } finally {
                lsmHarness.endSearch(opCtx);
            }
        }
        nextHasBeenCalled = false;
        foundTuple = false;
    }

    @Override
    public ITupleReference getTuple() {
        return frameTuple;
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
        if (foundTuple) {
            return operationalComponents.get(foundIn).getLSMComponentFilter();
        }
        return null;
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        // do nothing
    }

    @Override
    public void setFileId(int fileId) {
        // do nothing
    }

    @Override
    public boolean isExclusiveLatchNodes() {
        return false;
    }

    private void closeCursors() throws HyracksDataException {
        if (rangeCursors != null) {
            for (int i = 0; i < numBTrees; ++i) {
                if (rangeCursors[i] != null) {
                    rangeCursors[i].close();
                }
            }
        }
    }
}
