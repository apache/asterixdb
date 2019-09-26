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

package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;

public class LSMInvertedIndexRangeSearchCursor extends LSMIndexSearchCursor {

    // Assuming the cursor for all deleted-keys indexes are of the same type.
    private IIndexCursor[] deletedKeysBTreeCursors;
    protected BloomFilter[] bloomFilters;
    protected final long[] hashes = BloomFilter.createHashArray();
    protected IIndexAccessor[] deletedKeysBTreeAccessors;
    protected PermutingTupleReference keysOnlyTuple;
    protected RangePredicate keySearchPred;

    public LSMInvertedIndexRangeSearchCursor(ILSMIndexOperationContext opCtx) {
        super(opCtx, false, NoOpIndexCursorStats.INSTANCE);
    }

    @Override
    public void doOpen(ICursorInitialState initState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMInvertedIndexOpContext ctx = (LSMInvertedIndexOpContext) opCtx;
        LSMInvertedIndexRangeSearchCursorInitialState lsmInitState =
                (LSMInvertedIndexRangeSearchCursorInitialState) initState;
        List<ILSMComponent> components = lsmInitState.getOperationalComponents();
        cmp = lsmInitState.getOriginalKeyComparator();
        int numComponents = lsmInitState.getNumComponents();
        rangeCursors = new IIndexCursor[numComponents];
        for (int i = 0; i < numComponents; i++) {
            ILSMComponent component = components.get(i);
            IInvertedIndexAccessor invIndexAccessor =
                    (IInvertedIndexAccessor) component.getIndex().createAccessor(ctx.getIndexAccessParameters());
            rangeCursors[i] = invIndexAccessor.createRangeSearchCursor();
            invIndexAccessor.rangeSearch(rangeCursors[i], lsmInitState.getSearchPredicate());
        }
        lsmHarness = lsmInitState.getLSMHarness();
        operationalComponents = lsmInitState.getOperationalComponents();
        includeMutableComponent = lsmInitState.getIncludeMemComponent();

        // For searching the deleted-keys BTrees.
        this.keysOnlyTuple = lsmInitState.getKeysOnlyTuple();
        bloomFilters = new BloomFilter[numComponents];
        if (numComponents > 0) {
            deletedKeysBTreeAccessors = new IIndexAccessor[numComponents];
            deletedKeysBTreeCursors = new IIndexCursor[numComponents];
            for (int i = 0; i < operationalComponents.size(); i++) {
                ILSMComponent component = operationalComponents.get(i);
                if (component.getType() == LSMComponentType.MEMORY) {
                    // No need for a bloom filter for the in-memory BTree.
                    bloomFilters[i] = null;
                    deletedKeysBTreeAccessors[i] =
                            ((LSMInvertedIndexMemoryComponent) component).getBuddyIndex().createAccessor(iap);
                } else {
                    bloomFilters[i] = ((LSMInvertedIndexDiskComponent) component).getBloomFilter();
                    deletedKeysBTreeAccessors[i] =
                            ((LSMInvertedIndexDiskComponent) component).getBuddyIndex().createAccessor(iap);
                }
                deletedKeysBTreeCursors[i] = deletedKeysBTreeAccessors[i].createSearchCursor(false);
            }
        }
        MultiComparator keyCmp = lsmInitState.getKeyComparator();
        keySearchPred = new RangePredicate(keysOnlyTuple, keysOnlyTuple, true, true, keyCmp, keyCmp);

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
            if (bloomFilters[i] != null && !bloomFilters[i].contains(keysOnlyTuple, hashes)) {
                continue;
            }
            deletedKeysBTreeAccessors[i].search(deletedKeysBTreeCursors[i], keySearchPred);
            try {
                if (deletedKeysBTreeCursors[i].hasNext()) {
                    return true;
                }
            } finally {
                deletedKeysBTreeCursors[i].close();
            }
        }
        return false;
    }

    @Override
    public void doClose() throws HyracksDataException {
        try {
            super.doClose();
        } finally {
            if (deletedKeysBTreeCursors != null) {
                for (int i = 0; i < deletedKeysBTreeCursors.length; i++) {
                    deletedKeysBTreeCursors[i].close();
                }
            }
        }
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        try {
            super.doDestroy();
        } finally {
            if (deletedKeysBTreeCursors != null) {
                for (int i = 0; i < deletedKeysBTreeCursors.length; i++) {
                    deletedKeysBTreeCursors[i].destroy();
                }
                deletedKeysBTreeCursors = null;
            }
        }
    }

}
