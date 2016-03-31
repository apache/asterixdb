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

import java.util.ArrayList;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.BloomFilterAwareBTreePointSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;

public class LSMInvertedIndexRangeSearchCursor extends LSMIndexSearchCursor {

    // Assuming the cursor for all deleted-keys indexes are of the same type.
    private IIndexCursor[] deletedKeysBTreeCursors;
    protected ArrayList<IIndexAccessor> deletedKeysBTreeAccessors;
    protected PermutingTupleReference keysOnlyTuple;
    protected RangePredicate keySearchPred;

    public LSMInvertedIndexRangeSearchCursor(ILSMIndexOperationContext opCtx) {
        super(opCtx, false);
    }

    @Override
    public void next() throws HyracksDataException {
        super.next();
    }

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
        lsmHarness = lsmInitState.getLSMHarness();
        operationalComponents = lsmInitState.getOperationalComponents();
        includeMutableComponent = lsmInitState.getIncludeMemComponent();

        // For searching the deleted-keys BTrees.
        this.keysOnlyTuple = lsmInitState.getKeysOnlyTuple();
        deletedKeysBTreeAccessors = lsmInitState.getDeletedKeysBTreeAccessors();

        if (!deletedKeysBTreeAccessors.isEmpty()) {
            deletedKeysBTreeCursors = new IIndexCursor[deletedKeysBTreeAccessors.size()];
            for (int i = 0; i < operationalComponents.size(); i++) {
                ILSMComponent component = operationalComponents.get(i);
                if (component.getType() == LSMComponentType.MEMORY) {
                    // No need for a bloom filter for the in-memory BTree.
                    deletedKeysBTreeCursors[i] = deletedKeysBTreeAccessors.get(i).createSearchCursor(false);
                } else {
                    deletedKeysBTreeCursors[i] = new BloomFilterAwareBTreePointSearchCursor(
                            (IBTreeLeafFrame) lsmInitState.getgetDeletedKeysBTreeLeafFrameFactory().createFrame(),
                            false, ((LSMInvertedIndexDiskComponent) operationalComponents.get(i)).getBloomFilter());
                }
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
    protected boolean isDeleted(PriorityQueueElement checkElement) throws HyracksDataException, IndexException {
        keysOnlyTuple.reset(checkElement.getTuple());
        int end = checkElement.getCursorIndex();
        for (int i = 0; i < end; i++) {
            deletedKeysBTreeCursors[i].reset();
            try {
                deletedKeysBTreeAccessors.get(i).search(deletedKeysBTreeCursors[i], keySearchPred);
                if (deletedKeysBTreeCursors[i].hasNext()) {
                    return true;
                }
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            } finally {
                deletedKeysBTreeCursors[i].close();
            }
        }
        return false;
    }
}
