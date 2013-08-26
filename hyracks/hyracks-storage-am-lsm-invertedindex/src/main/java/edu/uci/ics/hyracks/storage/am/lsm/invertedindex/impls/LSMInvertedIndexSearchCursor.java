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
package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BloomFilterAwareBTreePointSearchCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.exceptions.OccurrenceThresholdPanicException;

/**
 * Searches the components one-by-one, completely consuming a cursor before moving on to the next one.
 * Therefore, the are no guarantees about sort order of the results.
 */
public class LSMInvertedIndexSearchCursor implements IIndexCursor {

    private IIndexAccessor currentAccessor;
    private IIndexCursor currentCursor;
    private int accessorIndex = -1;
    private boolean tupleConsumed = true;
    private ILSMHarness harness;
    private List<IIndexAccessor> indexAccessors;
    private ISearchPredicate searchPred;
    private ISearchOperationCallback searchCallback;

    // Assuming the cursor for all deleted-keys indexes are of the same type.
    private IIndexCursor[] deletedKeysBTreeCursors;
    private List<IIndexAccessor> deletedKeysBTreeAccessors;
    private RangePredicate keySearchPred;
    private ILSMIndexOperationContext opCtx;

    private List<ILSMComponent> operationalComponents;

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMInvertedIndexSearchCursorInitialState lsmInitState = (LSMInvertedIndexSearchCursorInitialState) initialState;
        harness = lsmInitState.getLSMHarness();
        operationalComponents = lsmInitState.getOperationalComponents();
        indexAccessors = lsmInitState.getIndexAccessors();
        opCtx = lsmInitState.getOpContext();
        accessorIndex = 0;
        this.searchPred = searchPred;
        this.searchCallback = lsmInitState.getSearchOperationCallback();

        // For searching the deleted-keys BTrees.
        deletedKeysBTreeAccessors = lsmInitState.getDeletedKeysBTreeAccessors();
        deletedKeysBTreeCursors = new IIndexCursor[deletedKeysBTreeAccessors.size()];

        for (int i = 0; i < operationalComponents.size(); i++) {
            ILSMComponent component = operationalComponents.get(i);
            if (component.getType() == LSMComponentType.MEMORY) {
                // No need for a bloom filter for the in-memory BTree.
                deletedKeysBTreeCursors[i] = deletedKeysBTreeAccessors.get(i).createSearchCursor();
            } else {
                deletedKeysBTreeCursors[i] = new BloomFilterAwareBTreePointSearchCursor((IBTreeLeafFrame) lsmInitState
                        .getgetDeletedKeysBTreeLeafFrameFactory().createFrame(), false,
                        ((LSMInvertedIndexDiskComponent) operationalComponents.get(i)).getBloomFilter());
            }
        }

        MultiComparator keyCmp = lsmInitState.getKeyComparator();
        keySearchPred = new RangePredicate(null, null, true, true, keyCmp, keyCmp);
    }

    protected boolean isDeleted(ITupleReference key) throws HyracksDataException, IndexException {
        keySearchPred.setLowKey(key, true);
        keySearchPred.setHighKey(key, true);
        for (int i = 0; i < accessorIndex; i++) {
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

    // Move to the next tuple that has not been deleted.
    private boolean nextValidTuple() throws HyracksDataException, IndexException {
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
    public boolean hasNext() throws HyracksDataException, IndexException {
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
            } catch (OccurrenceThresholdPanicException e) {
                throw e;
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
        // We assume that the underlying cursors materialize their results such that
        // there is no need to reposition the result cursor after reconciliation.
        if (!searchCallback.proceed(currentCursor.getTuple())) {
            searchCallback.reconcile(currentCursor.getTuple());
        }
    }

    @Override
    public void close() throws HyracksDataException {
        reset();
    }

    @Override
    public void reset() throws HyracksDataException {
        try {
            if (currentCursor != null) {
                currentCursor.close();
                currentCursor = null;
            }
            accessorIndex = 0;
        } finally {
            if (harness != null) {
                harness.endSearch(opCtx);
            }
        }
    }

    @Override
    public ITupleReference getTuple() {
        return currentCursor.getTuple();
    }
}
