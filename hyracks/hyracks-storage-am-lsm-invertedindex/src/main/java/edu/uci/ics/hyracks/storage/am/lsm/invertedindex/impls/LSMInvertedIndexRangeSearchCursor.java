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

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BloomFilterAwareBTreePointSearchCursor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexAccessor;

public class LSMInvertedIndexRangeSearchCursor extends LSMIndexSearchCursor {

    // Assuming the cursor for all deleted-keys indexes are of the same type.
    private IIndexCursor[] deletedKeysBTreeCursors;
    protected ArrayList<IIndexAccessor> deletedKeysBTreeAccessors;
    protected PermutingTupleReference keysOnlyTuple;
    protected RangePredicate keySearchPred;

    public LSMInvertedIndexRangeSearchCursor(ILSMIndexOperationContext opCtx) {
        super(opCtx);
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
                    deletedKeysBTreeCursors[i] = deletedKeysBTreeAccessors.get(i).createSearchCursor();
                } else {
                    deletedKeysBTreeCursors[i] = new BloomFilterAwareBTreePointSearchCursor((IBTreeLeafFrame) lsmInitState
                            .getgetDeletedKeysBTreeLeafFrameFactory().createFrame(), false,
                            ((LSMInvertedIndexDiskComponent) operationalComponents.get(i)).getBloomFilter());
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

    @Override
    protected void checkPriorityQueue() throws HyracksDataException, IndexException {
        while (!outputPriorityQueue.isEmpty() || needPush == true) {
            if (!outputPriorityQueue.isEmpty()) {
                PriorityQueueElement checkElement = outputPriorityQueue.peek();
                // If there is no previous tuple or the previous tuple can be ignored
                if (outputElement == null) {
                    if (isDeleted(checkElement)) {
                        // If the key has been deleted then pop it and set needPush to true.
                        // We cannot push immediately because the tuple may be
                        // modified if hasNext() is called
                        outputElement = outputPriorityQueue.poll();
                        needPush = true;
                    } else {
                        break;
                    }
                } else {
                    // Compare the previous tuple and the head tuple in the PQ
                    if (compare(cmp, outputElement.getTuple(), checkElement.getTuple()) == 0) {
                        // If the previous tuple and the head tuple are
                        // identical
                        // then pop the head tuple and push the next tuple from
                        // the tree of head tuple

                        // the head element of PQ is useless now
                        PriorityQueueElement e = outputPriorityQueue.poll();
                        pushIntoPriorityQueue(e);
                    } else {
                        // If the previous tuple and the head tuple are different
                        // the info of previous tuple is useless
                        if (needPush == true) {
                            pushIntoPriorityQueue(outputElement);
                            needPush = false;
                        }
                        outputElement = null;
                    }
                }
            } else {
                // the priority queue is empty and needPush
                pushIntoPriorityQueue(outputElement);
                needPush = false;
                outputElement = null;
            }
        }
    }
}
