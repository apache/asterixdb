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

package edu.uci.ics.hyracks.storage.am.lsm.btree.multithread;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeNotUpdateableException;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.AbstractIndexTestWorker;
import edu.uci.ics.hyracks.storage.am.common.TestOperationSelector;
import edu.uci.ics.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexNonExistentKeyException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTree.LSMBTreeAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;

public class LSMBTreeTestWorker extends AbstractIndexTestWorker {
    private final LSMBTree lsmBTree;
    private final int numKeyFields;
    private final ArrayTupleBuilder deleteTb;
    private final ArrayTupleReference deleteTuple = new ArrayTupleReference();

    public LSMBTreeTestWorker(DataGenThread dataGen, TestOperationSelector opSelector, IIndex index, int numBatches)
            throws HyracksDataException {
        super(dataGen, opSelector, index, numBatches);
        lsmBTree = (LSMBTree) index;
        numKeyFields = lsmBTree.getComparatorFactories().length;
        deleteTb = new ArrayTupleBuilder(numKeyFields);
    }

    @Override
    public void performOp(ITupleReference tuple, TestOperation op) throws HyracksDataException, IndexException {
        LSMBTreeAccessor accessor = (LSMBTreeAccessor) indexAccessor;
        IIndexCursor searchCursor = accessor.createSearchCursor();
        MultiComparator cmp = accessor.getMultiComparator();
        RangePredicate rangePred = new RangePredicate(tuple, tuple, true, true, cmp, cmp);

        switch (op) {
            case INSERT:
                try {
                    accessor.insert(tuple);
                } catch (TreeIndexDuplicateKeyException e) {
                    // Ignore duplicate keys, since we get random tuples.
                }
                break;

            case DELETE:
                // Create a tuple reference with only key fields.
                deleteTb.reset();
                for (int i = 0; i < numKeyFields; i++) {
                    deleteTb.addField(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
                }
                deleteTuple.reset(deleteTb.getFieldEndOffsets(), deleteTb.getByteArray());
                try {
                    accessor.delete(deleteTuple);
                } catch (TreeIndexNonExistentKeyException e) {
                    // Ignore non-existant keys, since we get random tuples.
                }
                break;

            case UPDATE:
                try {
                    accessor.update(tuple);
                } catch (TreeIndexNonExistentKeyException e) {
                    // Ignore non-existant keys, since we get random tuples.
                } catch (BTreeNotUpdateableException e) {
                    // Ignore not updateable exception due to numKeys == numFields.
                }
                break;

            case POINT_SEARCH:
                searchCursor.reset();
                rangePred.setLowKey(tuple, true);
                rangePred.setHighKey(tuple, true);
                accessor.search(searchCursor, rangePred);
                consumeCursorTuples(searchCursor);
                break;

            case SCAN:
                searchCursor.reset();
                rangePred.setLowKey(null, true);
                rangePred.setHighKey(null, true);
                accessor.search(searchCursor, rangePred);
                consumeCursorTuples(searchCursor);
                break;

            case MERGE:
                accessor.scheduleMerge(NoOpIOOperationCallback.INSTANCE, lsmBTree.getImmutableComponents());
                break;

            default:
                throw new HyracksDataException("Op " + op.toString() + " not supported.");
        }
    }
}
