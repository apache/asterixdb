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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.multithread;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.AbstractIndexTestWorker;
import edu.uci.ics.hyracks.storage.am.common.TestOperationSelector;
import edu.uci.ics.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTree;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTree.LSMRTreeAccessor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;

public class LSMRTreeTestWorker extends AbstractIndexTestWorker {

    private final LSMRTree lsmRTree;
    private final int numFields;
    private final ArrayTupleBuilder rearrangedTb;
    private final ArrayTupleReference rearrangedTuple = new ArrayTupleReference();

    public LSMRTreeTestWorker(DataGenThread dataGen, TestOperationSelector opSelector, IIndex index, int numBatches)
            throws HyracksDataException {
        super(dataGen, opSelector, index, numBatches);
        lsmRTree = (LSMRTree) index;
        numFields = lsmRTree.getFieldCount();
        rearrangedTb = new ArrayTupleBuilder(numFields);
    }

    @Override
    public void performOp(ITupleReference tuple, TestOperation op) throws HyracksDataException, IndexException {
        LSMRTreeAccessor accessor = (LSMRTreeAccessor) indexAccessor;
        ITreeIndexCursor searchCursor = accessor.createSearchCursor();
        MultiComparator cmp = accessor.getMultiComparator();
        SearchPredicate rangePred = new SearchPredicate(tuple, cmp);

        switch (op) {
            case INSERT:
                rearrangeTuple(tuple, cmp);
                accessor.insert(rearrangedTuple);
                break;

            case DELETE:
                rearrangeTuple(tuple, cmp);
                accessor.delete(rearrangedTuple);
                break;

            case SCAN:
                searchCursor.reset();
                rangePred.setSearchKey(null);
                accessor.search(searchCursor, rangePred);
                consumeCursorTuples(searchCursor);
                break;

            case MERGE:
                accessor.scheduleMerge(NoOpIOOperationCallback.INSTANCE, lsmRTree.getImmutableComponents());
                break;

            default:
                throw new HyracksDataException("Op " + op.toString() + " not supported.");
        }
    }

    private void rearrangeTuple(ITupleReference tuple, MultiComparator cmp) throws HyracksDataException {
        // Create a tuple with rearranged key values to make sure lower points
        // have larger coordinates than high points.
        rearrangedTb.reset();
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j));
            if (c > 0) {
                rearrangedTb.addField(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j));
            } else {
                rearrangedTb.addField(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            }
        }
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j));
            if (c > 0) {
                rearrangedTb.addField(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            } else {
                rearrangedTb.addField(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j));
            }
        }
        for (int i = cmp.getKeyFieldCount(); i < numFields; i++) {
            rearrangedTb.addField(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
        }
        rearrangedTuple.reset(rearrangedTb.getFieldEndOffsets(), rearrangedTb.getByteArray());
    }

    private void consumeCursorTuples(ITreeIndexCursor cursor) throws HyracksDataException, IndexException {
        try {
            while (cursor.hasNext()) {
                cursor.next();
            }
        } finally {
            cursor.close();
        }
    }
}
