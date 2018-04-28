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

package org.apache.hyracks.storage.am.lsm.rtree.multithread;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.AbstractIndexTestWorker;
import org.apache.hyracks.storage.am.common.TestOperationSelector;
import org.apache.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.datagen.DataGenThread;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTree;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTreeAccessor;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTreeOpContext;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;

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
    public void performOp(ITupleReference tuple, TestOperation op) throws HyracksDataException {
        LSMRTreeAccessor accessor = (LSMRTreeAccessor) indexAccessor;
        IIndexCursor searchCursor = accessor.createSearchCursor(false);
        LSMRTreeOpContext concreteCtx = (LSMRTreeOpContext) accessor.getCtx();
        MultiComparator cmp = concreteCtx.getCurrentRTreeOpContext().getCmp();
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
                searchCursor.close();
                rangePred.setSearchKey(null);
                accessor.search(searchCursor, rangePred);
                consumeCursorTuples(searchCursor);
                break;

            case MERGE:
                accessor.scheduleMerge(lsmRTree.getDiskComponents());
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

    private void consumeCursorTuples(ITreeIndexCursor cursor) throws HyracksDataException {
        try {
            while (cursor.hasNext()) {
                cursor.next();
            }
        } finally {
            cursor.destroy();
        }
    }
}
