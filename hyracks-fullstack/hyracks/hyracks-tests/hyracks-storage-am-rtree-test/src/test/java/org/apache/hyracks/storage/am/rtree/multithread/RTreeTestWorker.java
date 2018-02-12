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

package org.apache.hyracks.storage.am.rtree.multithread;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.AbstractIndexTestWorker;
import org.apache.hyracks.storage.am.common.TestOperationSelector;
import org.apache.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.datagen.DataGenThread;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;

public class RTreeTestWorker extends AbstractIndexTestWorker {

    private final RTree rtree;
    private final int numFields;
    private final ArrayTupleReference rearrangedTuple = new ArrayTupleReference();
    private final ArrayTupleBuilder rearrangedTb;

    public RTreeTestWorker(DataGenThread dataGen, TestOperationSelector opSelector, IIndex index, int numBatches)
            throws HyracksDataException {
        super(dataGen, opSelector, index, numBatches);
        rtree = (RTree) index;
        numFields = rtree.getFieldCount();
        rearrangedTb = new ArrayTupleBuilder(numFields);
    }

    @Override
    public void performOp(ITupleReference tuple, TestOperation op) throws HyracksDataException {
        RTree.RTreeAccessor accessor = (RTree.RTreeAccessor) indexAccessor;
        MultiComparator cmp = accessor.getOpContext().getCmp();
        SearchPredicate rangePred = new SearchPredicate(tuple, cmp);
        IIndexCursor searchCursor = accessor.createSearchCursor(false);
        try {
            ITreeIndexCursor diskOrderScanCursor = accessor.createDiskOrderScanCursor();
            try {
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
                        rangePred.setSearchKey(null);
                        accessor.search(searchCursor, rangePred);
                        try {
                            consumeCursorTuples(searchCursor);
                        } finally {
                            searchCursor.close();
                        }
                        break;

                    case DISKORDER_SCAN:
                        accessor.diskOrderScan(diskOrderScanCursor);
                        try {
                            consumeCursorTuples(diskOrderScanCursor);
                        } finally {
                            diskOrderScanCursor.close();
                        }
                        break;
                    default:
                        throw new HyracksDataException("Op " + op.toString() + " not supported.");
                }
            } finally {
                diskOrderScanCursor.destroy();
            }
        } finally {
            searchCursor.destroy();
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
}
