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
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.datagen.DataGenThread;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.MultiComparator;

public abstract class AbstractLSMRTreeTestWorker extends AbstractIndexTestWorker {

    protected final ITreeIndex lsmRTree;
    protected final int numFields;
    protected final ArrayTupleBuilder rearrangedTb;
    protected final ArrayTupleReference rearrangedTuple = new ArrayTupleReference();

    public AbstractLSMRTreeTestWorker(DataGenThread dataGen, TestOperationSelector opSelector, IIndex index,
            int numBatches) throws HyracksDataException {
        super(dataGen, opSelector, index, numBatches);
        lsmRTree = (ITreeIndex) index;
        numFields = lsmRTree.getFieldCount();
        rearrangedTb = new ArrayTupleBuilder(numFields);
    }

    protected void rearrangeTuple(ITupleReference tuple, MultiComparator cmp) throws HyracksDataException {
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

    protected void consumeCursorTuples(ITreeIndexCursor cursor) throws HyracksDataException {
        try {
            while (cursor.hasNext()) {
                cursor.next();
            }
        } finally {
            cursor.destroy();
        }
    }
}
