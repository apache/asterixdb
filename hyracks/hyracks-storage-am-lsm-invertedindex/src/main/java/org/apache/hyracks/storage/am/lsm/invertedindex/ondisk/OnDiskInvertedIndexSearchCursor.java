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

package org.apache.hyracks.storage.am.lsm.invertedindex.ondisk;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndexSearcher;

public class OnDiskInvertedIndexSearchCursor implements IIndexCursor {

    private List<ByteBuffer> resultBuffers;
    private int numResultBuffers;
    private int currentBufferIndex = 0;
    private int tupleIndex = 0;
    private final IInvertedIndexSearcher invIndexSearcher;
    private final IFrameTupleAccessor fta;
    private final FixedSizeTupleReference frameTuple;
    private final PermutingTupleReference resultTuple;

    public OnDiskInvertedIndexSearchCursor(IInvertedIndexSearcher invIndexSearcher, int numInvListFields) {
        this.invIndexSearcher = invIndexSearcher;
        this.fta = invIndexSearcher.createResultFrameTupleAccessor();
        this.frameTuple = (FixedSizeTupleReference) invIndexSearcher.createResultFrameTupleReference();
        // Project away the occurrence count from the result tuples.
        int[] fieldPermutation = new int[numInvListFields];
        for (int i = 0; i < numInvListFields; i++) {
            fieldPermutation[i] = i;
        }
        resultTuple = new PermutingTupleReference(fieldPermutation);
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        currentBufferIndex = 0;
        tupleIndex = 0;
        resultBuffers = invIndexSearcher.getResultBuffers();
        numResultBuffers = invIndexSearcher.getNumValidResultBuffers();
        if (numResultBuffers > 0) {
            fta.reset(resultBuffers.get(0));
        }
    }

    @Override
    public boolean hasNext() {
        if (currentBufferIndex < numResultBuffers && tupleIndex < fta.getTupleCount()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void next() {
        frameTuple.reset(fta.getBuffer().array(), fta.getTupleStartOffset(tupleIndex));
        resultTuple.reset(frameTuple);
        tupleIndex++;
        if (tupleIndex >= fta.getTupleCount()) {
            if (currentBufferIndex + 1 < numResultBuffers) {
                currentBufferIndex++;
                fta.reset(resultBuffers.get(currentBufferIndex));
                tupleIndex = 0;
            }
        }
    }

    @Override
    public ITupleReference getTuple() {
        return resultTuple;
    }

    @Override
    public void reset() {
        currentBufferIndex = 0;
        tupleIndex = 0;
        invIndexSearcher.reset();
        resultBuffers = invIndexSearcher.getResultBuffers();
        numResultBuffers = invIndexSearcher.getNumValidResultBuffers();
    }

    @Override
    public void close() throws HyracksDataException {
        currentBufferIndex = 0;
        tupleIndex = 0;
        resultBuffers = null;
        numResultBuffers = 0;
    }
}
