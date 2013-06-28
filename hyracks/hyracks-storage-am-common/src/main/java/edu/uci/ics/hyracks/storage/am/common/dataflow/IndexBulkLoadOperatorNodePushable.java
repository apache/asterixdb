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
package edu.uci.ics.hyracks.storage.am.common.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;

public class IndexBulkLoadOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
    private final IIndexOperatorDescriptor opDesc;
    private final IHyracksTaskContext ctx;
    private final float fillFactor;
    private final boolean verifyInput;
    private final long numElementsHint;
    private final boolean checkIfEmptyIndex;
    private final IIndexDataflowHelper indexHelper;
    private FrameTupleAccessor accessor;
    private IIndex index;
    private IIndexBulkLoader bulkLoader;
    private IRecordDescriptorProvider recDescProvider;
    private PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();

    public IndexBulkLoadOperatorNodePushable(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            int[] fieldPermutation, float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, IRecordDescriptorProvider recordDescProvider) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.indexHelper = opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(opDesc, ctx, partition);
        this.fillFactor = fillFactor;
        this.verifyInput = verifyInput;
        this.numElementsHint = numElementsHint;
        this.checkIfEmptyIndex = checkIfEmptyIndex;
        this.recDescProvider = recordDescProvider;
        tuple.setFieldPermutation(fieldPermutation);
    }

    @Override
    public void open() throws HyracksDataException {
        RecordDescriptor recDesc = recDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
        indexHelper.open();
        index = indexHelper.getIndexInstance();
        try {
            bulkLoader = index.createBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex);
        } catch (Exception e) {
            indexHelper.close();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            tuple.reset(accessor, i);
            try {
                bulkLoader.add(tuple);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            bulkLoader.end();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            indexHelper.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
    }
}