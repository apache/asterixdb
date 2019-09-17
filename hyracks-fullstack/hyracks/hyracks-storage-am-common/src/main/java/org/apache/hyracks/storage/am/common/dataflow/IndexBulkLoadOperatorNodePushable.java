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
package org.apache.hyracks.storage.am.common.dataflow;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;

public class IndexBulkLoadOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    protected final IHyracksTaskContext ctx;
    protected final float fillFactor;
    protected final boolean verifyInput;
    protected final long numElementsHint;
    protected final boolean checkIfEmptyIndex;
    protected final IIndexDataflowHelper indexHelper;
    protected final RecordDescriptor recDesc;
    protected final PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();
    protected final ITupleFilterFactory tupleFilterFactory;
    protected FrameTupleAccessor accessor;
    protected IIndex index;
    protected IIndexBulkLoader bulkLoader;
    protected ITupleFilter tupleFilter;
    protected FrameTupleReference frameTuple;

    public IndexBulkLoadOperatorNodePushable(IIndexDataflowHelperFactory indexDataflowHelperFactory,
            IHyracksTaskContext ctx, int partition, int[] fieldPermutation, float fillFactor, boolean verifyInput,
            long numElementsHint, boolean checkIfEmptyIndex, RecordDescriptor recDesc,
            ITupleFilterFactory tupleFilterFactory) throws HyracksDataException {
        this.ctx = ctx;
        this.indexHelper = indexDataflowHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
        this.fillFactor = fillFactor;
        this.verifyInput = verifyInput;
        this.numElementsHint = numElementsHint;
        this.checkIfEmptyIndex = checkIfEmptyIndex;
        this.recDesc = recDesc;
        this.tupleFilterFactory = tupleFilterFactory;
        tuple.setFieldPermutation(fieldPermutation);
    }

    @Override
    public void open() throws HyracksDataException {
        accessor = new FrameTupleAccessor(recDesc);
        indexHelper.open();
        index = indexHelper.getIndexInstance();
        try {
            writer.open();
            if (tupleFilterFactory != null) {
                tupleFilter = tupleFilterFactory.createTupleFilter(ctx);
                frameTuple = new FrameTupleReference();
            }
            initializeBulkLoader();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            if (tupleFilter != null) {
                frameTuple.reset(accessor, i);
                if (!tupleFilter.accept(frameTuple)) {
                    continue;
                }
            }
            tuple.reset(accessor, i);
            bulkLoader.add(tuple);
        }

        FrameUtils.flushFrame(buffer, writer);
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            // bulkloader can be null if an exception is thrown before it is initialized.
            if (bulkLoader != null) {
                bulkLoader.end();
            }
        } catch (Throwable th) {
            throw HyracksDataException.create(th);
        } finally {
            if (index != null) {
                // If index was opened!
                try {
                    indexHelper.close();
                } finally {
                    writer.close();
                }
            }
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }

    @Override
    public void fail() throws HyracksDataException {
        if (index != null) {
            writer.fail();
        }
    }

    protected void initializeBulkLoader() throws HyracksDataException {
        bulkLoader = index.createBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex,
                NoOpPageWriteCallback.INSTANCE);
    }
}