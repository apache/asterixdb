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
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndex;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;

public class IndexBulkLoadOperatorNodePushable extends
        AbstractUnaryInputUnaryOutputOperatorNodePushable {
    protected final IIndexOperatorDescriptor opDesc;
    protected final IHyracksTaskContext ctx;
    protected final float fillFactor;
    protected final boolean verifyInput;
    protected final long numElementsHint;
    protected final boolean checkIfEmptyIndex;
    protected final IIndexDataflowHelper indexHelper;
    protected FrameTupleAccessor accessor;
    protected IIndex index;
    protected IIndexBulkLoader bulkLoader;
    protected IRecordDescriptorProvider recDescProvider;
    protected PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();

    public IndexBulkLoadOperatorNodePushable(IIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, int[] fieldPermutation,
            float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex,
            IRecordDescriptorProvider recordDescProvider) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.indexHelper = opDesc.getIndexDataflowHelperFactory()
                .createIndexDataflowHelper(opDesc, ctx, partition);
        this.fillFactor = fillFactor;
        this.verifyInput = verifyInput;
        this.numElementsHint = numElementsHint;
        this.checkIfEmptyIndex = checkIfEmptyIndex;
        this.recDescProvider = recordDescProvider;
        tuple.setFieldPermutation(fieldPermutation);

    }

    @Override
    public void open() throws HyracksDataException {
        RecordDescriptor recDesc = recDescProvider.getInputRecordDescriptor(
                opDesc.getActivityId(), 0);
        accessor = new FrameTupleAccessor(recDesc);
        indexHelper.open();
        index = indexHelper.getIndexInstance();
        try {
            bulkLoader = index.createBulkLoader(fillFactor, verifyInput,
                    numElementsHint, checkIfEmptyIndex);
        } catch (Exception e) {
            indexHelper.close();
            throw new HyracksDataException(e);
        }
        writer.open();
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
        FrameUtils.flushFrame(buffer, writer);

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
        writer.close();
    }

    @Override
    public void fail() throws HyracksDataException {
    }
}