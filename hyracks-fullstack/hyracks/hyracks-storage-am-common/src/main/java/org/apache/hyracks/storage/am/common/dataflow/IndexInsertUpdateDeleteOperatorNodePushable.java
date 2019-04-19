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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.LocalResource;

public class IndexInsertUpdateDeleteOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    protected final IHyracksTaskContext ctx;
    protected final IIndexDataflowHelper indexHelper;
    protected final RecordDescriptor inputRecDesc;
    protected final IndexOperation op;
    protected final PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();
    protected FrameTupleAccessor accessor;
    protected FrameTupleReference frameTuple;
    protected IFrame writeBuffer;
    protected IIndexAccessor indexAccessor;
    protected ITupleFilter tupleFilter;
    protected IModificationOperationCallback modCallback;
    protected IIndex index;
    protected final IModificationOperationCallbackFactory modOpCallbackFactory;
    protected final ITupleFilterFactory tupleFilterFactory;

    public IndexInsertUpdateDeleteOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            IIndexDataflowHelperFactory indexHelperFactory, int[] fieldPermutation, RecordDescriptor inputRecDesc,
            IndexOperation op, IModificationOperationCallbackFactory modOpCallbackFactory,
            ITupleFilterFactory tupleFilterFactory) throws HyracksDataException {
        this.ctx = ctx;
        this.indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
        this.modOpCallbackFactory = modOpCallbackFactory;
        this.tupleFilterFactory = tupleFilterFactory;
        this.inputRecDesc = inputRecDesc;
        this.op = op;
        this.tuple.setFieldPermutation(fieldPermutation);
    }

    @Override
    public void open() throws HyracksDataException {
        accessor = new FrameTupleAccessor(inputRecDesc);
        writeBuffer = new VSizeFrame(ctx);
        indexHelper.open();
        index = indexHelper.getIndexInstance();
        try {
            writer.open();
            LocalResource resource = indexHelper.getResource();
            modCallback = modOpCallbackFactory.createModificationOperationCallback(resource, ctx, this);
            IIndexAccessParameters iap = new IndexAccessParameters(modCallback, NoOpOperationCallback.INSTANCE);
            indexAccessor = index.createAccessor(iap);
            if (tupleFilterFactory != null) {
                tupleFilter = tupleFilterFactory.createTupleFilter(ctx);
                frameTuple = new FrameTupleReference();
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            try {
                if (tupleFilter != null) {
                    frameTuple.reset(accessor, i);
                    if (!tupleFilter.accept(frameTuple)) {
                        continue;
                    }
                }
                tuple.reset(accessor, i);

                switch (op) {
                    case INSERT: {
                        try {
                            indexAccessor.insert(tuple);
                        } catch (HyracksDataException e) {
                            // ignore that exception to allow inserting existing keys which becomes an NoOp
                            if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                                throw e;
                            }
                        }
                        break;
                    }
                    case UPDATE: {
                        indexAccessor.update(tuple);
                        break;
                    }
                    case UPSERT: {
                        indexAccessor.upsert(tuple);
                        break;
                    }
                    case DELETE: {
                        try {
                            indexAccessor.delete(tuple);
                        } catch (HyracksDataException e) {
                            // ingnore that exception to allow deletions of non-existing keys
                            if (e.getErrorCode() != ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                                throw e;
                            }
                        }
                        break;
                    }
                    default: {
                        throw new HyracksDataException(
                                "Unsupported operation " + op + " in tree index InsertUpdateDelete operator");
                    }
                }
            } catch (HyracksDataException e) {
                throw e;
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }
        // Pass a copy of the frame to next op.
        writeBuffer.ensureFrameSize(buffer.capacity());
        FrameUtils.copyAndFlip(buffer, writeBuffer.getBuffer());
        FrameUtils.flushFrame(writeBuffer.getBuffer(), writer);
    }

    @Override
    public void close() throws HyracksDataException {
        if (index != null) {
            try {
                writer.close();
            } finally {
                indexHelper.close();
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        if (index != null) {
            writer.fail();
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }
}
