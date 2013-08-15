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
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilter;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilterFactory;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexNonExistentKeyException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;

public class IndexInsertUpdateDeleteOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    protected final IIndexOperatorDescriptor opDesc;
    protected final IHyracksTaskContext ctx;
    protected final IIndexDataflowHelper indexHelper;
    protected final IRecordDescriptorProvider recordDescProvider;
    protected final IndexOperation op;
    protected final PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();
    protected FrameTupleAccessor accessor;
    protected FrameTupleReference frameTuple;
    protected ByteBuffer writeBuffer;
    protected IIndexAccessor indexAccessor;
    protected ITupleFilter tupleFilter;
    protected IModificationOperationCallback modCallback;

    public IndexInsertUpdateDeleteOperatorNodePushable(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, int[] fieldPermutation, IRecordDescriptorProvider recordDescProvider, IndexOperation op) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.indexHelper = opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(opDesc, ctx, partition);
        this.recordDescProvider = recordDescProvider;
        this.op = op;
        tuple.setFieldPermutation(fieldPermutation);
    }

    @Override
    public void open() throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        accessor = new FrameTupleAccessor(ctx.getFrameSize(), inputRecDesc);
        writeBuffer = ctx.allocateFrame();
        writer.open();
        indexHelper.open();
        IIndex index = indexHelper.getIndexInstance();
        try {
            modCallback = opDesc.getModificationOpCallbackFactory().createModificationOperationCallback(
                    indexHelper.getResourceID(), index, ctx);
            indexAccessor = index.createAccessor(modCallback, NoOpOperationCallback.INSTANCE);
            ITupleFilterFactory tupleFilterFactory = opDesc.getTupleFilterFactory();
            if (tupleFilterFactory != null) {
                tupleFilter = tupleFilterFactory.createTupleFilter(indexHelper.getTaskContext());
                frameTuple = new FrameTupleReference();
            }
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
                        } catch (TreeIndexDuplicateKeyException e) {
                            // ingnore that exception to allow inserting existing keys which becomes an NoOp
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
                        } catch (TreeIndexNonExistentKeyException e) {
                            // ingnore that exception to allow deletions of non-existing keys
                        }
                        break;
                    }
                    default: {
                        throw new HyracksDataException("Unsupported operation " + op
                                + " in tree index InsertUpdateDelete operator");
                    }
                }
            } catch (HyracksDataException e) {
                throw e;
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }
        // Pass a copy of the frame to next op.
        System.arraycopy(buffer.array(), 0, writeBuffer.array(), 0, buffer.capacity());
        FrameUtils.flushFrame(writeBuffer, writer);
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            writer.close();
        } finally {
            indexHelper.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}
