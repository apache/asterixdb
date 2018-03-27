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
package org.apache.hyracks.storage.am.lsm.common.dataflow;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexInsertUpdateDeleteOperatorNodePushable;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFrameWriter;

public class LSMIndexInsertUpdateDeleteOperatorNodePushable extends IndexInsertUpdateDeleteOperatorNodePushable
        implements ILSMIndexFrameWriter {

    protected FrameTupleAppender appender;

    @Override
    public void open() throws HyracksDataException {
        super.open();
        appender = new FrameTupleAppender(writeBuffer);
    }

    public LSMIndexInsertUpdateDeleteOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            IIndexDataflowHelperFactory indexHelperFactory, int[] fieldPermutation, RecordDescriptor inputRecDesc,
            IndexOperation op, IModificationOperationCallbackFactory modCallbackFactory,
            ITupleFilterFactory tupleFilterFactory) throws HyracksDataException {
        super(ctx, partition, indexHelperFactory, fieldPermutation, inputRecDesc, op, modCallbackFactory,
                tupleFilterFactory);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        ILSMIndexAccessor lsmAccessor = (ILSMIndexAccessor) indexAccessor;
        int nextFlushTupleIndex = 0;
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
                        if (!lsmAccessor.tryInsert(tuple)) {
                            flushPartialFrame(nextFlushTupleIndex, i);
                            nextFlushTupleIndex = i;
                            lsmAccessor.insert(tuple);
                        }
                        break;
                    }
                    case DELETE: {
                        if (!lsmAccessor.tryDelete(tuple)) {
                            flushPartialFrame(nextFlushTupleIndex, i);
                            nextFlushTupleIndex = i;
                            lsmAccessor.delete(tuple);
                        }
                        break;
                    }
                    case UPSERT: {
                        if (!lsmAccessor.tryUpsert(tuple)) {
                            flushPartialFrame(nextFlushTupleIndex, i);
                            nextFlushTupleIndex = i;
                            lsmAccessor.upsert(tuple);
                        }
                        break;
                    }
                    case UPDATE: {
                        if (!lsmAccessor.tryUpdate(tuple)) {
                            flushPartialFrame(nextFlushTupleIndex, i);
                            nextFlushTupleIndex = i;
                            lsmAccessor.update(tuple);
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
        if (nextFlushTupleIndex == 0) {
            // No partial flushing was necessary. Forward entire frame.
            writeBuffer.ensureFrameSize(buffer.capacity());
            FrameUtils.copyAndFlip(buffer, writeBuffer.getBuffer());
            FrameUtils.flushFrame(writeBuffer.getBuffer(), writer);
        } else {
            // Flush remaining partial frame.
            flushPartialFrame(nextFlushTupleIndex, tupleCount);
        }
    }

    @Override
    public void flushPartialFrame(int startTupleIndex, int endTupleIndex) throws HyracksDataException {
        for (int i = startTupleIndex; i < endTupleIndex; i++) {
            FrameUtils.appendToWriter(writer, appender, accessor, i);
        }
        appender.write(writer, true);
    }
}
