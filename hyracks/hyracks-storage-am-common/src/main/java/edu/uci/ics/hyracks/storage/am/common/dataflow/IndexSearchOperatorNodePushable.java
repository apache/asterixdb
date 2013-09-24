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

import java.io.DataOutput;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;

public abstract class IndexSearchOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    protected final IIndexOperatorDescriptor opDesc;
    protected final IHyracksTaskContext ctx;
    protected final IIndexDataflowHelper indexHelper;
    protected FrameTupleAccessor accessor;

    protected ByteBuffer writeBuffer;
    protected FrameTupleAppender appender;
    protected ArrayTupleBuilder tb;
    protected DataOutput dos;

    protected IIndex index;
    protected ISearchPredicate searchPred;
    protected IIndexCursor cursor;
    protected IIndexAccessor indexAccessor;

    protected final RecordDescriptor inputRecDesc;
    protected final boolean retainInput;
    protected FrameTupleReference frameTuple;

    public IndexSearchOperatorNodePushable(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            IRecordDescriptorProvider recordDescProvider) {
        this.opDesc = opDesc;
        this.ctx = ctx;
        this.indexHelper = opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(opDesc, ctx, partition);
        this.retainInput = opDesc.getRetainInput();
        this.inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
    }

    protected abstract ISearchPredicate createSearchPredicate();

    protected abstract void resetSearchPredicate(int tupleIndex);

    protected IIndexCursor createCursor() {
        return indexAccessor.createSearchCursor();
    }

    @Override
    public void open() throws HyracksDataException {
        accessor = new FrameTupleAccessor(ctx.getFrameSize(), inputRecDesc);
        writer.open();
        indexHelper.open();
        index = indexHelper.getIndexInstance();
        try {
            searchPred = createSearchPredicate();
            writeBuffer = ctx.allocateFrame();
            tb = new ArrayTupleBuilder(recordDesc.getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender(ctx.getFrameSize());
            appender.reset(writeBuffer, true);
            ISearchOperationCallback searchCallback = opDesc.getSearchOpCallbackFactory()
                    .createSearchOperationCallback(indexHelper.getResourceID(), ctx);
            indexAccessor = index.createAccessor(NoOpOperationCallback.INSTANCE, searchCallback);
            cursor = createCursor();
            if (retainInput) {
                frameTuple = new FrameTupleReference();
            }
        } catch (Exception e) {
            indexHelper.close();
            throw new HyracksDataException(e);
        }
    }

    protected void writeSearchResults(int tupleIndex) throws Exception {
        while (cursor.hasNext()) {
            tb.reset();
            cursor.next();
            if (retainInput) {
                frameTuple.reset(accessor, tupleIndex);
                for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                    dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    tb.addFieldEndOffset();
                }
            }
            ITupleReference tuple = cursor.getTuple();
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
                tb.addFieldEndOffset();
            }
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                FrameUtils.flushFrame(writeBuffer, writer);
                appender.reset(writeBuffer, true);
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    throw new HyracksDataException("Record size (" + tb.getSize() + ") larger than frame size ("
                            + appender.getBuffer().capacity() + ")");
                }
            }
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount; i++) {
                resetSearchPredicate(i);
                cursor.reset();
                indexAccessor.search(cursor, searchPred);
                writeSearchResults(i);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(writeBuffer, writer);
            }
            writer.close();
            try {
                cursor.close();
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        } finally {
            indexHelper.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}
