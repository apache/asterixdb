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
package edu.uci.ics.hyracks.dataflow.std.util;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.SerializingDataWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.IOpenableDataWriterOperator;

public final class DeserializedOperatorNodePushable extends AbstractUnaryInputOperatorNodePushable {
    private final IHyracksTaskContext ctx;

    private final IOpenableDataWriterOperator delegate;

    private final FrameDeserializer deserializer;

    public DeserializedOperatorNodePushable(IHyracksTaskContext ctx, IOpenableDataWriterOperator delegate,
            RecordDescriptor inRecordDesc) {
        this.ctx = ctx;
        this.delegate = delegate;
        deserializer = inRecordDesc == null ? null : new FrameDeserializer(ctx.getFrameSize(), inRecordDesc);
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
            throws HyracksDataException {
        delegate.setDataWriter(index, new SerializingDataWriter(ctx, recordDesc, writer));
    }

    @Override
    public void close() throws HyracksDataException {
        delegate.close();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        deserializer.reset(buffer);
        while (!deserializer.done()) {
            delegate.writeData(deserializer.deserializeRecord());
        }
    }

    @Override
    public void open() throws HyracksDataException {
        delegate.open();
    }

    @Override
    public void fail() throws HyracksDataException {
        delegate.fail();
    }

    @Override
    public String getDisplayName() {
        return "Deserialized(" + delegate + ")";
    }
}