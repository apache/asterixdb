/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.invertedindex.dataflow;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IToken;

public class BinaryTokenizerOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private final IHyracksTaskContext ctx;
    private final IBinaryTokenizer tokenizer;
    private final int[] tokenFields;
    private final int[] projFields;
    private final RecordDescriptor inputRecDesc;
    private final RecordDescriptor outputRecDesc;

    private FrameTupleAccessor accessor;
    private ArrayTupleBuilder builder;
    private DataOutput builderDos;
    private FrameTupleAppender appender;
    private ByteBuffer writeBuffer;

    public BinaryTokenizerOperatorNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecDesc,
            RecordDescriptor outputRecDesc, IBinaryTokenizer tokenizer, int[] tokenFields, int[] projFields) {
        this.ctx = ctx;
        this.tokenizer = tokenizer;
        this.tokenFields = tokenFields;
        this.projFields = projFields;
        this.inputRecDesc = inputRecDesc;
        this.outputRecDesc = outputRecDesc;
    }

    @Override
    public void open() throws HyracksDataException {
        accessor = new FrameTupleAccessor(ctx.getFrameSize(), inputRecDesc);
        writeBuffer = ctx.allocateFrame();
        builder = new ArrayTupleBuilder(outputRecDesc.getFields().length);
        builderDos = builder.getDataOutput();
        appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(writeBuffer, true);
        writer.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);

        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {

            for (int j = 0; j < tokenFields.length; j++) {

                tokenizer.reset(
                        accessor.getBuffer().array(),
                        accessor.getTupleStartOffset(i) + accessor.getFieldSlotsLength()
                                + accessor.getFieldStartOffset(i, tokenFields[j]),
                        accessor.getFieldLength(i, tokenFields[j]));

                while (tokenizer.hasNext()) {
                    tokenizer.next();

                    builder.reset();
                    try {
                        IToken token = tokenizer.getToken();
                        token.serializeToken(builderDos);
                        builder.addFieldEndOffset();
                    } catch (IOException e) {
                        throw new HyracksDataException(e.getMessage());
                    }

                    for (int k = 0; k < projFields.length; k++) {
                        builder.addField(accessor, i, projFields[k]);
                    }

                    if (!appender.append(builder.getFieldEndOffsets(), builder.getByteArray(), 0, builder.getSize())) {
                        FrameUtils.flushFrame(writeBuffer, writer);
                        appender.reset(writeBuffer, true);
                        if (!appender
                                .append(builder.getFieldEndOffsets(), builder.getByteArray(), 0, builder.getSize())) {
                            throw new IllegalStateException();
                        }
                    }
                }
            }
        }

        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(writeBuffer, writer);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}
