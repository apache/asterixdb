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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.GrowableArray;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;

public class BinaryTokenizerOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private final IHyracksTaskContext ctx;
    private final IBinaryTokenizer tokenizer;
    private final int docField;
    private final int[] keyFields;
    private final boolean addNumTokensKey;
    private final RecordDescriptor inputRecDesc;
    private final RecordDescriptor outputRecDesc;

    private FrameTupleAccessor accessor;
    private ArrayTupleBuilder builder;
    private GrowableArray builderData;
    private FrameTupleAppender appender;
    private ByteBuffer writeBuffer;

    public BinaryTokenizerOperatorNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecDesc,
            RecordDescriptor outputRecDesc, IBinaryTokenizer tokenizer, int docField, int[] keyFields,
            boolean addNumTokensKey) {
        this.ctx = ctx;
        this.tokenizer = tokenizer;
        this.docField = docField;
        this.keyFields = keyFields;
        this.addNumTokensKey = addNumTokensKey;
        this.inputRecDesc = inputRecDesc;
        this.outputRecDesc = outputRecDesc;
    }

    @Override
    public void open() throws HyracksDataException {
        accessor = new FrameTupleAccessor(ctx.getFrameSize(), inputRecDesc);
        writeBuffer = ctx.allocateFrame();
        builder = new ArrayTupleBuilder(outputRecDesc.getFieldCount());
        builderData = builder.getFieldData();
        appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(writeBuffer, true);
        writer.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            short numTokens = 0;
            if (addNumTokensKey) {
                // Run through the tokens to get the total number of tokens.
                tokenizer.reset(
                        accessor.getBuffer().array(),
                        accessor.getTupleStartOffset(i) + accessor.getFieldSlotsLength()
                                + accessor.getFieldStartOffset(i, docField), accessor.getFieldLength(i, docField));
                while (tokenizer.hasNext()) {
                    tokenizer.next();
                    numTokens++;
                }
            }

            tokenizer.reset(
                    accessor.getBuffer().array(),
                    accessor.getTupleStartOffset(i) + accessor.getFieldSlotsLength()
                            + accessor.getFieldStartOffset(i, docField), accessor.getFieldLength(i, docField));
            while (tokenizer.hasNext()) {
                tokenizer.next();

                builder.reset();
                try {
                    IToken token = tokenizer.getToken();
                    token.serializeToken(builderData);
                    builder.addFieldEndOffset();
                    // Add number of tokens if requested.
                    if (addNumTokensKey) {
                        builder.getDataOutput().writeShort(numTokens);
                        builder.addFieldEndOffset();
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(e.getMessage());
                }

                for (int k = 0; k < keyFields.length; k++) {
                    builder.addField(accessor, i, keyFields[k]);
                }

                if (!appender.append(builder.getFieldEndOffsets(), builder.getByteArray(), 0, builder.getSize())) {
                    FrameUtils.flushFrame(writeBuffer, writer);
                    appender.reset(writeBuffer, true);
                    if (!appender.append(builder.getFieldEndOffsets(), builder.getByteArray(), 0, builder.getSize())) {
                        throw new HyracksDataException("Record size (" + builder.getSize() +") larger than frame size (" + appender.getBuffer().capacity() + ")");
                    }
                }
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(writeBuffer, writer);
        }
        writer.close();
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}
