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

package org.apache.hyracks.storage.am.lsm.invertedindex.dataflow;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IToken;

public class BinaryTokenizerOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private final IHyracksTaskContext ctx;
    private final IBinaryTokenizer tokenizer;
    private final int docField;
    private final int[] keyFields;
    private final boolean addNumTokensKey;
    private final boolean writeKeyFieldsFirst;
    private final RecordDescriptor inputRecDesc;
    private final RecordDescriptor outputRecDesc;

    private FrameTupleAccessor accessor;
    private ArrayTupleBuilder builder;
    private GrowableArray builderData;
    private FrameTupleAppender appender;

    public BinaryTokenizerOperatorNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecDesc,
            RecordDescriptor outputRecDesc, IBinaryTokenizer tokenizer, int docField, int[] keyFields,
            boolean addNumTokensKey, boolean writeKeyFieldsFirst) {
        this.ctx = ctx;
        this.tokenizer = tokenizer;
        this.docField = docField;
        this.keyFields = keyFields;
        this.addNumTokensKey = addNumTokensKey;
        this.inputRecDesc = inputRecDesc;
        this.outputRecDesc = outputRecDesc;
        this.writeKeyFieldsFirst = writeKeyFieldsFirst;
    }

    @Override
    public void open() throws HyracksDataException {
        accessor = new FrameTupleAccessor(inputRecDesc);
        builder = new ArrayTupleBuilder(outputRecDesc.getFieldCount());
        builderData = builder.getFieldData();
        appender = new FrameTupleAppender(new VSizeFrame(ctx), true);
        writer.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();

        for (int i = 0; i < tupleCount; i++) {
            short numTokens = 0;

            tokenizer.reset(
                    accessor.getBuffer().array(),
                    accessor.getTupleStartOffset(i) + accessor.getFieldSlotsLength()
                            + accessor.getFieldStartOffset(i, docField), accessor.getFieldLength(i, docField));

            if (addNumTokensKey) {
                // Get the total number of tokens.
                numTokens = tokenizer.getTokensCount();
            }

            // Write token and data into frame by following the order specified
            // in the writeKeyFieldsFirst field.
            while (tokenizer.hasNext()) {

                tokenizer.next();

                builder.reset();

                // Writing Order: token, number of token, keyfield1 ... n
                if (!writeKeyFieldsFirst) {
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

                }
                // Writing Order: keyfield1 ... n, token, number of token
                else {

                    for (int k = 0; k < keyFields.length; k++) {
                        builder.addField(accessor, i, keyFields[k]);
                    }

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

                }

                FrameUtils.appendToWriter(writer, appender, builder.getFieldEndOffsets(), builder.getByteArray(), 0,
                        builder.getSize());

            }

        }

    }

    @Override
    public void close() throws HyracksDataException {
        appender.flush(writer, true);
        writer.close();
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}
