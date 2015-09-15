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
package org.apache.asterix.metadata.declared;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class FieldExtractingAdapter implements IDatasourceAdapter {

    private static final long serialVersionUID = 1L;

    private final RecordDescriptor inRecDesc;

    private final RecordDescriptor outRecDesc;

    private final IDatasourceAdapter wrappedAdapter;

    private final FieldExtractingPushRuntime fefw;

    public FieldExtractingAdapter(IHyracksTaskContext ctx, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc,
            int[][] extractFields, ARecordType rType, IDatasourceAdapter wrappedAdapter) {
        this.inRecDesc = inRecDesc;
        this.outRecDesc = outRecDesc;
        this.wrappedAdapter = wrappedAdapter;
        fefw = new FieldExtractingPushRuntime(ctx, extractFields, rType);
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        fefw.setInputRecordDescriptor(0, inRecDesc);
        fefw.setFrameWriter(0, writer, outRecDesc);
        fefw.open();
        try {
            wrappedAdapter.start(partition, fefw);
        } catch (Throwable t) {
            fefw.fail();
            throw t;
        } finally {
            fefw.close();
        }
    }

    private static class FieldExtractingPushRuntime extends AbstractOneInputOneOutputOneFramePushRuntime {

        private final IHyracksTaskContext ctx;

        private final int[][] extractFields;

        private final ARecordType rType;

        private final int nullBitmapSize;

        private final ArrayTupleBuilder tb;

        public FieldExtractingPushRuntime(IHyracksTaskContext ctx, int[][] extractFields, ARecordType rType) {
            this.ctx = ctx;
            this.extractFields = extractFields;
            this.rType = rType;
            nullBitmapSize = ARecordType.computeNullBitmapSize(rType);
            tb = new ArrayTupleBuilder(extractFields.length + 1);
        }

        @Override
        public void open() throws HyracksDataException {
            initAccessAppendRef(ctx);
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            tAccess.reset(buffer);
            for (int i = 0; i < tAccess.getTupleCount(); ++i) {
                tb.reset();
                tRef.reset(tAccess, i);
                byte[] record = tRef.getFieldData(0);
                int recStart = tRef.getFieldStart(0);
                int recLength = tRef.getFieldLength(0);
                for (int f = 0; f < extractFields.length; ++f) {
                    try {
                        byte[] subRecord = record;
                        int subFStart = recStart;
                        int subFOffset = 0;
                        boolean isNull = false;
                        IAType subFType = rType;
                        int subFLen = recLength;
                        int subBitMapSize = nullBitmapSize;
                        byte[] subRecordTmp;

                        for (int j = 0; j < extractFields[f].length; j++) {
                            //Get offset for subfield
                            subFOffset = ARecordSerializerDeserializer.getFieldOffsetById(subRecord, subFStart,
                                    extractFields[f][j], subBitMapSize, ((ARecordType) subFType).isOpen());
                            if (subFOffset == 0) {
                                tb.getDataOutput().write(ATypeTag.NULL.serialize());
                                isNull = true;
                                break;
                            } else {
                                //Get type of subfield
                                subFType = ((ARecordType) subFType).getFieldTypes()[extractFields[f][j]];
                                try {
                                    //Get length of subfield
                                    subFLen = NonTaggedFormatUtil.getFieldValueLength(subRecord,
                                            subFStart + subFOffset, subFType.getTypeTag(), false);

                                    if (j < extractFields[f].length - 1) {
                                        subRecordTmp = new byte[subFLen + 1];
                                        subRecordTmp[0] = subFType.getTypeTag().serialize();
                                        System.arraycopy(subRecord, subFStart + subFOffset, subRecordTmp, 1, subFLen);
                                        subRecord = subRecordTmp;
                                        subFStart = 0;
                                        subBitMapSize = ARecordType.computeNullBitmapSize((ARecordType) subFType);
                                    }

                                } catch (AsterixException e) {
                                    throw new HyracksDataException(e);
                                }
                            }
                        }

                        if (!isNull) {
                            tb.getDataOutput().write(subFType.getTypeTag().serialize());
                            tb.getDataOutput().write(subRecord, subFStart + subFOffset, subFLen);
                        }

                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    tb.addFieldEndOffset();
                }
                tb.addField(record, recStart, tRef.getFieldLength(0));
                appendToFrameFromTupleBuilder(tb);
            }
        }

        @Override
        public void close() throws HyracksDataException {
            flushIfNotFailed();
        }
    }

}
