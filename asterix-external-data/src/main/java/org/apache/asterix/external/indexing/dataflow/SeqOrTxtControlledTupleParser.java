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
package org.apache.asterix.external.indexing.dataflow;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.external.indexing.input.ILookupReader;
import org.apache.asterix.metadata.external.IndexingConstants;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public class SeqOrTxtControlledTupleParser implements IControlledTupleParser {

    private ArrayTupleBuilder tb;
    private transient DataOutput dos;
    private final FrameTupleAppender appender;
    private boolean propagateInput;
    private int[] propagatedFields;
    private FrameTupleReference frameTuple;
    private IAsterixHDFSRecordParser parser;
    private ILookupReader reader;
    private int[] ridFields;
    private RecordDescriptor inRecDesc;
    private FrameTupleAccessor tupleAccessor;
    private ByteBufferInputStream bbis;
    private DataInputStream dis;
    private boolean retainNull;
    protected byte nullByte;
    protected ArrayTupleBuilder nullTupleBuild;

    public SeqOrTxtControlledTupleParser(IHyracksTaskContext ctx, IAsterixHDFSRecordParser parser,
            ILookupReader reader, boolean propagateInput, int[] propagatedFields, RecordDescriptor inRecDesc,
            int[] ridFields, boolean retainNull, INullWriterFactory iNullWriterFactory) throws HyracksDataException {
        appender = new FrameTupleAppender(new VSizeFrame(ctx));
        this.parser = parser;
        this.reader = reader;
        this.propagateInput = propagateInput;
        this.ridFields = ridFields;
        this.retainNull = retainNull;
        if (propagateInput) {
            tb = new ArrayTupleBuilder(propagatedFields.length + 1);
            frameTuple = new FrameTupleReference();
            this.propagatedFields = propagatedFields;
        } else {
            tb = new ArrayTupleBuilder(1);
        }
        dos = tb.getDataOutput();
        this.tupleAccessor = new FrameTupleAccessor(inRecDesc);
        bbis = new ByteBufferInputStream();
        dis = new DataInputStream(bbis);
        nullByte = ATypeTag.NULL.serialize();
        if (retainNull) {
            INullWriter nullWriter = iNullWriterFactory.createNullWriter();
            nullTupleBuild = new ArrayTupleBuilder(1);
            DataOutput out = nullTupleBuild.getDataOutput();
            try {
                nullWriter.writeNull(out);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            nullTupleBuild = null;
        }
    }

    @Override
    public void close(IFrameWriter writer) throws Exception {
        try {
            reader.close();
            appender.flush(writer, true);
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    @Override
    public void parseNext(IFrameWriter writer, ByteBuffer frameBuffer) throws HyracksDataException {
        try {
            int tupleCount = 0;
            int tupleIndex = 0;
            Object record;
            tupleAccessor.reset(frameBuffer);
            tupleCount = tupleAccessor.getTupleCount();
            int fieldSlotsLength = tupleAccessor.getFieldSlotsLength();
            // Loop over incoming tuples
            while (tupleIndex < tupleCount) {
                int tupleStartOffset = tupleAccessor.getTupleStartOffset(tupleIndex) + fieldSlotsLength;
                int fileNumberStartOffset = tupleAccessor.getFieldStartOffset(tupleIndex,
                        ridFields[IndexingConstants.FILE_NUMBER_FIELD_INDEX]);
                // Check if null <- for outer join ->
                if (frameBuffer.get(tupleStartOffset + fileNumberStartOffset) == nullByte) {
                    record = null;
                } else {
                    // Get file number
                    bbis.setByteBuffer(frameBuffer, tupleStartOffset + fileNumberStartOffset);
                    int fileNumber = ((AInt32) inRecDesc.getFields()[ridFields[IndexingConstants.FILE_NUMBER_FIELD_INDEX]]
                            .deserialize(dis)).getIntegerValue();
                    // Get record offset
                    bbis.setByteBuffer(
                            frameBuffer,
                            tupleStartOffset
                                    + tupleAccessor.getFieldStartOffset(tupleIndex,
                                            ridFields[IndexingConstants.RECORD_OFFSET_FIELD_INDEX]));
                    long recordOffset = ((AInt64) inRecDesc.getFields()[ridFields[IndexingConstants.RECORD_OFFSET_FIELD_INDEX]]
                            .deserialize(dis)).getLongValue();
                    // Read the record
                    record = reader.read(fileNumber, recordOffset);
                }
                if (record != null) {
                    tb.reset();
                    if (propagateInput) {
                        frameTuple.reset(tupleAccessor, tupleIndex);
                        for (int i = 0; i < propagatedFields.length; i++) {
                            dos.write(frameTuple.getFieldData(propagatedFields[i]),
                                    frameTuple.getFieldStart(propagatedFields[i]),
                                    frameTuple.getFieldLength(propagatedFields[i]));
                            tb.addFieldEndOffset();
                        }
                    }
                    // parse it
                    parser.parse(record, tb.getDataOutput());
                    tb.addFieldEndOffset();
                    addTupleToFrame(writer);
                } else if (propagateInput && retainNull) {
                    tb.reset();
                    frameTuple.reset(tupleAccessor, tupleIndex);
                    for (int i = 0; i < propagatedFields.length; i++) {
                        dos.write(frameTuple.getFieldData(propagatedFields[i]),
                                frameTuple.getFieldStart(propagatedFields[i]),
                                frameTuple.getFieldLength(propagatedFields[i]));
                        tb.addFieldEndOffset();
                    }
                    dos.write(nullTupleBuild.getByteArray());
                    tb.addFieldEndOffset();
                    addTupleToFrame(writer);
                }
                tupleIndex++;
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                reader.close();
            } catch (Exception e2) {
                e.addSuppressed(e2);
            }
            throw new HyracksDataException(e);
        }
    }

    private void addTupleToFrame(IFrameWriter writer) throws HyracksDataException {
        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            appender.flush(writer, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new IllegalStateException();
            }
        }
    }

}
