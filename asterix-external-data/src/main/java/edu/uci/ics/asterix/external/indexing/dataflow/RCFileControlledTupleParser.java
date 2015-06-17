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
package edu.uci.ics.asterix.external.indexing.dataflow;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.asterix.external.indexing.input.RCFileLookupReader;
import edu.uci.ics.asterix.metadata.external.IndexingConstants;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public class RCFileControlledTupleParser implements IControlledTupleParser {

    private ArrayTupleBuilder tb;
    private transient DataOutput dos;
    private final FrameTupleAppender appender;
    private boolean propagateInput;
    private int[] propagatedFields;
    private FrameTupleReference frameTuple;
    private IAsterixHDFSRecordParser parser;
    private RCFileLookupReader reader;
    private int[] ridFields;
    private RecordDescriptor inRecDesc;
    private FrameTupleAccessor tupleAccessor;
    private ByteBufferInputStream bbis;
    private DataInputStream dis;
    private boolean retainNull;
    protected byte nullByte;
    protected ArrayTupleBuilder nullTupleBuild;

    public RCFileControlledTupleParser(IHyracksTaskContext ctx, IAsterixHDFSRecordParser parser,
            RCFileLookupReader reader, boolean propagateInput, int[] propagatedFields, RecordDescriptor inRecDesc,
            int[] ridFields, boolean retainNull, INullWriterFactory iNullWriterFactory) throws HyracksDataException {
        appender = new FrameTupleAppender(new VSizeFrame(ctx));
        this.parser = parser;
        this.reader = reader;
        this.propagateInput = propagateInput;
        this.propagatedFields = propagatedFields;
        this.retainNull = retainNull;
        this.inRecDesc = inRecDesc;
        this.ridFields = ridFields;
        this.tupleAccessor = new FrameTupleAccessor(inRecDesc);
        if (propagateInput) {
            tb = new ArrayTupleBuilder(propagatedFields.length + 1);
        } else {
            tb = new ArrayTupleBuilder(1);
        }
        frameTuple = new FrameTupleReference();
        dos = tb.getDataOutput();
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
            Object object;
            tupleAccessor.reset(frameBuffer);
            tupleCount = tupleAccessor.getTupleCount();
            int fieldSlotsLength = tupleAccessor.getFieldSlotsLength();
            // Loop over tuples
            while (tupleIndex < tupleCount) {
                int tupleStartOffset = tupleAccessor.getTupleStartOffset(tupleIndex) + fieldSlotsLength;
                int fileNumberStartOffset = tupleAccessor.getFieldStartOffset(tupleIndex,
                        ridFields[IndexingConstants.FILE_NUMBER_FIELD_INDEX]);
                // Check if null <- for outer join ->
                if (frameBuffer.get(tupleStartOffset + fileNumberStartOffset) == nullByte) {
                    object = null;
                } else {
                    // Get file number
                    bbis.setByteBuffer(frameBuffer, tupleStartOffset + fileNumberStartOffset);
                    int fileNumber = ((AInt32) inRecDesc
                            .getFields()[ridFields[IndexingConstants.FILE_NUMBER_FIELD_INDEX]]
                            .deserialize(dis)).getIntegerValue();
                    // Get record group offset
                    bbis.setByteBuffer(
                            frameBuffer,
                            tupleStartOffset
                                    + tupleAccessor.getFieldStartOffset(tupleIndex,
                                    ridFields[IndexingConstants.RECORD_OFFSET_FIELD_INDEX]));
                    long recordOffset = ((AInt64) inRecDesc
                            .getFields()[ridFields[IndexingConstants.RECORD_OFFSET_FIELD_INDEX]]
                            .deserialize(dis)).getLongValue();
                    // Get row number
                    bbis.setByteBuffer(
                            frameBuffer,
                            tupleStartOffset
                                    + tupleAccessor.getFieldStartOffset(tupleIndex,
                                    ridFields[IndexingConstants.ROW_NUMBER_FIELD_INDEX]));
                    int rowNumber = ((AInt32) inRecDesc.getFields()[ridFields[IndexingConstants.ROW_NUMBER_FIELD_INDEX]]
                            .deserialize(dis)).getIntegerValue();

                    // Read record from external source
                    object = reader.read(fileNumber, recordOffset, rowNumber);
                }
                if (object != null) {
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
                    // parse record
                    parser.parse(object, tb.getDataOutput());
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
            // Something went wrong, try to close the reader and then throw an exception <-this should never happen->
            try {
                reader.close();
            } catch (Exception e1) {
                e.addSuppressed(e1);
            }
            throw new HyracksDataException(e);
        }
    }

    protected void addTupleToFrame(IFrameWriter writer) throws HyracksDataException {
        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            appender.flush(writer, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new IllegalStateException();
            }
        }
    }

}
