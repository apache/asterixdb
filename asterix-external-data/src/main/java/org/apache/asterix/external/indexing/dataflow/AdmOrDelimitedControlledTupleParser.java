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

import edu.uci.ics.asterix.external.indexing.input.AbstractHDFSLookupInputStream;
import edu.uci.ics.asterix.metadata.external.IndexingConstants;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.operators.file.IDataParser;
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

/**
 * class implementation for IControlledTupleParser. It provides common
 * functionality involved in parsing data in an external text format (adm or delimited text) in a pipelined manner and packing
 * frames with formed tuples.
 */
public class AdmOrDelimitedControlledTupleParser implements IControlledTupleParser {

    private ArrayTupleBuilder tb;
    private transient DataOutput dos;
    private final FrameTupleAppender appender;
    protected final ARecordType recType;
    private IDataParser parser;
    private boolean propagateInput;
    private int[] propagatedFields;
    private int[] ridFields;
    private RecordDescriptor inRecDesc;
    private FrameTupleAccessor tupleAccessor;
    private FrameTupleReference frameTuple;
    private ByteBufferInputStream bbis;
    private DataInputStream dis;
    private AbstractHDFSLookupInputStream in;
    private boolean parserInitialized = false;
    private boolean retainNull;
    protected byte nullByte;
    protected ArrayTupleBuilder nullTupleBuild;

    public AdmOrDelimitedControlledTupleParser(IHyracksTaskContext ctx, ARecordType recType,
            AbstractHDFSLookupInputStream in, boolean propagateInput, RecordDescriptor inRecDesc, IDataParser parser,
            int[] propagatedFields, int[] ridFields, boolean retainNull, INullWriterFactory iNullWriterFactory)
            throws HyracksDataException {
        this.recType = recType;
        this.in = in;
        this.propagateInput = propagateInput;
        this.retainNull = retainNull;
        this.inRecDesc = inRecDesc;
        this.propagatedFields = propagatedFields;
        this.ridFields = ridFields;
        this.parser = parser;
        this.tupleAccessor = new FrameTupleAccessor(inRecDesc);
        appender = new FrameTupleAppender(new VSizeFrame(ctx));
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
            in.close();
            appender.flush(writer, true);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void parseNext(IFrameWriter writer, ByteBuffer frameBuffer) throws HyracksDataException {
        try {
            int tupleCount = 0;
            int tupleIndex = 0;
            tupleAccessor.reset(frameBuffer);
            tupleCount = tupleAccessor.getTupleCount();
            int fieldSlotsLength = tupleAccessor.getFieldSlotsLength();
            // Loop over tuples
            while (tupleIndex < tupleCount) {
                boolean found = false;
                int tupleStartOffset = tupleAccessor.getTupleStartOffset(tupleIndex) + fieldSlotsLength;
                int fileNumberStartOffset = tupleAccessor.getFieldStartOffset(tupleIndex,
                        ridFields[IndexingConstants.FILE_NUMBER_FIELD_INDEX]);
                // Check if null <- for outer join ->
                if (frameBuffer.get(tupleStartOffset + fileNumberStartOffset) == nullByte) {
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
                    found = in.fetchRecord(fileNumber, recordOffset);
                }
                if (found) {
                    // Since we now know the inputStream is ready, we can safely initialize the parser
                    // We can't do that earlier since the parser will start pulling from the stream and if it is not ready,
                    // The parser will automatically release its resources
                    if (!parserInitialized) {
                        parser.initialize(in, recType, true);
                        parserInitialized = true;
                    }
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
                    parser.parse(tb.getDataOutput());
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
            // un expected error, we try to close the inputstream and throw an exception
            try {
                in.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            throw new HyracksDataException(e);
        }
    }

    // For debugging
    public void prettyPrint(FrameTupleAccessor tupleAccessor, RecordDescriptor recDesc) {
        ByteBufferInputStream bbis = new ByteBufferInputStream();
        DataInputStream dis = new DataInputStream(bbis);
        int tc = tupleAccessor.getTupleCount();
        System.err.println("TC: " + tc);
        for (int i = 0; i < tc; ++i) {
            System.err.print(i + ":(" + tupleAccessor.getTupleStartOffset(i) + ", "
                    + tupleAccessor.getTupleEndOffset(i) + ")[");
            for (int j = 0; j < tupleAccessor.getFieldCount(); ++j) {
                System.err.print(j + ":(" + tupleAccessor.getFieldStartOffset(i, j) + ", "
                        + tupleAccessor.getFieldEndOffset(i, j) + ") ");
                System.err.print("{");
                bbis.setByteBuffer(
                        tupleAccessor.getBuffer(),
                        tupleAccessor.getTupleStartOffset(i) + tupleAccessor.getFieldSlotsLength()
                                + tupleAccessor.getFieldStartOffset(i, j));
                try {
                    byte tag = dis.readByte();
                    if (tag == nullByte) {
                        System.err.print("NULL");
                    } else {
                        bbis.setByteBuffer(tupleAccessor.getBuffer(), tupleAccessor.getTupleStartOffset(i)
                                + tupleAccessor.getFieldSlotsLength() + tupleAccessor.getFieldStartOffset(i, j));
                        System.err.print(recDesc.getFields()[j].deserialize(dis));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.err.print("}");
            }
            System.err.println("]");
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
