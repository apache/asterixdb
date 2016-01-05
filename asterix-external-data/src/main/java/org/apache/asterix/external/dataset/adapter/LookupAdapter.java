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
package org.apache.asterix.external.dataset.adapter;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.external.api.ILookupRecordReader;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.indexing.RecordId;
import org.apache.asterix.external.indexing.RecordIdReader;
import org.apache.asterix.external.util.DataflowUtils;
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
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public final class LookupAdapter<T> implements IFrameWriter {

    private boolean propagateInput;
    private int[] propagatedFields;
    private boolean retainNull;
    private ArrayTupleBuilder tb;
    private FrameTupleAppender appender;
    private IRecordDataParser<T> dataParser;
    private ILookupRecordReader<? extends T> recordReader;
    private RecordIdReader ridReader;
    private FrameTupleAccessor tupleAccessor;
    private IFrameWriter writer;
    private FrameTupleReference frameTuple;
    private ArrayTupleBuilder nullTupleBuild;

    public LookupAdapter(IRecordDataParser<T> dataParser, ILookupRecordReader<? extends T> recordReader,
            RecordDescriptor inRecDesc, RecordIdReader ridReader, boolean propagateInput, int[] propagatedFields,
            boolean retainNull, INullWriterFactory iNullWriterFactory, IHyracksTaskContext ctx, IFrameWriter writer)
                    throws HyracksDataException {
        this.dataParser = dataParser;
        this.recordReader = recordReader;
        this.propagateInput = propagateInput;
        this.propagatedFields = propagatedFields;
        this.retainNull = retainNull;
        this.tupleAccessor = new FrameTupleAccessor(inRecDesc);
        this.ridReader = ridReader;
        ridReader.set(tupleAccessor, inRecDesc);
        configurePropagation(iNullWriterFactory);
        appender = new FrameTupleAppender(new VSizeFrame(ctx));
        this.writer = writer;
    }

    private void configurePropagation(INullWriterFactory iNullWriterFactory) {
        if (propagateInput) {
            tb = new ArrayTupleBuilder(propagatedFields.length + 1);
            frameTuple = new FrameTupleReference();
        } else {
            tb = new ArrayTupleBuilder(1);
        }
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
    public void fail() throws HyracksDataException {
        try {
            recordReader.fail();
        } catch (Throwable th) {
            throw new HyracksDataException(th);
        } finally {
            writer.fail();
        }
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();

    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        try {
            tupleAccessor.reset(buffer);
            int tupleIndex = 0;
            int tupleCount = tupleAccessor.getTupleCount();
            while (tupleIndex < tupleCount) {
                IRawRecord<? extends T> record = null;
                RecordId rid = ridReader.read(tupleIndex);
                if (rid != null) {
                    record = recordReader.read(rid);
                }
                tb.reset();
                if (propagateInput) {
                    propagate(tupleIndex);
                }
                if (record != null) {
                    dataParser.parse(record, tb.getDataOutput());
                    tb.addFieldEndOffset();
                    DataflowUtils.addTupleToFrame(appender, tb, writer);
                } else if (retainNull) {
                    tb.getDataOutput().write(nullTupleBuild.getByteArray());
                    tb.addFieldEndOffset();
                    DataflowUtils.addTupleToFrame(appender, tb, writer);
                }
                tupleIndex++;
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private void propagate(int idx) throws IOException {
        frameTuple.reset(tupleAccessor, idx);
        for (int i = 0; i < propagatedFields.length; i++) {
            tb.getDataOutput().write(frameTuple.getFieldData(propagatedFields[i]),
                    frameTuple.getFieldStart(propagatedFields[i]), frameTuple.getFieldLength(propagatedFields[i]));
            tb.addFieldEndOffset();
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            appender.flush(writer, true);
        } finally {
            writer.close();
        }
    }
}
