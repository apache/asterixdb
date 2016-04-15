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

package org.apache.hyracks.dataflow.std.parallel.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;

/**
 * @author michael
 */
public class ParallelRangeMapTaskState implements IStateObject/*extends AbstractStateObject*/{
    private IHyracksTaskContext ctx;
    private RunFileWriter out;
    private final RecordDescriptor keyDesc;

    public ParallelRangeMapTaskState(/*JobId jobId, Object taskId,*/RecordDescriptor keyDesc) {
        //        super(jobId, taskId);

        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] schema = new ISerializerDeserializer[keyDesc.getFieldCount()];
        for (int i = 0; i < keyDesc.getFieldCount(); i++) {
            schema[i] = keyDesc.getFields()[i];
        }
        this.keyDesc = new RecordDescriptor(schema);
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void fromBytes(DataInput in) throws IOException {
        // TODO Auto-generated method stub
    }

    public void open(IHyracksTaskContext context) throws HyracksDataException {
        this.ctx = context;
        FileReference file = context.getJobletContext().createManagedWorkspaceFile(
                ParallelRangeMapTaskState.class.getSimpleName());
        out = new RunFileWriter(file, context.getIOManager());
        out.open();
    }

    public void close() throws HyracksDataException {
        out.close();
    }

    public void appendFrame(ByteBuffer buf) throws HyracksDataException {
        out.nextFrame(buf);
    }

    public void writeOut(IFrameWriter writer, IFrame frame) throws HyracksDataException {
        RunFileReader in = out.createReader();
        writer.open();
        try {
            in.open();
            while (in.nextFrame(frame)) {
                writer.nextFrame(frame.getBuffer());
            }
            in.close();
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }

    public void deleteFile() {
        out.getFileReference().delete();
    }

    public IRangeMap getRangeMap() throws HyracksDataException {
        IFrame frame = new VSizeFrame(ctx);
        //        out.open();
        RunFileReader in = out.createReader();
        IFrameTupleAccessor inAccessor = new FrameTupleAccessor(keyDesc);
        int byteLen = 0;
        int tupleCount = 0;
        in.open();
        while (in.nextFrame(frame)) {
            inAccessor.reset(frame.getBuffer());
            int nTuple = inAccessor.getTupleCount();
            tupleCount += nTuple;
            for (int i = 0; i < nTuple; i++) {
                for (int j = 0; j < keyDesc.getFieldCount() - 1; j++) {
                    byteLen += inAccessor.getFieldLength(i, j);
                }
            }
        }
        in.close();
        in.open();
        byte[] byteRange = new byte[byteLen];
        int[] offRange = new int[tupleCount];
        int byteCur = 0;
        while (in.nextFrame(frame)) {
            inAccessor.reset(frame.getBuffer());
            int nTuple = inAccessor.getTupleCount();
            for (int i = 0; i < nTuple; i++) {
                offRange[i] = byteCur;
                for (int j = 0; j < keyDesc.getFieldCount() - 1; j++) {
                    int tStart = inAccessor.getTupleStartOffset(i);
                    int fStart = inAccessor.getFieldStartOffset(i, j);
                    int fEnd = inAccessor.getFieldEndOffset(i, j);
                    System.arraycopy(inAccessor.getBuffer().array(),
                            tStart + inAccessor.getFieldSlotsLength() + fStart, byteRange, byteCur, fEnd - fStart);
                    byteCur += (fEnd - fStart);
                }
            }
        }
        in.close();
        //        out.close();
        IRangeMap rangeMap = new RangeMap(keyDesc.getFieldCount() - 1, byteRange, offRange);
        return rangeMap;
    }

    @Override
    public JobId getJobId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getMemoryOccupancy() {
        // TODO Auto-generated method stub
        return 0;
    }

}
