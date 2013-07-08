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
package edu.uci.ics.hyracks.dataflow.std.file;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

/**
 * File writer to output plain text.
 */
public class PlainFileWriterOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private IFileSplitProvider fileSplitProvider;

    private String delim;

    /**
     * @param spec
     * @param inputArity
     * @param outputArity
     */
    public PlainFileWriterOperatorDescriptor(IOperatorDescriptorRegistry spec, IFileSplitProvider fileSplitProvider,
            String delim) {
        super(spec, 1, 0);
        this.fileSplitProvider = fileSplitProvider;
        this.delim = delim;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.api.dataflow.IActivityNode#createPushRuntime(edu.
     * uci.ics.hyracks.api.context.IHyracksContext,
     * edu.uci.ics.hyracks.api.job.IOperatorEnvironment,
     * edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider, int,
     * int)
     */
    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        // Output files
        final FileSplit[] splits = fileSplitProvider.getFileSplits();
        // Frame accessor
        final FrameTupleAccessor frameTupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(),
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
        // Record descriptor
        final RecordDescriptor recordDescriptor = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            private BufferedWriter out;

            private ByteBufferInputStream bbis;

            private DataInputStream di;

            @Override
            public void open() throws HyracksDataException {
                try {
                    out = new BufferedWriter(new FileWriter(splits[partition].getLocalFile().getFile()));
                    bbis = new ByteBufferInputStream();
                    di = new DataInputStream(bbis);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                try {
                    frameTupleAccessor.reset(buffer);
                    for (int tIndex = 0; tIndex < frameTupleAccessor.getTupleCount(); tIndex++) {
                        int start = frameTupleAccessor.getTupleStartOffset(tIndex)
                                + frameTupleAccessor.getFieldSlotsLength();
                        bbis.setByteBuffer(buffer, start);
                        Object[] record = new Object[recordDescriptor.getFieldCount()];
                        for (int i = 0; i < record.length; ++i) {
                            Object instance = recordDescriptor.getFields()[i].deserialize(di);
                            if (i == 0) {
                                out.write(String.valueOf(instance));
                            } else {
                                out.write(delim + String.valueOf(instance));
                            }
                        }
                        out.write("\n");
                    }
                } catch (IOException ex) {
                    throw new HyracksDataException(ex);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
            }

            @Override
            public void close() throws HyracksDataException {
                try {
                    out.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }

}
