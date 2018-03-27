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
package org.apache.hyracks.dataflow.std.file;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

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
     * org.apache.hyracks.api.dataflow.IActivityNode#createPushRuntime(edu.
     * uci.ics.hyracks.api.context.IHyracksContext,
     * org.apache.hyracks.api.job.IOperatorEnvironment,
     * org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider, int,
     * int)
     */
    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        // Output files
        final FileSplit[] splits = fileSplitProvider.getFileSplits();
        IIOManager ioManager = ctx.getIoManager();
        // Frame accessor
        final FrameTupleAccessor frameTupleAccessor =
                new FrameTupleAccessor(recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
        // Record descriptor
        final RecordDescriptor recordDescriptor = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            private BufferedWriter out;

            private ByteBufferInputStream bbis;

            private DataInputStream di;

            @Override
            public void open() throws HyracksDataException {
                try {
                    out = new BufferedWriter(new FileWriter(splits[partition].getFile(ioManager)));
                    bbis = new ByteBufferInputStream();
                    di = new DataInputStream(bbis);
                } catch (Exception e) {
                    throw HyracksDataException.create(e);
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
                    throw HyracksDataException.create(ex);
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
                    throw HyracksDataException.create(e);
                }
            }
        };
    }

}
