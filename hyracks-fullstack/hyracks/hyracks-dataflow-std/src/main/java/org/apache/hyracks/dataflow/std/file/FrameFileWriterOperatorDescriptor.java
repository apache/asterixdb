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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class FrameFileWriterOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private IFileSplitProvider fileSplitProvider;

    public FrameFileWriterOperatorDescriptor(IOperatorDescriptorRegistry spec, IFileSplitProvider fileSplitProvider) {
        super(spec, 1, 0);
        this.fileSplitProvider = fileSplitProvider;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
        final FileSplit[] splits = fileSplitProvider.getFileSplits();
        final IIOManager ioManager = ctx.getIoManager();
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            private OutputStream out;

            @Override
            public void open() throws HyracksDataException {
                try {
                    out = new FileOutputStream(splits[partition].getFile(ioManager));
                } catch (FileNotFoundException e) {
                    throw HyracksDataException.create(e);
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                try {
                    out.write(buffer.array());
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
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
