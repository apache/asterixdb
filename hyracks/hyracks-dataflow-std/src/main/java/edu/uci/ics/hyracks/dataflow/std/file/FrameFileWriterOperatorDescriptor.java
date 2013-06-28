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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

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
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            private OutputStream out;

            @Override
            public void open() throws HyracksDataException {
                try {
                    out = new FileOutputStream(splits[partition].getLocalFile().getFile());
                } catch (FileNotFoundException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                try {
                    out.write(buffer.array());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
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