/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.misc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class MaterializingOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    protected static final String MATERIALIZED_FILE = "materialized-file";
    protected static final String FRAME_COUNT = "frame-count";

    public MaterializingOperatorDescriptor(JobSpecification spec, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeTaskGraph(IActivityGraphBuilder builder) {
        MaterializerActivityNode ma = new MaterializerActivityNode();
        ReaderActivityNode ra = new ReaderActivityNode();

        builder.addTask(ma);
        builder.addSourceEdge(0, ma, 0);

        builder.addTask(ra);
        builder.addTargetEdge(0, ra, 0);

        builder.addBlockingEdge(ma, ra);
    }

    private final class MaterializerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private FileChannel out;
                private int frameCount;

                @Override
                public void open() throws HyracksDataException {
                    File outFile;
                    try {
                        outFile = ctx.getResourceManager().createFile("mat", ".dat");
                        out = new RandomAccessFile(outFile, "rw").getChannel();
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    env.set(MATERIALIZED_FILE, outFile.getAbsolutePath());
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    ++frameCount;
                    buffer.position(0);
                    buffer.limit(buffer.capacity());
                    int rem = buffer.capacity();
                    while (rem > 0) {
                        int c;
                        try {
                            c = out.write(buffer);
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                        rem -= c;
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    try {
                        env.set(FRAME_COUNT, frameCount);
                        out.close();
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                }

                @Override
                public void flush() throws HyracksDataException {
                }
            };
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return MaterializingOperatorDescriptor.this;
        }
    }

    private final class ReaderActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    try {
                        File inFile = new File((String) env.get(MATERIALIZED_FILE));
                        int frameCount = (Integer) env.get(FRAME_COUNT);
                        FileChannel in = new RandomAccessFile(inFile, "r").getChannel();
                        ByteBuffer frame = ctx.getResourceManager().allocateFrame();
                        writer.open();
                        for (int i = 0; i < frameCount; ++i) {
                            frame.clear();
                            int rem = frame.capacity();
                            while (rem > 0) {
                                int c = in.read(frame);
                                rem -= c;
                            }
                            frame.flip();
                            writer.nextFrame(frame);
                        }
                        writer.close();
                        in.close();
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                    env.set(MATERIALIZED_FILE, null);
                }
            };
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return MaterializingOperatorDescriptor.this;
        }
    }
}