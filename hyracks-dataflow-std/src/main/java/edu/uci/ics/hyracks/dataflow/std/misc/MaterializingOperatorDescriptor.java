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

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class MaterializingOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    protected static final String MATERIALIZED_FILE = "materialized-file";

    public MaterializingOperatorDescriptor(JobSpecification spec, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        MaterializerActivityNode ma = new MaterializerActivityNode(new ActivityId(odId, 0));
        ReaderActivityNode ra = new ReaderActivityNode(new ActivityId(odId, 1));

        builder.addActivity(ma);
        builder.addSourceEdge(0, ma, 0);

        builder.addActivity(ra);
        builder.addTargetEdge(0, ra, 0);

        builder.addBlockingEdge(ma, ra);
    }

    private final class MaterializerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MaterializerActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private RunFileWriter out;

                @Override
                public void open() throws HyracksDataException {
                    FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                            MaterializingOperatorDescriptor.class.getSimpleName());
                    out = new RunFileWriter(file, ctx.getIOManager());
                    out.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    out.nextFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    out.close();
                    env.set(MATERIALIZED_FILE, out);
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
        }
    }

    private final class ReaderActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public ReaderActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    ByteBuffer frame = ctx.allocateFrame();
                    RunFileWriter out = (RunFileWriter) env.get(MATERIALIZED_FILE);
                    RunFileReader in = out.createReader();
                    writer.open();
                    in.open();
                    while (in.nextFrame(frame)) {
                        frame.flip();
                        writer.nextFrame(frame);
                        frame.clear();
                    }
                    in.close();
                    writer.close();
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                    env.set(MATERIALIZED_FILE, null);
                }
            };
        }
    }
}