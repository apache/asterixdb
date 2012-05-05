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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class MaterializingOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final static int MATERIALIZER_ACTIVITY_ID = 0;
    private final static int READER_ACTIVITY_ID = 1;

    public MaterializingOperatorDescriptor(JobSpecification spec, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        MaterializerActivityNode ma = new MaterializerActivityNode(new ActivityId(odId, MATERIALIZER_ACTIVITY_ID));
        ReaderActivityNode ra = new ReaderActivityNode(new ActivityId(odId, READER_ACTIVITY_ID));

        builder.addActivity(ma);
        builder.addSourceEdge(0, ma, 0);

        builder.addActivity(ra);
        builder.addTargetEdge(0, ra, 0);

        builder.addBlockingEdge(ma, ra);
    }

    public static class MaterializerTaskState extends AbstractStateObject {
        private RunFileWriter out;

        public MaterializerTaskState() {
        }

        private MaterializerTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }
    }

    private final class MaterializerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MaterializerActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private MaterializerTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new MaterializerTaskState(ctx.getJobletContext().getJobId(), new TaskId(getActivityId(),
                            partition));
                    FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                            MaterializingOperatorDescriptor.class.getSimpleName());
                    state.out = new RunFileWriter(file, ctx.getIOManager());
                    state.out.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.out.nextFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.out.close();
                    ctx.setStateObject(state);
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
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    ByteBuffer frame = ctx.allocateFrame();
                    MaterializerTaskState state = (MaterializerTaskState) ctx.getStateObject(new TaskId(new ActivityId(
                            getOperatorId(), MATERIALIZER_ACTIVITY_ID), partition));
                    RunFileReader in = state.out.createReader();
                    writer.open();
                    try {
                        in.open();
                        while (in.nextFrame(frame)) {
                            frame.flip();
                            writer.nextFrame(frame);
                            frame.clear();
                        }
                        in.close();
                    } catch (Exception e) {
                        writer.fail();
                        throw new HyracksDataException(e);
                    } finally {
                        writer.close();
                    }
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                }
            };
        }
    }
}