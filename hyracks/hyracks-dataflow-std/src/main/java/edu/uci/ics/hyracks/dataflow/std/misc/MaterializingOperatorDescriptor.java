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
package edu.uci.ics.hyracks.dataflow.std.misc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class MaterializingOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final static int MATERIALIZER_ACTIVITY_ID = 0;
    private final static int READER_ACTIVITY_ID = 1;
    private final static int MATERIALIZER_READER_ACTIVITY_ID = 2;

    private boolean isSingleActivity;

    public MaterializingOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recordDescriptor) {
        this(spec, recordDescriptor, false);
    }

    public MaterializingOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recordDescriptor,
            boolean isSingleActivity) {
        super(spec, 1, 1);
        recordDescriptors[0] = recordDescriptor;
        this.isSingleActivity = isSingleActivity;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        if (isSingleActivity) {
            MaterializerReaderActivityNode mra = new MaterializerReaderActivityNode(new ActivityId(odId,
                    MATERIALIZER_READER_ACTIVITY_ID));

            builder.addActivity(this, mra);
            builder.addSourceEdge(0, mra, 0);
            builder.addTargetEdge(0, mra, 0);
        } else {
            MaterializerActivityNode ma = new MaterializerActivityNode(new ActivityId(odId, MATERIALIZER_ACTIVITY_ID));
            ReaderActivityNode ra = new ReaderActivityNode(new ActivityId(odId, READER_ACTIVITY_ID));

            builder.addActivity(this, ma);
            builder.addSourceEdge(0, ma, 0);

            builder.addActivity(this, ra);
            builder.addTargetEdge(0, ra, 0);

            builder.addBlockingEdge(ma, ra);
        }

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

        public void open(IHyracksTaskContext ctx) throws HyracksDataException {
            FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                    MaterializingOperatorDescriptor.class.getSimpleName());
            out = new RunFileWriter(file, ctx.getIOManager());
            out.open();
        }

        public void appendFrame(ByteBuffer buffer) throws HyracksDataException {
            out.nextFrame(buffer);
        }

        public void writeOut(IFrameWriter writer, ByteBuffer frame) throws HyracksDataException {
            RunFileReader in = out.createReader();
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
    }

    private final class MaterializerReaderActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MaterializerReaderActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private MaterializerTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new MaterializerTaskState(ctx.getJobletContext().getJobId(), new TaskId(getActivityId(),
                            partition));
                    state.open(ctx);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.appendFrame(buffer);
                }

                @Override
                public void fail() throws HyracksDataException {
                }

                @Override
                public void close() throws HyracksDataException {
                    state.out.close();
                    ByteBuffer frame = ctx.allocateFrame();
                    state.writeOut(writer, frame);
                }

            };
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
                    state.open(ctx);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.appendFrame(buffer);
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
                    state.writeOut(writer, frame);
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                }
            };
        }
    }
}