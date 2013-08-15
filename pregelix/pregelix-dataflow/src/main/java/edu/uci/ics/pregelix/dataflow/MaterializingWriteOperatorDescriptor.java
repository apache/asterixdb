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
package edu.uci.ics.pregelix.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.pregelix.dataflow.context.RuntimeContext;
import edu.uci.ics.pregelix.dataflow.state.MaterializerTaskState;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

public class MaterializingWriteOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private final static int MATERIALIZER_ACTIVITY_ID = 0;

    public MaterializingWriteOperatorDescriptor(JobSpecification spec, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        MaterializerActivityNode ma = new MaterializerActivityNode(new ActivityId(odId, MATERIALIZER_ACTIVITY_ID));

        builder.addActivity(this, ma);
        builder.addSourceEdge(0, ma, 0);
        builder.addTargetEdge(0, ma, 0);
    }

    private final class MaterializerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MaterializerActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private MaterializerTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    /** remove last iteration's state */
                    IterationUtils.removeIterationState(ctx, partition);
                    state = new MaterializerTaskState(ctx.getJobletContext().getJobId(), new TaskId(getActivityId(),
                            partition));
                    INCApplicationContext appContext = ctx.getJobletContext().getApplicationContext();
                    RuntimeContext context = (RuntimeContext) appContext.getApplicationObject();
                    FileReference file = context.createManagedWorkspaceFile(MaterializingWriteOperatorDescriptor.class
                            .getSimpleName());
                    state.setRunFileWriter(new RunFileWriter(file, ctx.getIOManager()));
                    state.getRunFileWriter().open();
                    writer.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.getRunFileWriter().nextFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.getRunFileWriter().close();
                    /**
                     * set iteration state
                     */
                    IterationUtils.setIterationState(ctx, partition, state);
                    writer.close();
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
        }
    }
}