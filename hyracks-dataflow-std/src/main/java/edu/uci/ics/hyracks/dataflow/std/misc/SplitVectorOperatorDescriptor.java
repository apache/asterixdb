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

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import edu.uci.ics.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;

public class SplitVectorOperatorDescriptor extends AbstractOperatorDescriptor {
    private class CollectActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        @Override
        public ActivityNodeId getActivityNodeId() {
            return id;
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return SplitVectorOperatorDescriptor.this;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            IOpenableDataWriterOperator op = new IOpenableDataWriterOperator() {
                private ArrayList<Object[]> buffer;

                @Override
                public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
                    throw new IllegalArgumentException();
                }

                @Override
                public void open() throws HyracksDataException {
                    buffer = new ArrayList<Object[]>();
                    env.set(BUFFER, buffer);
                }

                @Override
                public void close() throws HyracksDataException {

                }

                @Override
                public void writeData(Object[] data) throws HyracksDataException {
                    buffer.add(data);
                }
            };
            return new DeserializedOperatorNodePushable(ctx, op, recordDescProvider.getInputRecordDescriptor(
                    getOperatorId(), 0));
        }
    }

    private class SplitActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorDescriptor getOwner() {
            return SplitVectorOperatorDescriptor.this;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            IOpenableDataWriterOperator op = new IOpenableDataWriterOperator() {
                private IOpenableDataWriter<Object[]> writer;

                @Override
                public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
                    if (index != 0) {
                        throw new IllegalArgumentException();
                    }
                    this.writer = writer;
                }

                @Override
                public void open() throws HyracksDataException {
                }

                @Override
                public void close() throws HyracksDataException {
                }

                @Override
                public void writeData(Object[] data) throws HyracksDataException {
                    List<Object[]> buffer = (List<Object[]>) env.get(BUFFER);
                    int n = buffer.size();
                    int step = (int) Math.floor(n / (float) splits);
                    writer.open();
                    for (int i = 0; i < splits; ++i) {
                        writer.writeData(buffer.get(step * (i + 1) - 1));
                    }
                    writer.close();
                }
            };
            return new DeserializedOperatorNodePushable(ctx, op, recordDescProvider.getInputRecordDescriptor(
                    getOperatorId(), 0));
        }
    }

    private static final String BUFFER = "buffer";

    private static final long serialVersionUID = 1L;

    private final int splits;

    public SplitVectorOperatorDescriptor(JobSpecification spec, int splits, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.splits = splits;
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeTaskGraph(IActivityGraphBuilder builder) {
        CollectActivity ca = new CollectActivity();
        SplitActivity sa = new SplitActivity();

        builder.addTask(ca);
        builder.addSourceEdge(0, ca, 0);

        builder.addTask(sa);
        builder.addTargetEdge(0, sa, 0);

        builder.addBlockingEdge(ca, sa);
    }
}