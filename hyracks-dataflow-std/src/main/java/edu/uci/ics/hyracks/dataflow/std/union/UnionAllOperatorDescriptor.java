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
package edu.uci.ics.hyracks.dataflow.std.union;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
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
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputOperatorNodePushable;

public class UnionAllOperatorDescriptor extends AbstractOperatorDescriptor {
    public UnionAllOperatorDescriptor(JobSpecification spec, int nInputs, RecordDescriptor recordDescriptor) {
        super(spec, nInputs, 1);
        recordDescriptors[0] = recordDescriptor;
    }

    private static final long serialVersionUID = 1L;

    @Override
    public void contributeTaskGraph(IActivityGraphBuilder builder) {
        UnionActivityNode uba = new UnionActivityNode();
        builder.addTask(uba);
        for (int i = 0; i < inputArity; ++i) {
            builder.addSourceEdge(i, uba, i);
        }
        builder.addTargetEdge(0, uba, 0);
    }

    private class UnionActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public UnionActivityNode() {
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksStageletContext ctx, IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            RecordDescriptor inRecordDesc = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
            return new UnionOperator(ctx, inRecordDesc);
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return UnionAllOperatorDescriptor.this;
        }

    }

    private class UnionOperator extends AbstractUnaryOutputOperatorNodePushable {
        private int nOpened;

        private int nClosed;

        public UnionOperator(IHyracksStageletContext ctx, RecordDescriptor inRecordDesc) {
            nOpened = 0;
            nClosed = 0;
        }

        @Override
        public int getInputArity() {
            return inputArity;
        }

        @Override
        public IFrameWriter getInputFrameWriter(int index) {
            return new IFrameWriter() {
                @Override
                public void open() throws HyracksDataException {
                    synchronized (UnionOperator.this) {
                        if (++nOpened == 1) {
                            writer.open();
                        }
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    synchronized (UnionOperator.this) {
                        writer.nextFrame(buffer);
                    }
                }

                @Override
                public void flush() throws HyracksDataException {
                    synchronized (UnionOperator.this) {
                        writer.flush();
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    synchronized (UnionOperator.this) {
                        if (++nClosed == inputArity) {
                            writer.close();
                        }
                    }
                }
            };
        }
    }
}