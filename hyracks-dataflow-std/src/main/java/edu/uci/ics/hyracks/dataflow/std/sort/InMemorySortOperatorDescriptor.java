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
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class InMemorySortOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final String FRAMESORTER = "framesorter";

    private static final long serialVersionUID = 1L;
    private final int[] sortFields;
    private INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private IBinaryComparatorFactory[] comparatorFactories;

    public InMemorySortOperatorDescriptor(JobSpecification spec, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        this(spec, sortFields, null, comparatorFactories, recordDescriptor);
    }

    public InMemorySortOperatorDescriptor(JobSpecification spec, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.sortFields = sortFields;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeTaskGraph(IActivityGraphBuilder builder) {
        SortActivity sa = new SortActivity();
        MergeActivity ma = new MergeActivity();

        builder.addTask(sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addTask(ma);
        builder.addTargetEdge(0, ma, 0);

        builder.addBlockingEdge(sa, ma);
    }

    private class SortActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorDescriptor getOwner() {
            return InMemorySortOperatorDescriptor.this;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final FrameSorter frameSorter = new FrameSorter(ctx, sortFields, firstKeyNormalizerFactory,
                    comparatorFactories, recordDescriptors[0]);
            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                @Override
                public void open() throws HyracksDataException {
                    frameSorter.reset();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    frameSorter.insertFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    frameSorter.sortFrames();
                    env.set(FRAMESORTER, frameSorter);
                }

                @Override
                public void flush() throws HyracksDataException {
                }
            };
            return op;
        }
    }

    private class MergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorDescriptor getOwner() {
            return InMemorySortOperatorDescriptor.this;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    writer.open();
                    FrameSorter frameSorter = (FrameSorter) env.get(FRAMESORTER);
                    frameSorter.flushFrames(writer);
                    writer.close();
                    env.set(FRAMESORTER, null);
                }
            };
            return op;
        }
    }
}