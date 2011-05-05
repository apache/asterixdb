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
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
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

public class ExternalSortOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final String FRAMESORTER = "framesorter";
    private static final String RUNS = "runs";

    private static final long serialVersionUID = 1L;
    private final int[] sortFields;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final int framesLimit;

    public ExternalSortOperatorDescriptor(JobSpecification spec, int framesLimit, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        this(spec, framesLimit, sortFields, null, comparatorFactories, recordDescriptor);
    }

    public ExternalSortOperatorDescriptor(JobSpecification spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.sortFields = sortFields;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        if (framesLimit <= 1) {
            throw new IllegalStateException();// minimum of 2 fames (1 in,1 out)
        }
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        SortActivity sa = new SortActivity(new ActivityId(odId, 0));
        MergeActivity ma = new MergeActivity(new ActivityId(odId, 1));

        builder.addActivity(sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addActivity(ma);
        builder.addTargetEdge(0, ma, 0);

        builder.addBlockingEdge(sa, ma);
    }

    private class SortActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public SortActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final ExternalSortRunGenerator runGen = new ExternalSortRunGenerator(ctx, sortFields,
                    firstKeyNormalizerFactory, comparatorFactories, recordDescriptors[0], framesLimit);
            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                @Override
                public void open() throws HyracksDataException {
                    runGen.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    runGen.nextFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    runGen.close();
                    env.set(FRAMESORTER, runGen.getFrameSorter());
                    env.set(RUNS, runGen.getRuns());
                }

                @Override
                public void flush() throws HyracksDataException {
                    runGen.flush();
                }
            };
            return op;
        }
    }

    private class MergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MergeActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    List<IFrameReader> runs = (List<IFrameReader>) env.get(RUNS);
                    FrameSorter frameSorter = (FrameSorter) env.get(FRAMESORTER);
                    IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                    for (int i = 0; i < comparatorFactories.length; ++i) {
                        comparators[i] = comparatorFactories[i].createBinaryComparator();
                    }
                    ExternalSortRunMerger merger = new ExternalSortRunMerger(ctx, frameSorter, runs, sortFields,
                            comparators, recordDescriptors[0], framesLimit, writer);
                    merger.process();
                    env.set(FRAMESORTER, null);
                    env.set(RUNS, null);
                }
            };
            return op;
        }
    }
}