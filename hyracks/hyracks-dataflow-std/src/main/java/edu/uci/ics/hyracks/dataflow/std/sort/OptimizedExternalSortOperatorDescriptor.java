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
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * @author pouria
 *         Operator descriptor for sorting with replacement, consisting of two
 *         phases:
 *         - Run Generation: Denoted by OptimizedSortActivity below, in which
 *         sort runs get generated from the input data. This phases uses the
 *         Selection Tree and Memory Manager to benefit from the replacement
 *         selection optimization, to create runs which are longer than the
 *         available memory size.
 *         - Merging: Denoted by MergeActivity below, in which runs (generated
 *         in the previous phase) get merged via a merger. Each run has a single
 *         buffer in memory, and a priority queue is used to select the top
 *         tuple each time. Top tuple is send to a new run or output
 */

public class OptimizedExternalSortOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final int NO_LIMIT = -1;
    private static final long serialVersionUID = 1L;
    private static final int SORT_ACTIVITY_ID = 0;
    private static final int MERGE_ACTIVITY_ID = 1;

    private final int[] sortFields;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final int memSize;
    private final int outputLimit;

    public OptimizedExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        this(spec, framesLimit, NO_LIMIT, sortFields, null, comparatorFactories, recordDescriptor);
    }

    public OptimizedExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int outputLimit,
            int[] sortFields, IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        this(spec, framesLimit, outputLimit, sortFields, null, comparatorFactories, recordDescriptor);
    }

    public OptimizedExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int memSize, int outputLimit,
            int[] sortFields, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.memSize = memSize;
        this.outputLimit = outputLimit;
        this.sortFields = sortFields;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        if (memSize <= 1) {
            throw new IllegalStateException();// minimum of 2 fames (1 in,1 out)
        }
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        OptimizedSortActivity osa = new OptimizedSortActivity(new ActivityId(odId, SORT_ACTIVITY_ID));
        OptimizedMergeActivity oma = new OptimizedMergeActivity(new ActivityId(odId, MERGE_ACTIVITY_ID));

        builder.addActivity(this, osa);
        builder.addSourceEdge(0, osa, 0);

        builder.addActivity(this, oma);
        builder.addTargetEdge(0, oma, 0);

        builder.addBlockingEdge(osa, oma);
    }

    public static class OptimizedSortTaskState extends AbstractStateObject {
        private List<IFrameReader> runs;

        public OptimizedSortTaskState() {
        }

        private OptimizedSortTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }
    }

    private class OptimizedSortActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public OptimizedSortActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            final IRunGenerator runGen;
            if (outputLimit == NO_LIMIT) {
                runGen = new OptimizedExternalSortRunGenerator(ctx, sortFields, firstKeyNormalizerFactory,
                        comparatorFactories, recordDescriptors[0], memSize);
            } else {
                runGen = new OptimizedExternalSortRunGeneratorWithLimit(ctx, sortFields, firstKeyNormalizerFactory,
                        comparatorFactories, recordDescriptors[0], memSize, outputLimit);
            }

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
                    OptimizedSortTaskState state = new OptimizedSortTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    runGen.close();
                    state.runs = runGen.getRuns();
                    ctx.setStateObject(state);

                }

                @Override
                public void fail() throws HyracksDataException {
                    runGen.fail();
                }
            };
            return op;
        }
    }

    private class OptimizedMergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public OptimizedMergeActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    OptimizedSortTaskState state = (OptimizedSortTaskState) ctx.getStateObject(new TaskId(
                            new ActivityId(getOperatorId(), SORT_ACTIVITY_ID), partition));

                    List<IFrameReader> runs = state.runs;

                    IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                    for (int i = 0; i < comparatorFactories.length; ++i) {
                        comparators[i] = comparatorFactories[i].createBinaryComparator();
                    }

                    INormalizedKeyComputer nmkComputer = firstKeyNormalizerFactory == null ? null
                            : firstKeyNormalizerFactory.createNormalizedKeyComputer();
                    int necessaryFrames = Math.min(runs.size() + 2, memSize);
                    ExternalSortRunMerger merger = new ExternalSortRunMerger(ctx, outputLimit, runs, sortFields,
                            comparators, nmkComputer, recordDescriptors[0], necessaryFrames, writer);

                    merger.processWithReplacementSelection();

                }
            };
            return op;
        }
    }
}