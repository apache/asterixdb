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
package edu.uci.ics.hyracks.dataflow.std.group.sort;

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
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.sort.Algorithm;
import edu.uci.ics.hyracks.dataflow.std.sort.IFrameSorter;

/**
 * This Operator pushes group-by aggregation into the external sort.
 * After the in-memory sort, it aggregates the sorted data before writing it to a run file.
 * During the merge phase, it does an aggregation over sorted results.
 * 
 * @author yingyib
 */
public class SortGroupByOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int SORT_ACTIVITY_ID = 0;
    private static final int MERGE_ACTIVITY_ID = 1;

    private final int framesLimit;
    private final int[] sortFields;
    private final int[] groupFields;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IAggregatorDescriptorFactory mergeAggregatorFactory;
    private final IAggregatorDescriptorFactory partialAggregatorFactory;
    private final RecordDescriptor partialAggRecordDesc;
    private final RecordDescriptor outputRecordDesc;
    private final boolean finalStage;
    private Algorithm alg = Algorithm.MERGE_SORT;

    /***
     * @param spec
     *            , the Hyracks job specification
     * @param framesLimit
     *            , the frame limit for this operator
     * @param sortFields
     *            , the fields to sort
     * @param groupFields
     *            , the fields to group, which can be a prefix subset of sortFields
     * @param firstKeyNormalizerFactory
     *            , the normalized key computer factory of the first key
     * @param comparatorFactories
     *            , the comparator factories of sort keys
     * @param partialAggregatorFactory
     *            , for aggregating the input of this operator
     * @param mergeAggregatorFactory
     *            , for aggregating the intermediate data of this operator
     * @param partialAggRecordDesc
     *            , the record descriptor of intermediate data
     * @param outRecordDesc
     *            , the record descriptor of output data
     * @param finalStage
     *            , whether the operator is used for final stage aggregation
     */
    public SortGroupByOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            int[] groupFields, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory partialAggregatorFactory,
            IAggregatorDescriptorFactory mergeAggregatorFactory, RecordDescriptor partialAggRecordDesc,
            RecordDescriptor outRecordDesc, boolean finalStage) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.sortFields = sortFields;
        if (framesLimit <= 1) {
            throw new IllegalStateException();// minimum of 2 fames (1 in,1 out)
        }
        this.recordDescriptors[0] = outRecordDesc;

        this.groupFields = groupFields;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        this.mergeAggregatorFactory = mergeAggregatorFactory;
        this.partialAggregatorFactory = partialAggregatorFactory;
        this.partialAggRecordDesc = partialAggRecordDesc;
        this.outputRecordDesc = outRecordDesc;
        this.finalStage = finalStage;
    }

    /***
     * @param spec
     *            , the Hyracks job specification
     * @param framesLimit
     *            , the frame limit for this operator
     * @param sortFields
     *            , the fields to sort
     * @param groupFields
     *            , the fields to group, which can be a prefix subset of sortFields
     * @param firstKeyNormalizerFactory
     *            , the normalized key computer factory of the first key
     * @param comparatorFactories
     *            , the comparator factories of sort keys
     * @param partialAggregatorFactory
     *            , for aggregating the input of this operator
     * @param mergeAggregatorFactory
     *            , for aggregating the intermediate data of this operator
     * @param partialAggRecordDesc
     *            , the record descriptor of intermediate data
     * @param outRecordDesc
     *            , the record descriptor of output data
     * @param finalStage
     *            , whether the operator is used for final stage aggregation
     * @param alg
     *            , the in-memory sort algorithm
     */
    public SortGroupByOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            int[] groupFields, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory partialAggregatorFactory,
            IAggregatorDescriptorFactory mergeAggregatorFactory, RecordDescriptor partialAggRecordDesc,
            RecordDescriptor outRecordDesc, boolean finalStage, Algorithm alg) {
        this(spec, framesLimit, sortFields, groupFields, firstKeyNormalizerFactory, comparatorFactories,
                partialAggregatorFactory, mergeAggregatorFactory, partialAggRecordDesc, outRecordDesc, finalStage);
        this.alg = alg;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        SortActivity sa = new SortActivity(new ActivityId(odId, SORT_ACTIVITY_ID));
        MergeActivity ma = new MergeActivity(new ActivityId(odId, MERGE_ACTIVITY_ID));

        builder.addActivity(this, sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addActivity(this, ma);
        builder.addTargetEdge(0, ma, 0);

        builder.addBlockingEdge(sa, ma);
    }

    public static class SortTaskState extends AbstractStateObject {
        private List<IFrameReader> runs;
        private IFrameSorter frameSorter;

        public SortTaskState() {
        }

        private SortTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }
    }

    private class SortActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public SortActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private ExternalSortGroupByRunGenerator runGen;

                @Override
                public void open() throws HyracksDataException {
                    runGen = new ExternalSortGroupByRunGenerator(ctx, sortFields,
                            recordDescProvider.getInputRecordDescriptor(SortActivity.this.getActivityId(), 0),
                            framesLimit, groupFields, firstKeyNormalizerFactory, comparatorFactories,
                            partialAggregatorFactory, partialAggRecordDesc, alg);
                    runGen.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    runGen.nextFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    SortTaskState state = new SortTaskState(ctx.getJobletContext().getJobId(), new TaskId(
                            getActivityId(), partition));
                    runGen.close();
                    state.runs = runGen.getRuns();
                    state.frameSorter = runGen.getFrameSorter();
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

    private class MergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MergeActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    SortTaskState state = (SortTaskState) ctx.getStateObject(new TaskId(new ActivityId(getOperatorId(),
                            SORT_ACTIVITY_ID), partition));
                    List<IFrameReader> runs = state.runs;
                    IFrameSorter frameSorter = state.frameSorter;
                    int necessaryFrames = Math.min(runs.size() + 2, framesLimit);

                    IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                    for (int i = 0; i < comparators.length; i++) {
                        comparators[i] = comparatorFactories[i].createBinaryComparator();
                    }
                    INormalizedKeyComputer nkc = firstKeyNormalizerFactory == null ? null : firstKeyNormalizerFactory
                            .createNormalizedKeyComputer();

                    ExternalSortGroupByRunMerger merger = new ExternalSortGroupByRunMerger(ctx, frameSorter, runs,
                            sortFields, recordDescProvider.getInputRecordDescriptor(new ActivityId(odId,
                                    SORT_ACTIVITY_ID), 0), partialAggRecordDesc, outputRecordDesc, necessaryFrames,
                            writer, groupFields, nkc, comparators, partialAggregatorFactory, mergeAggregatorFactory,
                            !finalStage);
                    merger.process();
                }
            };
            return op;
        }
    }
}