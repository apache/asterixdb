/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractSorterOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    protected static final int SORT_ACTIVITY_ID = 0;
    protected static final int MERGE_ACTIVITY_ID = 1;
    protected final int[] sortFields;
    protected final INormalizedKeyComputerFactory[] keyNormalizerFactories;
    protected final IBinaryComparatorFactory[] comparatorFactories;
    protected final int framesLimit;

    public AbstractSorterOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.sortFields = sortFields;
        this.keyNormalizerFactories = keyNormalizerFactories;
        this.comparatorFactories = comparatorFactories;
        outRecDescs[0] = recordDescriptor;
    }

    public abstract SortActivity getSortActivity(ActivityId id);

    public abstract MergeActivity getMergeActivity(ActivityId id);

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        SortActivity sa = getSortActivity(new ActivityId(odId, SORT_ACTIVITY_ID));
        MergeActivity ma = getMergeActivity(new ActivityId(odId, MERGE_ACTIVITY_ID));

        builder.addActivity(this, sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addActivity(this, ma);
        builder.addTargetEdge(0, ma, 0);

        builder.addBlockingEdge(sa, ma);
    }

    public static class SortTaskState extends AbstractStateObject {
        List<GeneratedRunFileReader> generatedRunFileReaders;
        ISorter sorter;

        SortTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }
    }

    protected abstract class SortActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        protected SortActivity(ActivityId id) {
            super(id);
        }

        protected abstract IRunGenerator getRunGenerator(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider) throws HyracksDataException;

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private IRunGenerator runGen;

                @Override
                public void open() throws HyracksDataException {
                    runGen = getRunGenerator(ctx, recordDescProvider);
                    runGen.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    runGen.nextFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    SortTaskState state = new SortTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    runGen.close();
                    state.generatedRunFileReaders = runGen.getRuns();
                    state.sorter = runGen.getSorter();
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("InitialNumberOfRuns:" + runGen.getRuns().size());
                    }
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                    runGen.fail();
                }

                @Override
                public String getDisplayName() {
                    return "Sort (Run Generation)";
                }
            };
        }
    }

    protected abstract class MergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        protected MergeActivity(ActivityId id) {
            super(id);
        }

        protected abstract AbstractExternalSortRunMerger getSortRunMerger(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, List<GeneratedRunFileReader> runs,
                IBinaryComparator[] comparators, INormalizedKeyComputer nmkComputer, int necessaryFrames);

        @Override
        @SuppressWarnings("squid:S1188")
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {

                @Override
                public void initialize() throws HyracksDataException {
                    SortTaskState state = (SortTaskState) ctx
                            .getStateObject(new TaskId(new ActivityId(getOperatorId(), SORT_ACTIVITY_ID), partition));
                    List<GeneratedRunFileReader> runs = state.generatedRunFileReaders;
                    ISorter sorter = state.sorter;
                    IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                    for (int i = 0; i < comparatorFactories.length; ++i) {
                        comparators[i] = comparatorFactories[i].createBinaryComparator();
                    }
                    INormalizedKeyComputer nmkComputer = keyNormalizerFactories == null ? null
                            : keyNormalizerFactories[0].createNormalizedKeyComputer();
                    AbstractExternalSortRunMerger merger =
                            getSortRunMerger(ctx, recordDescProvider, runs, comparators, nmkComputer, framesLimit);
                    IFrameWriter wrappingWriter = null;
                    try {
                        if (runs.isEmpty()) {
                            wrappingWriter = merger.prepareSkipMergingFinalResultWriter(writer);
                            wrappingWriter.open();
                            if (sorter.hasRemaining()) {
                                sorter.flush(wrappingWriter);
                            }
                        } else {
                            // eagerly close the sorter here to release memory rather than in finally
                            sorter.close();
                            sorter = null;
                            wrappingWriter = merger.prepareFinalMergeResultWriter(writer);
                            wrappingWriter.open();
                            merger.process(wrappingWriter);
                        }
                    } catch (Throwable e) {
                        if (wrappingWriter != null) {
                            wrappingWriter.fail();
                        }
                        throw HyracksDataException.create(e);
                    } finally {
                        if (sorter != null) {
                            sorter.close();
                        }
                        if (wrappingWriter != null) {
                            wrappingWriter.close();
                        }
                    }
                }

                @Override
                public String getDisplayName() {
                    return "Sort (Run Merge)";
                }
            };
        }
    }
}
