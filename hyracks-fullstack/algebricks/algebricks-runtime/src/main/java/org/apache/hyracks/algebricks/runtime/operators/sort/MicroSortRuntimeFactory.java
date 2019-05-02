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
package org.apache.hyracks.algebricks.runtime.operators.sort;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.sort.Algorithm;
import org.apache.hyracks.dataflow.std.sort.ExternalSortRunGenerator;
import org.apache.hyracks.dataflow.std.sort.ExternalSortRunMerger;

public class MicroSortRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;
    private final int framesLimit;
    private final int[] sortFields;
    private final INormalizedKeyComputerFactory[] keyNormalizerFactories;
    private final IBinaryComparatorFactory[] comparatorFactories;

    public MicroSortRuntimeFactory(int[] sortFields, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IBinaryComparatorFactory[] comparatorFactories, int[] projectionList, int framesLimit) {
        this(sortFields, firstKeyNormalizerFactory != null
                ? new INormalizedKeyComputerFactory[] { firstKeyNormalizerFactory } : null, comparatorFactories,
                projectionList, framesLimit);
    }

    public MicroSortRuntimeFactory(int[] sortFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
            IBinaryComparatorFactory[] comparatorFactories, int[] projectionList, int framesLimit) {
        super(projectionList);
        // Obs: the projection list is currently ignored.
        if (projectionList != null) {
            throw new NotImplementedException("Cannot push projection into InMemorySortRuntime.");
        }
        this.sortFields = sortFields;
        this.keyNormalizerFactories = keyNormalizerFactories;
        this.comparatorFactories = comparatorFactories;
        this.framesLimit = framesLimit;
    }

    @Override
    public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws HyracksDataException {
        InMemorySortPushRuntime pushRuntime = new InMemorySortPushRuntime(ctx);
        ctx.registerDeallocatable(pushRuntime);
        return pushRuntime;
    }

    private class InMemorySortPushRuntime extends AbstractOneInputOneOutputPushRuntime implements IDeallocatable {
        final IHyracksTaskContext ctx;
        ExternalSortRunGenerator runsGenerator = null;
        ExternalSortRunMerger runsMerger = null;
        IFrameWriter wrappingWriter = null;

        private InMemorySortPushRuntime(IHyracksTaskContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void open() throws HyracksDataException {
            if (runsGenerator == null) {
                runsGenerator = new ExternalSortRunGenerator(ctx, sortFields, keyNormalizerFactories,
                        comparatorFactories, outputRecordDesc, Algorithm.MERGE_SORT, EnumFreeSlotPolicy.LAST_FIT,
                        framesLimit, Integer.MAX_VALUE);
            }
            // next writer will be opened later when preparing the merger
            isOpen = true;
            runsGenerator.open();
            runsGenerator.getSorter().reset();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            runsGenerator.nextFrame(buffer);
        }

        @Override
        public void close() throws HyracksDataException {
            Throwable failure = null;
            if (isOpen) {
                try {
                    if (!failed) {
                        runsGenerator.close();
                        createOrResetRunsMerger();
                        if (runsGenerator.getRuns().isEmpty()) {
                            wrappingWriter = runsMerger.prepareSkipMergingFinalResultWriter(writer);
                            wrappingWriter.open();
                            if (runsGenerator.getSorter().hasRemaining()) {
                                runsGenerator.getSorter().flush(wrappingWriter);
                            }
                        } else {
                            wrappingWriter = runsMerger.prepareFinalMergeResultWriter(writer);
                            wrappingWriter.open();
                            runsMerger.process(wrappingWriter);
                        }
                    }
                } catch (Throwable th) {
                    failure = th;
                    fail(th);
                } finally {
                    failure = CleanupUtils.close(wrappingWriter, failure);
                    wrappingWriter = null;
                }
            }
            isOpen = false;
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            failed = true;
            // clean up the runs if some have been generated. double close should be idempotent.
            if (runsGenerator != null) {
                List<GeneratedRunFileReader> runs = runsGenerator.getRuns();
                for (int i = 0, size = runs.size(); i < size; i++) {
                    try {
                        runs.get(i).close();
                    } catch (Throwable th) {
                        // ignore
                    }
                }
            }
            if (wrappingWriter != null) {
                wrappingWriter.fail();
            }
        }

        @Override
        public void deallocate() {
            if (runsGenerator != null) {
                try {
                    runsGenerator.getSorter().close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }

        private void createOrResetRunsMerger() {
            if (runsMerger == null) {
                IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                for (int i = 0; i < comparatorFactories.length; ++i) {
                    comparators[i] = comparatorFactories[i].createBinaryComparator();
                }
                INormalizedKeyComputer nmkComputer =
                        keyNormalizerFactories == null ? null : keyNormalizerFactories[0].createNormalizedKeyComputer();
                runsMerger = new ExternalSortRunMerger(ctx, runsGenerator.getRuns(), sortFields, comparators,
                        nmkComputer, outputRecordDesc, framesLimit, Integer.MAX_VALUE);
            } else {
                runsMerger.reset(runsGenerator.getRuns());
            }
        }
    }
}
