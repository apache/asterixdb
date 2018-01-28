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
package org.apache.hyracks.dataflow.std.group.external;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.ISpillableTable;
import org.apache.hyracks.dataflow.std.group.ISpillableTableFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExternalGroupBuildOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable
        implements IRunFileWriterGenerator {

    private static final Logger LOGGER = LogManager.getLogger();
    private final IHyracksTaskContext ctx;
    private final Object stateId;
    private final int[] keyFields;
    private final IBinaryComparator[] comparators;
    private final INormalizedKeyComputer firstNormalizerComputer;
    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final int framesLimit;
    private final ISpillableTableFactory spillableTableFactory;
    private final RecordDescriptor inRecordDescriptor;
    private final RecordDescriptor outRecordDescriptor;
    private final int tableSize;
    private final long fileSize;

    private ExternalHashGroupBy externalGroupBy;
    private ExternalGroupState state;
    private boolean isFailed = false;

    public ExternalGroupBuildOperatorNodePushable(IHyracksTaskContext ctx, Object stateId, int tableSize, long fileSize,
            int[] keyFields, int framesLimit, IBinaryComparatorFactory[] comparatorFactories,
            INormalizedKeyComputerFactory firstNormalizerFactory, IAggregatorDescriptorFactory aggregatorFactory,
            RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor,
            ISpillableTableFactory spillableTableFactory) {
        this.ctx = ctx;
        this.stateId = stateId;
        this.framesLimit = framesLimit;
        this.aggregatorFactory = aggregatorFactory;
        this.keyFields = keyFields;
        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.firstNormalizerComputer =
                firstNormalizerFactory == null ? null : firstNormalizerFactory.createNormalizedKeyComputer();
        this.spillableTableFactory = spillableTableFactory;
        this.inRecordDescriptor = inRecordDescriptor;
        this.outRecordDescriptor = outRecordDescriptor;
        this.tableSize = tableSize;
        this.fileSize = fileSize;
    }

    @Override
    public void open() throws HyracksDataException {
        state = new ExternalGroupState(ctx.getJobletContext().getJobId(), stateId);
        ISpillableTable table = spillableTableFactory.buildSpillableTable(ctx, tableSize, fileSize, keyFields,
                comparators, firstNormalizerComputer, aggregatorFactory, inRecordDescriptor, outRecordDescriptor,
                framesLimit, 0);
        RunFileWriter[] runFileWriters = new RunFileWriter[table.getNumPartitions()];
        this.externalGroupBy = new ExternalHashGroupBy(this, table, runFileWriters, inRecordDescriptor);

        state.setSpillableTable(table);
        state.setRuns(runFileWriters);
        state.setSpilledNumTuples(externalGroupBy.getSpilledNumTuples());
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        externalGroupBy.insert(buffer);
    }

    @Override
    public void fail() throws HyracksDataException {
        isFailed = true;
    }

    @Override
    public void close() throws HyracksDataException {
        if (isFailed && state.getRuns() != null) {
            for (RunFileWriter run : state.getRuns()) {
                if (run != null) {
                    run.erase();
                }
            }
        } else {
            externalGroupBy.flushSpilledPartitions();
            ctx.setStateObject(state);
            if (LOGGER.isDebugEnabled()) {
                int numOfPartition = state.getSpillableTable().getNumPartitions();
                int numOfSpilledPart = 0;
                for (int i = 0; i < numOfPartition; i++) {
                    if (state.getSpilledNumTuples()[i] > 0) {
                        numOfSpilledPart++;
                    }
                }
                LOGGER.debug("level 0:" + "build with " + numOfPartition + " partitions" + ", spilled "
                        + numOfSpilledPart + " partitions");
            }
        }
        state = null;
        externalGroupBy = null;
    }

    @Override
    public RunFileWriter getRunFileWriter() throws HyracksDataException {
        FileReference file = ctx.getJobletContext()
                .createManagedWorkspaceFile(ExternalGroupOperatorDescriptor.class.getSimpleName());
        return new RunFileWriter(file, ctx.getIoManager());
    }
}
