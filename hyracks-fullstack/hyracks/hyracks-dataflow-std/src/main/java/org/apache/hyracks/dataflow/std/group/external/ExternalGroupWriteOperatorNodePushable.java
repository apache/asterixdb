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

import java.util.ArrayList;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.group.AggregateType;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.ISpillableTable;
import org.apache.hyracks.dataflow.std.group.ISpillableTableFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExternalGroupWriteOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable
        implements IRunFileWriterGenerator {
    private static final Logger LOGGER = LogManager.getLogger();
    private final IHyracksTaskContext ctx;
    private final Object stateId;
    private final ISpillableTableFactory spillableTableFactory;
    private final RecordDescriptor partialAggRecordDesc;
    private final RecordDescriptor outRecordDesc;
    private final IAggregatorDescriptorFactory mergeAggregatorFactory;
    private final int[] mergeGroupFields;
    private final IBinaryComparator[] groupByComparators;
    private final int frameLimit;
    private final INormalizedKeyComputer nmkComputer;
    private final ArrayList<RunFileWriter> generatedRuns = new ArrayList<>();

    public ExternalGroupWriteOperatorNodePushable(IHyracksTaskContext ctx, Object stateId,
            ISpillableTableFactory spillableTableFactory, RecordDescriptor partialAggRecordDesc,
            RecordDescriptor outRecordDesc, int framesLimit, int[] groupFields,
            INormalizedKeyComputerFactory nmkFactory, IBinaryComparatorFactory[] comparatorFactories,
            IAggregatorDescriptorFactory aggregatorFactory) {
        this.ctx = ctx;
        this.stateId = stateId;
        this.spillableTableFactory = spillableTableFactory;
        this.frameLimit = framesLimit;
        this.nmkComputer = nmkFactory == null ? null : nmkFactory.createNormalizedKeyComputer();

        this.partialAggRecordDesc = partialAggRecordDesc;
        this.outRecordDesc = outRecordDesc;

        this.mergeAggregatorFactory = aggregatorFactory;

        //create merge group fields
        int numGroupFields = groupFields.length;
        mergeGroupFields = new int[numGroupFields];
        for (int i = 0; i < numGroupFields; i++) {
            mergeGroupFields[i] = i;
        }

        //setup comparators for grouping
        groupByComparators = new IBinaryComparator[Math.min(mergeGroupFields.length, comparatorFactories.length)];
        for (int i = 0; i < groupByComparators.length; i++) {
            groupByComparators[i] = comparatorFactories[i].createBinaryComparator();
        }
    }

    @Override
    public void initialize() throws HyracksDataException {
        ExternalGroupState aggState = (ExternalGroupState) ctx.getStateObject(stateId);
        ISpillableTable table = aggState.getSpillableTable();
        RunFileWriter[] partitionRuns = aggState.getRuns();
        int[] numberOfTuples = aggState.getSpilledNumTuples();
        try {
            writer.open();
            doPass(table, partitionRuns, numberOfTuples, writer, 1); // level 0 use used at build stage.
        } catch (Exception e) {
            try {
                for (RunFileWriter run : generatedRuns) {
                    run.erase();
                }
            } finally {
                writer.fail();
            }
            throw e;
        } finally {
            writer.close();
        }
    }

    private void doPass(ISpillableTable table, RunFileWriter[] runs, int[] numOfTuples, IFrameWriter writer, int level)
            throws HyracksDataException {
        assert table.getNumPartitions() == runs.length;
        for (int i = 0; i < runs.length; i++) {
            if (runs[i] == null) {
                table.flushFrames(i, writer, AggregateType.FINAL);
            }
        }
        table.close();

        for (int i = 0; i < runs.length; i++) {
            if (runs[i] != null) {
                // Calculates the hash table size (# of unique hash values) based on the budget and a tuple size.
                int memoryBudgetInBytes = ctx.getInitialFrameSize() * frameLimit;
                int groupByColumnsCount = mergeGroupFields.length;
                int hashTableCardinality = ExternalGroupOperatorDescriptor.calculateGroupByTableCardinality(
                        memoryBudgetInBytes, groupByColumnsCount, ctx.getInitialFrameSize());
                hashTableCardinality = Math.min(hashTableCardinality, numOfTuples[i]);
                ISpillableTable partitionTable = spillableTableFactory.buildSpillableTable(ctx, hashTableCardinality,
                        runs[i].getFileSize(), mergeGroupFields, groupByComparators, nmkComputer,
                        mergeAggregatorFactory, partialAggRecordDesc, outRecordDesc, frameLimit, level);
                RunFileWriter[] runFileWriters = new RunFileWriter[partitionTable.getNumPartitions()];
                int[] sizeInTuplesNextLevel =
                        buildGroup(runs[i].createDeleteOnCloseReader(), partitionTable, runFileWriters);
                for (int idFile = 0; idFile < runFileWriters.length; idFile++) {
                    if (runFileWriters[idFile] != null) {
                        generatedRuns.add(runFileWriters[idFile]);
                    }
                }

                if (LOGGER.isDebugEnabled()) {
                    int numOfSpilledPart = 0;
                    for (int x = 0; x < numOfTuples.length; x++) {
                        if (numOfTuples[x] > 0) {
                            numOfSpilledPart++;
                        }
                    }
                    LOGGER.debug("level " + level + ":" + "build with " + numOfTuples.length + " partitions"
                            + ", spilled " + numOfSpilledPart + " partitions");
                }
                doPass(partitionTable, runFileWriters, sizeInTuplesNextLevel, writer, level + 1);
            }
        }
    }

    private int[] buildGroup(RunFileReader reader, ISpillableTable table, RunFileWriter[] runFileWriters)
            throws HyracksDataException {
        ExternalHashGroupBy groupBy = new ExternalHashGroupBy(this, table, runFileWriters, partialAggRecordDesc);
        reader.open();
        try {
            VSizeFrame frame = new VSizeFrame(ctx);
            while (reader.nextFrame(frame)) {
                groupBy.insert(frame.getBuffer());
            }
            groupBy.flushSpilledPartitions();
        } finally {
            reader.close();
        }
        return groupBy.getSpilledNumTuples();
    }

    @Override
    public RunFileWriter getRunFileWriter() throws HyracksDataException {
        FileReference newRun = ctx.getJobletContext()
                .createManagedWorkspaceFile(ExternalGroupOperatorDescriptor.class.getSimpleName());
        return new RunFileWriter(newRun, ctx.getIoManager());
    }
}
