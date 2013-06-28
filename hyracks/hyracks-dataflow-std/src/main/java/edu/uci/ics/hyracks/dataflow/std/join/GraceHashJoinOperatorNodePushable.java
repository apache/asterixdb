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
package edu.uci.ics.hyracks.dataflow.std.join;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.RepartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.structures.ISerializableTable;
import edu.uci.ics.hyracks.dataflow.std.structures.SerializableHashTable;

class GraceHashJoinOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {
    private final IHyracksTaskContext ctx;
    private final Object state0Id;
    private final Object state1Id;
    private final int[] keys0;
    private final int[] keys1;
    private final IBinaryHashFunctionFactory[] hashFunctionFactories;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final INullWriterFactory[] nullWriterFactories;
    private final RecordDescriptor rd0;
    private final RecordDescriptor rd1;
    private final int recordsPerFrame;
    private final double factor;
    private final int numPartitions;
    private final boolean isLeftOuter;
    private final IPredicateEvaluator predEvaluator;

    GraceHashJoinOperatorNodePushable(IHyracksTaskContext ctx, Object state0Id, Object state1Id, int recordsPerFrame,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionFactory[] hashFunctionFactories,
            IBinaryComparatorFactory[] comparatorFactories, INullWriterFactory[] nullWriterFactories,
            RecordDescriptor rd1, RecordDescriptor rd0, RecordDescriptor outRecordDescriptor, int numPartitions,
            IPredicateEvaluator predEval, boolean isLeftOuter) {
        this.ctx = ctx;
        this.state0Id = state0Id;
        this.state1Id = state1Id;
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.hashFunctionFactories = hashFunctionFactories;
        this.comparatorFactories = comparatorFactories;
        this.nullWriterFactories = nullWriterFactories;
        this.rd0 = rd0;
        this.rd1 = rd1;
        this.numPartitions = numPartitions;
        this.recordsPerFrame = recordsPerFrame;
        this.factor = factor;
        this.predEvaluator = predEval;
        this.isLeftOuter = isLeftOuter;
    }

    @Override
    public void initialize() throws HyracksDataException {
        GraceHashJoinPartitionState rState = (GraceHashJoinPartitionState) ctx.getStateObject(state0Id);
        GraceHashJoinPartitionState sState = (GraceHashJoinPartitionState) ctx.getStateObject(state1Id);
        RunFileWriter[] buildWriters = sState.getRunWriters();
        RunFileWriter[] probeWriters = rState.getRunWriters();

        IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        ITuplePartitionComputer hpcRep0 = new RepartitionComputerFactory(numPartitions,
                new FieldHashPartitionComputerFactory(keys0, hashFunctionFactories)).createPartitioner();
        ITuplePartitionComputer hpcRep1 = new RepartitionComputerFactory(numPartitions,
                new FieldHashPartitionComputerFactory(keys1, hashFunctionFactories)).createPartitioner();

        final INullWriter[] nullWriters1 = isLeftOuter ? new INullWriter[nullWriterFactories.length] : null;
        if (isLeftOuter) {
            for (int i = 0; i < nullWriterFactories.length; i++) {
                nullWriters1[i] = nullWriterFactories[i].createNullWriter();
            }
        }

        writer.open();// open for probe

        try {

            ByteBuffer buffer = ctx.allocateFrame();// input
            // buffer
            int tableSize = (int) (numPartitions * recordsPerFrame * factor);
            ISerializableTable table = new SerializableHashTable(tableSize, ctx);

            for (int partitionid = 0; partitionid < numPartitions; partitionid++) {
                RunFileWriter buildWriter = buildWriters[partitionid];
                RunFileWriter probeWriter = probeWriters[partitionid];
                if ((buildWriter == null && !isLeftOuter) || probeWriter == null) {
                    continue;
                }
                table.reset();
                InMemoryHashJoin joiner = new InMemoryHashJoin(ctx, tableSize, new FrameTupleAccessor(
                        ctx.getFrameSize(), rd0), hpcRep0, new FrameTupleAccessor(ctx.getFrameSize(), rd1), hpcRep1,
                        new FrameTuplePairComparator(keys0, keys1, comparators), isLeftOuter, nullWriters1, table, predEvaluator);

                // build
                if (buildWriter != null) {
                    RunFileReader buildReader = buildWriter.createReader();
                    buildReader.open();
                    while (buildReader.nextFrame(buffer)) {
                        ByteBuffer copyBuffer = ctx.allocateFrame();
                        FrameUtils.copy(buffer, copyBuffer);
                        joiner.build(copyBuffer);
                        buffer.clear();
                    }
                    buildReader.close();
                }

                // probe
                RunFileReader probeReader = probeWriter.createReader();
                probeReader.open();
                while (probeReader.nextFrame(buffer)) {
                    joiner.join(buffer, writer);
                    buffer.clear();
                }
                probeReader.close();
                joiner.closeJoin(writer);
            }
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }
}