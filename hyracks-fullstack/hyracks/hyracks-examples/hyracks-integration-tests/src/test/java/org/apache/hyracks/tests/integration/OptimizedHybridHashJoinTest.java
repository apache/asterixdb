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
package org.apache.hyracks.tests.integration;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Random;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoin;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class OptimizedHybridHashJoinTest {
    int frameSize = 32768;
    int totalNumberOfFrames = 10;
    IHyracksTaskContext ctx = TestUtils.create(frameSize);
    OptimizedHybridHashJoin hhj;
    static IBinaryHashFunctionFamily[] propHashFunctionFactories = { MurmurHash3BinaryHashFunctionFamily.INSTANCE };
    static IBinaryHashFunctionFamily[] buildHashFunctionFactories = { MurmurHash3BinaryHashFunctionFamily.INSTANCE };
    static String probeRelName = "RelR";
    static String buildRelName = "RelS";
    static ITuplePairComparator comparator;
    static RecordDescriptor probeRd;
    static RecordDescriptor buildRd;
    static ITuplePartitionComputer probeHpc;
    static ITuplePartitionComputer buildHpc;
    static IPredicateEvaluator predEval;
    int memSizeInFrames = -1;
    int numOfPartitions = -1;
    boolean isLeftOuter = false;
    static int[] probeKeys = { 0 };
    static int[] buildKeys = { 0 };
    private final Random rnd = new Random(50);

    @BeforeClass
    public static void classSetUp() {
        comparator = Mockito.mock(ITuplePairComparator.class);
        probeHpc = new FieldHashPartitionComputerFamily(probeKeys, propHashFunctionFactories).createPartitioner(0);
        buildHpc = new FieldHashPartitionComputerFamily(buildKeys, buildHashFunctionFactories).createPartitioner(0);
    }

    @Test
    public void SmallRecords_AllRelationsInMemory() throws HyracksDataException {

        VSizeFrame frame = new VSizeFrame(ctx, ctx.getInitialFrameSize());
        generateIntFrame(frame);
        probeRd = new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
        buildRd = new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
        memSizeInFrames = 55;
        numOfPartitions = 5;
        testJoin(memSizeInFrames, numOfPartitions, frame);
    }

    @Test
    public void SmallRecords_SomeRelationsInMemory() throws HyracksDataException {

        VSizeFrame frame = new VSizeFrame(ctx, ctx.getInitialFrameSize());
        generateIntFrame(frame);
        probeRd = new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
        buildRd = new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
        memSizeInFrames = 40;
        numOfPartitions = 5;
        testJoin(memSizeInFrames, numOfPartitions, frame);
    }

    @Test
    public void SmallRecords_AllRelationsSpill() throws HyracksDataException {

        VSizeFrame frame = new VSizeFrame(ctx, ctx.getInitialFrameSize());
        generateIntFrame(frame);
        probeRd = new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
        buildRd = new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE });
        memSizeInFrames = 5;
        numOfPartitions = 5;
        testJoin(memSizeInFrames, numOfPartitions, frame);
    }

    @Test
    public void LargeRecords_AllRelationsInMemory() throws HyracksDataException {

        VSizeFrame frame = new VSizeFrame(ctx, ctx.getInitialFrameSize());
        generateStringFrame(frame, ctx.getInitialFrameSize() * 3);
        probeRd = new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer() });
        buildRd = new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer() });
        memSizeInFrames = 55;
        numOfPartitions = 5;
        testJoin(memSizeInFrames, numOfPartitions, frame);
    }

    @Test
    public void LargeRecords_SomeRelationsInMemory() throws HyracksDataException {

        VSizeFrame frame = new VSizeFrame(ctx, ctx.getInitialFrameSize());
        generateStringFrame(frame, ctx.getInitialFrameSize() * 3);
        probeRd = new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer() });
        buildRd = new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer() });
        memSizeInFrames = 40;
        numOfPartitions = 5;
        testJoin(memSizeInFrames, numOfPartitions, frame);
    }

    @Test
    public void LargeRecords_AllRelationsSpill() throws HyracksDataException {

        VSizeFrame frame = new VSizeFrame(ctx, ctx.getInitialFrameSize());
        generateStringFrame(frame, ctx.getInitialFrameSize() * 3);
        probeRd = new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer() });
        buildRd = new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer() });
        memSizeInFrames = 5;
        numOfPartitions = 5;
        testJoin(memSizeInFrames, numOfPartitions, frame);
    }

    private void testJoin(int memSizeInFrames, int numOfPartitions, VSizeFrame frame) throws HyracksDataException {

        hhj = new OptimizedHybridHashJoin(ctx, memSizeInFrames, numOfPartitions, probeRelName, buildRelName, comparator,
                probeRd, buildRd, probeHpc, buildHpc, predEval, isLeftOuter, null);

        hhj.initBuild();

        for (int i = 0; i < totalNumberOfFrames; i++) {
            hhj.build(frame.getBuffer());
        }

        hhj.closeBuild();
        checkOneFrameReservedPerSpilledPartitions();
        IFrameWriter writer = new IFrameWriter() {
            @Override
            public void open() throws HyracksDataException {
                //Not implemented as this method is only used to pass the frames of the given partition
                //to the in memory joiner. As such, only next frame is important.
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            }

            @Override
            public void fail() throws HyracksDataException {
                //Not implemented as this method is only used to pass the frames of the given partition
                //to the in memory joiner. As such, only next frame is important.
            }

            @Override
            public void close() throws HyracksDataException {
                //Not implemented as this method is only used to pass the frames of the given partition
                //to the in memory joiner. As such, only next frame is important.
            }
        };
        hhj.initProbe();
        for (int i = 0; i < totalNumberOfFrames; i++) {
            hhj.probe(frame.getBuffer(), writer);
            checkOneFrameReservedPerSpilledPartitions();
        }

    }

    private void checkOneFrameReservedPerSpilledPartitions() throws HyracksDataException {
        //Make sure that there is one frame reserved for each spilled partition.
        int frameSize = ctx.getInitialFrameSize();
        int totalSize = 0;
        int inMemTuples = 0;
        BitSet spilledStatus = hhj.getPartitionStatus();
        for (int i = spilledStatus.nextClearBit(0); i >= 0 && i < numOfPartitions; i =
                spilledStatus.nextClearBit(i + 1)) {
            totalSize += hhj.getPartitionSize(i);
            inMemTuples += hhj.getBuildPartitionSizeInTup(i);
        }
        if (memSizeInFrames * frameSize - totalSize
                - SerializableHashTable.getExpectedTableByteSize(inMemTuples, frameSize) < spilledStatus.cardinality()
                        * frameSize) {
            throw new HyracksDataException("There should be at least one frame reserved for each spilled partition.");
        }
    }

    private void generateIntFrame(VSizeFrame frame) throws HyracksDataException {
        int fieldCount = 1;
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(frame, true);
        while (appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            TupleUtils.createIntegerTuple(tb, tuple, rnd.nextInt());
            tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        }
    }

    private void generateStringFrame(VSizeFrame frame, int length) throws HyracksDataException {
        int fieldCount = 1;
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(frame, true);
        ISerializerDeserializer[] fieldSerdes = { new UTF8StringSerializerDeserializer() };
        String data = "";
        for (int i = 0; i < length; i++) {
            data += "X";
        }
        TupleUtils.createTuple(tb, tuple, fieldSerdes, data);
        tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
    }
}
