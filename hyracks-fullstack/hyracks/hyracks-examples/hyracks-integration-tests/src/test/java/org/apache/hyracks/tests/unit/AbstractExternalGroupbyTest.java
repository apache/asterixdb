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

package org.apache.hyracks.tests.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.AvgFieldGroupAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.AvgFieldMergeAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public abstract class AbstractExternalGroupbyTest {

    ISerializerDeserializer[] inFields = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
            new UTF8StringSerializerDeserializer(), };

    ISerializerDeserializer[] aggrFields = new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer(), // key
            IntegerSerializerDeserializer.INSTANCE, // sum
            IntegerSerializerDeserializer.INSTANCE, // count
            FloatSerializerDeserializer.INSTANCE, // avg
    };

    RecordDescriptor inRecordDesc = new RecordDescriptor(inFields);

    RecordDescriptor outputRec = new RecordDescriptor(aggrFields);

    IBinaryComparatorFactory[] comparatorFactories =
            new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE };

    INormalizedKeyComputerFactory normalizedKeyComputerFactory = new UTF8StringNormalizedKeyComputerFactory();

    IAggregatorDescriptorFactory partialAggrInPlace = new MultiFieldsAggregatorFactory(
            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(0, false),
                    new CountFieldAggregatorFactory(false), new AvgFieldGroupAggregatorFactory(0, false) });

    IAggregatorDescriptorFactory finalAggrInPlace = new MultiFieldsAggregatorFactory(
            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false),
                    new IntSumFieldAggregatorFactory(2, false), new AvgFieldMergeAggregatorFactory(3, false) });

    IAggregatorDescriptorFactory partialAggrInState = new MultiFieldsAggregatorFactory(
            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(0, true),
                    new CountFieldAggregatorFactory(true), new AvgFieldGroupAggregatorFactory(0, true) });

    IAggregatorDescriptorFactory finalAggrInState = new MultiFieldsAggregatorFactory(
            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, true),
                    new IntSumFieldAggregatorFactory(2, true), new AvgFieldMergeAggregatorFactory(3, true) });

    int[] keyFields = new int[] { 1 };
    int[] keyFieldsAfterPartial = new int[] { 0 };

    class ResultValidateWriter implements IFrameWriter {

        final Map<Integer, String> keyValueMap;
        FrameTupleAccessor resultAccessor = new FrameTupleAccessor(outputRec);

        class Result {
            Result(int i) {
                sum = i;
                count = 1;
            }

            int sum;
            int count;
        }

        private Map<String, Result> answer;

        public ResultValidateWriter(Map<Integer, String> keyValueMap) {
            this.keyValueMap = keyValueMap;
            answer = new HashMap<>();
        }

        @Override
        public void open() throws HyracksDataException {
            keyValueMap.forEach((key, value) -> {
                Result result = answer.get(value);
                if (result == null) {
                    answer.put(value, new Result(key));
                } else {
                    result.sum += key;
                    result.count++;
                }
            });
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            resultAccessor.reset(buffer);
            ByteBufferInputStream bbis = new ByteBufferInputStream();
            DataInputStream di = new DataInputStream(bbis);

            Object[] outRecord = new Object[outputRec.getFieldCount()];

            for (int tid = 0; tid < resultAccessor.getTupleCount(); tid++) {
                for (int fid = 0; fid < outputRec.getFieldCount(); fid++) {
                    bbis.setByteBuffer(resultAccessor.getBuffer(),
                            resultAccessor.getAbsoluteFieldStartOffset(tid, fid));
                    outRecord[fid] = outputRec.getFields()[fid].deserialize(di);
                }
                Result result = answer.remove(outRecord[0]);
                assertNotNull(result);
                assertEquals(result.sum, (int) outRecord[1]);
                assertEquals(result.count, (int) outRecord[2]);
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            Assert.fail();
        }

        @Override
        public void close() throws HyracksDataException {
            assertEquals(0, answer.size());
        }
    }

    @Test
    public void testBuildAndMergeNormalFrameInMem() throws HyracksDataException {
        int tableSize = 101;
        int numFrames = 23;
        int frameSize = 256;
        int minDataSize = frameSize;
        int minRecordSize = 20;
        int maxRecordSize = 50;
        testBuildAndMerge(tableSize, numFrames, frameSize, minDataSize, minRecordSize, maxRecordSize, null);
    }

    @Test
    public void testBuildAndMergeNormalFrameSpill() throws HyracksDataException {
        int tableSize = 101;
        int numFrames = 23;
        int frameSize = 256;
        int minDataSize = frameSize * 40;
        int minRecordSize = 20;
        int maxRecordSize = 50;
        testBuildAndMerge(tableSize, numFrames, frameSize, minDataSize, minRecordSize, maxRecordSize, null);
    }

    @Test
    public void testBuildAndMergeBigObj() throws HyracksDataException {
        int tableSize = 101;
        int numFrames = 23;
        int frameSize = 256;
        int minDataSize = frameSize * 40;
        int minRecordSize = 20;
        int maxRecordSize = 50;
        HashMap<Integer, String> bigRecords = AbstractRunGeneratorTest.generateBigObject(frameSize, 3);
        testBuildAndMerge(tableSize, numFrames, frameSize, minDataSize, minRecordSize, maxRecordSize, bigRecords);
    }

    protected abstract void initial(IHyracksTaskContext ctx, int tableSize, int numFrames) throws HyracksDataException;

    protected abstract IFrameWriter getBuilder();

    protected abstract IOperatorNodePushable getMerger();

    private void testBuildAndMerge(int tableSize, int numFrames, int frameSize, int minDataSize, int minRecordSize,
            int maxRecordSize, Map<Integer, String> specialData) throws HyracksDataException {

        IHyracksTaskContext ctx = TestUtils.create(frameSize);
        initial(ctx, tableSize, numFrames);
        ArrayList<IFrame> input = new ArrayList<>();
        Map<Integer, String> keyValueMap = new HashMap<>();
        AbstractRunGeneratorTest.prepareData(ctx, input, minDataSize, minRecordSize, maxRecordSize, specialData,
                keyValueMap);

        ResultValidateWriter writer = new ResultValidateWriter(keyValueMap);

        try {
            getBuilder().open();
            for (IFrame frame : input) {
                getBuilder().nextFrame(frame.getBuffer());
            }
        } finally {
            getBuilder().close();
        }
        getMerger().setOutputFrameWriter(0, writer, outputRec);
        getMerger().initialize();
    }

}
