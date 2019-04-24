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

import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.sort.AbstractSortRunGenerator;
import org.apache.hyracks.dataflow.std.sort.util.GroupFrameAccessor;
import org.apache.hyracks.dataflow.std.sort.util.GroupVSizeFrame;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Test;

public abstract class AbstractRunGeneratorTest {
    static TestUtils testUtils = new TestUtils();
    static ISerializerDeserializer[] SerDers = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
            new UTF8StringSerializerDeserializer() };
    static RecordDescriptor RecordDesc = new RecordDescriptor(SerDers);
    static Random GRandom = new Random(0);
    static int[] SortFields = new int[] { 0, 1 };
    static IBinaryComparatorFactory[] ComparatorFactories = new IBinaryComparatorFactory[] {
            IntegerBinaryComparatorFactory.INSTANCE, UTF8StringBinaryComparatorFactory.INSTANCE };

    static void assertMaxFrameSizesAreAllEqualsTo(List<GeneratedRunFileReader> maxSize, int pageSize) {
        for (int i = 0; i < maxSize.size(); i++) {
            assertTrue(maxSize.get(i).getMaxFrameSize() == pageSize);
        }
    }

    abstract AbstractSortRunGenerator[] getSortRunGenerator(IHyracksTaskContext ctx, int frameLimit,
            int numOfInputRecord) throws HyracksDataException;

    protected List<List<GeneratedRunFileReader>> testSortRecords(int pageSize, int frameLimit, int numRuns,
            int minRecordSize, int maxRecordSize, HashMap<Integer, String> specialData) throws HyracksDataException {
        IHyracksTaskContext ctx = testUtils.create(pageSize);

        HashMap<Integer, String> keyValuePair = new HashMap<>();
        List<IFrame> frameList = new ArrayList<>();
        prepareData(ctx, frameList, pageSize * frameLimit * numRuns, minRecordSize, maxRecordSize, specialData,
                keyValuePair);

        List<List<GeneratedRunFileReader>> results = new ArrayList<>();
        AbstractSortRunGenerator[] runGenerators = getSortRunGenerator(ctx, frameLimit, keyValuePair.size());
        for (AbstractSortRunGenerator runGenerator : runGenerators) {
            runGenerator.open();
            for (IFrame frame : frameList) {
                runGenerator.nextFrame(frame.getBuffer());
            }
            runGenerator.close();
            matchResult(ctx, runGenerator.getRuns(), keyValuePair);
            results.add(runGenerator.getRuns());
        }
        return results;
    }

    static void matchResult(IHyracksTaskContext ctx, List<GeneratedRunFileReader> runs,
            Map<Integer, String> keyValuePair) throws HyracksDataException {
        HashMap<Integer, String> copyMap2 = new HashMap<>(keyValuePair);
        int maxFrameSizes = 0;
        for (GeneratedRunFileReader run : runs) {
            maxFrameSizes = Math.max(maxFrameSizes, run.getMaxFrameSize());
        }
        GroupVSizeFrame gframe = new GroupVSizeFrame(ctx, maxFrameSizes);
        GroupFrameAccessor gfta = new GroupFrameAccessor(ctx.getInitialFrameSize(), RecordDesc);
        assertReadSorted(runs, gfta, gframe, copyMap2);
    }

    static int assertFTADataIsSorted(IFrameTupleAccessor fta, Map<Integer, String> keyValuePair, int preKey)
            throws HyracksDataException {

        ByteBufferInputStream bbis = new ByteBufferInputStream();
        DataInputStream di = new DataInputStream(bbis);
        for (int i = 0; i < fta.getTupleCount(); i++) {
            bbis.setByteBuffer(fta.getBuffer(),
                    fta.getTupleStartOffset(i) + fta.getFieldStartOffset(i, 0) + fta.getFieldSlotsLength());
            int key = (int) RecordDesc.getFields()[0].deserialize(di);
            bbis.setByteBuffer(fta.getBuffer(),
                    fta.getTupleStartOffset(i) + fta.getFieldStartOffset(i, 1) + fta.getFieldSlotsLength());
            String value = (String) RecordDesc.getFields()[1].deserialize(di);
            if (!keyValuePair.containsKey(key)) {
                assertTrue(false);
            }
            if (!keyValuePair.get(key).equals(value)) {
                assertTrue(false);
            }
            keyValuePair.remove(key);
            assertTrue(key >= preKey);
            preKey = key;
        }
        return preKey;
    }

    static void assertReadSorted(List<GeneratedRunFileReader> runs, IFrameTupleAccessor fta, IFrame frame,
            Map<Integer, String> keyValuePair) throws HyracksDataException {

        assertTrue(runs.size() > 0);
        for (GeneratedRunFileReader run : runs) {
            try {
                run.open();
                int preKey = Integer.MIN_VALUE;
                while (run.nextFrame(frame)) {
                    fta.reset(frame.getBuffer());
                    preKey = assertFTADataIsSorted(fta, keyValuePair, preKey);
                }
            } finally {
                run.close();
            }
        }
        assertTrue(keyValuePair.isEmpty());
    }

    static void prepareData(IHyracksTaskContext ctx, List<IFrame> frameList, int minDataSize, int minRecordSize,
            int maxRecordSize, Map<Integer, String> specialData, Map<Integer, String> keyValuePair)
            throws HyracksDataException {

        ArrayTupleBuilder tb = new ArrayTupleBuilder(RecordDesc.getFieldCount());
        FrameTupleAppender appender = new FrameTupleAppender();

        int datasize = 0;
        if (specialData != null) {
            for (Map.Entry<Integer, String> entry : specialData.entrySet()) {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, entry.getKey());
                tb.addField(new UTF8StringSerializerDeserializer(), entry.getValue());

                VSizeFrame frame =
                        new VSizeFrame(ctx, FrameHelper.calcAlignedFrameSizeToStore(tb.getFieldEndOffsets().length,
                                tb.getSize(), ctx.getInitialFrameSize()));
                appender.reset(frame, true);
                assertTrue(appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize()));
                frameList.add(frame);
                datasize += frame.getFrameSize();
            }
            keyValuePair.putAll(specialData);
        }

        VSizeFrame frame = new VSizeFrame(ctx, ctx.getInitialFrameSize());
        appender.reset(frame, true);
        while (datasize < minDataSize) {
            tb.reset();
            int key = GRandom.nextInt(minDataSize + 1);
            if (!keyValuePair.containsKey(key)) {
                String value = generateRandomRecord(minRecordSize, maxRecordSize);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, key);
                tb.addField(new UTF8StringSerializerDeserializer(), value);

                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    frameList.add(frame);
                    datasize += frame.getFrameSize();
                    frame = new VSizeFrame(ctx, FrameHelper.calcAlignedFrameSizeToStore(tb.getFieldEndOffsets().length,
                            tb.getSize(), ctx.getInitialFrameSize()));
                    appender.reset(frame, true);
                    assertTrue(appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize()));
                }

                keyValuePair.put(key, value);
            }
        }
        if (appender.getTupleCount() > 0) {
            frameList.add(frame);
        }

    }

    static String generateRandomRecord(int minRecordSize, int maxRecordSize) throws HyracksDataException {
        int size = GRandom.nextInt(maxRecordSize - minRecordSize + 1) + minRecordSize;
        return generateRandomFixSizedString(size);

    }

    static String generateRandomFixSizedString(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (; size >= 0; --size) {
            char ch = (char) (GRandom.nextInt(26) + 97);
            sb.append(ch);
        }
        return sb.toString();
    }

    static HashMap<Integer, String> generateBigObject(int pageSize, int times) {
        HashMap<Integer, String> map = new HashMap<>(1);
        for (int i = 1; i < times; i++) {
            map.put(GRandom.nextInt(), generateRandomFixSizedString(pageSize * i));
        }
        return map;
    }

    @Test
    public void testAllSmallRecords() throws HyracksDataException {
        int pageSize = 512;
        int frameLimit = 4;
        int numRuns = 2;
        int minRecordSize = pageSize / 8;
        int maxRecordSize = pageSize / 8;
        List<List<GeneratedRunFileReader>> maxSizes =
                testSortRecords(pageSize, frameLimit, numRuns, minRecordSize, maxRecordSize, null);
        for (List<GeneratedRunFileReader> maxSize : maxSizes) {
            assertMaxFrameSizesAreAllEqualsTo(maxSize, pageSize);
        }
    }

    @Test
    public void testAllManySmallRecords() throws HyracksDataException {
        int pageSize = 10240;
        int frameLimit = 4;
        int numRuns = 2;
        int minRecordSize = pageSize / 8;
        int maxRecordSize = pageSize / 8;
        List<List<GeneratedRunFileReader>> maxSizes =
                testSortRecords(pageSize, frameLimit, numRuns, minRecordSize, maxRecordSize, null);
        for (List<GeneratedRunFileReader> maxSize : maxSizes) {
            assertMaxFrameSizesAreAllEqualsTo(maxSize, pageSize);
        }
    }

    @Test
    public void testAllLargeRecords() throws HyracksDataException {
        int pageSize = 2048;
        int frameLimit = 4;
        int numRuns = 2;
        int minRecordSize = pageSize;
        int maxRecordSize = (int) (pageSize * 1.8);
        List<List<GeneratedRunFileReader>> maxSizes =
                testSortRecords(pageSize, frameLimit, numRuns, minRecordSize, maxRecordSize, null);
        for (List<GeneratedRunFileReader> maxSize : maxSizes) {
            assertMaxFrameSizesAreAllEqualsTo(maxSize, pageSize * 2);
        }
    }

    @Test
    public void testMixedLargeRecords() throws HyracksDataException {
        int pageSize = 128;
        int frameLimit = 4;
        int numRuns = 4;
        int minRecordSize = 20;
        int maxRecordSize = pageSize / 2;
        HashMap<Integer, String> specialPair = generateBigObject(pageSize / 2, frameLimit - 1);
        List<List<GeneratedRunFileReader>> sizes =
                testSortRecords(pageSize, frameLimit, numRuns, minRecordSize, maxRecordSize, specialPair);
        for (List<GeneratedRunFileReader> size : sizes) {
            int max = 0;
            for (GeneratedRunFileReader run : size) {
                max = Math.max(max, run.getMaxFrameSize());
            }
            assertTrue(max <= pageSize * (frameLimit - 1) && max >= pageSize * 2);
        }
    }

    @Test(expected = HyracksDataException.class)
    public void testTooBigRecordWillThrowException() throws HyracksDataException {
        int pageSize = 1024;
        int frameLimit = 8;
        int numRuns = 8;
        HashMap<Integer, String> specialPair = generateBigObject(pageSize, frameLimit);
        int minRecordSize = 10;
        int maxRecordSize = pageSize / 2;
        testSortRecords(pageSize, frameLimit, numRuns, minRecordSize, maxRecordSize, specialPair);
    }
}
