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

import static org.apache.hyracks.tests.unit.AbstractRunGeneratorTest.*;
import static org.junit.Assert.*;

import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.sort.Algorithm;
import org.apache.hyracks.dataflow.std.sort.ExternalSortRunGenerator;
import org.apache.hyracks.dataflow.std.sort.RunMergingFrameReader;
import org.apache.hyracks.dataflow.std.sort.util.GroupVSizeFrame;
import org.junit.Test;

import junit.extensions.PA;

public class RunMergingFrameReaderTest {
    static IBinaryComparator[] Comparators = new IBinaryComparator[] { ComparatorFactories[0].createBinaryComparator(),
            ComparatorFactories[1].createBinaryComparator(), };

    static class TestFrameReader implements IFrameReader {

        private final int pageSize;
        private final int numFrames;
        private final int minRecordSize;
        private final int maxRecordSize;
        private final TreeMap<Integer, String> result = new TreeMap<>();
        int maxFrameSize;

        ArrayTupleBuilder tb = new ArrayTupleBuilder(RecordDesc.getFieldCount());
        FrameTupleAppender appender = new FrameTupleAppender();
        private Iterator<Map.Entry<Integer, String>> iterator;
        private Map.Entry<Integer, String> lastEntry;

        TestFrameReader(int pageSize, int numFrames, int minRecordSize, int maxRecordSize) {
            this.pageSize = pageSize;
            this.numFrames = numFrames;
            this.minRecordSize = minRecordSize;
            this.maxRecordSize = maxRecordSize;
            this.maxFrameSize = pageSize;
        }

        @Override
        public void open() throws HyracksDataException {
            result.clear();
            int maxTupleSize = prepareSortedData(numFrames * pageSize, minRecordSize, maxRecordSize, null, result);
            maxFrameSize = FrameHelper.calcAlignedFrameSizeToStore(0, maxTupleSize, pageSize);
            iterator = result.entrySet().iterator();
        }

        @Override
        public boolean nextFrame(IFrame frame) throws HyracksDataException {
            if (lastEntry == null && !iterator.hasNext()) {
                return false;
            }
            if (lastEntry == null) {
                lastEntry = iterator.next();
            }
            appender.reset(frame, true);
            while (true) {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, lastEntry.getKey());
                tb.addField(new UTF8StringSerializerDeserializer(), lastEntry.getValue());
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    break;
                } else {
                    if (iterator.hasNext()) {
                        lastEntry = iterator.next();
                    } else {
                        lastEntry = null;
                        break;
                    }

                }
            }
            //            printFrame(frame.getBuffer());
            return true;
        }

        @Override
        public void close() throws HyracksDataException {
        }
    }

    static int prepareSortedData(int minDataSize, int minRecordSize, int maxRecordSize,
            Map<Integer, String> specialData, Map<Integer, String> result) throws HyracksDataException {

        ArrayTupleBuilder tb = new ArrayTupleBuilder(RecordDesc.getFieldCount());

        int datasize = 0;
        int maxtuple = 0;
        if (specialData != null) {
            for (Map.Entry<Integer, String> entry : specialData.entrySet()) {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, entry.getKey());
                tb.addField(new UTF8StringSerializerDeserializer(), entry.getValue());
                int size = tb.getSize() + tb.getFieldEndOffsets().length * 4;
                datasize += size;
                if (size > maxtuple) {
                    maxtuple = size;
                }
            }
            result.putAll(specialData);
        }

        while (datasize < minDataSize) {
            String value = generateRandomRecord(minRecordSize, maxRecordSize);
            tb.reset();
            int key = GRandom.nextInt(datasize + 1);
            if (!result.containsKey(key)) {
                tb.addField(IntegerSerializerDeserializer.INSTANCE, key);
                tb.addField(new UTF8StringSerializerDeserializer(), value);
                int size = tb.getSize() + tb.getFieldEndOffsets().length * 4;
                datasize += size;
                if (size > maxtuple) {
                    maxtuple = size;
                }
                if (datasize < minDataSize) {
                    result.put(key, value);
                }
            }
        }

        return maxtuple;
    }

    @Test
    public void testOnlyOneRunShouldMerge() throws HyracksDataException {
        int pageSize = 128;
        int numRuns = 1;
        int numFramesPerRun = 1;
        int minRecordSize = pageSize / 10;
        int maxRecordSize = pageSize / 8;

        IHyracksTaskContext ctx = testUtils.create(pageSize);
        List<Map<Integer, String>> keyValueMapList = new ArrayList<>(numRuns);
        List<TestFrameReader> readerList = new ArrayList<>(numRuns);
        List<IFrame> frameList = new ArrayList<>(numRuns);
        prepareRandomInputRunList(ctx, pageSize, numRuns, numFramesPerRun, minRecordSize, maxRecordSize, readerList,
                frameList, keyValueMapList);

        RunMergingFrameReader reader =
                new RunMergingFrameReader(ctx, readerList, frameList, SortFields, Comparators, null, RecordDesc);
        testMergeSucceed(ctx, reader, keyValueMapList);
    }

    @Test
    public void testNormalRunMerge() throws HyracksDataException {

        int pageSize = 128;
        int numRuns = 2;
        int numFramesPerRun = 2;
        int minRecordSize = pageSize / 10;
        int maxRecordSize = pageSize / 8;

        IHyracksTaskContext ctx = testUtils.create(pageSize);
        List<Map<Integer, String>> keyValueMapList = new ArrayList<>(numRuns);
        List<TestFrameReader> readerList = new ArrayList<>(numRuns);
        List<IFrame> frameList = new ArrayList<>(numRuns);
        prepareRandomInputRunList(ctx, pageSize, numRuns, numFramesPerRun, minRecordSize, maxRecordSize, readerList,
                frameList, keyValueMapList);

        RunMergingFrameReader reader =
                new RunMergingFrameReader(ctx, readerList, frameList, SortFields, Comparators, null, RecordDesc);
        testMergeSucceed(ctx, reader, keyValueMapList);
    }

    @Test
    public void testNormalRunMergeWithTopK() throws HyracksDataException {

        int pageSize = 128;
        int numRuns = 2;
        int numFramesPerRun = 2;
        int minRecordSize = pageSize / 10;
        int maxRecordSize = pageSize / 8;

        for (int topK = 1; topK < pageSize * numRuns * numFramesPerRun / maxRecordSize / 2; topK++) {
            IHyracksTaskContext ctx = testUtils.create(pageSize);
            List<Map<Integer, String>> keyValueMapList = new ArrayList<>(numRuns);
            List<TestFrameReader> readerList = new ArrayList<>(numRuns);
            List<IFrame> frameList = new ArrayList<>(numRuns);
            prepareRandomInputRunList(ctx, pageSize, numRuns, numFramesPerRun, minRecordSize, maxRecordSize, readerList,
                    frameList, keyValueMapList);

            RunMergingFrameReader reader = new RunMergingFrameReader(ctx, readerList, frameList, SortFields,
                    Comparators, null, RecordDesc, topK);
            int totoalCount = testMergeSucceedInner(ctx, reader, keyValueMapList);
            int newCount = 0;
            for (Map<Integer, String> x : keyValueMapList) {
                newCount += x.size();
            }
            assertEquals(topK + newCount, totoalCount);
        }
    }

    private void testMergeSucceed(IHyracksTaskContext ctx, RunMergingFrameReader reader,
            List<Map<Integer, String>> keyValueMapList) throws HyracksDataException {

        testMergeSucceedInner(ctx, reader, keyValueMapList);
        assertAllKeyValueIsConsumed(keyValueMapList);
        reader.close();
    }

    private int testMergeSucceedInner(IHyracksTaskContext ctx, RunMergingFrameReader reader,
            List<Map<Integer, String>> keyValueMapList) throws HyracksDataException {

        IFrame frame = new VSizeFrame(ctx);
        reader.open();
        int count = 0;
        for (int i = 0; i < keyValueMapList.size(); i++) {
            keyValueMapList.set(i, new TreeMap<>(keyValueMapList.get(i)));
            count += keyValueMapList.get(i).size();
        }
        while (reader.nextFrame(frame)) {
            assertFrameIsSorted(frame, keyValueMapList);
        }
        return count;
    }

    @Test
    public void testOneLargeRunMerge() throws HyracksDataException {
        int pageSize = 64;
        int numRuns = 2;
        int numFramesPerRun = 1;
        int minRecordSize = pageSize / 10;
        int maxRecordSize = pageSize / 8;

        IHyracksTaskContext ctx = testUtils.create(pageSize);
        List<Map<Integer, String>> keyValueMap = new ArrayList<>();
        List<TestFrameReader> readerList = new ArrayList<>();
        List<IFrame> frameList = new ArrayList<>();
        prepareRandomInputRunList(ctx, pageSize, numRuns, numFramesPerRun, minRecordSize, maxRecordSize, readerList,
                frameList, keyValueMap);

        minRecordSize = pageSize;
        maxRecordSize = pageSize;
        numFramesPerRun = 4;
        prepareRandomInputRunList(ctx, pageSize, numRuns, numFramesPerRun, minRecordSize, maxRecordSize, readerList,
                frameList, keyValueMap);

        minRecordSize = pageSize * 2;
        maxRecordSize = pageSize * 2;
        numFramesPerRun = 6;
        prepareRandomInputRunList(ctx, pageSize, numRuns, numFramesPerRun, minRecordSize, maxRecordSize, readerList,
                frameList, keyValueMap);

        RunMergingFrameReader reader =
                new RunMergingFrameReader(ctx, readerList, frameList, SortFields, Comparators, null, RecordDesc);
        testMergeSucceed(ctx, reader, keyValueMap);
    }

    @Test
    public void testRunFileReader() throws HyracksDataException {
        int pageSize = 128;
        int numRuns = 4;
        int numFramesPerRun = 4;
        int minRecordSize = pageSize / 10;
        int maxRecordSize = pageSize / 2;

        IHyracksTaskContext ctx = testUtils.create(pageSize);
        ExternalSortRunGenerator runGenerator = new ExternalSortRunGenerator(ctx, SortFields, null, ComparatorFactories,
                RecordDesc, Algorithm.MERGE_SORT, numFramesPerRun);

        runGenerator.open();
        Map<Integer, String> keyValuePair = new HashMap<>();
        List<IFrame> frameList = new ArrayList<>();
        prepareData(ctx, frameList, pageSize * numFramesPerRun * numRuns, minRecordSize, maxRecordSize, null,
                keyValuePair);
        for (IFrame frame : frameList) {
            runGenerator.nextFrame(frame.getBuffer());
        }

        numFramesPerRun = 2;
        minRecordSize = pageSize;
        maxRecordSize = pageSize;
        frameList.clear();
        prepareData(ctx, frameList, pageSize * numFramesPerRun * numRuns, minRecordSize, maxRecordSize, null,
                keyValuePair);
        for (IFrame frame : frameList) {
            runGenerator.nextFrame(frame.getBuffer());
        }
        runGenerator.close();
        List<IFrame> inFrame = new ArrayList<>(runGenerator.getRuns().size());
        for (GeneratedRunFileReader max : runGenerator.getRuns()) {
            inFrame.add(new GroupVSizeFrame(ctx, max.getMaxFrameSize()));
        }

        // Let each run file reader not delete the run file when it is read and closed.
        for (GeneratedRunFileReader run : runGenerator.getRuns()) {
            PA.setValue(run, "deleteAfterClose", false);
        }
        matchResult(ctx, runGenerator.getRuns(), keyValuePair);

        List<IFrameReader> runs = new ArrayList<>();
        for (GeneratedRunFileReader run : runGenerator.getRuns()) {
            runs.add(run);
        }
        RunMergingFrameReader reader =
                new RunMergingFrameReader(ctx, runs, inFrame, SortFields, Comparators, null, RecordDesc);

        IFrame outFrame = new VSizeFrame(ctx);
        reader.open();
        while (reader.nextFrame(outFrame)) {
            assertFrameIsSorted(outFrame, Arrays.asList(keyValuePair));
        }
        reader.close();
        assertAllKeyValueIsConsumed(Arrays.asList(keyValuePair));
    }

    private void assertAllKeyValueIsConsumed(List<Map<Integer, String>> keyValueMapList) {
        for (Map<Integer, String> map : keyValueMapList) {
            assertTrue(map.isEmpty());
        }
    }

    private void assertFrameIsSorted(IFrame frame, List<Map<Integer, String>> keyValueMapList)
            throws HyracksDataException {
        FrameTupleAccessor fta = new FrameTupleAccessor(RecordDesc);

        ByteBufferInputStream bbis = new ByteBufferInputStream();
        DataInputStream di = new DataInputStream(bbis);

        fta.reset(frame.getBuffer());
        //        fta.prettyPrint();
        int preKey = Integer.MIN_VALUE;
        for (int i = 0; i < fta.getTupleCount(); i++) {
            bbis.setByteBuffer(fta.getBuffer(),
                    fta.getTupleStartOffset(i) + fta.getFieldStartOffset(i, 0) + fta.getFieldSlotsLength());
            int key = (int) RecordDesc.getFields()[0].deserialize(di);
            bbis.setByteBuffer(fta.getBuffer(),
                    fta.getTupleStartOffset(i) + fta.getFieldStartOffset(i, 1) + fta.getFieldSlotsLength());
            String value = (String) RecordDesc.getFields()[1].deserialize(di);

            boolean found = false;
            for (Map<Integer, String> map : keyValueMapList) {
                if (map.containsKey(key) && map.get(key).equals(value)) {
                    found = true;
                    map.remove(key);
                    break;
                }
            }
            assertTrue(found);
            assertTrue(preKey <= key);
            preKey = key;
        }
    }

    static void prepareRandomInputRunList(IHyracksTaskContext ctx, int pageSize, int numRuns, int numFramesPerRun,
            int minRecordSize, int maxRecordSize, List<TestFrameReader> readerList, List<IFrame> frameList,
            List<Map<Integer, String>> keyValueMap) throws HyracksDataException {
        for (int i = 0; i < numRuns; i++) {
            readerList.add(new TestFrameReader(pageSize, numFramesPerRun, minRecordSize, maxRecordSize));
            frameList.add(new VSizeFrame(ctx, readerList.get(readerList.size() - 1).maxFrameSize));
            keyValueMap.add(readerList.get(readerList.size() - 1).result);
        }
    }

}
