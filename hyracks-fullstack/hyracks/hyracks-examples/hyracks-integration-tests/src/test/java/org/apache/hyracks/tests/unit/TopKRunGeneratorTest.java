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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.normalizers.IntegerNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.std.sort.AbstractSortRunGenerator;
import org.apache.hyracks.dataflow.std.sort.HeapSortRunGenerator;
import org.apache.hyracks.dataflow.std.sort.HybridTopKSortRunGenerator;
import org.junit.Test;

public class TopKRunGeneratorTest {

    static final int PAGE_SIZE = 512;
    static final int NUM_PAGES = 80;
    static final int SORT_FRAME_LIMIT = 4;

    enum ORDER {
        INORDER,
        REVERSE
    }

    public class InMemorySortDataValidator implements IFrameWriter {

        InMemorySortDataValidator(Map<Integer, String> answer) {
            this.answer = answer;
        }

        Map<Integer, String> answer;
        FrameTupleAccessor accessor;
        int preKey = Integer.MIN_VALUE;

        @Override
        public void open() throws HyracksDataException {
            accessor = new FrameTupleAccessor(RecordDesc);
            preKey = Integer.MIN_VALUE;
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            accessor.reset(buffer);
            preKey = assertFTADataIsSorted(accessor, answer, preKey);
        }

        @Override
        public void fail() throws HyracksDataException {

        }

        @Override
        public void close() throws HyracksDataException {
            assertTrue(answer.isEmpty());
        }

        @Override
        public void flush() throws HyracksDataException {
        }
    }

    @Test
    public void testReverseOrderedDataShouldNotGenerateAnyRuns() throws HyracksDataException {
        int topK = 1;
        IHyracksTaskContext ctx = AbstractRunGeneratorTest.testUtils.create(PAGE_SIZE);
        HeapSortRunGenerator sorter = new HeapSortRunGenerator(ctx, SORT_FRAME_LIMIT, topK, SortFields, null,
                ComparatorFactories, RecordDesc);

        testInMemoryOnly(ctx, topK, ORDER.REVERSE, sorter);
    }

    @Test
    public void testAlreadySortedDataShouldNotGenerateAnyRuns() throws HyracksDataException {
        int topK = SORT_FRAME_LIMIT;
        IHyracksTaskContext ctx = AbstractRunGeneratorTest.testUtils.create(PAGE_SIZE);
        HeapSortRunGenerator sorter = new HeapSortRunGenerator(ctx, SORT_FRAME_LIMIT, topK, SortFields, null,
                ComparatorFactories, RecordDesc);

        testInMemoryOnly(ctx, topK, ORDER.INORDER, sorter);
    }

    @Test
    public void testHybridTopKShouldNotGenerateAnyRuns() throws HyracksDataException {
        int topK = 1;
        IHyracksTaskContext ctx = AbstractRunGeneratorTest.testUtils.create(PAGE_SIZE);
        AbstractSortRunGenerator sorter = new HybridTopKSortRunGenerator(ctx, SORT_FRAME_LIMIT, topK, SortFields, null,
                ComparatorFactories, RecordDesc);

        testInMemoryOnly(ctx, topK, ORDER.REVERSE, sorter);
    }

    @Test
    public void testHybridTopKWithOneNormalizedKey() throws HyracksDataException {
        int topK = SORT_FRAME_LIMIT;
        IHyracksTaskContext ctx = AbstractRunGeneratorTest.testUtils.create(PAGE_SIZE);
        AbstractSortRunGenerator sorter = new HybridTopKSortRunGenerator(ctx, SORT_FRAME_LIMIT, topK, SortFields,
                new INormalizedKeyComputerFactory[] { new IntegerNormalizedKeyComputerFactory() }, ComparatorFactories,
                RecordDesc);
        testInMemoryOnly(ctx, topK, ORDER.REVERSE, sorter);
    }

    @Test
    public void testHybridTopKWithTwoNormalizedKeys() throws HyracksDataException {
        int topK = SORT_FRAME_LIMIT;
        IHyracksTaskContext ctx = AbstractRunGeneratorTest.testUtils.create(PAGE_SIZE);
        AbstractSortRunGenerator sorter = new HybridTopKSortRunGenerator(ctx,
                SORT_FRAME_LIMIT, topK, SortFields, new INormalizedKeyComputerFactory[] {
                        new IntegerNormalizedKeyComputerFactory(), new UTF8StringNormalizedKeyComputerFactory() },
                ComparatorFactories, RecordDesc);
        testInMemoryOnly(ctx, topK, ORDER.REVERSE, sorter);
    }

    private void testInMemoryOnly(IHyracksTaskContext ctx, int topK, ORDER order, AbstractSortRunGenerator sorter)
            throws HyracksDataException {
        Map<Integer, String> keyValuePair = null;
        switch (order) {
            case INORDER:
                keyValuePair = new TreeMap<>();
                break;
            case REVERSE:
                keyValuePair = new TreeMap<>(Collections.reverseOrder());
                break;
        }

        List<IFrame> frameList = new ArrayList<>();
        int minDataSize = PAGE_SIZE * NUM_PAGES * 4 / 5;
        int minRecordSize = 64;
        int maxRecordSize = 64;

        AbstractRunGeneratorTest.prepareData(ctx, frameList, minDataSize, minRecordSize, maxRecordSize, null,
                keyValuePair);

        assert topK > 0;

        ByteBuffer buffer = prepareSortedData(keyValuePair);

        Map<Integer, String> topKAnswer = getTopKAnswer(keyValuePair, topK);

        doSort(sorter, buffer);

        validateResult(sorter, topKAnswer);
    }

    private void validateResult(AbstractSortRunGenerator sorter, Map<Integer, String> topKAnswer)
            throws HyracksDataException {

        InMemorySortDataValidator validator = new InMemorySortDataValidator(topKAnswer);
        validator.open();
        sorter.getSorter().flush(validator);
        validator.close();
    }

    private void doSort(AbstractSortRunGenerator sorter, ByteBuffer buffer) throws HyracksDataException {

        sorter.open();
        sorter.nextFrame(buffer);
        sorter.close();
    }

    private Map<Integer, String> getTopKAnswer(Map<Integer, String> keyValuePair, int topK) {

        TreeMap<Integer, String> copy = new TreeMap<>(keyValuePair);

        Map<Integer, String> answer = new TreeMap<>();
        for (Map.Entry<Integer, String> entry : copy.entrySet()) {
            if (answer.size() < topK) {
                answer.put(entry.getKey(), entry.getValue());
            } else {
                break;
            }
        }
        return answer;
    }

    private ByteBuffer prepareSortedData(Map<Integer, String> keyValuePair) throws HyracksDataException {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE * NUM_PAGES);
        IFrame inputFrame = new FixedSizeFrame(buffer);
        FrameTupleAppender appender = new FrameTupleAppender();
        appender.reset(inputFrame, true);
        ArrayTupleBuilder builder = new ArrayTupleBuilder(RecordDesc.getFieldCount());

        for (Map.Entry<Integer, String> entry : keyValuePair.entrySet()) {
            builder.reset();
            builder.addField(SerDers[0], entry.getKey());
            builder.addField(SerDers[1], entry.getValue());
            appender.append(builder.getFieldEndOffsets(), builder.getByteArray(), 0, builder.getSize());
        }
        return buffer;
    }
}
