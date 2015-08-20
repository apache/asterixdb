/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.hyracks.tests.unit;

import static edu.uci.ics.hyracks.tests.unit.AbstractRunGeneratorTest.ComparatorFactories;
import static edu.uci.ics.hyracks.tests.unit.AbstractRunGeneratorTest.RecordDesc;
import static edu.uci.ics.hyracks.tests.unit.AbstractRunGeneratorTest.SerDers;
import static edu.uci.ics.hyracks.tests.unit.AbstractRunGeneratorTest.SortFields;
import static edu.uci.ics.hyracks.tests.unit.AbstractRunGeneratorTest.assertFTADataIsSorted;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

import edu.uci.ics.hyracks.api.comm.FixedSizeFrame;
import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.sort.AbstractSortRunGenerator;
import edu.uci.ics.hyracks.dataflow.std.sort.HybridTopKSortRunGenerator;
import edu.uci.ics.hyracks.dataflow.std.sort.HeapSortRunGenerator;

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
    }

    @Test
    public void testReverseOrderedDataShouldNotGenerateAnyRuns() throws HyracksDataException {
        int topK = 1;
        IHyracksTaskContext ctx = AbstractRunGeneratorTest.testUtils.create(PAGE_SIZE);
        HeapSortRunGenerator sorter = new HeapSortRunGenerator(ctx, SORT_FRAME_LIMIT, topK,
                SortFields, null, ComparatorFactories, RecordDesc);

        testInMemoryOnly(ctx, topK, ORDER.REVERSE, sorter);
    }

    @Test
    public void testAlreadySortedDataShouldNotGenerateAnyRuns() throws HyracksDataException {
        int topK = SORT_FRAME_LIMIT;
        IHyracksTaskContext ctx = AbstractRunGeneratorTest.testUtils.create(PAGE_SIZE);
        HeapSortRunGenerator sorter = new HeapSortRunGenerator(ctx, SORT_FRAME_LIMIT, topK,
                SortFields, null, ComparatorFactories, RecordDesc);

        testInMemoryOnly(ctx, topK, ORDER.INORDER, sorter);
    }

    @Test
    public void testHybridTopKShouldNotGenerateAnyRuns() throws HyracksDataException {
        int topK = 1;
        IHyracksTaskContext ctx = AbstractRunGeneratorTest.testUtils.create(PAGE_SIZE);
        AbstractSortRunGenerator sorter = new HybridTopKSortRunGenerator(ctx, SORT_FRAME_LIMIT, topK,
                SortFields, null, ComparatorFactories, RecordDesc);

        testInMemoryOnly(ctx, topK, ORDER.REVERSE, sorter);
    }

    @Test
    public void testHybridTopKShouldSwitchToFrameSorterWhenFlushed() {
        int topK = 1;
        IHyracksTaskContext ctx = AbstractRunGeneratorTest.testUtils.create(PAGE_SIZE);
        AbstractSortRunGenerator sorter = new HybridTopKSortRunGenerator(ctx, SORT_FRAME_LIMIT, topK,
                SortFields, null, ComparatorFactories, RecordDesc);

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
        int minRecordSize = 16;
        int maxRecordSize = 64;

        AbstractRunGeneratorTest
                .prepareData(ctx, frameList, minDataSize, minRecordSize, maxRecordSize, null, keyValuePair);

        assert topK > 0;

        ByteBuffer buffer = prepareSortedData(keyValuePair);

        Map<Integer, String> topKAnswer = getTopKAnswer(keyValuePair, topK);

        doSort(sorter, buffer);

        assertEquals(0, sorter.getRuns().size());
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
