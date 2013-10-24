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
package edu.uci.ics.hyracks.dataflow.std.group.hash;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

class GroupingHashTable {
    /**
     * The pointers in the link store 3 int values for each entry in the
     * hashtable: (bufferIdx, tIndex, accumulatorIdx).
     * 
     * @author vinayakb
     */
    private static class Link {
        private static final int INIT_POINTERS_SIZE = 9;

        int[] pointers;
        int size;

        Link() {
            pointers = new int[INIT_POINTERS_SIZE];
            size = 0;
        }

        void add(int bufferIdx, int tIndex, int accumulatorIdx) {
            while (size + 3 > pointers.length) {
                pointers = Arrays.copyOf(pointers, pointers.length * 2);
            }
            pointers[size++] = bufferIdx;
            pointers[size++] = tIndex;
            pointers[size++] = accumulatorIdx;
        }
    }

    private static final int INIT_AGG_STATE_SIZE = 8;
    private final IHyracksTaskContext ctx;

    private final List<ByteBuffer> buffers;
    private final Link[] table;
    /**
     * Aggregate states: a list of states for all groups maintained in the main
     * memory.
     */
    private AggregateState[] aggregateStates;
    private int accumulatorSize;

    private int lastBIndex;
    private final int[] storedKeys;
    private final int[] keys;
    private final IBinaryComparator[] comparators;
    private final FrameTuplePairComparator ftpc;
    private final ITuplePartitionComputer tpc;
    private final IAggregatorDescriptor aggregator;

    private final FrameTupleAppender appender;

    private final FrameTupleAccessor storedKeysAccessor;

    private final ArrayTupleBuilder stateTupleBuilder, outputTupleBuilder;

    GroupingHashTable(IHyracksTaskContext ctx, int[] fields, IBinaryComparatorFactory[] comparatorFactories,
            ITuplePartitionComputerFactory tpcf, IAggregatorDescriptorFactory aggregatorFactory,
            RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, int tableSize)
            throws HyracksDataException {
        this.ctx = ctx;

        buffers = new ArrayList<ByteBuffer>();
        table = new Link[tableSize];

        keys = fields;
        storedKeys = new int[fields.length];
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] storedKeySerDeser = new ISerializerDeserializer[fields.length];
        for (int i = 0; i < fields.length; ++i) {
            storedKeys[i] = i;
            storedKeySerDeser[i] = inRecordDescriptor.getFields()[fields[i]];
        }

        comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        ftpc = new FrameTuplePairComparator(fields, storedKeys, comparators);
        tpc = tpcf.createPartitioner();

        int[] keyFieldsInPartialResults = new int[fields.length];
        for (int i = 0; i < keyFieldsInPartialResults.length; i++) {
            keyFieldsInPartialResults[i] = i;
        }

        this.aggregator = aggregatorFactory.createAggregator(ctx, inRecordDescriptor, outRecordDescriptor, fields,
                keyFieldsInPartialResults, null);

        this.aggregateStates = new AggregateState[INIT_AGG_STATE_SIZE];
        accumulatorSize = 0;

        RecordDescriptor storedKeysRecordDescriptor = new RecordDescriptor(storedKeySerDeser);
        storedKeysAccessor = new FrameTupleAccessor(ctx.getFrameSize(), storedKeysRecordDescriptor);
        lastBIndex = -1;

        appender = new FrameTupleAppender(ctx.getFrameSize());

        addNewBuffer();

        if (fields.length < outRecordDescriptor.getFields().length) {
            stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);
        } else {
            stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length + 1);
        }
        outputTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);
    }

    private void addNewBuffer() throws HyracksDataException {
        ByteBuffer buffer = ctx.allocateFrame();
        buffer.position(0);
        buffer.limit(buffer.capacity());
        buffers.add(buffer);
        appender.reset(buffer, true);
        ++lastBIndex;
    }

    void insert(FrameTupleAccessor accessor, int tIndex) throws Exception {
        int entry = tpc.partition(accessor, tIndex, table.length);
        Link link = table[entry];
        if (link == null) {
            link = table[entry] = new Link();
        }
        int saIndex = -1;
        for (int i = 0; i < link.size; i += 3) {
            int sbIndex = link.pointers[i];
            int stIndex = link.pointers[i + 1];
            storedKeysAccessor.reset(buffers.get(sbIndex));
            int c = ftpc.compare(accessor, tIndex, storedKeysAccessor, stIndex);
            if (c == 0) {
                saIndex = link.pointers[i + 2];
                break;
            }
        }
        if (saIndex < 0) {
            // Did not find the key. Insert a new entry.
            saIndex = accumulatorSize++;
            // Add keys

            // Add aggregation fields
            AggregateState newState = aggregator.createAggregateStates();

            stateTupleBuilder.reset();
            for (int k = 0; k < keys.length; k++) {
                stateTupleBuilder.addField(accessor, tIndex, keys[k]);
            }

            aggregator.init(stateTupleBuilder, accessor, tIndex, newState);

            if (!appender.appendSkipEmptyField(stateTupleBuilder.getFieldEndOffsets(),
                    stateTupleBuilder.getByteArray(), 0, stateTupleBuilder.getSize())) {
                addNewBuffer();
                if (!appender.appendSkipEmptyField(stateTupleBuilder.getFieldEndOffsets(),
                        stateTupleBuilder.getByteArray(), 0, stateTupleBuilder.getSize())) {
                    throw new HyracksDataException("Cannot init the aggregate state in a single frame.");
                }
            }

            if (accumulatorSize >= aggregateStates.length) {
                aggregateStates = Arrays.copyOf(aggregateStates, aggregateStates.length * 2);
            }

            aggregateStates[saIndex] = newState;

            link.add(lastBIndex, appender.getTupleCount() - 1, saIndex);

        } else {
            aggregator.aggregate(accessor, tIndex, null, 0, aggregateStates[saIndex]);
        }
    }

    void write(IFrameWriter writer) throws HyracksDataException {
        ByteBuffer buffer = ctx.allocateFrame();
        appender.reset(buffer, true);

        for (int i = 0; i < table.length; ++i) {
            Link link = table[i];
            if (link != null) {
                for (int j = 0; j < link.size; j += 3) {
                    int bIndex = link.pointers[j];
                    int tIndex = link.pointers[j + 1];
                    int aIndex = link.pointers[j + 2];
                    ByteBuffer keyBuffer = buffers.get(bIndex);
                    storedKeysAccessor.reset(keyBuffer);

                    // copy keys
                    outputTupleBuilder.reset();
                    for (int k = 0; k < storedKeys.length; k++) {
                        outputTupleBuilder.addField(storedKeysAccessor, tIndex, storedKeys[k]);
                    }

                    aggregator.outputFinalResult(outputTupleBuilder, storedKeysAccessor, tIndex,
                            aggregateStates[aIndex]);

                    if (!appender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        writer.nextFrame(buffer);
                        appender.reset(buffer, true);
                        if (!appender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            throw new HyracksDataException("Cannot write the aggregation output into a frame.");
                        }
                    }

                }
            }
        }
        if (appender.getTupleCount() != 0) {
            writer.nextFrame(buffer);
        }
    }

    void close() throws HyracksDataException {
        for (AggregateState aState : aggregateStates) {
            aState.close();
        }
    }
}