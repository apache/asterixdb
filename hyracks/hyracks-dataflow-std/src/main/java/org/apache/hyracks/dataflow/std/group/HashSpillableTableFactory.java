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
package org.apache.hyracks.dataflow.std.group;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class HashSpillableTableFactory implements ISpillableTableFactory {

    private static final long serialVersionUID = 1L;
    private final ITuplePartitionComputerFactory tpcf;
    private final int tableSize;

    public HashSpillableTableFactory(ITuplePartitionComputerFactory tpcf, int tableSize) {
        this.tpcf = tpcf;
        this.tableSize = tableSize;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.hyracks.dataflow.std.aggregations.ISpillableTableFactory#
     * buildSpillableTable(org.apache.hyracks.api.context.IHyracksTaskContext,
     * int[], org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory[],
     * org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory,
     * edu.
     * uci.ics.hyracks.dataflow.std.aggregations.IFieldAggregateDescriptorFactory
     * [], org.apache.hyracks.api.dataflow.value.RecordDescriptor,
     * org.apache.hyracks.api.dataflow.value.RecordDescriptor, int)
     */
    @Override
    public ISpillableTable buildSpillableTable(final IHyracksTaskContext ctx, final int[] keyFields,
            IBinaryComparatorFactory[] comparatorFactories, INormalizedKeyComputerFactory firstKeyNormalizerFactory,
            IAggregatorDescriptorFactory aggregateFactory, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, final int framesLimit) throws HyracksDataException {
        final int[] storedKeys = new int[keyFields.length];
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] storedKeySerDeser = new ISerializerDeserializer[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            storedKeys[i] = i;
            storedKeySerDeser[i] = inRecordDescriptor.getFields()[keyFields[i]];
        }

        RecordDescriptor internalRecordDescriptor = outRecordDescriptor;
        final FrameTupleAccessor storedKeysAccessor1 = new FrameTupleAccessor(internalRecordDescriptor);
        final FrameTupleAccessor storedKeysAccessor2 = new FrameTupleAccessor(internalRecordDescriptor);

        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final FrameTuplePairComparator ftpcPartial = new FrameTuplePairComparator(keyFields, storedKeys, comparators);

        final FrameTuplePairComparator ftpcTuple = new FrameTuplePairComparator(storedKeys, storedKeys, comparators);

        final ITuplePartitionComputer tpc = tpcf.createPartitioner();

        final INormalizedKeyComputer nkc = firstKeyNormalizerFactory == null ? null : firstKeyNormalizerFactory
                .createNormalizedKeyComputer();

        int[] keyFieldsInPartialResults = new int[keyFields.length];
        for (int i = 0; i < keyFieldsInPartialResults.length; i++) {
            keyFieldsInPartialResults[i] = i;
        }

        final IAggregatorDescriptor aggregator = aggregateFactory.createAggregator(ctx, inRecordDescriptor,
                outRecordDescriptor, keyFields, keyFieldsInPartialResults, null);

        final AggregateState aggregateState = aggregator.createAggregateStates();

        final ArrayTupleBuilder stateTupleBuilder;
        if (keyFields.length < outRecordDescriptor.getFields().length) {
            stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);
        } else {
            stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length + 1);
        }

        final ArrayTupleBuilder outputTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        return new ISpillableTable() {

            private int lastBufIndex;

            private IFrame outputFrame;
            private FrameTupleAppender outputAppender;

            private FrameTupleAppender stateAppender = new FrameTupleAppender();

            private final ISerializableTable table = new SerializableHashTable(tableSize, ctx);
            private final TuplePointer storedTuplePointer = new TuplePointer();
            private final List<IFrame> frames = new ArrayList<>();

            /**
             * A tuple is "pointed" to by 3 entries in the tPointers array. [0]
             * = Frame index in the "Frames" list, [1] = Tuple index in the
             * frame, [2] = Poor man's normalized key for the tuple.
             */
            private int[] tPointers;

            @Override
            public void sortFrames() throws HyracksDataException {
                int sfIdx = storedKeys[0];
                int totalTCount = table.getTupleCount();
                tPointers = new int[totalTCount * 3];
                int ptr = 0;

                for (int i = 0; i < tableSize; i++) {
                    int entry = i;
                    int offset = 0;
                    do {
                        table.getTuplePointer(entry, offset, storedTuplePointer);
                        if (storedTuplePointer.frameIndex < 0)
                            break;
                        tPointers[ptr * 3] = entry;
                        tPointers[ptr * 3 + 1] = offset;
                        table.getTuplePointer(entry, offset, storedTuplePointer);
                        int fIndex = storedTuplePointer.frameIndex;
                        int tIndex = storedTuplePointer.tupleIndex;
                        storedKeysAccessor1.reset(frames.get(fIndex).getBuffer());
                        int tStart = storedKeysAccessor1.getTupleStartOffset(tIndex);
                        int f0StartRel = storedKeysAccessor1.getFieldStartOffset(tIndex, sfIdx);
                        int f0EndRel = storedKeysAccessor1.getFieldEndOffset(tIndex, sfIdx);
                        int f0Start = f0StartRel + tStart + storedKeysAccessor1.getFieldSlotsLength();
                        tPointers[ptr * 3 + 2] = nkc == null ? 0 : nkc.normalize(storedKeysAccessor1.getBuffer()
                                .array(), f0Start, f0EndRel - f0StartRel);
                        ptr++;
                        offset++;
                    } while (true);
                }
                /**
                 * Sort using quick sort
                 */
                if (tPointers.length > 0) {
                    sort(tPointers, 0, totalTCount);
                }
            }

            @Override
            public void reset() {
                lastBufIndex = -1;
                tPointers = null;
                table.reset();
                aggregator.reset();
            }

            @Override
            public boolean insert(FrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                if (lastBufIndex < 0)
                    nextAvailableFrame();
                int entry = tpc.partition(accessor, tIndex, tableSize);
                boolean foundGroup = false;
                int offset = 0;
                do {
                    table.getTuplePointer(entry, offset++, storedTuplePointer);
                    if (storedTuplePointer.frameIndex < 0)
                        break;
                    storedKeysAccessor1.reset(frames.get(storedTuplePointer.frameIndex).getBuffer());
                    int c = ftpcPartial.compare(accessor, tIndex, storedKeysAccessor1, storedTuplePointer.tupleIndex);
                    if (c == 0) {
                        foundGroup = true;
                        break;
                    }
                } while (true);

                if (!foundGroup) {

                    stateTupleBuilder.reset();

                    for (int k = 0; k < keyFields.length; k++) {
                        stateTupleBuilder.addField(accessor, tIndex, keyFields[k]);
                    }

                    aggregator.init(stateTupleBuilder, accessor, tIndex, aggregateState);
                    if (!stateAppender.appendSkipEmptyField(stateTupleBuilder.getFieldEndOffsets(),
                            stateTupleBuilder.getByteArray(), 0, stateTupleBuilder.getSize())) {
                        if (!nextAvailableFrame()) {
                            return false;
                        }
                        if (!stateAppender.appendSkipEmptyField(stateTupleBuilder.getFieldEndOffsets(),
                                stateTupleBuilder.getByteArray(), 0, stateTupleBuilder.getSize())) {
                            throw new HyracksDataException("Cannot init external aggregate state in a frame.");
                        }
                    }

                    storedTuplePointer.frameIndex = lastBufIndex;
                    storedTuplePointer.tupleIndex = stateAppender.getTupleCount() - 1;
                    table.insert(entry, storedTuplePointer);
                } else {

                    aggregator.aggregate(accessor, tIndex, storedKeysAccessor1, storedTuplePointer.tupleIndex,
                            aggregateState);

                }
                return true;
            }

            @Override
            public List<IFrame> getFrames() {
                return frames;
            }

            @Override
            public int getFrameCount() {
                return lastBufIndex;
            }

            @Override
            public void flushFrames(IFrameWriter writer, boolean isPartial) throws HyracksDataException {
                if (outputFrame == null) {
                    outputFrame = new VSizeFrame(ctx);
                }

                if (outputAppender == null) {
                    outputAppender = new FrameTupleAppender();
                }

                outputAppender.reset(outputFrame, true);

                if (tPointers == null) {
                    // Not sorted
                    for (int i = 0; i < tableSize; ++i) {
                        int entry = i;
                        int offset = 0;
                        do {
                            table.getTuplePointer(entry, offset++, storedTuplePointer);
                            if (storedTuplePointer.frameIndex < 0)
                                break;
                            int bIndex = storedTuplePointer.frameIndex;
                            int tIndex = storedTuplePointer.tupleIndex;

                            storedKeysAccessor1.reset(frames.get(bIndex).getBuffer());

                            outputTupleBuilder.reset();
                            for (int k = 0; k < storedKeys.length; k++) {
                                outputTupleBuilder.addField(storedKeysAccessor1, tIndex, storedKeys[k]);
                            }

                            if (isPartial) {

                                aggregator.outputPartialResult(outputTupleBuilder, storedKeysAccessor1, tIndex,
                                        aggregateState);

                            } else {

                                aggregator.outputFinalResult(outputTupleBuilder, storedKeysAccessor1, tIndex,
                                        aggregateState);
                            }

                            if (!outputAppender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                                    outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                                outputAppender.flush(writer, true);
                                if (!outputAppender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                                        outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                                    throw new HyracksDataException(
                                            "The output item is too large to be fit into a frame.");
                                }
                            }

                        } while (true);
                    }
                    outputAppender.flush(writer, true);
                    aggregator.close();
                    return;
                }
                int n = tPointers.length / 3;
                for (int ptr = 0; ptr < n; ptr++) {
                    int tableIndex = tPointers[ptr * 3];
                    int rowIndex = tPointers[ptr * 3 + 1];
                    table.getTuplePointer(tableIndex, rowIndex, storedTuplePointer);
                    int frameIndex = storedTuplePointer.frameIndex;
                    int tupleIndex = storedTuplePointer.tupleIndex;
                    // Get the frame containing the value
                    IFrame buffer = frames.get(frameIndex);
                    storedKeysAccessor1.reset(buffer.getBuffer());

                    outputTupleBuilder.reset();
                    for (int k = 0; k < storedKeys.length; k++) {
                        outputTupleBuilder.addField(storedKeysAccessor1, tupleIndex, storedKeys[k]);
                    }

                    if (isPartial) {

                        aggregator.outputPartialResult(outputTupleBuilder, storedKeysAccessor1, tupleIndex,
                                aggregateState);

                    } else {

                        aggregator.outputFinalResult(outputTupleBuilder, storedKeysAccessor1, tupleIndex,
                                aggregateState);
                    }

                    if (!outputAppender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        outputAppender.flush(writer, true);
                        if (!outputAppender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            throw new HyracksDataException("The output item is too large to be fit into a frame.");
                        }
                    }
                }
                outputAppender.flush(writer, true);
                aggregator.close();
            }

            @Override
            public void close() {
                lastBufIndex = -1;
                tPointers = null;
                table.close();
                frames.clear();
                aggregateState.close();
            }

            /**
             * Set the working frame to the next available frame in the frame
             * list. There are two cases:<br>
             * 1) If the next frame is not initialized, allocate a new frame. 2)
             * When frames are already created, they are recycled.
             *
             * @return Whether a new frame is added successfully.
             * @throws HyracksDataException
             */
            private boolean nextAvailableFrame() throws HyracksDataException {
                // Return false if the number of frames is equal to the limit.
                if (lastBufIndex + 1 >= framesLimit)
                    return false;

                if (frames.size() < framesLimit) {
                    // Insert a new frame
                    IFrame frame = new VSizeFrame(ctx);
                    frames.add(frame);
                    stateAppender.reset(frame, true);
                    lastBufIndex = frames.size() - 1;
                } else {
                    // Reuse an old frame
                    lastBufIndex++;
                    stateAppender.reset(frames.get(lastBufIndex), true);
                }
                return true;
            }

            private void sort(int[] tPointers, int offset, int length) throws HyracksDataException {
                int m = offset + (length >> 1);
                int mTable = tPointers[m * 3];
                int mRow = tPointers[m * 3 + 1];
                int mNormKey = tPointers[m * 3 + 2];

                table.getTuplePointer(mTable, mRow, storedTuplePointer);
                int mFrame = storedTuplePointer.frameIndex;
                int mTuple = storedTuplePointer.tupleIndex;
                storedKeysAccessor1.reset(frames.get(mFrame).getBuffer());

                int a = offset;
                int b = a;
                int c = offset + length - 1;
                int d = c;
                while (true) {
                    while (b <= c) {
                        int bTable = tPointers[b * 3];
                        int bRow = tPointers[b * 3 + 1];
                        int bNormKey = tPointers[b * 3 + 2];
                        int cmp = 0;
                        if (bNormKey != mNormKey) {
                            cmp = ((((long) bNormKey) & 0xffffffffL) < (((long) mNormKey) & 0xffffffffL)) ? -1 : 1;
                        } else {
                            table.getTuplePointer(bTable, bRow, storedTuplePointer);
                            int bFrame = storedTuplePointer.frameIndex;
                            int bTuple = storedTuplePointer.tupleIndex;
                            storedKeysAccessor2.reset(frames.get(bFrame).getBuffer());
                            cmp = ftpcTuple.compare(storedKeysAccessor2, bTuple, storedKeysAccessor1, mTuple);
                        }
                        if (cmp > 0) {
                            break;
                        }
                        if (cmp == 0) {
                            swap(tPointers, a++, b);
                        }
                        ++b;
                    }
                    while (c >= b) {
                        int cTable = tPointers[c * 3];
                        int cRow = tPointers[c * 3 + 1];
                        int cNormKey = tPointers[c * 3 + 2];
                        int cmp = 0;
                        if (cNormKey != mNormKey) {
                            cmp = ((((long) cNormKey) & 0xffffffffL) < (((long) mNormKey) & 0xffffffffL)) ? -1 : 1;
                        } else {
                            table.getTuplePointer(cTable, cRow, storedTuplePointer);
                            int cFrame = storedTuplePointer.frameIndex;
                            int cTuple = storedTuplePointer.tupleIndex;
                            storedKeysAccessor2.reset(frames.get(cFrame).getBuffer());
                            cmp = ftpcTuple.compare(storedKeysAccessor2, cTuple, storedKeysAccessor1, mTuple);
                        }
                        if (cmp < 0) {
                            break;
                        }
                        if (cmp == 0) {
                            swap(tPointers, c, d--);
                        }
                        --c;
                    }
                    if (b > c)
                        break;
                    swap(tPointers, b++, c--);
                }

                int s;
                int n = offset + length;
                s = Math.min(a - offset, b - a);
                vecswap(tPointers, offset, b - s, s);
                s = Math.min(d - c, n - d - 1);
                vecswap(tPointers, b, n - s, s);

                if ((s = b - a) > 1) {
                    sort(tPointers, offset, s);
                }
                if ((s = d - c) > 1) {
                    sort(tPointers, n - s, s);
                }
            }

            private void swap(int x[], int a, int b) {
                for (int i = 0; i < 3; ++i) {
                    int t = x[a * 3 + i];
                    x[a * 3 + i] = x[b * 3 + i];
                    x[b * 3 + i] = t;
                }
            }

            private void vecswap(int x[], int a, int b, int n) {
                for (int i = 0; i < n; i++, a++, b++) {
                    swap(x, a, b);
                }
            }

        };
    }

}
