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

import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

public class HashSpillableTableFactory implements ISpillableTableFactory {

    private static Logger LOGGER = Logger.getLogger(HashSpillableTableFactory.class.getName());
    private static final double FUDGE_FACTOR = 1.1;
    private static final long serialVersionUID = 1L;
    private final IBinaryHashFunctionFamily[] hashFunctionFamilies;

    public HashSpillableTableFactory(IBinaryHashFunctionFamily[] hashFunctionFamilies) {
        this.hashFunctionFamilies = hashFunctionFamilies;
    }

    @Override
    public ISpillableTable buildSpillableTable(final IHyracksTaskContext ctx, int suggestTableSize, long dataBytesSize,
            final int[] keyFields, final IBinaryComparator[] comparators,
            final INormalizedKeyComputer firstKeyNormalizerFactory, IAggregatorDescriptorFactory aggregateFactory,
            RecordDescriptor inRecordDescriptor, RecordDescriptor outRecordDescriptor, final int framesLimit,
            final int seed) throws HyracksDataException {
        if (framesLimit < 2) {
            throw new HyracksDataException("The frame limit is too small to partition the data");
        }
        final int tableSize = suggestTableSize;

        final int[] intermediateResultKeys = new int[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            intermediateResultKeys[i] = i;
        }

        final FrameTuplePairComparator ftpcInputCompareToAggregate = new FrameTuplePairComparator(keyFields,
                intermediateResultKeys, comparators);

        final ITuplePartitionComputer tpc = new FieldHashPartitionComputerFamily(keyFields, hashFunctionFamilies)
                .createPartitioner(seed);

        final IAggregatorDescriptor aggregator = aggregateFactory.createAggregator(ctx, inRecordDescriptor,
                outRecordDescriptor, keyFields, intermediateResultKeys, null);

        final AggregateState aggregateState = aggregator.createAggregateStates();

        final ArrayTupleBuilder stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        //TODO(jf) research on the optimized partition size
        final int numPartitions = getNumOfPartitions((int) (dataBytesSize / ctx.getInitialFrameSize()),
                framesLimit - 1);
        final int entriesPerPartition = (int) Math.ceil(1.0 * tableSize / numPartitions);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("create hashtable, table size:" + tableSize + " file size:" + dataBytesSize + "  partitions:"
                    + numPartitions);
        }

        final ArrayTupleBuilder outputTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        final ISerializableTable hashTableForTuplePointer = new SerializableHashTable(tableSize, ctx);

        return new ISpillableTable() {

            private final TuplePointer pointer = new TuplePointer();
            private final BitSet spilledSet = new BitSet(numPartitions);
            final IPartitionedTupleBufferManager bufferManager = new VPartitionTupleBufferManager(ctx,
                    PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledSet),
                    numPartitions, framesLimit * ctx.getInitialFrameSize());

            final ITuplePointerAccessor bufferAccessor = bufferManager.getTupleAccessor(outRecordDescriptor);

            private final PreferToSpillFullyOccupiedFramePolicy spillPolicy = new PreferToSpillFullyOccupiedFramePolicy(
                    bufferManager, spilledSet, ctx.getInitialFrameSize());

            private final FrameTupleAppender outputAppender = new FrameTupleAppender(new VSizeFrame(ctx));

            @Override
            public void close() throws HyracksDataException {
                hashTableForTuplePointer.close();
                aggregator.close();
            }

            @Override
            public void clear(int partition) throws HyracksDataException {
                for (int p = getFirstEntryInHashTable(partition); p < getLastEntryInHashTable(partition); p++) {
                    hashTableForTuplePointer.delete(p);
                }
                bufferManager.clearPartition(partition);
            }

            private int getPartition(int entryInHashTable) {
                return entryInHashTable / entriesPerPartition;
            }

            private int getFirstEntryInHashTable(int partition) {
                return partition * entriesPerPartition;
            }

            private int getLastEntryInHashTable(int partition) {
                return Math.min(tableSize, (partition + 1) * entriesPerPartition);
            }

            @Override
            public boolean insert(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                int entryInHashTable = tpc.partition(accessor, tIndex, tableSize);
                for (int i = 0; i < hashTableForTuplePointer.getTupleCount(entryInHashTable); i++) {
                    hashTableForTuplePointer.getTuplePointer(entryInHashTable, i, pointer);
                    bufferAccessor.reset(pointer);
                    int c = ftpcInputCompareToAggregate.compare(accessor, tIndex, bufferAccessor);
                    if (c == 0) {
                        aggregateExistingTuple(accessor, tIndex, bufferAccessor, pointer.tupleIndex);
                        return true;
                    }
                }

                return insertNewAggregateEntry(entryInHashTable, accessor, tIndex);
            }

            private boolean insertNewAggregateEntry(int entryInHashTable, IFrameTupleAccessor accessor, int tIndex)
                    throws HyracksDataException {
                initStateTupleBuilder(accessor, tIndex);
                int pid = getPartition(entryInHashTable);

                if (!bufferManager.insertTuple(pid, stateTupleBuilder.getByteArray(),
                        stateTupleBuilder.getFieldEndOffsets(), 0, stateTupleBuilder.getSize(), pointer)) {
                    return false;
                }
                hashTableForTuplePointer.insert(entryInHashTable, pointer);
                return true;
            }

            private void initStateTupleBuilder(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                stateTupleBuilder.reset();
                for (int k = 0; k < keyFields.length; k++) {
                    stateTupleBuilder.addField(accessor, tIndex, keyFields[k]);
                }
                aggregator.init(stateTupleBuilder, accessor, tIndex, aggregateState);
            }

            private void aggregateExistingTuple(IFrameTupleAccessor accessor, int tIndex,
                    ITuplePointerAccessor bufferAccessor, int tupleIndex) throws HyracksDataException {
                aggregator.aggregate(accessor, tIndex, bufferAccessor, tupleIndex, aggregateState);
            }

            @Override
            public int flushFrames(int partition, IFrameWriter writer, AggregateType type) throws HyracksDataException {
                int count = 0;
                for (int hashEntryPid = getFirstEntryInHashTable(partition); hashEntryPid < getLastEntryInHashTable(
                        partition); hashEntryPid++) {
                    count += hashTableForTuplePointer.getTupleCount(hashEntryPid);
                    for (int tid = 0; tid < hashTableForTuplePointer.getTupleCount(hashEntryPid); tid++) {
                        hashTableForTuplePointer.getTuplePointer(hashEntryPid, tid, pointer);
                        bufferAccessor.reset(pointer);
                        outputTupleBuilder.reset();
                        for (int k = 0; k < intermediateResultKeys.length; k++) {
                            outputTupleBuilder.addField(bufferAccessor.getBuffer().array(),
                                    bufferAccessor.getAbsFieldStartOffset(intermediateResultKeys[k]),
                                    bufferAccessor.getFieldLength(intermediateResultKeys[k]));
                        }

                        boolean hasOutput = false;
                        switch (type) {
                            case PARTIAL:
                                hasOutput = aggregator.outputPartialResult(outputTupleBuilder, bufferAccessor,
                                        pointer.tupleIndex, aggregateState);
                                break;
                            case FINAL:
                                hasOutput = aggregator.outputFinalResult(outputTupleBuilder, bufferAccessor,
                                        pointer.tupleIndex, aggregateState);
                                break;
                        }

                        if (hasOutput && !outputAppender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            outputAppender.write(writer, true);
                            if (!outputAppender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                                    outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                                throw new HyracksDataException("The output item is too large to be fit into a frame.");
                            }
                        }
                    }
                }
                outputAppender.write(writer, true);
                spilledSet.set(partition);
                return count;
            }

            @Override
            public int getNumPartitions() {
                return bufferManager.getNumPartitions();
            }

            @Override
            public int findVictimPartition(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                int entryInHashTable = tpc.partition(accessor, tIndex, tableSize);
                int partition = getPartition(entryInHashTable);
                return spillPolicy.selectVictimPartition(partition);
            }
        };
    }

    private int getNumOfPartitions(int nubmerOfFramesForData, int frameLimit) {
        if (frameLimit > nubmerOfFramesForData) {
            return 1; // all in memory, we will create a big partition
        }
        int numberOfPartitions = (int) (Math
                .ceil((nubmerOfFramesForData * FUDGE_FACTOR - frameLimit) / (frameLimit - 1)));
        if (numberOfPartitions <= 0) {
            numberOfPartitions = 1; //becomes in-memory hash
        }
        if (numberOfPartitions > frameLimit) {
            numberOfPartitions = (int) Math.ceil(Math.sqrt(nubmerOfFramesForData * FUDGE_FACTOR));
            return Math.max(2, Math.min(numberOfPartitions, frameLimit));
        }
        return numberOfPartitions;
    }

}
