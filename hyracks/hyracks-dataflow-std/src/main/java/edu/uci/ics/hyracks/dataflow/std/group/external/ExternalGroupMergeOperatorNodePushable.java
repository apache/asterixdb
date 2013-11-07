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
package edu.uci.ics.hyracks.dataflow.std.group.external;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.ISpillableTable;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceEntry;
import edu.uci.ics.hyracks.dataflow.std.util.ReferencedPriorityQueue;

class ExternalGroupMergeOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {
    private final IHyracksTaskContext ctx;
    private final Object stateId;
    private final int[] keyFields;
    private final IBinaryComparator[] comparators;
    private final INormalizedKeyComputer nmkComputer;
    private final AggregateState aggregateState;
    private final ArrayTupleBuilder tupleBuilder;
    private final int[] storedKeys;
    private final IAggregatorDescriptor aggregator;
    private final boolean isOutputSorted;
    private final int framesLimit;
    private final RecordDescriptor outRecordDescriptor;
    /**
     * Input frames, one for each run file.
     */
    private List<ByteBuffer> inFrames;
    /**
     * Output frame.
     */
    private ByteBuffer outFrame, writerFrame;
    private final FrameTupleAppender outAppender;
    private FrameTupleAppender writerAppender;
    private LinkedList<RunFileReader> runs;
    private ExternalGroupState aggState;
    private ArrayTupleBuilder finalTupleBuilder;
    /**
     * how many frames to be read ahead once
     */
    private int runFrameLimit = 1;
    private int[] currentFrameIndexInRun;
    private int[] currentRunFrames;
    private final FrameTupleAccessor outFrameAccessor;

    ExternalGroupMergeOperatorNodePushable(IHyracksTaskContext ctx, Object stateId,
            IBinaryComparatorFactory[] comparatorFactories, INormalizedKeyComputerFactory nmkFactory, int[] keyFields,
            IAggregatorDescriptorFactory mergerFactory, boolean isOutputSorted, int framesLimit,
            RecordDescriptor outRecordDescriptor) throws HyracksDataException {
        this.stateId = stateId;
        this.keyFields = keyFields;
        comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.nmkComputer = nmkFactory == null ? null : nmkFactory.createNormalizedKeyComputer();
        int[] keyFieldsInPartialResults = new int[keyFields.length];
        for (int i = 0; i < keyFieldsInPartialResults.length; i++) {
            keyFieldsInPartialResults[i] = i;
        }

        aggregator = mergerFactory.createAggregator(ctx, outRecordDescriptor, outRecordDescriptor, keyFields,
                keyFieldsInPartialResults, writer);
        aggregateState = aggregator.createAggregateStates();

        storedKeys = new int[keyFields.length];
        /**
         * Get the list of the fields in the stored records.
         */
        for (int i = 0; i < keyFields.length; ++i) {
            storedKeys[i] = i;
        }

        tupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);
        this.ctx = ctx;
        outAppender = new FrameTupleAppender(ctx.getFrameSize());
        outFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), outRecordDescriptor);
        this.isOutputSorted = isOutputSorted;
        this.framesLimit = framesLimit;
        this.outRecordDescriptor = outRecordDescriptor;
    }

    public void initialize() throws HyracksDataException {
        aggState = (ExternalGroupState) ctx.getStateObject(stateId);
        runs = aggState.getRuns();
        writer.open();
        try {
            if (runs.size() <= 0) {
                ISpillableTable gTable = aggState.getSpillableTable();
                if (gTable != null) {
                    if (isOutputSorted)
                        gTable.sortFrames();
                    gTable.flushFrames(writer, false);
                }
                gTable = null;
                aggState = null;
            } else {
                aggState = null;
                runs = new LinkedList<RunFileReader>(runs);
                inFrames = new ArrayList<ByteBuffer>();
                outFrame = ctx.allocateFrame();
                outAppender.reset(outFrame, true);
                outFrameAccessor.reset(outFrame);
                while (runs.size() > 0) {
                    try {
                        doPass(runs);
                    } catch (Exception e) {
                        throw new HyracksDataException(e);
                    }
                }
                inFrames.clear();
            }
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {
            aggregateState.close();
            writer.close();
        }
    }

    private void doPass(LinkedList<RunFileReader> runs) throws HyracksDataException {
        FileReference newRun = null;
        IFrameWriter writer = this.writer;
        boolean finalPass = false;

        while (inFrames.size() + 2 < framesLimit) {
            inFrames.add(ctx.allocateFrame());
        }
        int runNumber;
        if (runs.size() + 2 <= framesLimit) {
            finalPass = true;
            runFrameLimit = (framesLimit - 2) / runs.size();
            runNumber = runs.size();
        } else {
            runNumber = framesLimit - 2;
            newRun = ctx.getJobletContext().createManagedWorkspaceFile(
                    ExternalGroupOperatorDescriptor.class.getSimpleName());
            writer = new RunFileWriter(newRun, ctx.getIOManager());
            writer.open();
        }
        try {
            currentFrameIndexInRun = new int[runNumber];
            currentRunFrames = new int[runNumber];
            /**
             * Create file readers for each input run file, only for
             * the ones fit into the inFrames
             */
            RunFileReader[] runFileReaders = new RunFileReader[runNumber];
            FrameTupleAccessor[] tupleAccessors = new FrameTupleAccessor[inFrames.size()];
            Comparator<ReferenceEntry> comparator = createEntryComparator(comparators);
            ReferencedPriorityQueue topTuples = new ReferencedPriorityQueue(ctx.getFrameSize(), outRecordDescriptor,
                    runNumber, comparator, keyFields, nmkComputer);
            /**
             * current tuple index in each run
             */
            int[] tupleIndices = new int[runNumber];

            for (int i = 0; i < runNumber; i++) {
                int runIndex = topTuples.peek().getRunid();
                tupleIndices[runIndex] = 0;
                // Load the run file
                runFileReaders[runIndex] = runs.get(runIndex);
                runFileReaders[runIndex].open();

                currentRunFrames[runIndex] = 0;
                currentFrameIndexInRun[runIndex] = runIndex * runFrameLimit;
                for (int j = 0; j < runFrameLimit; j++) {
                    int frameIndex = currentFrameIndexInRun[runIndex] + j;
                    if (runFileReaders[runIndex].nextFrame(inFrames.get(frameIndex))) {
                        tupleAccessors[frameIndex] = new FrameTupleAccessor(ctx.getFrameSize(), outRecordDescriptor);
                        tupleAccessors[frameIndex].reset(inFrames.get(frameIndex));
                        currentRunFrames[runIndex]++;
                        if (j == 0)
                            setNextTopTuple(runIndex, tupleIndices, runFileReaders, tupleAccessors, topTuples);
                    } else {
                        break;
                    }
                }
            }

            /**
             * Start merging
             */
            while (!topTuples.areRunsExhausted()) {
                /**
                 * Get the top record
                 */
                ReferenceEntry top = topTuples.peek();
                int tupleIndex = top.getTupleIndex();
                int runIndex = topTuples.peek().getRunid();
                FrameTupleAccessor fta = top.getAccessor();

                int currentTupleInOutFrame = outFrameAccessor.getTupleCount() - 1;
                if (currentTupleInOutFrame < 0
                        || compareFrameTuples(fta, tupleIndex, outFrameAccessor, currentTupleInOutFrame) != 0) {
                    /**
                     * Initialize the first output record Reset the
                     * tuple builder
                     */

                    tupleBuilder.reset();

                    for (int k = 0; k < storedKeys.length; k++) {
                        tupleBuilder.addField(fta, tupleIndex, storedKeys[k]);
                    }

                    aggregator.init(tupleBuilder, fta, tupleIndex, aggregateState);

                    if (!outAppender.appendSkipEmptyField(tupleBuilder.getFieldEndOffsets(),
                            tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
                        flushOutFrame(writer, finalPass);
                        if (!outAppender.appendSkipEmptyField(tupleBuilder.getFieldEndOffsets(),
                                tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
                            throw new HyracksDataException(
                                    "The partial result is too large to be initialized in a frame.");
                        }
                    }

                } else {
                    /**
                     * if new tuple is in the same group of the
                     * current aggregator do merge and output to the
                     * outFrame
                     */

                    aggregator.aggregate(fta, tupleIndex, outFrameAccessor, currentTupleInOutFrame, aggregateState);

                }
                tupleIndices[runIndex]++;
                setNextTopTuple(runIndex, tupleIndices, runFileReaders, tupleAccessors, topTuples);
            }

            if (outAppender.getTupleCount() > 0) {
                flushOutFrame(writer, finalPass);
                outAppender.reset(outFrame, true);
            }

            aggregator.close();

            runs.subList(0, runNumber).clear();
            /**
             * insert the new run file into the beginning of the run
             * file list
             */
            if (!finalPass) {
                runs.add(0, ((RunFileWriter) writer).createReader());
            }
        } finally {
            if (!finalPass) {
                writer.close();
            }
        }
    }

    private void flushOutFrame(IFrameWriter writer, boolean isFinal) throws HyracksDataException {

        if (finalTupleBuilder == null) {
            finalTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);
        }

        if (writerFrame == null) {
            writerFrame = ctx.allocateFrame();
        }

        if (writerAppender == null) {
            writerAppender = new FrameTupleAppender(ctx.getFrameSize());
            writerAppender.reset(writerFrame, true);
        }

        outFrameAccessor.reset(outFrame);

        for (int i = 0; i < outFrameAccessor.getTupleCount(); i++) {

            finalTupleBuilder.reset();

            for (int k = 0; k < storedKeys.length; k++) {
                finalTupleBuilder.addField(outFrameAccessor, i, storedKeys[k]);
            }

            if (isFinal) {

                aggregator.outputFinalResult(finalTupleBuilder, outFrameAccessor, i, aggregateState);

            } else {

                aggregator.outputPartialResult(finalTupleBuilder, outFrameAccessor, i, aggregateState);
            }

            if (!writerAppender.appendSkipEmptyField(finalTupleBuilder.getFieldEndOffsets(),
                    finalTupleBuilder.getByteArray(), 0, finalTupleBuilder.getSize())) {
                FrameUtils.flushFrame(writerFrame, writer);
                writerAppender.reset(writerFrame, true);
                if (!writerAppender.appendSkipEmptyField(finalTupleBuilder.getFieldEndOffsets(),
                        finalTupleBuilder.getByteArray(), 0, finalTupleBuilder.getSize())) {
                    throw new HyracksDataException("Aggregation output is too large to be fit into a frame.");
                }
            }
        }
        if (writerAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(writerFrame, writer);
            writerAppender.reset(writerFrame, true);
        }

        outAppender.reset(outFrame, true);
    }

    private void setNextTopTuple(int runIndex, int[] tupleIndices, RunFileReader[] runCursors,
            FrameTupleAccessor[] tupleAccessors, ReferencedPriorityQueue topTuples) throws HyracksDataException {
        int runStart = runIndex * runFrameLimit;
        boolean existNext = false;
        if (tupleAccessors[currentFrameIndexInRun[runIndex]] == null || runCursors[runIndex] == null) {
            /**
             * run already closed
             */
            existNext = false;
        } else if (currentFrameIndexInRun[runIndex] - runStart < currentRunFrames[runIndex] - 1) {
            /**
             * not the last frame for this run
             */
            existNext = true;
            if (tupleIndices[runIndex] >= tupleAccessors[currentFrameIndexInRun[runIndex]].getTupleCount()) {
                tupleIndices[runIndex] = 0;
                currentFrameIndexInRun[runIndex]++;
            }
        } else if (tupleIndices[runIndex] < tupleAccessors[currentFrameIndexInRun[runIndex]].getTupleCount()) {
            /**
             * the last frame has expired
             */
            existNext = true;
        } else {
            /**
             * If all tuples in the targeting frame have been
             * checked.
             */
            tupleIndices[runIndex] = 0;
            currentFrameIndexInRun[runIndex] = runStart;
            /**
             * read in batch
             */
            currentRunFrames[runIndex] = 0;
            for (int j = 0; j < runFrameLimit; j++) {
                int frameIndex = currentFrameIndexInRun[runIndex] + j;
                if (runCursors[runIndex].nextFrame(inFrames.get(frameIndex))) {
                    tupleAccessors[frameIndex].reset(inFrames.get(frameIndex));
                    existNext = true;
                    currentRunFrames[runIndex]++;
                } else {
                    break;
                }
            }
        }

        if (existNext) {
            topTuples.popAndReplace(tupleAccessors[currentFrameIndexInRun[runIndex]], tupleIndices[runIndex]);
        } else {
            topTuples.pop();
            closeRun(runIndex, runCursors, tupleAccessors);
        }
    }

    /**
     * Close the run file, and also the corresponding readers and
     * input frame.
     * 
     * @param index
     * @param runCursors
     * @param tupleAccessor
     * @throws HyracksDataException
     */
    private void closeRun(int index, RunFileReader[] runCursors, IFrameTupleAccessor[] tupleAccessor)
            throws HyracksDataException {
        if (runCursors[index] != null) {
            runCursors[index].close();
            runCursors[index] = null;
            int frameOffset = index * runFrameLimit;
            for (int j = 0; j < runFrameLimit; j++) {
                tupleAccessor[frameOffset + j] = null;
            }
        }
    }

    private int compareFrameTuples(IFrameTupleAccessor fta1, int j1, IFrameTupleAccessor fta2, int j2) {
        byte[] b1 = fta1.getBuffer().array();
        byte[] b2 = fta2.getBuffer().array();
        for (int f = 0; f < keyFields.length; ++f) {
            int fIdx = f;
            int s1 = fta1.getTupleStartOffset(j1) + fta1.getFieldSlotsLength() + fta1.getFieldStartOffset(j1, fIdx);
            int l1 = fta1.getFieldLength(j1, fIdx);
            int s2 = fta2.getTupleStartOffset(j2) + fta2.getFieldSlotsLength() + fta2.getFieldStartOffset(j2, fIdx);
            int l2_start = fta2.getFieldStartOffset(j2, fIdx);
            int l2_end = fta2.getFieldEndOffset(j2, fIdx);
            int l2 = l2_end - l2_start;
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private Comparator<ReferenceEntry> createEntryComparator(final IBinaryComparator[] comparators) {
        return new Comparator<ReferenceEntry>() {

            @Override
            public int compare(ReferenceEntry o1, ReferenceEntry o2) {
                FrameTupleAccessor fta1 = (FrameTupleAccessor) o1.getAccessor();
                FrameTupleAccessor fta2 = (FrameTupleAccessor) o2.getAccessor();
                int j1 = o1.getTupleIndex();
                int j2 = o2.getTupleIndex();
                byte[] b1 = fta1.getBuffer().array();
                byte[] b2 = fta2.getBuffer().array();
                for (int f = 0; f < keyFields.length; ++f) {
                    int fIdx = f;
                    int s1 = fta1.getTupleStartOffset(j1) + fta1.getFieldSlotsLength()
                            + fta1.getFieldStartOffset(j1, fIdx);
                    int l1 = fta1.getFieldEndOffset(j1, fIdx) - fta1.getFieldStartOffset(j1, fIdx);
                    int s2 = fta2.getTupleStartOffset(j2) + fta2.getFieldSlotsLength()
                            + fta2.getFieldStartOffset(j2, fIdx);
                    int l2 = fta2.getFieldEndOffset(j2, fIdx) - fta2.getFieldStartOffset(j2, fIdx);
                    int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
                    if (c != 0) {
                        return c;
                    }
                }
                return 0;
            }

        };
    }
}