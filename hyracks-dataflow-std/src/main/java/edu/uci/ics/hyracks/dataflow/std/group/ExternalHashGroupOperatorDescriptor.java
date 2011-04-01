/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceEntry;
import edu.uci.ics.hyracks.dataflow.std.util.ReferencedPriorityQueue;

/**
 * This is an implementation of the external hash group operator.
 * The motivation of this operator is that when tuples are processed in
 * parallel, distinguished aggregating keys partitioned on one node may exceed
 * the main memory, so aggregation results should be output onto the disk to
 * make space for aggregating more input tuples.
 */
public class ExternalHashGroupOperatorDescriptor extends AbstractOperatorDescriptor {

    /**
     * The input frame identifier (in the job environment)
     */
    private static final String GROUPTABLES = "gtables";

    /**
     * The runs files identifier (in the job environment)
     */
    private static final String RUNS = "runs";

    /**
     * The fields used for grouping (grouping keys).
     */
    private final int[] keyFields;

    /**
     * The comparator for checking the grouping conditions, corresponding to the {@link #keyFields}.
     */
    private final IBinaryComparatorFactory[] comparatorFactories;

    /**
     * The aggregator factory for the aggregating field, corresponding to the {@link #aggregateFields}.
     */
    private IAccumulatingAggregatorFactory aggregatorFactory;

    /**
     * The maximum number of frames in the main memory.
     */
    private final int framesLimit;

    /**
     * Indicate whether the final output will be sorted or not.
     */
    private final boolean sortOutput;

    /**
     * Partition computer factory
     */
    private final ITuplePartitionComputerFactory tpcf;

    /**
     * The size of the in-memory table, which should be specified now by the
     * creator of this operator descriptor.
     */
    private final int tableSize;

    /**
     * XXX Logger for debug information
     */
    private static Logger LOGGER = Logger.getLogger(ExternalHashGroupOperatorDescriptor.class.getName());

    /**
     * Constructor of the external hash group operator descriptor.
     * 
     * @param spec
     * @param keyFields
     *            The fields as keys of grouping.
     * @param framesLimit
     *            The maximum number of frames to be used in memory.
     * @param sortOutput
     *            Whether the output should be sorted or not. Note that if the
     *            input data is large enough for external grouping, the output
     *            will be sorted surely. The only case that when the output is
     *            not sorted is when the size of the input data can be grouped
     *            in memory and this parameter is false.
     * @param tpcf
     *            The partitioner.
     * @param comparatorFactories
     *            The comparators.
     * @param aggregatorFactory
     *            The aggregators.
     * @param recordDescriptor
     *            The record descriptor for the input data.
     * @param tableSize
     *            The maximum size of the in memory table usable to this
     *            operator.
     */
    public ExternalHashGroupOperatorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit,
            boolean sortOutput, ITuplePartitionComputerFactory tpcf, IBinaryComparatorFactory[] comparatorFactories,
            IAccumulatingAggregatorFactory aggregatorFactory, RecordDescriptor recordDescriptor, int tableSize) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        if (framesLimit <= 1) {
            // Minimum of 2 frames: 1 for input records, and 1 for output
            // aggregation results.
            throw new IllegalStateException();
        }
        this.aggregatorFactory = aggregatorFactory;
        this.keyFields = keyFields;
        this.comparatorFactories = comparatorFactories;

        this.sortOutput = sortOutput;

        this.tpcf = tpcf;

        this.tableSize = tableSize;

        // Set the record descriptor. Note that since this operator is a unary
        // operator,
        // only the first record descritpor is used here.
        recordDescriptors[0] = recordDescriptor;
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor#contributeTaskGraph
     * (edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder)
     */
    @Override
    public void contributeTaskGraph(IActivityGraphBuilder builder) {
        PartialAggregateActivity partialAggAct = new PartialAggregateActivity();
        MergeActivity mergeAct = new MergeActivity();

        builder.addTask(partialAggAct);
        builder.addSourceEdge(0, partialAggAct, 0);

        builder.addTask(mergeAct);
        builder.addTargetEdge(0, mergeAct, 0);

        // FIXME Block or not?
        builder.addBlockingEdge(partialAggAct, mergeAct);

    }

    private class PartialAggregateActivity extends AbstractActivityNode {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
                final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            // Create the in-memory hash table
            final SpillableGroupingHashTable gTable = new SpillableGroupingHashTable(ctx, keyFields,
                    comparatorFactories, tpcf, aggregatorFactory, recordDescProvider.getInputRecordDescriptor(
                            getOperatorId(), 0), recordDescriptors[0],
                    // Always take one frame for the input records
                    framesLimit - 1, tableSize);
            // Create the tuple accessor
            final FrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(),
                    recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0));
            // Create the partial aggregate activity node
            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {

                /**
                 * Run files
                 */
                private LinkedList<RunFileReader> runs;

                @Override
                public void close() throws HyracksDataException {
                    if (gTable.getFrameCount() >= 0) {
                        if (runs.size() <= 0) {
                            // All in memory
                            env.set(GROUPTABLES, gTable);
                        } else {
                            // flush the memory into the run file.
                            flushFramesToRun();
                        }
                    }
                    env.set(RUNS, runs);
                }

                @Override
                public void flush() throws HyracksDataException {

                }

                /**
                 * Process the next input buffer.
                 * The actual insertion is processed in {@link #gTable}. It will
                 * check whether it is possible to contain the data into the
                 * main memory or not. If not, it will indicate the operator to
                 * flush the content of the table into a run file.
                 */
                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor.reset(buffer);
                    int tupleCount = accessor.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        // If the group table is too large, flush the table into
                        // a run file.
                        if (!gTable.insert(accessor, i)) {
                            flushFramesToRun();
                            if (!gTable.insert(accessor, i))
                                throw new HyracksDataException(
                                        "Failed to insert a new buffer into the aggregate operator!");
                        }
                    }

                }

                @Override
                public void open() throws HyracksDataException {
                    runs = new LinkedList<RunFileReader>();
                    gTable.reset();
                }

                /**
                 * Flush the content of the group table into a run file.
                 * During the flushing, the hash table will be sorted as first.
                 * After that, a run file handler is initialized and the hash
                 * table is flushed into the run file.
                 * 
                 * @throws HyracksDataException
                 */
                private void flushFramesToRun() throws HyracksDataException {
                    // Sort the contents of the hash table.
                    gTable.sortFrames();
                    FileReference runFile;
                    try {
                        runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                                ExternalHashGroupOperatorDescriptor.class.getSimpleName());
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    RunFileWriter writer = new RunFileWriter(runFile, ctx.getIOManager());
                    writer.open();
                    try {
                        gTable.flushFrames(writer, true);
                    } catch (Exception ex) {
                        throw new HyracksDataException(ex);
                    } finally {
                        writer.close();
                    }
                    gTable.reset();
                    runs.add(((RunFileWriter) writer).createReader());
                    LOGGER.warning("Created run file: " + runFile.getFile().getAbsolutePath());
                }

            };

            return op;
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return ExternalHashGroupOperatorDescriptor.this;
        }

    }

    private class MergeActivity extends AbstractActivityNode {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }
            IOperatorNodePushable op = new AbstractUnaryOutputSourceOperatorNodePushable() {
                /**
                 * Input frames, one for each run file.
                 */
                private List<ByteBuffer> inFrames;

                /**
                 * Output frame.
                 */
                private ByteBuffer outFrame;

                /**
                 * List of the run files to be merged
                 */
                LinkedList<RunFileReader> runs;

                /**
                 * Tuple appender for the output frame {@link #outFrame}.
                 */
                private FrameTupleAppender outFrameAppender;

                private ISpillableAccumulatingAggregator visitingAggregator;
                private ArrayTupleBuilder visitingKeyTuple;

                @SuppressWarnings("unchecked")
                @Override
                public void initialize() throws HyracksDataException {
                    runs = (LinkedList<RunFileReader>) env.get(RUNS);
                    writer.open();

                    try {
                        if (runs.size() <= 0) {
                            // If the aggregate results can be fit into
                            // memory...
                            SpillableGroupingHashTable gTable = (SpillableGroupingHashTable) env.get(GROUPTABLES);
                            if (gTable != null) {
                                gTable.flushFrames(writer, sortOutput);
                            }
                            env.set(GROUPTABLES, null);
                        } else {
                            // Otherwise, merge the run files into a single file
                            inFrames = new ArrayList<ByteBuffer>();
                            outFrame = ctx.allocateFrame();
                            outFrameAppender = new FrameTupleAppender(ctx.getFrameSize());
                            outFrameAppender.reset(outFrame, true);
                            for (int i = 0; i < framesLimit - 1; ++i) {
                                inFrames.add(ctx.allocateFrame());
                            }
                            int passCount = 0;
                            while (runs.size() > 0) {
                                passCount++;
                                try {
                                    doPass(runs, passCount);
                                } catch (Exception e) {
                                    throw new HyracksDataException(e);
                                }
                            }
                        }

                    } finally {
                        writer.close();
                    }
                    env.set(RUNS, null);
                }

                /**
                 * Merge the run files once.
                 * 
                 * @param runs
                 * @param passCount
                 * @throws HyracksDataException
                 * @throws IOException
                 */
                private void doPass(LinkedList<RunFileReader> runs, int passCount) throws HyracksDataException,
                        IOException {
                    FileReference newRun = null;
                    IFrameWriter writer = this.writer;
                    boolean finalPass = false;

                    int[] storedKeys = new int[keyFields.length];
                    // Get the list of the fields in the stored records.
                    for (int i = 0; i < keyFields.length; ++i) {
                        storedKeys[i] = i;
                    }

                    // Release the space not used
                    if (runs.size() + 1 <= framesLimit) {
                        // If there are run files no more than the available
                        // frame slots...
                        // No run file to be generated, since the result can be
                        // directly
                        // outputted into the output frame for write.
                        finalPass = true;
                        for (int i = inFrames.size() - 1; i >= runs.size(); i--) {
                            inFrames.remove(i);
                        }
                    } else {
                        // Otherwise, a new run file will be created
                        newRun = ctx.getJobletContext().createManagedWorkspaceFile(
                                ExternalHashGroupOperatorDescriptor.class.getSimpleName());
                        writer = new RunFileWriter(newRun, ctx.getIOManager());
                        writer.open();
                    }
                    try {
                        // Create run file read handler for each input frame
                        RunFileReader[] runFileReaders = new RunFileReader[inFrames.size()];
                        // Create input frame accessor
                        FrameTupleAccessor[] tupleAccessors = new FrameTupleAccessor[inFrames.size()];
                        Comparator<ReferenceEntry> comparator = createEntryComparator(comparators);
                        ReferencedPriorityQueue topTuples = new ReferencedPriorityQueue(ctx.getFrameSize(),
                                recordDescriptors[0], inFrames.size(), comparator);
                        // For the index of tuples visited in each frame.
                        int[] tupleIndexes = new int[inFrames.size()];
                        for (int i = 0; i < inFrames.size(); i++) {
                            tupleIndexes[i] = 0;
                            int runIndex = topTuples.peek().getRunid();
                            runFileReaders[runIndex] = runs.get(runIndex);
                            runFileReaders[runIndex].open();
                            // Load the first frame of the file into the main
                            // memory
                            if (runFileReaders[runIndex].nextFrame(inFrames.get(runIndex))) {
                                // initialize the tuple accessor for the frame
                                tupleAccessors[runIndex] = new FrameTupleAccessor(ctx.getFrameSize(),
                                        recordDescriptors[0]);
                                tupleAccessors[runIndex].reset(inFrames.get(runIndex));
                                setNextTopTuple(runIndex, tupleIndexes, runFileReaders, tupleAccessors, topTuples);
                            } else {
                                closeRun(runIndex, runFileReaders, tupleAccessors);
                            }
                        }
                        // Merge
                        // Get a key holder for the current working
                        // aggregator keys
                        visitingAggregator = null;
                        visitingKeyTuple = null;
                        // Loop on all run files, and update the key
                        // holder.
                        while (!topTuples.areRunsExhausted()) {
                            // Get the top record
                            ReferenceEntry top = topTuples.peek();
                            int tupleIndex = top.getTupleIndex();
                            int runIndex = topTuples.peek().getRunid();
                            FrameTupleAccessor fta = top.getAccessor();
                            if (visitingAggregator == null) {
                                // Initialize the aggregator
                                visitingAggregator = aggregatorFactory.createSpillableAggregator(ctx,
                                        recordDescriptors[0], recordDescriptors[0]);
                                // Initialize the partial aggregation result
                                visitingAggregator.initFromPartial(fta, tupleIndex, keyFields);
                                visitingKeyTuple = new ArrayTupleBuilder(recordDescriptors[0].getFields().length);
                                for (int i = 0; i < keyFields.length; i++) {
                                    visitingKeyTuple.addField(fta, tupleIndex, keyFields[i]);
                                }
                            } else {
                                if (compareTupleWithFrame(visitingKeyTuple, fta, tupleIndex, storedKeys, keyFields,
                                        comparators) == 0) {
                                    // If the two partial results are on the
                                    // same key
                                    visitingAggregator.accumulatePartialResult(fta, tupleIndex, keyFields);
                                } else {
                                    // Otherwise, write the partial result back
                                    // to the output frame
                                    if (!visitingAggregator.output(outFrameAppender, visitingKeyTuple)) {
                                        FrameUtils.flushFrame(outFrame, writer);
                                        outFrameAppender.reset(outFrame, true);
                                        if (!visitingAggregator.output(outFrameAppender, visitingKeyTuple)) {
                                            throw new IllegalStateException();
                                        }
                                    }
                                    // Reset the partial aggregation result
                                    visitingAggregator.initFromPartial(fta, tupleIndex, keyFields);
                                    visitingKeyTuple.reset();
                                    for (int i = 0; i < keyFields.length; i++) {
                                        visitingKeyTuple.addField(fta, tupleIndex, keyFields[i]);
                                    }
                                }
                            }
                            tupleIndexes[runIndex]++;
                            setNextTopTuple(runIndex, tupleIndexes, runFileReaders, tupleAccessors, topTuples);
                        }
                        // Output the last aggregation result in the frame
                        if (visitingAggregator != null) {
                            if (!visitingAggregator.output(outFrameAppender, visitingKeyTuple)) {
                                FrameUtils.flushFrame(outFrame, writer);
                                outFrameAppender.reset(outFrame, true);
                                if (!visitingAggregator.output(outFrameAppender, visitingKeyTuple)) {
                                    throw new IllegalStateException();
                                }
                            }
                        }
                        // Output data into run file writer after all tuples
                        // have been checked
                        if (outFrameAppender.getTupleCount() > 0) {
                            FrameUtils.flushFrame(outFrame, writer);
                            outFrameAppender.reset(outFrame, true);
                        }
                        // empty the input frames
                        runs.subList(0, inFrames.size()).clear();
                        // insert the new run file into the beginning of the run
                        // file list
                        if (!finalPass) {
                            runs.add(0, ((RunFileWriter) writer).createReader());
                        }
                    } catch (Exception ex) {
                        throw new HyracksDataException(ex);
                    } finally {
                        if (!finalPass) {
                            writer.close();
                        }
                    }
                }

                /**
                 * Insert the tuple into the priority queue.
                 * 
                 * @param runIndex
                 * @param tupleIndexes
                 * @param runCursors
                 * @param tupleAccessors
                 * @param topTuples
                 * @throws IOException
                 */
                private void setNextTopTuple(int runIndex, int[] tupleIndexes, RunFileReader[] runCursors,
                        FrameTupleAccessor[] tupleAccessors, ReferencedPriorityQueue topTuples) throws IOException {
                    boolean exists = hasNextTuple(runIndex, tupleIndexes, runCursors, tupleAccessors);
                    if (exists) {
                        topTuples.popAndReplace(tupleAccessors[runIndex], tupleIndexes[runIndex]);
                    } else {
                        topTuples.pop();
                        closeRun(runIndex, runCursors, tupleAccessors);
                    }
                }

                /**
                 * Check whether there are any more tuples to be checked for the
                 * given run file from the corresponding input frame.
                 * If the input frame for this run file is exhausted, load a new
                 * frame of the run file into the input frame.
                 * 
                 * @param runIndex
                 * @param tupleIndexes
                 * @param runCursors
                 * @param tupleAccessors
                 * @return
                 * @throws IOException
                 */
                private boolean hasNextTuple(int runIndex, int[] tupleIndexes, RunFileReader[] runCursors,
                        FrameTupleAccessor[] tupleAccessors) throws IOException {

                    if (tupleAccessors[runIndex] == null || runCursors[runIndex] == null) {
                        /*
                         * Return false if the targeting run file is not
                         * available, or the frame for the run file is not
                         * available.
                         */
                        return false;
                    } else if (tupleIndexes[runIndex] >= tupleAccessors[runIndex].getTupleCount()) {
                        /*
                         * If all tuples in the targeting frame have been
                         * checked.
                         */
                        ByteBuffer buf = tupleAccessors[runIndex].getBuffer(); // same-as-inFrames.get(runIndex)
                        // Refill the buffer with contents from the run file.
                        if (runCursors[runIndex].nextFrame(buf)) {
                            tupleIndexes[runIndex] = 0;
                            return hasNextTuple(runIndex, tupleIndexes, runCursors, tupleAccessors);
                        } else {
                            return false;
                        }
                    } else {
                        return true;
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
                    runCursors[index].close();
                    runCursors[index] = null;
                    tupleAccessor[index] = null;
                }

                /**
                 * Compare a tuple (in the format of a {@link ArrayTupleBuilder} ) with a record in a frame (in the format of a {@link FrameTupleAccessor}). Comparing keys and comparators
                 * are specified for this method as inputs.
                 * 
                 * @param tuple0
                 * @param accessor1
                 * @param tIndex1
                 * @param keys0
                 * @param keys1
                 * @param comparators
                 * @return
                 */
                private int compareTupleWithFrame(ArrayTupleBuilder tuple0, FrameTupleAccessor accessor1, int tIndex1,
                        int[] keys0, int[] keys1, IBinaryComparator[] comparators) {
                    int tStart1 = accessor1.getTupleStartOffset(tIndex1);
                    int fStartOffset1 = accessor1.getFieldSlotsLength() + tStart1;

                    for (int i = 0; i < keys0.length; ++i) {
                        int fIdx0 = keys0[i];
                        int fStart0 = (i == 0 ? 0 : tuple0.getFieldEndOffsets()[fIdx0 - 1]);
                        int fEnd0 = tuple0.getFieldEndOffsets()[fIdx0];
                        int fLen0 = fEnd0 - fStart0;

                        int fIdx1 = keys1[i];
                        int fStart1 = accessor1.getFieldStartOffset(tIndex1, fIdx1);
                        int fEnd1 = accessor1.getFieldEndOffset(tIndex1, fIdx1);
                        int fLen1 = fEnd1 - fStart1;

                        int c = comparators[i].compare(tuple0.getByteArray(), fStart0, fLen0, accessor1.getBuffer()
                                .array(), fStart1 + fStartOffset1, fLen1);
                        if (c != 0) {
                            return c;
                        }
                    }
                    return 0;
                }
            };
            return op;
        }

        @Override
        public IOperatorDescriptor getOwner() {
            return ExternalHashGroupOperatorDescriptor.this;
        }

        private Comparator<ReferenceEntry> createEntryComparator(final IBinaryComparator[] comparators) {
            return new Comparator<ReferenceEntry>() {
                public int compare(ReferenceEntry tp1, ReferenceEntry tp2) {
                    FrameTupleAccessor fta1 = (FrameTupleAccessor) tp1.getAccessor();
                    FrameTupleAccessor fta2 = (FrameTupleAccessor) tp2.getAccessor();
                    int j1 = (Integer) tp1.getTupleIndex();
                    int j2 = (Integer) tp2.getTupleIndex();
                    byte[] b1 = fta1.getBuffer().array();
                    byte[] b2 = fta2.getBuffer().array();
                    for (int f = 0; f < keyFields.length; ++f) {
                        int fIdx = keyFields[f];
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
}
