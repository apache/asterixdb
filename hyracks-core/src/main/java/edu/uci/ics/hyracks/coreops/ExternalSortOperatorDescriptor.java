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
package edu.uci.ics.hyracks.coreops;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePullable;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.context.HyracksContext;
import edu.uci.ics.hyracks.coreops.base.AbstractActivityNode;
import edu.uci.ics.hyracks.coreops.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.util.ReferenceEntry;
import edu.uci.ics.hyracks.coreops.util.ReferencedPriorityQueue;

public class ExternalSortOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final String IN_FRAMES = "inFrames";
    private static final String TPOINTERS = "tPointers";
    private static final String RUNS = "runs";

    private static final long serialVersionUID = 1L;
    private final int[] sortFields;
    private IBinaryComparatorFactory[] comparatorFactories;
    private final int framesLimit;

    public ExternalSortOperatorDescriptor(JobSpecification spec, int framesLimit, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.sortFields = sortFields;
        this.comparatorFactories = comparatorFactories;
        if (framesLimit <= 1) {
            throw new IllegalStateException();// minimum of 2 fames (1 in,1 out)
        }
        recordDescriptors[0] = recordDescriptor;
    }

    @Override
    public void contributeTaskGraph(IActivityGraphBuilder builder) {
        SortActivity sa = new SortActivity();
        MergeActivity ma = new MergeActivity();

        builder.addTask(sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addTask(ma);
        builder.addTargetEdge(0, ma, 0);

        builder.addBlockingEdge(sa, ma);
    }

    private class SortActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorDescriptor getOwner() {
            return ExternalSortOperatorDescriptor.this;
        }

        @Override
        public IOperatorNodePullable createPullRuntime(HyracksContext ctx, JobPlan plan, IOperatorEnvironment env,
                int partition) {
            return null;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final HyracksContext ctx, JobPlan plan,
                final IOperatorEnvironment env, int partition) {
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }
            IOperatorNodePushable op = new IOperatorNodePushable() {
                private final FrameTupleAccessor fta1 = new FrameTupleAccessor(ctx, recordDescriptors[0]);
                private final FrameTupleAccessor fta2 = new FrameTupleAccessor(ctx, recordDescriptors[0]);
                private List<ByteBuffer> inFrames;
                private ByteBuffer outFrame;
                private LinkedList<File> runs;
                private int activeInFrame;

                @Override
                public void open() throws HyracksDataException {
                    inFrames = new ArrayList<ByteBuffer>();
                    outFrame = ctx.getResourceManager().allocateFrame();
                    runs = new LinkedList<File>();
                    activeInFrame = 0;
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (activeInFrame + 1 >= framesLimit) { // + 1 outFrame.
                        try {
                            createRunFromInFrames(inFrames.size());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    ByteBuffer copy;
                    buffer.position(0);
                    buffer.limit(buffer.capacity());
                    if (runs.size() <= 0) {
                        copy = ctx.getResourceManager().allocateFrame();
                        copy.put(buffer);
                        inFrames.add(copy);
                    } else {
                        copy = inFrames.get(activeInFrame);
                        copy.put(buffer);
                    }
                    ++activeInFrame;
                }

                @Override
                public void close() throws HyracksDataException {
                    env.set(IN_FRAMES, inFrames);
                    env.set(RUNS, runs);
                    if (activeInFrame > 0) {
                        if (runs.size() <= 0) {
                            long[] tPointers = getSortedTPointers(activeInFrame);
                            env.set(TPOINTERS, tPointers);
                        } else {
                            createRunFromInFrames(activeInFrame);
                        }
                    }
                }

                private void createRunFromInFrames(int nBuffers) throws HyracksDataException {
                    File runFile;
                    try {
                        runFile = ctx.getResourceManager().createFile(
                                ExternalSortOperatorDescriptor.class.getSimpleName(), ".run");
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    RunFileWriter writer = new RunFileWriter(runFile);
                    writer.open();
                    try {
                        flushFrames(ctx, inFrames, outFrame, getSortedTPointers(nBuffers), writer);
                    } finally {
                        writer.close();
                    }
                    runs.add(runFile);
                    activeInFrame = 0;
                }

                private long[] getSortedTPointers(int nBuffers) {
                    FrameTupleAccessor accessor = new FrameTupleAccessor(ctx, recordDescriptors[0]);
                    int totalTCount = 0;
                    for (int i = 0; i < nBuffers; ++i) {
                        accessor.reset(inFrames.get(i));
                        totalTCount += accessor.getTupleCount();
                    }
                    long[] tPointers = new long[totalTCount];
                    int ptr = 0;
                    for (int i = 0; i < nBuffers; ++i) {
                        accessor.reset(inFrames.get(i));
                        int tCount = accessor.getTupleCount();
                        for (int j = 0; j < tCount; ++j) {
                            tPointers[ptr++] = (((long) i) << 32) + j;
                        }
                    }
                    if (tPointers.length > 0) {
                        sort(tPointers, 0, tPointers.length);
                    }
                    return tPointers;
                }

                private void sort(long[] tPointers, int offset, int length) {
                    int m = offset + (length >> 1);
                    long v = tPointers[m];

                    int a = offset;
                    int b = a;
                    int c = offset + length - 1;
                    int d = c;
                    while (true) {
                        while (b <= c && compare(tPointers[b], v) <= 0) {
                            if (compare(tPointers[b], v) == 0) {
                                swap(tPointers, a++, b);
                            }
                            ++b;
                        }
                        while (c >= b && compare(tPointers[c], v) >= 0) {
                            if (compare(tPointers[c], v) == 0) {
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

                private void swap(long x[], int a, int b) {
                    long t = x[a];
                    x[a] = x[b];
                    x[b] = t;
                }

                private void vecswap(long x[], int a, int b, int n) {
                    for (int i = 0; i < n; i++, a++, b++) {
                        swap(x, a, b);
                    }
                }

                private int compare(long tp1, long tp2) {
                    int i1 = (int) ((tp1 >> 32) & 0xffffffff);
                    int j1 = (int) (tp1 & 0xffffffff);
                    int i2 = (int) ((tp2 >> 32) & 0xffffffff);
                    int j2 = (int) (tp2 & 0xffffffff);
                    ByteBuffer buf1 = inFrames.get(i1);
                    ByteBuffer buf2 = inFrames.get(i2);
                    byte[] b1 = buf1.array();
                    byte[] b2 = buf2.array();
                    fta1.reset(buf1);
                    fta2.reset(buf2);
                    for (int f = 0; f < sortFields.length; ++f) {
                        int fIdx = sortFields[f];
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

                @Override
                public void setFrameWriter(int index, IFrameWriter writer) {
                    throw new IllegalArgumentException();
                }
            };
            return op;
        }

        @Override
        public boolean supportsPullInterface() {
            return false;
        }

        @Override
        public boolean supportsPushInterface() {
            return false;
        }
    }

    private class MergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorDescriptor getOwner() {
            return ExternalSortOperatorDescriptor.this;
        }

        @Override
        public IOperatorNodePullable createPullRuntime(HyracksContext ctx, JobPlan plan, IOperatorEnvironment env,
                int partition) {
            return null;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final HyracksContext ctx, JobPlan plan,
                final IOperatorEnvironment env, int partition) {
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }
            IOperatorNodePushable op = new IOperatorNodePushable() {
                private IFrameWriter writer;
                private List<ByteBuffer> inFrames;
                private ByteBuffer outFrame;
                LinkedList<File> runs;
                private FrameTupleAppender outFrameAppender;

                @Override
                public void open() throws HyracksDataException {
                    inFrames = (List<ByteBuffer>) env.get(IN_FRAMES);
                    outFrame = ctx.getResourceManager().allocateFrame();
                    runs = (LinkedList<File>) env.get(RUNS);
                    outFrameAppender = new FrameTupleAppender(ctx);
                    outFrameAppender.reset(outFrame, true);
                    writer.open();
                    try {
                        if (runs.size() <= 0) {
                            long[] tPointers = (long[]) env.get(TPOINTERS);
                            if (tPointers != null) {
                                flushFrames(ctx, inFrames, outFrame, tPointers, writer);
                                env.set(TPOINTERS, null);
                            }
                        } else {
                            int passCount = 0;
                            while (runs.size() > 0) {
                                passCount++;
                                try {
                                    doPass(runs, passCount);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    } finally {
                        writer.close();
                    }
                    env.set(IN_FRAMES, null);
                    env.set(RUNS, null);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    throw new IllegalStateException();
                }

                @Override
                public void close() throws HyracksDataException {
                    // do nothing
                }

                @Override
                public void setFrameWriter(int index, IFrameWriter writer) {
                    if (index != 0) {
                        throw new IllegalArgumentException();
                    }
                    this.writer = writer;
                }

                // creates a new run from runs that can fit in memory.
                private void doPass(LinkedList<File> runs, int passCount) throws ClassNotFoundException, Exception {
                    File newRun = null;
                    IFrameWriter writer = this.writer;
                    boolean finalPass = false;
                    if (runs.size() + 1 <= framesLimit) { // + 1 outFrame
                        finalPass = true;
                        for (int i = inFrames.size() - 1; i >= runs.size(); i--) {
                            inFrames.remove(i);
                        }
                    } else {
                        newRun = ctx.getResourceManager().createFile(
                                ExternalSortOperatorDescriptor.class.getSimpleName(), ".run");
                        writer = new RunFileWriter(newRun);
                    }
                    RunFileReader[] runCursors = new RunFileReader[inFrames.size()];
                    FrameTupleAccessor[] tupleAccessors = new FrameTupleAccessor[inFrames.size()];
                    Comparator<ReferenceEntry> comparator = createEntryComparator(comparators);
                    ReferencedPriorityQueue topTuples = new ReferencedPriorityQueue(ctx, recordDescriptors[0],
                            inFrames.size(), comparator);
                    int[] tupleIndexes = new int[inFrames.size()];
                    for (int i = 0; i < inFrames.size(); i++) {
                        tupleIndexes[i] = 0;
                        int runIndex = topTuples.peek().getRunid();
                        runCursors[runIndex] = new RunFileReader(runs.get(runIndex));
                        runCursors[runIndex].open();
                        if (runCursors[runIndex].nextFrame(inFrames.get(runIndex))) {
                            tupleAccessors[runIndex] = new FrameTupleAccessor(ctx, recordDescriptors[0]);
                            tupleAccessors[runIndex].reset(inFrames.get(runIndex));
                            setNextTopTuple(runIndex, tupleIndexes, runCursors, tupleAccessors, topTuples);
                        } else {
                            closeRun(runIndex, runCursors, tupleAccessors);
                        }
                    }

                    while (!topTuples.areRunsExhausted()) {
                        ReferenceEntry top = topTuples.peek();
                        int runIndex = top.getRunid();
                        FrameTupleAccessor fta = top.getAccessor();
                        int tupleIndex = top.getTupleIndex();

                        if (!outFrameAppender.append(fta, tupleIndex)) {
                            flushFrame(outFrame, writer);
                            outFrameAppender.reset(outFrame, true);
                            if (!outFrameAppender.append(fta, tupleIndex)) {
                                throw new IllegalStateException();
                            }
                        }

                        ++tupleIndexes[runIndex];
                        setNextTopTuple(runIndex, tupleIndexes, runCursors, tupleAccessors, topTuples);
                    }
                    if (outFrameAppender.getTupleCount() > 0) {
                        flushFrame(outFrame, writer);
                        outFrameAppender.reset(outFrame, true);
                    }
                    runs.subList(0, inFrames.size()).clear();
                    if (!finalPass) {
                        runs.add(0, newRun);
                    }
                }

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

                private boolean hasNextTuple(int runIndex, int[] tupleIndexes, RunFileReader[] runCursors,
                        FrameTupleAccessor[] tupleAccessors) throws IOException {
                    if (tupleAccessors[runIndex] == null || runCursors[runIndex] == null) {
                        return false;
                    } else if (tupleIndexes[runIndex] >= tupleAccessors[runIndex].getTupleCount()) {
                        ByteBuffer buf = tupleAccessors[runIndex].getBuffer(); // same-as-inFrames.get(runIndex)
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

                private void closeRun(int index, RunFileReader[] runCursors, FrameTupleAccessor[] tupleAccessor) {
                    runCursors[index] = null;
                    tupleAccessor[index] = null;
                }
            };
            return op;
        }

        @Override
        public boolean supportsPullInterface() {
            return false;
        }

        @Override
        public boolean supportsPushInterface() {
            return true;
        }
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
                for (int f = 0; f < sortFields.length; ++f) {
                    int fIdx = sortFields[f];
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

    private void flushFrames(HyracksContext ctx, List<ByteBuffer> inFrames, ByteBuffer outFrame, long[] tPointers,
            IFrameWriter writer) throws HyracksDataException {
        FrameTupleAccessor accessor = new FrameTupleAccessor(ctx, recordDescriptors[0]);
        FrameTupleAppender outFrameAppender = new FrameTupleAppender(ctx);
        for (ByteBuffer buf : inFrames) {
            buf.position(0);
            buf.limit(buf.capacity());
        }
        outFrameAppender.reset(outFrame, true);
        for (int ptr = 0; ptr < tPointers.length; ++ptr) {
            long tp = tPointers[ptr];
            int i = (int) ((tp >> 32) & 0xffffffff);
            int j = (int) (tp & 0xffffffff);
            ByteBuffer buffer = inFrames.get(i);
            accessor.reset(buffer);
            if (!outFrameAppender.append(accessor, j)) {
                flushFrame(outFrame, writer);
                outFrameAppender.reset(outFrame, true);
                if (!outFrameAppender.append(accessor, j)) {
                    throw new IllegalStateException();
                }
            }
        }
        if (outFrameAppender.getTupleCount() > 0) {
            flushFrame(outFrame, writer);
            outFrame.position(0);
            outFrame.limit(outFrame.capacity());
        }
    }

    private void flushFrame(ByteBuffer frame, IFrameWriter writer) throws HyracksDataException {
        frame.position(0);
        frame.limit(frame.capacity());
        writer.nextFrame(frame);
    }

    private class RunFileWriter implements IFrameWriter {
        private final File file;
        private FileChannel channel;

        public RunFileWriter(File file) {
            this.file = file;
        }

        @Override
        public void open() throws HyracksDataException {
            RandomAccessFile raf;
            try {
                raf = new RandomAccessFile(file, "rw");
            } catch (FileNotFoundException e) {
                throw new HyracksDataException(e);
            }
            channel = raf.getChannel();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            int remain = buffer.capacity();
            while (remain > 0) {
                int len;
                try {
                    len = channel.write(buffer);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
                if (len < 0) {
                    throw new HyracksDataException("Error writing data");
                }
                remain -= len;
            }
        }

        @Override
        public void close() throws HyracksDataException {
            try {
                channel.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    public static class RunFileReader implements IFrameReader {
        private final File file;
        private FileChannel channel;

        public RunFileReader(File file) throws FileNotFoundException {
            this.file = file;
        }

        @Override
        public void open() throws HyracksDataException {
            RandomAccessFile raf;
            try {
                raf = new RandomAccessFile(file, "r");
            } catch (FileNotFoundException e) {
                throw new HyracksDataException(e);
            }
            channel = raf.getChannel();
        }

        @Override
        public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
            buffer.clear();
            int remain = buffer.capacity();
            while (remain > 0) {
                int len;
                try {
                    len = channel.read(buffer);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
                if (len < 0) {
                    return false;
                }
                remain -= len;
            }
            return true;
        }

        @Override
        public void close() throws HyracksDataException {
            try {
                channel.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
    }
}