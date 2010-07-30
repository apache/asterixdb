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
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;

public class InMemorySortOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final String BUFFERS = "buffers";
    private static final String TPOINTERS = "tpointers";

    private static final long serialVersionUID = 1L;
    private final int[] sortFields;
    private IBinaryComparatorFactory[] comparatorFactories;

    public InMemorySortOperatorDescriptor(JobSpecification spec, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.sortFields = sortFields;
        this.comparatorFactories = comparatorFactories;
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
            return InMemorySortOperatorDescriptor.this;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; ++i) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }
            IOperatorNodePushable op = new IOperatorNodePushable() {
                private List<ByteBuffer> buffers;

                private final FrameTupleAccessor fta1 = new FrameTupleAccessor(ctx, recordDescriptors[0]);
                private final FrameTupleAccessor fta2 = new FrameTupleAccessor(ctx, recordDescriptors[0]);

                @Override
                public void open() throws HyracksDataException {
                    buffers = new ArrayList<ByteBuffer>();
                    env.set(BUFFERS, buffers);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    ByteBuffer copy = ctx.getResourceManager().allocateFrame();
                    FrameUtils.copy(buffer, copy);
                    buffers.add(copy);
                }

                @Override
                public void close() throws HyracksDataException {
                    FrameTupleAccessor accessor = new FrameTupleAccessor(ctx, recordDescriptors[0]);
                    int nBuffers = buffers.size();
                    int totalTCount = 0;
                    for (int i = 0; i < nBuffers; ++i) {
                        accessor.reset(buffers.get(i));
                        totalTCount += accessor.getTupleCount();
                    }
                    long[] tPointers = new long[totalTCount];
                    int ptr = 0;
                    for (int i = 0; i < nBuffers; ++i) {
                        accessor.reset(buffers.get(i));
                        int tCount = accessor.getTupleCount();
                        for (int j = 0; j < tCount; ++j) {
                            tPointers[ptr++] = (((long) i) << 32) + j;
                        }
                    }
                    if (tPointers.length > 0) {
                        sort(tPointers, 0, tPointers.length);
                    }
                    env.set(TPOINTERS, tPointers);
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
                    ByteBuffer buf1 = buffers.get(i1);
                    ByteBuffer buf2 = buffers.get(i2);
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
                public void setFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                    throw new IllegalArgumentException();
                }
            };
            return op;
        }
    }

    private class MergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        @Override
        public IOperatorDescriptor getOwner() {
            return InMemorySortOperatorDescriptor.this;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            IOperatorNodePushable op = new IOperatorNodePushable() {
                private IFrameWriter writer;

                @Override
                public void open() throws HyracksDataException {
                    List<ByteBuffer> buffers = (List<ByteBuffer>) env.get(BUFFERS);
                    long[] tPointers = (long[]) env.get(TPOINTERS);
                    FrameTupleAccessor accessor = new FrameTupleAccessor(ctx, recordDescriptors[0]);
                    FrameTupleAppender appender = new FrameTupleAppender(ctx);
                    ByteBuffer outFrame = ctx.getResourceManager().allocateFrame();
                    writer.open();
                    appender.reset(outFrame, true);
                    for (int ptr = 0; ptr < tPointers.length; ++ptr) {
                        long tp = tPointers[ptr];
                        int i = (int) ((tp >> 32) & 0xffffffff);
                        int j = (int) (tp & 0xffffffff);
                        ByteBuffer buffer = buffers.get(i);
                        accessor.reset(buffer);
                        if (!appender.append(accessor, j)) {
                            flushFrame(outFrame);
                            appender.reset(outFrame, true);
                            if (!appender.append(accessor, j)) {
                                throw new IllegalStateException();
                            }
                        }
                    }
                    if (appender.getTupleCount() > 0) {
                        flushFrame(outFrame);
                    }
                    writer.close();
                    env.set(BUFFERS, null);
                    env.set(TPOINTERS, null);
                }

                private void flushFrame(ByteBuffer frame) throws HyracksDataException {
                    frame.position(0);
                    frame.limit(frame.capacity());
                    writer.nextFrame(frame);
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
                public void setFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                    if (index != 0) {
                        throw new IllegalArgumentException();
                    }
                    this.writer = writer;
                }
            };
            return op;
        }
    }
}