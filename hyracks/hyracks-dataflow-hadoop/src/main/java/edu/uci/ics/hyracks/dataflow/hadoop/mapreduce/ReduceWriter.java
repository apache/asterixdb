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
package edu.uci.ics.hyracks.dataflow.hadoop.mapreduce;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class ReduceWriter<K2, V2, K3, V3> implements IFrameWriter {
    private final IHyracksTaskContext ctx;
    private final HadoopHelper helper;
    private final int[] groupFields;
    private final FrameTupleAccessor accessor0;
    private final FrameTupleAccessor accessor1;
    private final ByteBuffer copyFrame;
    private final IBinaryComparator[] comparators;
    private final KVIterator kvi;
    private final Reducer<K2, V2, K3, V3> reducer;
    private final RecordWriter<K3, V3> recordWriter;
    private final TaskAttemptID taId;
    private final TaskAttemptContext taskAttemptContext;

    private boolean first;
    private boolean groupStarted;
    private List<ByteBuffer> group;
    private int bPtr;
    private FrameTupleAppender fta;
    private Counter keyCounter;
    private Counter valueCounter;

    public ReduceWriter(IHyracksTaskContext ctx, HadoopHelper helper, int[] groupFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor,
            Reducer<K2, V2, K3, V3> reducer, RecordWriter<K3, V3> recordWriter, TaskAttemptID taId,
            TaskAttemptContext taskAttemptContext) throws HyracksDataException {
        this.ctx = ctx;
        this.helper = helper;
        this.groupFields = groupFields;
        accessor0 = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        accessor1 = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        copyFrame = ctx.allocateFrame();
        accessor1.reset(copyFrame);
        comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.reducer = reducer;
        this.recordWriter = recordWriter;
        this.taId = taId;
        this.taskAttemptContext = taskAttemptContext;

        kvi = new KVIterator(ctx, helper, recordDescriptor);
    }

    @Override
    public void open() throws HyracksDataException {
        first = true;
        groupStarted = false;
        group = new ArrayList<ByteBuffer>();
        bPtr = 0;
        group.add(ctx.allocateFrame());
        fta = new FrameTupleAppender(ctx.getFrameSize());
        keyCounter = new Counter() {
        };
        valueCounter = new Counter() {
        };
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor0.reset(buffer);
        int nTuples = accessor0.getTupleCount();
        for (int i = 0; i < nTuples; ++i) {
            if (first) {
                groupInit();
                first = false;
            } else {
                if (i == 0) {
                    switchGroupIfRequired(accessor1, accessor1.getTupleCount() - 1, accessor0, i);
                } else {
                    switchGroupIfRequired(accessor0, i - 1, accessor0, i);
                }
            }
            accumulate(accessor0, i);
        }
        FrameUtils.copy(buffer, copyFrame);
    }

    private void accumulate(FrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
        if (!fta.append(accessor, tIndex)) {
            ++bPtr;
            if (group.size() <= bPtr) {
                group.add(ctx.allocateFrame());
            }
            fta.reset(group.get(bPtr), true);
            if (!fta.append(accessor, tIndex)) {
                throw new HyracksDataException("Record size ("
                        + (accessor.getTupleEndOffset(tIndex) - accessor.getTupleStartOffset(tIndex))
                        + ") larger than frame size (" + group.get(bPtr).capacity() + ")");
            }
        }
    }

    private void switchGroupIfRequired(FrameTupleAccessor prevTupleAccessor, int prevTupleIndex,
            FrameTupleAccessor currTupleAccessor, int currTupleIndex) throws HyracksDataException {
        if (!sameGroup(prevTupleAccessor, prevTupleIndex, currTupleAccessor, currTupleIndex)) {
            reduce();
            groupInit();
        }
    }

    private void groupInit() {
        groupStarted = true;
        bPtr = 0;
        fta.reset(group.get(0), true);
    }

    private void reduce() throws HyracksDataException {
        kvi.reset(group, bPtr + 1);
        try {
            Reducer<K2, V2, K3, V3>.Context rCtx = reducer.new Context(helper.getConfiguration(), taId, kvi,
                    keyCounter, valueCounter, recordWriter, null, null,
                    (RawComparator<K2>) helper.getRawGroupingComparator(), (Class<K2>) helper.getJob()
                            .getMapOutputKeyClass(), (Class<V2>) helper.getJob().getMapOutputValueClass());
            reducer.run(rCtx);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        groupStarted = false;
    }

    private boolean sameGroup(FrameTupleAccessor a1, int t1Idx, FrameTupleAccessor a2, int t2Idx) {
        for (int i = 0; i < comparators.length; ++i) {
            int fIdx = groupFields[i];
            int s1 = a1.getTupleStartOffset(t1Idx) + a1.getFieldSlotsLength() + a1.getFieldStartOffset(t1Idx, fIdx);
            int l1 = a1.getFieldLength(t1Idx, fIdx);
            int s2 = a2.getTupleStartOffset(t2Idx) + a2.getFieldSlotsLength() + a2.getFieldStartOffset(t2Idx, fIdx);
            int l2 = a2.getFieldLength(t2Idx, fIdx);
            if (comparators[i].compare(a1.getBuffer().array(), s1, l1, a2.getBuffer().array(), s2, l2) != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() throws HyracksDataException {
        if (groupStarted) {
            reduce();
        }
        try {
            recordWriter.close(taskAttemptContext);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
    }
}
