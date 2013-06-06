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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.util.Progress;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class KVIterator implements RawKeyValueIterator {
    private final HadoopHelper helper;
    private FrameTupleAccessor accessor;
    private DataInputBuffer kBuffer;
    private DataInputBuffer vBuffer;
    private List<ByteBuffer> buffers;
    private int bSize;
    private int bPtr;
    private int tIdx;
    private boolean eog;

    public KVIterator(IHyracksTaskContext ctx, HadoopHelper helper, RecordDescriptor recordDescriptor) {
        this.helper = helper;
        accessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        kBuffer = new DataInputBuffer();
        vBuffer = new DataInputBuffer();
    }

    void reset(List<ByteBuffer> buffers, int bSize) {
        this.buffers = buffers;
        this.bSize = bSize;
        bPtr = 0;
        tIdx = 0;
        eog = false;
        if (bSize > 0) {
            accessor.reset(buffers.get(0));
            tIdx = -1;
        } else {
            eog = true;
        }
    }

    @Override
    public DataInputBuffer getKey() throws IOException {
        return kBuffer;
    }

    @Override
    public DataInputBuffer getValue() throws IOException {
        return vBuffer;
    }

    @Override
    public boolean next() throws IOException {
        while (true) {
            if (eog) {
                return false;
            }
            ++tIdx;
            if (accessor.getTupleCount() <= tIdx) {
                ++bPtr;
                if (bPtr >= bSize) {
                    eog = true;
                    continue;
                }
                tIdx = -1;
                accessor.reset(buffers.get(bPtr));
                continue;
            }
            kBuffer.reset(accessor.getBuffer().array(),
                    FrameUtils.getAbsoluteFieldStartOffset(accessor, tIdx, helper.KEY_FIELD_INDEX),
                    accessor.getFieldLength(tIdx, helper.KEY_FIELD_INDEX));
            vBuffer.reset(accessor.getBuffer().array(),
                    FrameUtils.getAbsoluteFieldStartOffset(accessor, tIdx, helper.VALUE_FIELD_INDEX),
                    accessor.getFieldLength(tIdx, helper.VALUE_FIELD_INDEX));
            break;
        }
        return true;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Progress getProgress() {
        return null;
    }
}
