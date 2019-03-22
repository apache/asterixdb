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

package org.apache.hyracks.dataflow.std.sort.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

/**
 * This {@code GroupFrameAccessor} access a group of logical frames which are stored in one physical
 * continuous ByteBuffer. It is used in a RunFileReader which can read several frames at once, and we
 * can use this accessor to parse the returned data as one frame. In the caller's view there is only
 * one frame which simply the caller's work.
 */
public class GroupFrameAccessor implements IFrameTupleAccessor {

    private class InnerFrameInfo implements Comparable<MutableInt> {
        int start;
        int length;
        int tupleCount;

        InnerFrameInfo(int start, int length, int tupleCount) {
            this.start = start;
            this.length = length;
            this.tupleCount = tupleCount;
        }

        @Override
        public int compareTo(MutableInt other) {
            return -Integer.compare(other.intValue(), tupleCount);
        }
    }

    private final RecordDescriptor recordDescriptor;
    private final int minFrameSize;
    private final FrameTupleAccessor frameTupleAccessor;
    private final List<InnerFrameInfo> innerFrameInfos;
    private final MutableInt binarySearchKey;
    private int lastFrameId;
    // the start tuple index of the last accessed frame (inclusive)
    private int lastFrameStart;
    // the end tuple index of the last accessed frame (exclusive)
    private int lastFrameEnd;
    private ByteBuffer buffer;

    public GroupFrameAccessor(int minFrameSize, RecordDescriptor recordDescriptor) {
        this.minFrameSize = minFrameSize;
        this.recordDescriptor = (recordDescriptor);
        this.frameTupleAccessor = new FrameTupleAccessor(recordDescriptor);
        this.innerFrameInfos = new ArrayList<>();
        binarySearchKey = new MutableInt();
    }

    @Override
    public int getFieldCount() {
        return recordDescriptor.getFieldCount();
    }

    @Override
    public int getFieldSlotsLength() {
        return frameTupleAccessor.getFieldSlotsLength();
    }

    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        return frameTupleAccessor.getFieldEndOffset(resetSubTupleAccessor(tupleIndex), fIdx);
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return frameTupleAccessor.getFieldStartOffset(resetSubTupleAccessor(tupleIndex), fIdx);
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return frameTupleAccessor.getFieldLength(resetSubTupleAccessor(tupleIndex), fIdx);
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        return frameTupleAccessor.getTupleLength(resetSubTupleAccessor(tupleIndex));
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return frameTupleAccessor.getTupleEndOffset(resetSubTupleAccessor(tupleIndex));
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        return frameTupleAccessor.getTupleStartOffset(resetSubTupleAccessor(tupleIndex));
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        return frameTupleAccessor.getAbsoluteFieldStartOffset(resetSubTupleAccessor(tupleIndex), fIdx);
    }

    @Override
    public int getTupleCount() {
        return innerFrameInfos.size() > 0 ? innerFrameInfos.get(innerFrameInfos.size() - 1).tupleCount : 0;
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void reset(ByteBuffer buffer) {
        this.buffer = buffer;
        this.lastFrameId = -1;
        this.lastFrameStart = -1;
        this.lastFrameEnd = -1;
        parseGroupedBuffer(0, buffer.limit());
    }

    private void parseGroupedBuffer(int start, int stop) {
        this.innerFrameInfos.clear();
        int i = start;
        while (i < stop) {
            int unitSize = FrameHelper.deserializeNumOfMinFrame(buffer, i) * minFrameSize;
            if (unitSize == 0) { // run consumed.
                break;
            }
            if (i + unitSize > stop) { // contains future partial run, stop here
                break;
            }
            frameTupleAccessor.reset(buffer, i, unitSize);
            this.innerFrameInfos
                    .add(new InnerFrameInfo(i, unitSize, getTupleCount() + frameTupleAccessor.getTupleCount()));
            i += unitSize;
        }
        buffer.position(i); // reading stops here.
    }

    private int resetSubTupleAccessor(int tupleIndex) {
        assert tupleIndex < getTupleCount();
        if (tupleIndex >= lastFrameStart && tupleIndex < lastFrameEnd) {
            // a special optimization path
            // since GroupFrameAccessor is used by merge, it is expected that tuples are accessed sequentially
            // thus, if tuple still fit into the last frame, we do not need to perform binary search
            return tupleIndex - lastFrameStart;
        }
        // we perform binary search to get the frame Id
        binarySearchKey.setValue(tupleIndex);
        int subFrameId = Collections.binarySearch(innerFrameInfos, binarySearchKey);
        if (subFrameId >= 0) {
            subFrameId++;
        } else {
            subFrameId = -subFrameId - 1;
        }
        frameTupleAccessor.reset(buffer, innerFrameInfos.get(subFrameId).start, innerFrameInfos.get(subFrameId).length);
        lastFrameId = subFrameId;
        lastFrameStart = lastFrameId > 0 ? innerFrameInfos.get(lastFrameId - 1).tupleCount : 0;
        lastFrameEnd = innerFrameInfos.get(lastFrameId).tupleCount;
        return tupleIndex - lastFrameStart;
    }

}
