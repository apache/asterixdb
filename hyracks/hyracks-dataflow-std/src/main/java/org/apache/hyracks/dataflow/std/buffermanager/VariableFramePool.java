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

package org.apache.hyracks.dataflow.std.buffermanager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class VariableFramePool implements IFramePool {
    public static final int UNLIMITED_MEMORY = -1;

    private final IHyracksFrameMgrContext ctx;
    private final int minFrameSize;
    private final int memBudget;

    private int allocateMem;
    private ArrayList<ByteBuffer> buffers; // the unused slots were sorted by size increasingly.
    private BitSet used; // the merged one also marked as used.

    /**
     * The constructor of the VariableFramePool.
     *
     * @param ctx
     * @param memBudgetInBytes
     *            the given memory budgets to allocate the frames. If it less than 0, it will be treated as unlimited budgets
     */
    public VariableFramePool(IHyracksFrameMgrContext ctx, int memBudgetInBytes) {
        this.ctx = ctx;
        this.minFrameSize = ctx.getInitialFrameSize();
        this.allocateMem = 0;
        if (memBudgetInBytes == UNLIMITED_MEMORY) {
            this.memBudget = Integer.MAX_VALUE;
            this.buffers = new ArrayList<>();
            this.used = new BitSet();
        } else {
            this.memBudget = memBudgetInBytes;
            this.buffers = new ArrayList<>(memBudgetInBytes / minFrameSize);
            this.used = new BitSet(memBudgetInBytes / minFrameSize);
        }
    }

    @Override
    public int getMinFrameSize() {
        return minFrameSize;
    }

    @Override
    public int getMemoryBudgetBytes() {
        return memBudget;
    }

    @Override
    public ByteBuffer allocateFrame(int frameSize) throws HyracksDataException {
        int frameId = findExistingFrame(frameSize);
        if (frameId >= 0) {
            return reuseFrame(frameId);
        }
        if (haveEnoughFreeSpace(frameSize)) {
            return createNewFrame(frameSize);
        }
        return mergeExistingFrames(frameSize);

    }

    private boolean haveEnoughFreeSpace(int frameSize) {
        return frameSize + allocateMem <= memBudget;
    }

    private static int getFirstUnusedPos(BitSet used) {
        return used.nextClearBit(0);
    }

    private static int getLastUnusedPos(BitSet used, int lastPos) {
        return used.previousClearBit(lastPos);
    }

    private static int binarySearchUnusedBuffer(ArrayList<ByteBuffer> buffers, BitSet used, int frameSize) {
        int l = getFirstUnusedPos(used); // to skip the merged null buffers
        int h = getLastUnusedPos(used, (buffers.size() - 1)) + 1; // to skip the newly created buffers
        if (l >= h) {
            return -1;
        }
        int highest = h;
        int mid = (l + h) / 2;
        while (l < h) {
            ByteBuffer buffer = buffers.get(mid);
            if (buffer.capacity() == frameSize) {
                break;
            }
            if (buffer.capacity() < frameSize) {
                l = mid + 1;
            } else {
                h = mid;
            }
            mid = (l + h) / 2;
        }
        mid = used.nextClearBit(mid);
        return mid < highest ? mid : -1;
    }

    private int findExistingFrame(int frameSize) {
        return binarySearchUnusedBuffer(buffers, used, frameSize);
    }

    private ByteBuffer reuseFrame(int id) {
        used.set(id);
        buffers.get(id).clear();
        return buffers.get(id);
    }

    private ByteBuffer createNewFrame(int frameSize) throws HyracksDataException {
        buffers.add(ctx.allocateFrame(frameSize));
        allocateMem += frameSize;
        return reuseFrame(buffers.size() - 1);
    }

    /**
     * The merging sequence is from the smallest to the largest order.
     * Once the buffer get merged, it will be remove from the list in order to free the object.
     * And the index spot of it will be marked as used.
     *
     * @param frameSize
     * @return
     * @throws HyracksDataException
     */
    private ByteBuffer mergeExistingFrames(int frameSize) throws HyracksDataException {
        int mergedSize = memBudget - allocateMem;
        int highBound = getLastUnusedPos(used, buffers.size() - 1) + 1;
        for (int i = getFirstUnusedPos(used); i < highBound; ++i) {
            if (!used.get(i)) {
                mergedSize += deAllocateFrame(i);
                if (mergedSize >= frameSize) {
                    return createNewFrame(mergedSize);
                }
            }
        }
        return null;
    }

    private int deAllocateFrame(int id) {
        ByteBuffer frame = buffers.get(id);
        ctx.deallocateFrames(frame.capacity());
        buffers.set(id, null);
        used.set(id);
        allocateMem -= frame.capacity();
        return frame.capacity();
    }

    @Override
    public void reset() {
        removeEmptySpot(buffers);
        Collections.sort(buffers, sizeByteBufferComparator);
        used.clear();
    }

    private static void removeEmptySpot(List<ByteBuffer> buffers) {
        for (int i = 0; i < buffers.size();) {
            if (buffers.get(i) == null) {
                buffers.remove(i);
            } else {
                i++;
            }
        }
    }

    @Override
    public void close() {
        buffers.clear();
        used.clear();
        allocateMem = 0;
    }

    private static Comparator<ByteBuffer> sizeByteBufferComparator = new Comparator<ByteBuffer>() {
        @Override
        public int compare(ByteBuffer o1, ByteBuffer o2) {
            if (o1.capacity() == o2.capacity()) {
                return 0;
            }
            return o1.capacity() < o2.capacity() ? -1 : 1;
        }
    };
}
