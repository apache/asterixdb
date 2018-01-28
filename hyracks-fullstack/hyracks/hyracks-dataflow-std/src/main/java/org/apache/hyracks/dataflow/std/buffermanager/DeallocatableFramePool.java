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
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class DeallocatableFramePool implements IDeallocatableFramePool {

    private final IHyracksFrameMgrContext ctx;
    private final int memBudget;
    private int allocated;
    private LinkedList<ByteBuffer> buffers;

    public DeallocatableFramePool(IHyracksFrameMgrContext ctx, int memBudgetInBytes) {
        this.ctx = ctx;
        this.memBudget = memBudgetInBytes;
        this.allocated = 0;
        this.buffers = new LinkedList<>();
    }

    @Override
    public int getMinFrameSize() {
        return ctx.getInitialFrameSize();
    }

    @Override
    public int getMemoryBudgetBytes() {
        return memBudget;
    }

    @Override
    public ByteBuffer allocateFrame(int frameSize) throws HyracksDataException {
        ByteBuffer buffer = findExistingFrame(frameSize);
        if (buffer != null) {
            return buffer;
        }
        if (haveEnoughFreeSpace(frameSize)) {
            return createNewFrame(frameSize);
        }
        return mergeExistingFrames(frameSize);
    }

    private ByteBuffer mergeExistingFrames(int frameSize) throws HyracksDataException {
        int mergedSize = memBudget - allocated;
        for (Iterator<ByteBuffer> iter = buffers.iterator(); iter.hasNext();) {
            ByteBuffer buffer = iter.next();
            iter.remove();
            mergedSize += buffer.capacity();
            ctx.deallocateFrames(buffer.capacity());
            allocated -= buffer.capacity();
            if (mergedSize >= frameSize) {
                return createNewFrame(mergedSize);
            }
        }
        return null;

    }

    private ByteBuffer createNewFrame(int frameSize) throws HyracksDataException {
        allocated += frameSize;
        return ctx.allocateFrame(frameSize);
    }

    private boolean haveEnoughFreeSpace(int frameSize) {
        return allocated + frameSize <= memBudget;
    }

    private ByteBuffer findExistingFrame(int frameSize) {
        for (Iterator<ByteBuffer> iter = buffers.iterator(); iter.hasNext();) {
            ByteBuffer next = iter.next();
            if (next.capacity() >= frameSize) {
                iter.remove();
                return next;
            }
        }
        return null;
    }

    @Override
    public void deAllocateBuffer(ByteBuffer buffer) {
        if (buffer.capacity() != ctx.getInitialFrameSize()) {
            // simply deallocate the Big Object frame
            ctx.deallocateFrames(buffer.capacity());
            allocated -= buffer.capacity();
        } else {
            buffers.add(buffer);
        }
    }

    @Override
    public void reset() {
        allocated = 0;
        buffers.clear();
    }

    @Override
    public void close() {
        for (Iterator<ByteBuffer> iter = buffers.iterator(); iter.hasNext();) {
            ByteBuffer next = iter.next();
            ctx.deallocateFrames(next.capacity());
            iter.remove();
        }
        allocated = 0;
        buffers.clear();
    }
}
