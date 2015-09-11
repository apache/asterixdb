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

package org.apache.hyracks.dataflow.std.sort.buffermanager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.util.IntSerDeUtils;

public class VariableFrameMemoryManager implements IFrameBufferManager {

    private class PhysicalFrameOffset {
        IFrame physicalFrame;
        int physicalOffset;

        PhysicalFrameOffset(IFrame frame, int offset) {
            physicalFrame = frame;
            physicalOffset = offset;
        }
    }

    private class LogicalFrameStartSize {
        ByteBuffer logicalFrame;
        int logicalStart;
        int logicalSize;

        LogicalFrameStartSize(ByteBuffer frame, int start, int size) {
            logicalFrame = frame;
            logicalStart = start;
            logicalSize = size;
        }
    }

    private final IFramePool framePool;
    private List<PhysicalFrameOffset> physicalFrameOffsets;
    private List<LogicalFrameStartSize> logicalFrameStartSizes;
    private final IFrameFreeSlotPolicy freeSlotPolicy;

    public VariableFrameMemoryManager(IFramePool framePool, IFrameFreeSlotPolicy freeSlotPolicy) {
        this.framePool = framePool;
        this.freeSlotPolicy = freeSlotPolicy;
        int maxFrames = framePool.getMemoryBudgetBytes() / framePool.getMinFrameSize();
        this.physicalFrameOffsets = new ArrayList<>(maxFrames);
        this.logicalFrameStartSizes = new ArrayList<>(maxFrames);
    }

    private int findAvailableFrame(int frameSize) throws HyracksDataException {
        int frameId = freeSlotPolicy.popBestFit(frameSize);
        if (frameId >= 0) {
            return frameId;
        }
        ByteBuffer buffer = framePool.allocateFrame(frameSize);
        if (buffer != null) {
            IntSerDeUtils.putInt(buffer.array(), FrameHelper.getTupleCountOffset(buffer.capacity()), 0);
            physicalFrameOffsets.add(new PhysicalFrameOffset(new FixedSizeFrame(buffer), 0));
            return physicalFrameOffsets.size() - 1;
        }
        return -1;
    }

    @Override
    public void reset() throws HyracksDataException {
        physicalFrameOffsets.clear();
        logicalFrameStartSizes.clear();
        freeSlotPolicy.reset();
        framePool.reset();
    }

    @Override
    public ByteBuffer getFrame(int frameIndex) {
        return logicalFrameStartSizes.get(frameIndex).logicalFrame;
    }

    @Override
    public int getFrameStartOffset(int frameIndex) {
        return logicalFrameStartSizes.get(frameIndex).logicalStart;
    }

    @Override
    public int getFrameSize(int frameIndex) {
        return logicalFrameStartSizes.get(frameIndex).logicalSize;
    }

    @Override
    public int getNumFrames() {
        return logicalFrameStartSizes.size();
    }

    @Override
    public int insertFrame(ByteBuffer frame) throws HyracksDataException {
        int frameSize = frame.capacity();
        int physicalFrameId = findAvailableFrame(frameSize);
        if (physicalFrameId < 0) {
            return -1;
        }
        ByteBuffer buffer = physicalFrameOffsets.get(physicalFrameId).physicalFrame.getBuffer();
        int offset = physicalFrameOffsets.get(physicalFrameId).physicalOffset;
        System.arraycopy(frame.array(), 0, buffer.array(), offset, frameSize);
        if (offset + frameSize < buffer.capacity()) {
            freeSlotPolicy.pushNewFrame(physicalFrameId, buffer.capacity() - offset - frameSize);
        }
        physicalFrameOffsets.get(physicalFrameId).physicalOffset = offset + frameSize;
        logicalFrameStartSizes.add(new LogicalFrameStartSize(buffer, offset, frameSize));
        return logicalFrameStartSizes.size() - 1;
    }

    @Override
    public void close() {
        physicalFrameOffsets.clear();
        logicalFrameStartSizes.clear();
        freeSlotPolicy.reset();
        framePool.close();
    }
}
