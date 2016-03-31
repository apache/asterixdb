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
import java.util.List;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.util.IntSerDeUtils;

public class VariableFrameMemoryManager implements IFrameBufferManager {

    class PhysicalFrameOffset {
        ByteBuffer physicalFrame;
        int physicalOffset;

        PhysicalFrameOffset(ByteBuffer frame, int offset) {
            physicalFrame = frame;
            physicalOffset = offset;
        }
    }

    private final IFramePool framePool;
    private List<PhysicalFrameOffset> physicalFrameOffsets;
    private List<BufferInfo> logicalFrameStartSizes;
    private final IFrameFreeSlotPolicy freeSlotPolicy;

    public VariableFrameMemoryManager(IFramePool framePool, IFrameFreeSlotPolicy freeSlotPolicy) {
        this.framePool = framePool;
        this.freeSlotPolicy = freeSlotPolicy;
        this.physicalFrameOffsets = new ArrayList<>();
        this.logicalFrameStartSizes = new ArrayList<>();
    }

    private int findAvailableFrame(int frameSize) throws HyracksDataException {
        int frameId = freeSlotPolicy.popBestFit(frameSize);
        if (frameId >= 0) {
            return frameId;
        }
        ByteBuffer buffer = framePool.allocateFrame(frameSize);
        if (buffer != null) {
            IntSerDeUtils.putInt(buffer.array(), FrameHelper.getTupleCountOffset(buffer.capacity()), 0);
            physicalFrameOffsets.add(new PhysicalFrameOffset(buffer, 0));
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
    public BufferInfo getFrame(int frameIndex, BufferInfo info) {
        info.reset(logicalFrameStartSizes.get(frameIndex));
        return info;
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
        PhysicalFrameOffset frameOffset = physicalFrameOffsets.get(physicalFrameId);
        ByteBuffer buffer = frameOffset.physicalFrame;
        int offset = frameOffset.physicalOffset;
        System.arraycopy(frame.array(), 0, buffer.array(), offset, frameSize);
        if (offset + frameSize < buffer.capacity()) {
            freeSlotPolicy.pushNewFrame(physicalFrameId, buffer.capacity() - offset - frameSize);
        }
        frameOffset.physicalOffset = offset + frameSize;
        logicalFrameStartSizes.add(new BufferInfo(buffer, offset, frameSize));
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
