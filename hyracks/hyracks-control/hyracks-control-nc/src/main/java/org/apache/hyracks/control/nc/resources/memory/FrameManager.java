/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.hyracks.control.nc.resources.memory;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameConstants;
import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.context.IHyracksFrameMgrContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class FrameManager implements IHyracksFrameMgrContext {

    private final int minFrameSize;

    public FrameManager(int minFrameSize) {
        this.minFrameSize = minFrameSize;
    }

    @Override
    public int getInitialFrameSize() {
        return minFrameSize;
    }

    @Override
    public ByteBuffer allocateFrame() throws HyracksDataException {
        return allocateFrame(minFrameSize);
    }

    @Override
    public ByteBuffer allocateFrame(int bytes) throws HyracksDataException {
        if (bytes % minFrameSize != 0) {
            throw new HyracksDataException("The size should be an integral multiple of the default frame size");
        }
        ByteBuffer buffer = ByteBuffer.allocate(bytes);
        if (bytes / minFrameSize > FrameConstants.MAX_NUM_MINFRAME) {
            throw new HyracksDataException(
                    "Unable to allocate frame larger than:" + FrameConstants.MAX_NUM_MINFRAME + " bytes");
        }
        FrameHelper.serializeFrameSize(buffer, (byte) (bytes / minFrameSize));
        return (ByteBuffer) buffer.clear();
    }

    @Override
    public ByteBuffer reallocateFrame(ByteBuffer tobeDeallocate, int newSizeInBytes, boolean copyOldData)
            throws HyracksDataException {
        if (!copyOldData) {
            deallocateFrames(tobeDeallocate.capacity());
            return allocateFrame(newSizeInBytes);
        } else {
            ByteBuffer buffer = allocateFrame(newSizeInBytes);
            int limit = Math.min(newSizeInBytes, tobeDeallocate.capacity());
            int pos = Math.min(limit, tobeDeallocate.position());
            tobeDeallocate.position(0);
            tobeDeallocate.limit(limit);
            buffer.put(tobeDeallocate);
            buffer.position(pos);

            if (newSizeInBytes / minFrameSize > FrameConstants.MAX_NUM_MINFRAME) {
                throw new HyracksDataException("Unable to allocate frame of size bigger than MinFrameSize * "
                        + FrameConstants.MAX_NUM_MINFRAME);
            }
            FrameHelper.serializeFrameSize(buffer, (byte) (newSizeInBytes / minFrameSize));
            return buffer;
        }
    }

    @Override
    public void deallocateFrames(int bytes) {
        //TODO make a global memory manager to allocate and deallocate the frames.
    }
}
