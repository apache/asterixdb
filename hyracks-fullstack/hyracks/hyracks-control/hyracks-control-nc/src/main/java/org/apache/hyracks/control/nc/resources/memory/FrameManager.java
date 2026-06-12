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

package org.apache.hyracks.control.nc.resources.memory;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.hyracks.api.comm.FrameConstants;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobKind;
import org.apache.hyracks.api.resources.memory.IFrameProfiler;

public class FrameManager implements IHyracksFrameMgrContext {

    private final int minFrameSize;
    private final JobId jobId;
    private final JobKind jobKind;
    private final IFrameProfiler profiler;

    public FrameManager(int minFrameSize, IFrameProfiler profiler, JobId jobId, JobKind jobKind) {
        this.minFrameSize = minFrameSize;
        this.profiler = Objects.requireNonNull(profiler);
        this.jobId = jobId; // may be null as we sometimes use frames outside the context of a job
        this.jobKind = jobKind;
    }

    @Deprecated // this constructor will create a frame manager that is not profiled
    public FrameManager(int minFrameSize) {
        this(minFrameSize, IFrameProfiler.NOOP_FRAME_PROFILER, null, null);
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
        ByteBuffer buffer = allocateFrameOnly(bytes);
        profiler.reportAllocate(bytes, jobId, jobKind);
        return buffer;
    }

    private ByteBuffer allocateFrameOnly(int bytes) throws HyracksDataException {
        if (bytes % minFrameSize != 0) {
            throw new HyracksDataException("The size should be an integral multiple of the default frame size");
        }
        if (bytes > FrameConstants.MAX_FRAMESIZE) {
            throw new HyracksDataException(
                    "Unable to allocate frame larger than:" + FrameConstants.MAX_FRAMESIZE + " bytes");
        }
        ByteBuffer buffer = ByteBuffer.allocate(bytes);
        FrameHelper.serializeFrameSize(buffer, bytes / minFrameSize);
        return buffer.clear();
    }

    @Override
    public ByteBuffer reallocateFrame(ByteBuffer tobeDeallocate, int newSizeInBytes, boolean copyOldData)
            throws HyracksDataException {
        if (!copyOldData) {
            deallocateFrames(tobeDeallocate.capacity());
            return allocateFrame(newSizeInBytes);
        } else {
            if (newSizeInBytes > FrameConstants.MAX_FRAMESIZE) {
                throw new HyracksDataException(
                        "Unable to allocate frame of size bigger than: " + FrameConstants.MAX_FRAMESIZE + " bytes");
            }
            ByteBuffer buffer = allocateFrameOnly(newSizeInBytes);
            int limit = Math.min(newSizeInBytes, tobeDeallocate.capacity());
            int pos = Math.min(limit, tobeDeallocate.position());
            tobeDeallocate.position(0);
            tobeDeallocate.limit(limit);
            buffer.put(tobeDeallocate);
            buffer.position(pos);

            FrameHelper.serializeFrameSize(buffer, newSizeInBytes / minFrameSize);
            profiler.reportReallocate(limit, newSizeInBytes, jobId, jobKind);
            return buffer;
        }
    }

    @Override
    public IFrameProfiler getProfiler() {
        return profiler;
    }

    @Override
    public JobKind getJobKind() {
        return jobKind;
    }

    @Override
    public void deallocateFrames(int bytes) {
        //TODO make a global memory manager to allocate and deallocate the frames.
    }
}
