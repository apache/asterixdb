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

package org.apache.hyracks.api.context;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobKind;
import org.apache.hyracks.api.resources.memory.IFrameProfiler;
import org.apache.hyracks.api.util.HyracksThrowingFunction;

public interface IHyracksFrameMgrContext {

    HyracksThrowingFunction<IHyracksFrameMgrContext, VSizeFrame> DEFAULT_VSIZE_FRAME_GENERATOR = VSizeFrame::new;

    int getInitialFrameSize();

    ByteBuffer allocateFrame() throws HyracksDataException;

    ByteBuffer allocateFrame(int bytes) throws HyracksDataException;

    ByteBuffer reallocateFrame(ByteBuffer tobeDeallocate, int newSizeInBytes, boolean copyOldData)
            throws HyracksDataException;

    IFrameProfiler getProfiler();

    /**
     * Returns the {@link JobKind} associated with this frame manager context, or {@code null}
     * if not operating within a specific job kind context.
     */
    default JobKind getJobKind() {
        return null;
    }

    void deallocateFrames(int bytes);

    default VSizeFrame allocateVSizeFrame() throws HyracksDataException {
        return DEFAULT_VSIZE_FRAME_GENERATOR.process(this);
    }
}
