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
package org.apache.hyracks.api.resources.memory;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobKind;

/**
 * Interface for profiling frame (buffer) allocation, reallocation, and reclamation events
 * in the Hyracks memory management subsystem. Implementations may record, log, or analyze
 * memory usage patterns for jobs and operators.
 * <p>
 * All methods receive a {@link JobId} and a {@link JobKind} (which may be {@code null} if unknown).
 * Implementations must be null-safe with respect to {@code jobKind}.
 */
public interface IFrameProfiler {
    /**
     * Reports the allocation of a new frame buffer.
     *
     * @param size    the size of the allocated frame, in bytes
     * @param jobId   the job identifier
     * @param jobKind the kind of job (may be {@code null})
     */
    void reportAllocate(int size, JobId jobId, JobKind jobKind);

    /**
     * Reports the reallocation (resize) of a frame buffer.
     *
     * @param oldSize the previous size of the frame, in bytes
     * @param newSize the new size of the frame, in bytes
     * @param jobId   the job identifier
     * @param jobKind the kind of job (may be {@code null})
     */
    void reportReallocate(int oldSize, int newSize, JobId jobId, JobKind jobKind);

    IFrameProfiler NOOP_FRAME_PROFILER = new IFrameProfiler() {
        @Override
        public void reportAllocate(int size, JobId jobId, JobKind jobKind) {
            // no-op
        }

        @Override
        public void reportReallocate(int oldSize, int newSize, JobId jobId, JobKind jobKind) {
            // no-op
        }
    };
}
