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
package org.apache.hyracks.storage.common.buffercache;

import java.nio.ByteBuffer;

/**
 * Interface for a buffer-based pool managing column buffer allocation.
 *
 * The contract is:
 * - Callers must reserve semaphore permits (blocking or non-blocking) before acquiring buffers.
 * - Semaphore permits must be released when no longer needed.
 * - Buffers are acquired via getBuffer() and returned via recycle().
 * - The pool may block or fail to allocate if semaphore permits are exhausted.
 */
public interface IColumnBufferPool {

    /**
     * Reserve the specified number of buffers, blocking if necessary until available.
     * @param requestedBuffers number of buffers to reserve
     * @throws InterruptedException if interrupted while waiting
     */
    void reserve(int requestedBuffers) throws InterruptedException;

    /**
     * Attempt to reserve the specified number of buffers without blocking.
     * @param requestedBuffers number of buffers to reserve
     * @return true if buffers were reserved, false otherwise
     */
    boolean tryReserve(int requestedBuffers);

    /**
     * Release the specified number of previously reserved buffers.
     * @param buffers number of buffers to release
     */
    void release(int buffers);

    /**
     * Acquire a buffer from the pool. Requires a reserved buffer.
     * @return a ByteBuffer for use
     */
    ByteBuffer getBuffer();

    /**
     * Return a buffer to the pool for reuse.
     * @param buffer the buffer to recycle
     */
    void recycle(ByteBuffer buffer);

    /**
     * Returns the maximum number of buffers that can be reserved from this pool.
     * This represents the upper bound on concurrent buffer allocations.
     *
     * @return the maximum number of buffers available in the pool
     */
    int getMaxReservedBuffers();

    /**
     * Close the pool and release any resources.
     */
    void close();
}
