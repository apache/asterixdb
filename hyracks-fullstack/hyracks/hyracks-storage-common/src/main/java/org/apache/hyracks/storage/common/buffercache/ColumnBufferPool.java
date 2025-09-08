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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * ColumnBufferPool: a semaphore-based buffer pool for columnar buffer management.
 *
 * NOTE: getBuffer() and recycle() are called very frequently and are designed to be as lightweight as possible.
 */
public class ColumnBufferPool implements IColumnBufferPool, ILifeCycleComponent {
    protected static final Logger LOGGER = LogManager.getLogger();

    private final BlockingQueue<ByteBuffer> bufferPool;
    private final AtomicLong totalAllocatedMemoryInBytes;
    private final AtomicLong totalPooledMemoryInBytes;
    private final AtomicLong numAllocNew;

    // Semaphore for buffer-based allocation
    private final Semaphore bufferSemaphore;
    private final long maxBufferPoolMemoryLimit;
    private final double columnBufferPoolMemoryPercentage;
    private final int columnBufferInBytes;
    private final int columnBufferPoolMaxSize;
    private final int maxBuffers;

    // Timeout for buffer reservation in milliseconds
    private final long reserveTimeoutMillis;

    /**
     * @param columnBufferInBytes buffer size in bytes
     * @param columnBufferPoolMaxSize max number of buffers in pool
     * @param columnBufferPoolMemoryPercentage max percentage of total memory for pool
     * @param reserveTimeoutMillis timeout in milliseconds for buffer reservation
     */
    public ColumnBufferPool(int columnBufferInBytes, int columnBufferPoolMaxSize,
            double columnBufferPoolMemoryPercentage, long reserveTimeoutMillis) {
        this.totalAllocatedMemoryInBytes = new AtomicLong(0);
        this.bufferPool = new ArrayBlockingQueue<>(columnBufferPoolMaxSize);
        this.columnBufferPoolMemoryPercentage = columnBufferPoolMemoryPercentage;
        this.columnBufferPoolMaxSize = columnBufferPoolMaxSize;
        this.reserveTimeoutMillis = reserveTimeoutMillis;
        this.maxBufferPoolMemoryLimit = getMaxBufferPoolMemoryLimit(columnBufferInBytes,
                columnBufferPoolMemoryPercentage, columnBufferPoolMaxSize);
        this.maxBuffers = (int) (maxBufferPoolMemoryLimit / columnBufferInBytes);
        this.bufferSemaphore = new Semaphore(maxBuffers, true);
        this.columnBufferInBytes = columnBufferInBytes;
        this.totalPooledMemoryInBytes = new AtomicLong(0);
        this.numAllocNew = new AtomicLong(0);
        initializePool();
        LOGGER.info(
                "ColumnBufferPool initialized: columnBufferPoolMaxSize={}, maxBufferPoolMemoryLimit={}, maxBuffers={}, columnBufferInBytes={}, reserveTimeoutMillis={}",
                columnBufferPoolMaxSize, maxBufferPoolMemoryLimit, maxBuffers, columnBufferInBytes,
                reserveTimeoutMillis);
    }

    /**
     * Pre-allocate buffers for fast buffer access.
     * The number of buffers pre-allocated is the minimum of half the pool size and the memory limit.
     */
    private void initializePool() {
        int halfPoolSize = columnBufferPoolMaxSize / 2;
        int numBuffersToAllocate = Math.min(halfPoolSize, maxBuffers);

        for (int i = 0; i < numBuffersToAllocate; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(columnBufferInBytes);
            bufferPool.add(buffer);
            totalPooledMemoryInBytes.addAndGet(columnBufferInBytes);
        }
        LOGGER.info("ColumnBufferPool pre-allocated {} buffers ({} bytes each)", numBuffersToAllocate,
                columnBufferInBytes);
    }

    /**
     * Reserve the specified number of buffers, blocking up to the configured timeout if necessary.
     * @param requestedBuffers number of buffers to reserve
     * @throws InterruptedException if interrupted while waiting
     * @throws IllegalStateException if unable to reserve within the timeout
     */
    @Override
    public void reserve(int requestedBuffers) throws InterruptedException {
        if (requestedBuffers <= 0) {
            return;
        }
        boolean acquired = bufferSemaphore.tryAcquire(requestedBuffers, reserveTimeoutMillis, TimeUnit.MILLISECONDS);
        if (!acquired) {
            LOGGER.error("Failed to reserve {} buffers within {} ms ({} sec)", requestedBuffers, reserveTimeoutMillis,
                    TimeUnit.MILLISECONDS.toSeconds(reserveTimeoutMillis));
            throw new IllegalStateException("Timeout while reserving column buffers (" + requestedBuffers + ") after "
                    + reserveTimeoutMillis + " ms");
        }
    }

    @Override
    public boolean tryReserve(int requestedBuffers) {
        if (requestedBuffers <= 0) {
            return true;
        }
        return bufferSemaphore.tryAcquire(requestedBuffers);
    }

    @Override
    public void release(int buffers) {
        if (buffers <= 0) {
            return;
        }
        bufferSemaphore.release(buffers);
    }

    /**
     * Fast path for buffer acquisition. Called very frequently.
     */
    @Override
    public ByteBuffer getBuffer() {
        // Fast path: try to poll from pool
        ByteBuffer buffer = bufferPool.poll();
        if (buffer != null) {
            totalPooledMemoryInBytes.addAndGet(-columnBufferInBytes);
            buffer.clear();
            return buffer;
        }

        // Slow path: allocate new buffer if quota allows
        ensureAvailableQuota();

        numAllocNew.incrementAndGet();
        buffer = ByteBuffer.allocate(columnBufferInBytes);
        totalAllocatedMemoryInBytes.addAndGet(columnBufferInBytes);
        return buffer;
    }

    /**
     * Fast path for buffer recycling. Called very frequently.
     */
    @Override
    public void recycle(ByteBuffer buffer) {
        if (buffer == null) {
            throw new IllegalStateException("buffer is null");
        }

        // Try to return to pool; if full, discard
        if (bufferPool.offer(buffer)) {
            totalPooledMemoryInBytes.addAndGet(columnBufferInBytes);
        } else {
            totalAllocatedMemoryInBytes.addAndGet(-columnBufferInBytes);
        }
    }

    @Override
    public int getMaxReservedBuffers() {
        return maxBuffers;
    }

    /**
     * Ensures that the total allocated memory does not exceed the maxBufferPoolMemoryLimit limit.
     * Throws if the limit would be exceeded.
     */
    private void ensureAvailableQuota() {
        long spaceAcquiredByPool = (long) columnBufferPoolMaxSize * columnBufferInBytes;
        long totalAllocated = totalAllocatedMemoryInBytes.get();
        long totalIfAllocated = totalAllocated + spaceAcquiredByPool;
        if (totalIfAllocated > maxBufferPoolMemoryLimit) {
            LOGGER.error(
                    "Cannot allocate more buffers, memory limit reached. "
                            + "totalAllocatedMemoryInBytes={}, maxBufferPoolMemoryLimit={}, columnBufferPoolMaxSize={}, columnBufferInBytes={}, "
                            + "columnBufferPoolMemoryPercentage={}, totalIfAllocated={}, numAllocNew={}, availableBuffers={}. "
                            + "Consider increasing storageColumnBufferPoolMemoryPercentage (current: {}).",
                    totalAllocated, maxBufferPoolMemoryLimit, columnBufferPoolMaxSize, columnBufferInBytes,
                    columnBufferPoolMemoryPercentage, totalIfAllocated, numAllocNew.get(),
                    bufferSemaphore.availablePermits(), columnBufferPoolMemoryPercentage);
            throw new IllegalStateException("Cannot allocate more buffers, maxBufferPoolMemoryLimit ("
                    + maxBufferPoolMemoryLimit + ") reached.");
        }
    }

    private long getMaxBufferPoolMemoryLimit(int columnBufferInBytes, double columnBufferPoolMemoryPercentage,
            int columnBufferPoolMaxSize) {
        long totalMemory = Runtime.getRuntime().totalMemory();
        return (long) Math.max(totalMemory * (columnBufferPoolMemoryPercentage / 100),
                columnBufferInBytes * columnBufferPoolMaxSize);
    }

    @Override
    public void close() {
        bufferPool.clear();
        totalPooledMemoryInBytes.set(0);
        totalAllocatedMemoryInBytes.set(0);
        LOGGER.info("ColumnBufferPool closed. numAllocNew={}", numAllocNew.get());
    }

    @Override
    public void start() {
        // No-op
    }

    @Override
    public void dumpState(OutputStream os) throws IOException {
        long pooledBytes = totalPooledMemoryInBytes.get();
        long totalAllocatedBytes = totalAllocatedMemoryInBytes.get();
        String buffer = "ColumnBufferPool State:\n" + "columnBufferPoolMaxSize: " + columnBufferPoolMaxSize + "\n"
                + "Max Buffers: " + maxBuffers + "\n" + "Total Pooled Memory (bytes): " + pooledBytes + "\n"
                + "Total Allocated Memory (bytes): " + totalAllocatedBytes + "\n" + "Number of Buffers Allocated New: "
                + numAllocNew.get() + "\n" + "Available Buffers: " + bufferSemaphore.availablePermits();
        os.write(buffer.getBytes());
        LOGGER.info("dumpState called:\n{}", buffer);
    }

    @Override
    public void stop(boolean dumpState, OutputStream ouputStream) throws IOException {
        close();
        if (dumpState && ouputStream != null) {
            dumpState(ouputStream);
        }
    }
}
