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
package org.apache.hyracks.storage.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.storage.common.buffercache.ColumnBufferPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ColumnBufferPoolTest {

    private ColumnBufferPool columnBufferPool;
    private static final int COLUMN_BUFFER_GRANULE_BYTES = 4096; // 4KB
    private static final int POOL_SIZE = 10;
    private static final long COLUMN_BUFFER_MAX_MEMORY_LIMIT = getColumnBufferPoolMaxMemoryLimit(10); // 10% of runtime memory
    private static final long RESERVE_TIMEOUT_MILLIS = 5000; // 5 seconds timeout

    @Before
    public void setUp() {
        columnBufferPool = new ColumnBufferPool(COLUMN_BUFFER_GRANULE_BYTES, POOL_SIZE, COLUMN_BUFFER_MAX_MEMORY_LIMIT,
                RESERVE_TIMEOUT_MILLIS);
    }

    @After
    public void tearDown() {
        if (columnBufferPool != null) {
            columnBufferPool.close();
        }
    }

    @Test
    public void testBasicCreditReservationAndRelease() throws InterruptedException {
        // Reserve credits
        columnBufferPool.reserve(5);
        // Release credits
        columnBufferPool.release(5);
    }

    @Test
    public void testReserveExtraCreditsFor0thTuple() throws InterruptedException {
        // Simulate scenario: initially reserve 3 credits, then need 8 total for 0th tuple
        columnBufferPool.reserve(3);
        // Need 5 more credits for 0th tuple (blocking call)
        columnBufferPool.reserve(5); // This should block until credits are available
        // Release all credits
        columnBufferPool.release(8);
    }

    @Test
    public void testTryAcquiringExtraCreditsForNon0thTuple() throws InterruptedException {
        // Simulate scenario: initially have 2 credits, try to get 5 total (non-blocking)
        columnBufferPool.reserve(2);
        // Try to acquire 3 more credits for non-0th tuple (should succeed if available)
        boolean success = columnBufferPool.tryReserve(3);
        assertTrue("Should be able to reserve extra credits", success);
        // Release all credits
        columnBufferPool.release(5);
    }

    @Test
    public void testTryReserveFailure() throws InterruptedException {
        // Reserve most credits
        int maxCredits = columnBufferPool.getMaxReservedBuffers();
        columnBufferPool.reserve(maxCredits - 2);
        // Try to reserve more than available
        boolean success = columnBufferPool.tryReserve(5);
        assertFalse("Should fail to reserve more credits than available", success);
        // Release credits
        columnBufferPool.release(maxCredits - 2);
    }

    @Test(expected = IllegalStateException.class, timeout = 10000)
    public void testReserveTimeout() throws InterruptedException {
        // Use a shorter timeout for this test
        ColumnBufferPool shortTimeoutPool =
                new ColumnBufferPool(COLUMN_BUFFER_GRANULE_BYTES, POOL_SIZE, COLUMN_BUFFER_MAX_MEMORY_LIMIT, 100); // 100ms timeout
        try {
            // Reserve all credits
            int maxCredits = shortTimeoutPool.getMaxReservedBuffers();
            shortTimeoutPool.reserve(maxCredits);
            // This should timeout and throw IllegalStateException
            shortTimeoutPool.reserve(1);
        } finally {
            shortTimeoutPool.close();
        }
    }

    @Test
    public void testBufferAcquisitionAndRelease() throws InterruptedException {
        // Reserve credits for buffers
        columnBufferPool.reserve(3);
        List<ByteBuffer> acquiredBuffers = new ArrayList<>();
        // Acquire buffers
        for (int i = 0; i < 3; i++) {
            ByteBuffer buffer = columnBufferPool.getBuffer();
            assertNotNull("Buffer should not be null", buffer);
            assertEquals("Buffer should have correct capacity", COLUMN_BUFFER_GRANULE_BYTES, buffer.capacity());
            acquiredBuffers.add(buffer);
        }
        // Release buffers
        for (ByteBuffer buffer : acquiredBuffers) {
            columnBufferPool.recycle(buffer);
        }
        // Release credits
        columnBufferPool.release(3);
    }

    @Test
    public void testBufferAcquisitionAndReleaseInBatches() throws InterruptedException {
        // Step 1: Reserve credits upfront
        columnBufferPool.reserve(6);

        List<ByteBuffer> acquiredBuffers = new ArrayList<>();

        // Step 2: Acquire buffers for first batch
        for (int i = 0; i < 3; i++) {
            ByteBuffer buffer = columnBufferPool.getBuffer();
            assertNotNull(buffer);
            assertEquals(COLUMN_BUFFER_GRANULE_BYTES, buffer.capacity());
            acquiredBuffers.add(buffer);
        }

        // Step 3: Release buffers after first batch (flush happens)
        for (ByteBuffer buffer : acquiredBuffers) {
            columnBufferPool.recycle(buffer);
        }
        acquiredBuffers.clear();

        // Step 4: Acquire buffers for second batch
        for (int i = 0; i < 2; i++) {
            ByteBuffer buffer = columnBufferPool.getBuffer();
            assertNotNull(buffer);
            acquiredBuffers.add(buffer);
        }

        // Step 5: Release buffers after second batch
        for (ByteBuffer buffer : acquiredBuffers) {
            columnBufferPool.recycle(buffer);
        }

        // Step 6: Release remaining credits
        columnBufferPool.release(6);
    }

    @Test
    public void testCompleteThreadLifecycle() throws InterruptedException {
        // Step 1: Reserve initial credits
        columnBufferPool.reserve(4);

        // Step 2: Reserve extra credits for 0th tuple
        columnBufferPool.reserve(3); // Now have 7 total

        // Step 3: Process multiple batches
        for (int batch = 0; batch < 3; batch++) {
            List<ByteBuffer> batchBuffers = new ArrayList<>();

            // Acquire buffers for this batch
            for (int i = 0; i < 2; i++) {
                ByteBuffer buffer = columnBufferPool.getBuffer();
                assertNotNull(buffer);
                batchBuffers.add(buffer);
            }

            // Simulate processing
            Thread.sleep(10);

            // Release buffers after batch processing
            for (ByteBuffer buffer : batchBuffers) {
                columnBufferPool.recycle(buffer);
            }
        }

        // Step 4: Release remaining credits
        columnBufferPool.release(7);
    }

    @Test
    public void testMultipleThreadsConcurrentExecution() throws InterruptedException {
        // Use shorter timeout to make timeouts happen faster in test
        ColumnBufferPool testPool =
                new ColumnBufferPool(COLUMN_BUFFER_GRANULE_BYTES, POOL_SIZE, COLUMN_BUFFER_MAX_MEMORY_LIMIT, 500); // 500ms timeout

        // Helper for timestamped log
        java.util.function.Consumer<String> logWithTimestamp = msg -> {
            System.out.println("[" + java.time.Instant.now() + "] " + msg);
        };

        try {
            int numThreads = 5; // More threads to increase contention
            int maxCredits = testPool.getMaxReservedBuffers();
            logWithTimestamp.accept("maxCredits: " + maxCredits);
            int creditsPerThread = (maxCredits * 3) / 4; // Each thread wants 75% of total credits - forces contention!

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(numThreads);
            AtomicInteger successfulThreads = new AtomicInteger(0);
            AtomicInteger threadOrder = new AtomicInteger(0);
            AtomicInteger totalRetries = new AtomicInteger(0);

            for (int threadId = 0; threadId < numThreads; threadId++) {
                final int id = threadId;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        int threadRetryCount = 0;
                        boolean threadSuccessful = false;

                        // Thread-level retry: retry the entire thread operation
                        while (!threadSuccessful && threadRetryCount < 100) {
                            int totalReservedCredits = 0; // Track credits reserved in this attempt
                            try {
                                if (threadRetryCount > 0) {
                                    totalRetries.incrementAndGet();
                                    logWithTimestamp
                                            .accept("Thread " + id + " starting retry attempt #" + threadRetryCount);
                                    // Exponential backoff with thread-specific variation before retrying
                                    int backoffTime = 100 + (threadRetryCount * 50) + (id * 25); // Thread-specific backoff
                                    Thread.sleep(backoffTime);
                                }

                                int myOrder = threadOrder.incrementAndGet();

                                // Step 1: Reserve initial credits (no individual retry - fail fast)
                                int initialCredits = creditsPerThread / 2;
                                long startTime = System.currentTimeMillis();
                                testPool.reserve(initialCredits);
                                totalReservedCredits += initialCredits; // Track successful reservation
                                long reserveTime = System.currentTimeMillis() - startTime;

                                logWithTimestamp.accept("Thread " + id + " (order: " + myOrder
                                        + ") reserved initial credits (" + initialCredits + " credits) in "
                                        + reserveTime + "ms"
                                        + (threadRetryCount > 0 ? " on thread retry #" + threadRetryCount : ""));

                                // Step 2: Reserve extra credits for 0th tuple (no individual retry - fail fast)
                                int extraCredits = creditsPerThread / 2;
                                startTime = System.currentTimeMillis();
                                testPool.reserve(extraCredits); // Now have creditsPerThread total
                                totalReservedCredits += extraCredits; // Track successful reservation
                                reserveTime = System.currentTimeMillis() - startTime;

                                logWithTimestamp.accept("Thread " + id + " reserved extra credits (" + extraCredits
                                        + " credits) in " + reserveTime + "ms");

                                // Step 3: Hold credits for longer than timeout to force other threads to timeout
                                // Only the first thread (or first few) will succeed initially
                                // Varied sleep patterns per thread to create different contention scenarios
                                int baseSleep = 600 + (id * 100); // Thread 0: 600ms, Thread 1: 700ms, etc.
                                Thread.sleep(baseSleep);

                                // Step 4: Process some buffers while holding many credits
                                List<ByteBuffer> buffers = new ArrayList<>();
                                for (int i = 0; i < Math.min(3, creditsPerThread); i++) {
                                    ByteBuffer buffer = testPool.getBuffer();
                                    buffers.add(buffer);
                                }

                                // Simulate longer processing to ensure timeouts occur
                                // Different processing times per thread to create realistic workload patterns
                                int processingTime = 150 + (id * 50); // Thread 0: 150ms, Thread 1: 200ms, etc.
                                Thread.sleep(processingTime);

                                // Release buffers but keep credits reserved
                                for (ByteBuffer buffer : buffers) {
                                    testPool.recycle(buffer);
                                }

                                // Hold credits even longer to guarantee timeouts
                                // Staggered final sleep to create cascading release pattern
                                int finalHoldTime = 250 + (id * 75); // Thread 0: 250ms, Thread 1: 325ms, etc.
                                Thread.sleep(finalHoldTime);

                                // Step 5: Finally release all credits (allowing other threads to proceed)
                                testPool.release(creditsPerThread);
                                logWithTimestamp.accept("Thread " + id
                                        + " completed successfully and released all credits" + (threadRetryCount > 0
                                                ? " after " + threadRetryCount + " thread retries" : ""));

                                threadSuccessful = true; // Mark thread as successful
                                successfulThreads.incrementAndGet();

                            } catch (IllegalStateException e) {
                                // Timeout occurred - cleanup partial credits and retry entire thread operation
                                threadRetryCount++;
                                if (totalReservedCredits > 0) {
                                    testPool.release(totalReservedCredits);
                                    logWithTimestamp
                                            .accept("Thread " + id + " timeout, released " + totalReservedCredits
                                                    + " partial credits, will retry entire thread operation (attempt "
                                                    + threadRetryCount + ")");
                                } else {
                                    logWithTimestamp.accept(
                                            "Thread " + id + " timeout, will retry entire thread operation (attempt "
                                                    + threadRetryCount + ")");
                                }

                            } catch (Exception e) {
                                // Other exceptions - cleanup partial credits and retry entire thread operation
                                threadRetryCount++;
                                if (totalReservedCredits > 0) {
                                    testPool.release(totalReservedCredits);
                                    logWithTimestamp.accept("Thread " + id + " error: " + e.getMessage() + ", released "
                                            + totalReservedCredits
                                            + " partial credits, will retry entire thread operation (attempt "
                                            + threadRetryCount + ")");
                                } else {
                                    logWithTimestamp.accept("Thread " + id + " error: " + e.getMessage()
                                            + ", will retry entire thread operation (attempt " + threadRetryCount
                                            + ")");
                                }
                            }
                        }

                        if (!threadSuccessful) {
                            logWithTimestamp.accept(
                                    "Thread " + id + " failed after " + threadRetryCount + " thread retry attempts");
                        }

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        completionLatch.countDown();
                    }
                });
            }

            // Start all threads at once to maximize contention
            startLatch.countDown();

            // Wait for completion with much longer timeout since threads will be holding credits for 1+ seconds and retrying
            assertTrue("All threads should complete", completionLatch.await(120, TimeUnit.SECONDS));
            assertEquals("All threads should succeed", numThreads, successfulThreads.get());

            // Verify that thread-level retries actually occurred (confirms timeout/retry mechanism)
            int totalRetriesCount = totalRetries.get();
            logWithTimestamp.accept("Total thread retries across all threads: " + totalRetriesCount);
            assertTrue("Should have some thread retries due to contention", totalRetriesCount > 0);

            executor.shutdown();
            assertTrue("Executor should terminate", executor.awaitTermination(15, TimeUnit.SECONDS));

        } finally {
            testPool.close();
        }
    }

    @Test
    public void testPoolCapacity() throws InterruptedException {
        // Reserve credits for buffer operations
        columnBufferPool.reserve(POOL_SIZE);

        List<ByteBuffer> acquiredBuffers = new ArrayList<>();

        // Acquire buffers up to initial pool size (POOL_SIZE/2)
        for (int i = 0; i < POOL_SIZE / 2; i++) {
            ByteBuffer buffer = columnBufferPool.getBuffer();
            assertNotNull("Buffer " + i + " should not be null", buffer);
            assertEquals("Buffer should have correct capacity", COLUMN_BUFFER_GRANULE_BYTES, buffer.capacity());
            acquiredBuffers.add(buffer);
        }

        // Acquire additional buffers (should allocate new ones)
        int additionalBuffers = POOL_SIZE / 2;
        for (int i = 0; i < additionalBuffers; i++) {
            ByteBuffer buffer = columnBufferPool.getBuffer();
            assertNotNull("Additional buffer " + i + " should not be null", buffer);
            assertEquals("Buffer should have correct capacity", COLUMN_BUFFER_GRANULE_BYTES, buffer.capacity());
            acquiredBuffers.add(buffer);
        }

        assertEquals("Total buffers should match reserved credits", POOL_SIZE, acquiredBuffers.size());

        // Release all buffers
        for (ByteBuffer buffer : acquiredBuffers) {
            columnBufferPool.recycle(buffer);
        }

        // Verify pool can still acquire buffers after release
        ByteBuffer testBuffer = columnBufferPool.getBuffer();
        assertNotNull("Should be able to acquire buffer after release", testBuffer);
        assertEquals("Buffer should have correct capacity", COLUMN_BUFFER_GRANULE_BYTES, testBuffer.capacity());

        // Release the test buffer
        columnBufferPool.recycle(testBuffer);

        // Release credits
        columnBufferPool.release(POOL_SIZE);
    }

    @Test
    public void testBootstrapInitialization() throws InterruptedException {
        // Verify the pool can acquire pre-initialized buffers
        columnBufferPool.reserve(POOL_SIZE / 2);

        // Acquire all pre-initialized buffers
        List<ByteBuffer> buffers = new ArrayList<>();
        for (int i = 0; i < POOL_SIZE / 2; i++) {
            ByteBuffer buffer = columnBufferPool.getBuffer();
            assertNotNull("Pre-initialized buffer " + i + " should be available", buffer);
            assertEquals("Buffer should have correct capacity", COLUMN_BUFFER_GRANULE_BYTES, buffer.capacity());
            buffers.add(buffer);
        }

        // Release buffers
        for (ByteBuffer buffer : buffers) {
            columnBufferPool.recycle(buffer);
        }

        // Release credits
        columnBufferPool.release(POOL_SIZE / 2);
    }

    @Test
    public void testCreditManagement() throws InterruptedException {
        int maxCredits = columnBufferPool.getMaxReservedBuffers();
        assertTrue("Max credits should be positive", maxCredits > 0);
        // Reserve credits
        columnBufferPool.reserve(5);
        // Try to reserve more
        boolean success = columnBufferPool.tryReserve(3);
        assertTrue("Should be able to reserve more credits", success);
        // Release all credits
        columnBufferPool.release(8);
    }

    @Test
    public void testBufferRecycling() throws InterruptedException {
        // Reserve credits
        columnBufferPool.reserve(2);
        // Acquire buffer
        ByteBuffer buffer1 = columnBufferPool.getBuffer();
        assertNotNull(buffer1);
        // Recycle buffer
        columnBufferPool.recycle(buffer1);
        // Acquire another buffer (should potentially reuse)
        ByteBuffer buffer2 = columnBufferPool.getBuffer();
        assertNotNull(buffer2);
        // Recycle second buffer
        columnBufferPool.recycle(buffer2);
        // Release credits
        columnBufferPool.release(2);
    }

    @Test(expected = IllegalStateException.class)
    public void testMemoryQuotaEnforcement() throws InterruptedException {
        // Create a small pool to test quota enforcement
        ColumnBufferPool smallPool = new ColumnBufferPool(1024, 2, getColumnBufferPoolMaxMemoryLimit(0.001), 1000); // Very small percentage
        try {
            int maxCredits = smallPool.getMaxReservedBuffers();
            smallPool.reserve(maxCredits);
            // Try to acquire more buffers than the quota allows
            List<ByteBuffer> buffers = new ArrayList<>();
            for (int i = 0; i < maxCredits + 10; i++) { // Try to exceed quota
                ByteBuffer buffer = smallPool.getBuffer();
                buffers.add(buffer);
            }
        } finally {
            smallPool.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testRecycleNullBuffer() {
        columnBufferPool.recycle(null);
    }

    @Test
    public void testEdgeCases() throws InterruptedException {
        // Test zero credits
        columnBufferPool.reserve(0); // Should be no-op
        columnBufferPool.release(0); // Should be no-op
        // Test negative credits
        assertTrue("tryReserve with negative should return true", columnBufferPool.tryReserve(-1));
        // Test max credits
        int maxCredits = columnBufferPool.getMaxReservedBuffers();
        assertTrue("Max credits should be positive", maxCredits > 0);
    }

    // Helper method to access maxCredits
    private int getMaxCredits() {
        return columnBufferPool.getMaxReservedBuffers();
    }

    private static long getColumnBufferPoolMaxMemoryLimit(double percentage) {
        return (long) ((percentage / 100) * Runtime.getRuntime().maxMemory());
    }
}
