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
package org.apache.asterix.cloud.clients.profiler.limiter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class TokenBasedRateLimiter implements IRateLimiter {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long SECOND_NANO = TimeUnit.SECONDS.toNanos(1);
    private final AtomicLong throttleCount = new AtomicLong();
    private final long acquireTimeoutNano;
    private final int maxTokensPerSecond;
    private final Semaphore semaphore;
    private final AtomicLong lastRefillTime;

    /**
     * Token-based request limiter
     *
     * @param maxRequestsPerSecond maximum number of requests per seconds
     * @param acquireTimeoutMillis timeout to refill and retry acquiring a token
     */
    public TokenBasedRateLimiter(int maxRequestsPerSecond, long acquireTimeoutMillis) {
        this.maxTokensPerSecond = maxRequestsPerSecond;
        this.acquireTimeoutNano = TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMillis);
        this.semaphore = new Semaphore(maxRequestsPerSecond);
        this.lastRefillTime = new AtomicLong(System.nanoTime());
    }

    @Override
    public void acquire() {
        while (true) {
            refillTokens();
            try {
                if (semaphore.tryAcquire(acquireTimeoutNano, TimeUnit.NANOSECONDS)) {
                    return;
                }
                throttleCount.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.debug("Interrupted while waiting for acquiring a request token", e);
                return;
            }
        }
    }

    @Override
    public long getThrottleCount() {
        return throttleCount.get();
    }

    private void refillTokens() {
        long refillTime = lastRefillTime.get();
        long now = System.nanoTime();
        long elapsedTime = now - refillTime;
        if (elapsedTime > SECOND_NANO && lastRefillTime.compareAndSet(refillTime, now)) {
            int delta = maxTokensPerSecond - semaphore.availablePermits();
            if (delta > 0) {
                semaphore.release(delta);
            }
        }
    }
}
