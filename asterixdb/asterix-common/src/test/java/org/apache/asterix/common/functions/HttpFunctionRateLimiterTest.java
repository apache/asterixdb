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
package org.apache.asterix.common.functions;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class HttpFunctionRateLimiterTest {

    private static final int RATE = 10;
    private static final long SECOND_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final long INTERVAL_NANOS = SECOND_NANOS / RATE;
    private static final long HOUR_NANOS = TimeUnit.HOURS.toNanos(1);

    @Test
    public void rateOfTenSendsTenAtOnceThenTenEachSecond() {
        HttpFunctionRateLimiter limiter = new HttpFunctionRateLimiter(RATE);
        long now = System.nanoTime();
        assertBurst(limiter, now, RATE);
        for (int i = 1; i <= 2 * RATE; i++) {
            assertEquals("request " + (RATE + i), i * INTERVAL_NANOS, limiter.reserve(now));
        }
    }

    @Test
    public void idleTimeRefillsTheBucketUpToOneSecond() {
        HttpFunctionRateLimiter limiter = new HttpFunctionRateLimiter(RATE);
        long now = System.nanoTime();
        assertBurst(limiter, now, RATE);
        long secondLater = now + SECOND_NANOS;
        assertBurst(limiter, secondLater, RATE);
        assertEquals(INTERVAL_NANOS, limiter.reserve(secondLater));
        long hourLater = now + HOUR_NANOS;
        assertBurst(limiter, hourLater, RATE);
        assertEquals(INTERVAL_NANOS, limiter.reserve(hourLater));
    }

    @Test
    public void rateOfZeroIsNoLimit() {
        HttpFunctionRateLimiter limiter = new HttpFunctionRateLimiter(0);
        long now = System.nanoTime();
        assertBurst(limiter, now, 1000);
    }

    private static void assertBurst(HttpFunctionRateLimiter limiter, long now, int requests) {
        for (int i = 0; i < requests; i++) {
            assertEquals("request " + (i + 1), 0, limiter.reserve(now));
        }
    }
}
