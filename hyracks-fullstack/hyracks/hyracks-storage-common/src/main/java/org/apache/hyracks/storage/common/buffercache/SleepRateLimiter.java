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

import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.google.common.util.concurrent.RateLimiter;

/**
 * A wrapper of the RateLimiter implementation from {@link RateLimiter}
 *
 */
public class SleepRateLimiter implements IRateLimiter {
    /**
     * Defines the maximum storage capacity of the rate limiter, i.e., the number of permits it stores.
     * Maybe make configurable in the future
     */
    private static final double MAX_BURST_SECONDS = 1.0;

    private final RateLimiter rateLimiterImpl;

    public static IRateLimiter create(long ratePerSecond) {
        if (ratePerSecond > 0) {
            return new SleepRateLimiter(ratePerSecond, MAX_BURST_SECONDS);
        } else {
            return NoOpRateLimiter.INSTANCE;
        }
    }

    public SleepRateLimiter(long ratePerSecond, double maxBurstSeconds) {
        rateLimiterImpl = RateLimiter.create(ratePerSecond, (long) (maxBurstSeconds * 1000), TimeUnit.MILLISECONDS);
    }

    @Override
    public void setRate(double ratePerSecond) {
        rateLimiterImpl.setRate(ratePerSecond);
    }

    @Override
    public void request(int permits) throws HyracksDataException {
        rateLimiterImpl.acquire(permits);
    }

}
