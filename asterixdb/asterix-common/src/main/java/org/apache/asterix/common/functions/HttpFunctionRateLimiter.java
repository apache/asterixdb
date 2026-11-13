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

import java.util.concurrent.TimeUnit;

/**
 * Bounds the rate at which the external HTTP functions of a node send requests: a token bucket that fills with
 * {@code rate} tokens a second, up to one second's worth, from which each request takes one. It is kept as
 * the time at which the bucket is empty, the generic cell rate algorithm, so that taking a token is constant time.
 */
public final class HttpFunctionRateLimiter {

    private final long intervalNanos;
    // how far the schedule may run ahead of now before a request waits: a full bucket's worth of tokens but one
    private final long toleranceNanos;
    // the System.nanoTime() at which the bucket would be empty again had nothing waited; guarded by this
    private long emptyAtNanos;

    /**
     * @param rate the tokens added a second, and the most the bucket holds, or 0 for no limit
     */
    public HttpFunctionRateLimiter(int rate) {
        intervalNanos = rate == 0 ? 0 : TimeUnit.SECONDS.toNanos(1) / rate;
        toleranceNanos = TimeUnit.SECONDS.toNanos(1) - intervalNanos;
        emptyAtNanos = System.nanoTime();
    }

    /**
     * Takes a token.
     *
     * @param nowNanos the current {@link System#nanoTime()}
     * @return how many nanoseconds the caller must wait before it sends its request
     */
    public synchronized long reserve(long nowNanos) {
        long sendNanos = Math.max(nowNanos, emptyAtNanos - toleranceNanos);
        emptyAtNanos = Math.max(emptyAtNanos, nowNanos) + intervalNanos;
        return sendNanos - nowNanos;
    }
}
