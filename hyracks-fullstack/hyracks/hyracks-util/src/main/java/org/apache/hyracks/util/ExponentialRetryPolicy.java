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
package org.apache.hyracks.util;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ExponentialRetryPolicy implements IRetryPolicy {

    private static final int DEFAULT_MAX_RETRIES = 10;
    private static final long DEFAULT_INITIAL_DELAY_IN_MILLIS = 100;
    private static final long DEFAULT_MAX_DELAY_IN_MILLIS = Long.MAX_VALUE;
    private final int maxRetries;
    private final long initialDelay;
    private final long maxDelay;
    private int attempt = 0;

    /**
     * Default constructor for ExponentialRetryPolicy.
     * Initializes with default max retries, initial delay, and max delay.
     */
    public ExponentialRetryPolicy() {
        this(DEFAULT_MAX_RETRIES, DEFAULT_INITIAL_DELAY_IN_MILLIS, DEFAULT_MAX_DELAY_IN_MILLIS);
    }

    /**
     * ExponentialRetryPolicy with specified max retries, initial delay, and max delay.
     *
     * @param maxRetries   the maximum number of retries
     * @param initialDelay the initial delay in milliseconds
     * @param maxDelay     the maximum delay in milliseconds
     */
    public ExponentialRetryPolicy(int maxRetries, long initialDelay, long maxDelay) {
        this.maxRetries = maxRetries;
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
    }

    /**
     * ExponentialRetryPolicy with specified max retries.
     * Initializes with default initial delay and max delay.
     *
     * @param maxRetries the maximum number of retries
     */
    public ExponentialRetryPolicy(int maxRetries) {
        this(maxRetries, DEFAULT_INITIAL_DELAY_IN_MILLIS, DEFAULT_MAX_DELAY_IN_MILLIS);
    }

    /**
     * ExponentialRetryPolicy with specified max retries and max delay.
     * Initializes with default initial delay.
     *
     * @param maxRetries the maximum number of retries
     * @param maxDelay   the maximum delay in milliseconds
     */
    public ExponentialRetryPolicy(int maxRetries, long maxDelay) {
        this(maxRetries, DEFAULT_INITIAL_DELAY_IN_MILLIS, maxDelay);
    }

    @Override
    public boolean retry(Throwable failure) {
        if (attempt < maxRetries) {
            try {
                long delay = initialDelay * (1L << attempt);
                TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextLong(1 + Long.min(delay, maxDelay)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            attempt++;
            return true;
        }
        return false;
    }
}
