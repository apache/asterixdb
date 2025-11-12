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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExponentialRetryPolicy implements IRetryPolicy {

    private static final Logger LOGGER = LogManager.getLogger();

    public static final String CLOUD_UNSTABLE_MODE = "cloud.unstable.mode";
    private static final int DEFAULT_MAX_RETRIES = 10;
    private static final long DEFAULT_INITIAL_DELAY_IN_MILLIS = 100;
    private static final long DEFAULT_MAX_DELAY_IN_MILLIS = Long.MAX_VALUE - 1;
    private static final int UNSTABLE_NUMBER_OF_RETRIES = 100;
    private final int maxRetries;
    private final long maxDelay;
    private int attempt = 0;
    private long delay;
    private boolean printDebugLines = true;

    /**
     * Default constructor for ExponentialRetryPolicy.
     * Initializes with default max retries, initial delay, and max delay.
     */
    public ExponentialRetryPolicy() {
        this(isUnstable() ? UNSTABLE_NUMBER_OF_RETRIES : DEFAULT_MAX_RETRIES, DEFAULT_INITIAL_DELAY_IN_MILLIS,
                isUnstable() ? 0 : DEFAULT_MAX_DELAY_IN_MILLIS);
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
        this.maxDelay = Long.min(maxDelay, DEFAULT_MAX_DELAY_IN_MILLIS);
        this.delay = Long.min(initialDelay, this.maxDelay);
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
        printDebugLines = false;
    }

    @Override
    public boolean retry(Throwable failure) throws InterruptedException {
        if (failure instanceof IllegalArgumentException) {
            return false;
        }
        if (attempt < maxRetries) {
            long sleepTime = ThreadLocalRandom.current().nextLong(1 + delay);
            if (printDebugLines) {
                LOGGER.info("Retrying after {}ms, attempt: {}/{}", sleepTime, attempt + 1, maxRetries);
            }
            TimeUnit.MILLISECONDS.sleep(sleepTime);
            attempt++;
            delay = delay > maxDelay / 2 ? maxDelay : delay * 2;
            return true;
        }
        return false;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getInitialDelay() {
        return delay;
    }

    public long getMaxDelay() {
        return maxDelay;
    }

    private static boolean isUnstable() {
        return Boolean.getBoolean(CLOUD_UNSTABLE_MODE);
    }
}
