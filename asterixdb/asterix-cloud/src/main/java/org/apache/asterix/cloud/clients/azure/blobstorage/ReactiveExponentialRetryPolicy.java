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
package org.apache.asterix.cloud.clients.azure.blobstorage;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.cloud.util.CloudRetryableRequestUtil;
import org.apache.hyracks.util.ExponentialRetryPolicy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import reactor.util.retry.Retry;

/**
 * Utility methods for building Reactor {@link Retry} policies that behave similarly to the
 * blocking {@link ExponentialRetryPolicy}.
 */
public class ReactiveExponentialRetryPolicy {

    private static final Logger LOGGER = LogManager.getLogger();

    private ReactiveExponentialRetryPolicy() {
    }

    /**
     * Creates a {@link Retry} instance based on an {@link ExponentialRetryPolicy}.
     *
     * @param policy the blocking policy to mirror. If {@code null}, defaults are used.
     * @return a Reactor {@link Retry} behaving similarly to the provided policy.
     */
    public static Retry retryPolicy(ExponentialRetryPolicy policy) {
        ExponentialRetryPolicy effectivePolicy = policy != null ? policy
                : new ExponentialRetryPolicy(CloudRetryableRequestUtil.NUMBER_OF_RETRIES,
                        CloudRetryableRequestUtil.MAX_DELAY_BETWEEN_RETRIES);
        long initialDelay = Math.max(0L, effectivePolicy.getInitialDelay());
        long maxDelay = Math.max(0L, effectivePolicy.getMaxDelay());
        int maxRetries = Math.max(0, effectivePolicy.getMaxRetries());
        long maxAttempts = Math.max(1L, (long) maxRetries + 1L);
        return Retry.backoff(maxAttempts, Duration.ofMillis(initialDelay)).maxBackoff(Duration.ofMillis(maxDelay))
                .filter(ReactiveExponentialRetryPolicy::isRetryable).doBeforeRetry(signal -> {
                    long retriesSoFar = signal.totalRetries();
                    long delayMillis = computeDelayMillis(initialDelay, maxDelay, retriesSoFar);
                    long attempt = retriesSoFar + 1;
                    LOGGER.info("Retrying after {}ms, attempt {}/{}", delayMillis, attempt, maxRetries);
                }).transientErrors(true);
    }

    private static long computeDelayMillis(long initialDelay, long maxDelay, long retriesSoFar) {
        if (initialDelay <= 0 || maxDelay <= 0) {
            return 0L;
        }

        long delay = initialDelay;
        for (long i = 0; i < retriesSoFar; i++) {
            delay = delay > maxDelay / 2 ? maxDelay : delay * 2;
        }

        long jitteredDelay = ThreadLocalRandom.current().nextLong(1, delay + 1);

        return Math.min(jitteredDelay, maxDelay);
    }

    private static boolean isRetryable(Throwable error) {
        if (error instanceof IllegalArgumentException) {
            return false;
        }
        if (ExceptionUtils.causedByInterrupt(error)) {
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }
}
