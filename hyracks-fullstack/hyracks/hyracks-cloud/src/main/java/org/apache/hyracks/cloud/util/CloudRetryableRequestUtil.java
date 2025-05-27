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
package org.apache.hyracks.cloud.util;

import static org.apache.hyracks.cloud.io.request.ICloudRequest.asReturnableRequest;

import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.cloud.io.request.ICloudBeforeRetryRequest;
import org.apache.hyracks.cloud.io.request.ICloudRequest;
import org.apache.hyracks.cloud.io.request.ICloudRetryPredicate;
import org.apache.hyracks.cloud.io.request.ICloudReturnableRequest;
import org.apache.hyracks.util.ExponentialRetryPolicy;
import org.apache.hyracks.util.IRetryPolicy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.cloud.BaseServiceException;

import software.amazon.awssdk.core.exception.SdkException;

/**
 * Run {@link ICloudRequest} and {@link ICloudReturnableRequest} with retries
 */
public class CloudRetryableRequestUtil {
    /**
     * Whether simulating/testing unstable cloud environment or not. This value affects the number of retries.
     * Set this as a system property to 'true' to indicate running an unstable cloud environment.
     *
     * @see System#setProperty(String, String)
     */
    public static final String CLOUD_UNSTABLE_MODE = "cloud.unstable.mode";
    private static final int STABLE_NUMBER_OF_RETRIES = 10;
    private static final long STABLE_MAX_DELAY_BETWEEN_RETRIES_IN_MILLIS = 10_000;
    private static final int UNSTABLE_NUMBER_OF_RETRIES = 100;
    private static final int UNSTABLE_MAX_DELAY_BETWEEN_RETRIES_IN_MILLIS = 0;
    private static final Logger LOGGER = LogManager.getLogger();
    private static final int NUMBER_OF_RETRIES = getNumberOfRetries();
    private static final long MAX_DELAY_BETWEEN_RETRIES = getMaxDelayBetweenRetries();

    private static final ICloudRetryPredicate RETRY_ALWAYS_PREDICATE = e -> true;
    private static final ICloudBeforeRetryRequest NO_OP_BEFORE_RETRY = () -> {
    };

    private CloudRetryableRequestUtil() {
    }

    /**
     * Run an idempotent request and will retry if failed or interrupted
     *
     * @param request request to run
     */
    public static void run(ICloudRequest request) throws HyracksDataException {
        run(request, NO_OP_BEFORE_RETRY);
    }

    /**
     * Run a none-idempotent request and will retry if failed or interrupted.
     * As the operation is not idempotent, {@link ICloudBeforeRetryRequest} ensures the idempotency of the provided operation
     *
     * @param request request to run
     * @param retry   a pre-retry routine to make the operation idempotent
     */
    public static void run(ICloudRequest request, ICloudBeforeRetryRequest retry) throws HyracksDataException {
        run(asReturnableRequest(request), retry);
    }

    /**
     * Run an idempotent returnable request and will retry if failed or interrupted.
     *
     * @param request request to run
     * @param <T>     return type
     * @return a value of return type
     */
    public static <T> T run(ICloudReturnableRequest<T> request) throws HyracksDataException {
        return run(request, NO_OP_BEFORE_RETRY);
    }

    /**
     * Run a none-idempotent returnable request and will retry if failed or interrupted.
     * As the operation is not idempotent, {@link ICloudBeforeRetryRequest} ensures the idempotency of the provided operation
     *
     * @param request request to run
     * @param <T>     return type
     * @param retry   a pre-retry routine to make the operation idempotent
     * @return a value of return type
     */
    public static <T> T run(ICloudReturnableRequest<T> request, ICloudBeforeRetryRequest retry)
            throws HyracksDataException {
        boolean interrupted = Thread.interrupted();
        try {
            while (true) {
                try {
                    return doRun(request, retry, RETRY_ALWAYS_PREDICATE);
                } catch (Throwable e) {
                    // First, clear the interrupted flag
                    interrupted |= Thread.interrupted();
                    if (ExceptionUtils.causedByInterrupt(e)) {
                        interrupted = true;
                    } else {
                        // The cause isn't an interruption, rethrow
                        throw e;
                    }
                    retry.beforeRetry();
                    LOGGER.warn("Ignored interrupting ICloudReturnableRequest", e);
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Run an idempotent request and will retry if failed.
     * This will not retry if the thread is interrupted
     *
     * @param request request to run
     */
    public static void runWithNoRetryOnInterruption(ICloudRequest request) throws HyracksDataException {
        runWithNoRetryOnInterruption(request, RETRY_ALWAYS_PREDICATE);
    }

    /**
     * Run a none-idempotent request and will retry if failed
     * This will not retry if the thread is interrupted.
     * As the operation is not idempotent, {@link ICloudBeforeRetryRequest} ensures the idempotency of the provided operation
     *
     * @param request request to run
     * @param retry   a pre-retry routine to make the operation idempotent
     */
    public static void runWithNoRetryOnInterruption(ICloudRequest request, ICloudBeforeRetryRequest retry)
            throws HyracksDataException {
        doRun(request, retry);
    }

    public static void runWithNoRetryOnInterruption(ICloudRequest request, ICloudRetryPredicate shouldRetry)
            throws HyracksDataException {
        doRun(request, NO_OP_BEFORE_RETRY, shouldRetry);
    }

    public static <T> T runWithNoRetryOnInterruption(ICloudReturnableRequest<T> request) throws HyracksDataException {
        return runWithNoRetryOnInterruption(request, NO_OP_BEFORE_RETRY, RETRY_ALWAYS_PREDICATE);
    }

    public static <T> T runWithNoRetryOnInterruption(ICloudReturnableRequest<T> request, ICloudBeforeRetryRequest retry)
            throws HyracksDataException {
        return runWithNoRetryOnInterruption(request, retry, RETRY_ALWAYS_PREDICATE);
    }

    public static <T> T runWithNoRetryOnInterruption(ICloudReturnableRequest<T> request,
            ICloudRetryPredicate shouldRetry) throws HyracksDataException {
        return runWithNoRetryOnInterruption(request, NO_OP_BEFORE_RETRY, shouldRetry);
    }

    public static <T> T runWithNoRetryOnInterruption(ICloudReturnableRequest<T> request, ICloudBeforeRetryRequest retry,
            ICloudRetryPredicate shouldRetry) throws HyracksDataException {
        return doRun(request, retry, shouldRetry);
    }

    private static <T> T doRun(ICloudReturnableRequest<T> request, ICloudBeforeRetryRequest retry,
            ICloudRetryPredicate shouldRetry) throws HyracksDataException {
        int attempt = 1;
        IRetryPolicy retryPolicy = null;
        while (true) {
            try {
                return request.call();
            } catch (IOException | BaseServiceException | SdkException e) {
                if (!shouldRetry.test(e)) {
                    throw HyracksDataException.create(e);
                }
                if (retryPolicy == null) {
                    retryPolicy = new ExponentialRetryPolicy(NUMBER_OF_RETRIES, MAX_DELAY_BETWEEN_RETRIES);
                }
                if (ExceptionUtils.causedByInterrupt(e) && !Thread.currentThread().isInterrupted()) {
                    LOGGER.warn("Lost suppressed interrupt during ICloudReturnableRequest", e);
                    Thread.currentThread().interrupt();
                }
                try {
                    if (Thread.currentThread().isInterrupted() || !retryPolicy.retry(e)) {
                        throw HyracksDataException.create(e);
                    }
                } catch (InterruptedException interruptedEx) {
                    throw HyracksDataException.create(interruptedEx);
                }
                attempt++;
                retry.beforeRetry();
                LOGGER.warn("Failed to perform ICloudReturnableRequest, performing {}/{}", attempt, NUMBER_OF_RETRIES,
                        e);
            }
        }
    }

    private static void doRun(ICloudRequest request, ICloudBeforeRetryRequest retry) throws HyracksDataException {
        doRun(request, retry, RETRY_ALWAYS_PREDICATE);
    }

    private static void doRun(ICloudRequest request, ICloudBeforeRetryRequest retry, ICloudRetryPredicate shouldRetry)
            throws HyracksDataException {
        doRun(asReturnableRequest(request), retry, shouldRetry);
    }

    private static int getNumberOfRetries() {
        boolean unstable = Boolean.getBoolean(CLOUD_UNSTABLE_MODE);
        return unstable ? UNSTABLE_NUMBER_OF_RETRIES : STABLE_NUMBER_OF_RETRIES;
    }

    private static long getMaxDelayBetweenRetries() {
        boolean unstable = Boolean.getBoolean(CLOUD_UNSTABLE_MODE);
        return unstable ? UNSTABLE_MAX_DELAY_BETWEEN_RETRIES_IN_MILLIS : STABLE_MAX_DELAY_BETWEEN_RETRIES_IN_MILLIS;
    }
}
