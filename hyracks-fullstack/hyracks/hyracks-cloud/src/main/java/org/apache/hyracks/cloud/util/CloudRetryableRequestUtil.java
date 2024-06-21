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

import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.cloud.io.request.ICloudBeforeRetryRequest;
import org.apache.hyracks.cloud.io.request.ICloudRequest;
import org.apache.hyracks.cloud.io.request.ICloudReturnableRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final int STABLE_NUMBER_OF_RETRIES = 5;
    private static final int UNSTABLE_NUMBER_OF_RETRIES = 100;
    private static final Logger LOGGER = LogManager.getLogger();
    private static final int NUMBER_OF_RETRIES = getNumberOfRetries();

    private static final ICloudBeforeRetryRequest NO_OP_RETRY = () -> {
    };

    private CloudRetryableRequestUtil() {
    }

    /**
     * Run an idempotent request and will retry if failed or interrupted
     *
     * @param request request to run
     */
    public static void run(ICloudRequest request) throws HyracksDataException {
        run(request, NO_OP_RETRY);
    }

    /**
     * Run a none-idempotent request and will retry if failed or interrupted.
     * As the operation is not idempotent, {@link ICloudBeforeRetryRequest} ensures the idempotency of the provided operation
     *
     * @param request request to run
     * @param retry   a pre-retry routine to make the operation idempotent
     */
    public static void run(ICloudRequest request, ICloudBeforeRetryRequest retry) throws HyracksDataException {
        boolean interrupted = Thread.interrupted();
        try {
            while (true) {
                try {
                    doRun(request, retry);
                    break;
                } catch (Throwable e) {
                    // First, clear the interrupted flag
                    interrupted |= Thread.interrupted();
                    if (!ExceptionUtils.causedByInterrupt(e)) {
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
     * Run an idempotent returnable request and will retry if failed or interrupted.
     *
     * @param request request to run
     * @param <T>     return type
     * @return a value of return type
     */
    public static <T> T run(ICloudReturnableRequest<T> request) throws HyracksDataException {
        return run(request, NO_OP_RETRY);
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
                    return doRun(request, retry);
                } catch (Throwable e) {
                    // First, clear the interrupted flag
                    interrupted |= Thread.interrupted();
                    if (!ExceptionUtils.causedByInterrupt(e)) {
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
        doRun(request, NO_OP_RETRY);
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

    private static <T> T doRun(ICloudReturnableRequest<T> request, ICloudBeforeRetryRequest retry)
            throws HyracksDataException {
        int attempt = 1;
        while (true) {
            try {
                return request.call();
            } catch (IOException e) {
                if (attempt > NUMBER_OF_RETRIES) {
                    throw HyracksDataException.create(e);
                }
                attempt++;
                retry.beforeRetry();
                LOGGER.warn("Failed to perform ICloudReturnableRequest, performing {}/{}", attempt, NUMBER_OF_RETRIES,
                        e);
            }
        }
    }

    private static void doRun(ICloudRequest request, ICloudBeforeRetryRequest retry) throws HyracksDataException {
        int attempt = 1;
        while (true) {
            try {
                request.call();
                break;
            } catch (IOException e) {
                if (attempt > NUMBER_OF_RETRIES) {
                    throw HyracksDataException.create(e);
                }
                attempt++;
                retry.beforeRetry();
                LOGGER.warn("Failed to perform ICloudRequest, performing {}/{}", attempt, NUMBER_OF_RETRIES, e);
            }
        }
    }

    private static int getNumberOfRetries() {
        boolean unstable = Boolean.getBoolean(CLOUD_UNSTABLE_MODE);
        return unstable ? UNSTABLE_NUMBER_OF_RETRIES : STABLE_NUMBER_OF_RETRIES;
    }

}
