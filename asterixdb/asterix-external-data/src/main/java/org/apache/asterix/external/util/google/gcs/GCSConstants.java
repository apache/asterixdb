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
package org.apache.asterix.external.util.google.gcs;

import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

import java.util.concurrent.CancellationException;

import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.api.gax.retrying.TimedAttemptSettings;
import com.google.cloud.ExceptionHandler;
import com.google.cloud.storage.StorageRetryStrategy;

public class GCSConstants {

    private static final Logger LOGGER = LogManager.getLogger();

    private GCSConstants() {
        throw new AssertionError("do not instantiate");
    }

    // Key max length
    public static final int MAX_KEY_LENGTH_IN_BYTES = 1024;

    public static final String APPLICATION_DEFAULT_CREDENTIALS_FIELD_NAME = "applicationDefaultCredentials";
    public static final String IMPERSONATE_SERVICE_ACCOUNT_FIELD_NAME = "impersonateServiceAccount";
    public static final String JSON_CREDENTIALS_FIELD_NAME = "jsonCredentials";
    public static final String ENDPOINT_FIELD_NAME = "endpoint";
    public static final String STORAGE_PREFIX = "prefix";

    // hadoop internal configuration
    public static final String HADOOP_GCS_PROTOCOL = "gs";
    public static final String MAX_BATCH_THREADS = "fs.gs.batch.threads";
    public static final String HADOOP_ENDPOINT = "fs.gs.storage.root.url";
    public static final String HADOOP_SUPPORT_COMPRESSED = "fs.gs.inputstream.support.gzip.encoding.enable";

    // hadoop credentials
    public static final String HADOOP_AUTH_TYPE = "fs.gs.auth.type";
    public static final String HADOOP_AUTH_UNAUTHENTICATED = "UNAUTHENTICATED";

    public static class JsonCredentials {
        public static final String PRIVATE_KEY_ID = "private_key_id";
        public static final String PRIVATE_KEY = "private_key";
        public static final String CLIENT_EMAIL = "client_email";
    }

    public static class HadoopAuthServiceAccount {
        public static final String IMPERSONATE_SERVICE_ACCOUNT = "fs.gs.auth.impersonation.service.account";
        public static final String PRIVATE_KEY_ID = "fs.gs.auth.service.account.private.key.id";
        public static final String PRIVATE_KEY = "fs.gs.auth.service.account.private.key";
        public static final String CLIENT_EMAIL = "fs.gs.auth.service.account.email";
    }

    public static final StorageRetryStrategy DEFAULT_NO_RETRY_ON_THREAD_INTERRUPT_STRATEGY;
    static {
        StorageRetryStrategy defaultStrategy = StorageRetryStrategy.getDefaultStorageRetryStrategy();
        ExceptionHandler defaultIdempotentHandler = (ExceptionHandler) defaultStrategy.getIdempotentHandler();
        ExceptionHandler defaultNonIdempotentHandler = (ExceptionHandler) defaultStrategy.getNonidempotentHandler();

        ResultRetryAlgorithm<Object> noRetryOnThreadInterruptIdempotentHandler = new ResultRetryAlgorithm<>() {
            @Override
            public TimedAttemptSettings createNextAttempt(Throwable prevThrowable, Object prevResponse,
                    TimedAttemptSettings prevSettings) {
                return defaultIdempotentHandler.createNextAttempt(prevThrowable, prevResponse, prevSettings);
            }

            @Override
            public boolean shouldRetry(Throwable prevThrowable, Object prevResponse) throws CancellationException {
                if (ExceptionUtils.causedByInterrupt(prevThrowable) || Thread.currentThread().isInterrupted()) {
                    interruptRequest(prevThrowable);
                }
                return defaultIdempotentHandler.shouldRetry(prevThrowable, prevResponse);
            }
        };

        ResultRetryAlgorithm<Object> noRetryOnThreadInterruptNonIdempotentHandler = new ResultRetryAlgorithm<>() {
            @Override
            public TimedAttemptSettings createNextAttempt(Throwable prevThrowable, Object prevResponse,
                    TimedAttemptSettings prevSettings) {
                return defaultNonIdempotentHandler.createNextAttempt(prevThrowable, prevResponse, prevSettings);
            }

            @Override
            public boolean shouldRetry(Throwable prevThrowable, Object prevResponse) throws CancellationException {
                if (ExceptionUtils.causedByInterrupt(prevThrowable) || Thread.currentThread().isInterrupted()) {
                    interruptRequest(prevThrowable);
                }
                return defaultNonIdempotentHandler.shouldRetry(prevThrowable, prevResponse);
            }
        };

        DEFAULT_NO_RETRY_ON_THREAD_INTERRUPT_STRATEGY = new StorageRetryStrategy() {
            private static final long serialVersionUID = 1L;

            @Override
            public ResultRetryAlgorithm<?> getIdempotentHandler() {
                return noRetryOnThreadInterruptIdempotentHandler;
            }

            @Override
            public ResultRetryAlgorithm<?> getNonidempotentHandler() {
                return noRetryOnThreadInterruptNonIdempotentHandler;
            }
        };
    }

    /**
     * Throwing a CancellationException will cause the GCS client to abort the whole operation, not only stop retrying
     */
    private static void interruptRequest(Throwable th) {
        Thread.currentThread().interrupt();
        CancellationException ex = new CancellationException("Request was interrupted, aborting retries and request");
        if (th != null) {
            ex.initCause(th);
        }
        String stackTrace = getStackTrace(ex);
        LOGGER.debug("Request was interrupted, aborting retries and request\n{}", stackTrace);
        throw ex;
    }
}
