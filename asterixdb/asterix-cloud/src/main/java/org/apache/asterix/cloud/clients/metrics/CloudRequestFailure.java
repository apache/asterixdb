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
package org.apache.asterix.cloud.clients.metrics;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeoutException;

/**
 * Why a request to cloud storage failed, as told apart for metrics. Provider errors are mapped onto this closed set so
 * that no error text, key or path reaches a metric label.
 */
public enum CloudRequestFailure {
    NOT_FOUND("not_found"),
    ACCESS_DENIED("access_denied"),
    THROTTLED("throttled"),
    TIMEOUT("timeout"),
    CONNECTION("connection"),
    SERVER_ERROR("server_error"),
    CLIENT_ERROR("client_error"),
    OTHER("other");

    private final String label;

    CloudRequestFailure(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }

    /**
     * @param status the HTTP status of the response
     */
    public static CloudRequestFailure ofStatus(int status) {
        return switch (status) {
            case 404 -> NOT_FOUND;
            case 401, 403 -> ACCESS_DENIED;
            case 429, 503 -> THROTTLED;
            case 408 -> TIMEOUT;
            default -> status >= 500 ? SERVER_ERROR : status >= 400 ? CLIENT_ERROR : OTHER;
        };
    }

    /**
     * Classifies a failure which carried no HTTP status, from the exception and its causes.
     */
    public static CloudRequestFailure ofTransportFailure(Throwable th) {
        for (Throwable cause = th; cause != null; cause = cause.getCause()) {
            if (cause instanceof SocketTimeoutException || cause instanceof TimeoutException
                    || cause.getClass().getSimpleName().contains("Timeout")) {
                return TIMEOUT;
            }
            if (cause instanceof IOException) {
                return CONNECTION;
            }
        }
        return OTHER;
    }
}
