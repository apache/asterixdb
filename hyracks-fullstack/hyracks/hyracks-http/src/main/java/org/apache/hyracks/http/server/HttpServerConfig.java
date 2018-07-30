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
package org.apache.hyracks.http.server;

import org.apache.hyracks.util.annotations.ThreadSafe;

@ThreadSafe
public class HttpServerConfig {

    private int maxRequestSize;
    private int threadCount;
    private int requestQueueSize;
    private int maxRequestChunkSize;
    private int maxResponseChunkSize;
    private int maxRequestHeaderSize;
    private int maxRequestInitialLineLength;

    private HttpServerConfig() {
    }

    public static HttpServerConfig of(int threadCount, int requestQueueSize, int maxRequestSize,
            int maxRequestInitialLineLength, int maxRequestHeaderSize, int maxRequestChunkSize,
            int maxResponseChunkSize) {
        final HttpServerConfig config = new HttpServerConfig();
        config.threadCount = threadCount;
        config.requestQueueSize = requestQueueSize;
        config.maxRequestSize = maxRequestSize;
        config.maxRequestInitialLineLength = maxRequestInitialLineLength;
        config.maxRequestHeaderSize = maxRequestHeaderSize;
        config.maxRequestChunkSize = maxRequestChunkSize;
        config.maxResponseChunkSize = maxResponseChunkSize;
        return config;
    }

    public int getMaxRequestSize() {
        return maxRequestSize;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public int getRequestQueueSize() {
        return requestQueueSize;
    }

    public int getMaxRequestChunkSize() {
        return maxRequestChunkSize;
    }

    public int getMaxResponseChunkSize() {
        return maxResponseChunkSize;
    }

    public int getMaxRequestHeaderSize() {
        return maxRequestHeaderSize;
    }

    public int getMaxRequestInitialLineLength() {
        return maxRequestInitialLineLength;
    }
}
