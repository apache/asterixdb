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

import org.apache.hyracks.util.annotations.NotThreadSafe;

@NotThreadSafe
public class HttpServerConfigBuilder {

    private static final int MAX_REQUEST_CHUNK_SIZE = 262144;
    private static final int MAX_REQUEST_HEADER_SIZE = 262144;
    private static final int MAX_REQUEST_INITIAL_LINE_LENGTH = 131072;
    private static final int RESPONSE_CHUNK_SIZE = 4096;
    private static final int DEFAULT_THREAD_COUNT = 16;
    private static final int DEFAULT_MAX_QUEUE_SIZE = 256;

    private int maxRequestSize = Integer.MAX_VALUE;
    private int threadCount = DEFAULT_THREAD_COUNT;
    private int requestQueueSize = DEFAULT_MAX_QUEUE_SIZE;
    private int maxRequestChunkSize = MAX_REQUEST_CHUNK_SIZE;
    private int maxResponseChunkSize = RESPONSE_CHUNK_SIZE;
    private int maxRequestHeaderSize = MAX_REQUEST_HEADER_SIZE;
    private int maxRequestInitialLineLength = MAX_REQUEST_INITIAL_LINE_LENGTH;

    private HttpServerConfigBuilder() {
    }

    public static HttpServerConfig createDefault() {
        return new HttpServerConfigBuilder().build();
    }

    public static HttpServerConfigBuilder custom() {
        return new HttpServerConfigBuilder();
    }

    public HttpServerConfigBuilder setMaxRequestSize(int maxRequestSize) {
        this.maxRequestSize = maxRequestSize;
        return this;
    }

    public HttpServerConfigBuilder setThreadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
    }

    public HttpServerConfigBuilder setRequestQueueSize(int requestQueueSize) {
        this.requestQueueSize = requestQueueSize;
        return this;
    }

    public HttpServerConfigBuilder setMaxRequestChunkSize(int maxRequestChunkSize) {
        this.maxRequestChunkSize = maxRequestChunkSize;
        return this;
    }

    public HttpServerConfigBuilder setMaxResponseChunkSize(int maxResponseChunkSize) {
        this.maxResponseChunkSize = maxResponseChunkSize;
        return this;
    }

    public HttpServerConfigBuilder setMaxRequestHeaderSize(int maxRequestHeaderSize) {
        this.maxRequestHeaderSize = maxRequestHeaderSize;
        return this;
    }

    public HttpServerConfigBuilder setMaxRequestInitialLineLength(int maxRequestInitialLineLength) {
        this.maxRequestInitialLineLength = maxRequestInitialLineLength;
        return this;
    }

    public HttpServerConfig build() {
        return HttpServerConfig.of(threadCount, requestQueueSize, maxRequestSize, maxRequestInitialLineLength,
                maxRequestHeaderSize, maxRequestChunkSize, maxResponseChunkSize);
    }
}
