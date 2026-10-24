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
package org.apache.asterix.cloud.clients.google.gcs;

import java.io.IOException;

import org.apache.asterix.cloud.clients.metrics.CloudRequestFailure;
import org.apache.asterix.cloud.clients.metrics.CloudRequestListeners;
import org.apache.asterix.cloud.clients.metrics.CloudRequestOperation;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;

/**
 * Reports each HTTP request the GCS client sends to the {@link CloudRequestListeners}: its duration up to the response
 * headers, and why it failed. The GCS client exposes no per-request hook of its own, so this sits under its HTTP
 * transport.
 */
final class GCSRequestMetricsHttpClient extends CloseableHttpClient {
    private final CloseableHttpClient delegate;

    GCSRequestMetricsHttpClient(CloseableHttpClient delegate) {
        this.delegate = delegate;
    }

    @Override
    protected CloseableHttpResponse doExecute(HttpHost target, HttpRequest request, HttpContext context)
            throws IOException {
        CloudRequestOperation op = operation(request.getRequestLine().getMethod(), request.getRequestLine().getUri());
        long start = System.nanoTime();
        CloseableHttpResponse response;
        try {
            response = delegate.execute(target, request, context);
        } catch (IOException | RuntimeException e) {
            CloudRequestListeners.requestCompleted(op, System.nanoTime() - start,
                    CloudRequestFailure.ofTransportFailure(e));
            throw e;
        }
        int status = response.getStatusLine().getStatusCode();
        // 308 is how a resumable upload acknowledges a chunk, so anything short of 4xx succeeded
        CloudRequestListeners.requestCompleted(op, System.nanoTime() - start,
                status >= 400 ? CloudRequestFailure.ofStatus(status) : null);
        return response;
    }

    /**
     * Maps a request to the JSON API onto an operation, from its method and URI. Object names are URL-encoded in the
     * path, so the only unescaped slashes are the API's own.
     */
    static CloudRequestOperation operation(String method, String uri) {
        int query = uri.indexOf('?');
        String path = query < 0 ? uri : uri.substring(0, query);
        String params = query < 0 ? "" : uri.substring(query + 1);
        int bucket = path.indexOf("/b/");
        int bucketEnd = bucket < 0 ? -1 : path.indexOf('/', bucket + 3);
        // what follows the bucket: "/o" for the collection, "/o/<name>[/...]" for one object
        String objects = bucketEnd < 0 ? "" : path.substring(bucketEnd);
        boolean object = objects.startsWith("/o/");
        return switch (method) {
            case "GET" -> object ? (params.contains("alt=media") ? CloudRequestOperation.GET : CloudRequestOperation.HEAD)
                    : objects.equals("/o") ? CloudRequestOperation.LIST : CloudRequestOperation.OTHER;
            case "POST" -> objects.equals("/o") && path.contains("/upload/")
                    ? (params.contains("uploadType=resumable") ? CloudRequestOperation.MULTIPART_UPLOAD
                            : CloudRequestOperation.WRITE)
                    : object && (objects.contains("/rewriteTo/") || objects.contains("/copyTo/"))
                            ? CloudRequestOperation.COPY
                            // GCSCloudClient batches nothing but deletes
                            : path.contains("/batch/") ? CloudRequestOperation.DELETE : CloudRequestOperation.OTHER;
            // an upload PUT carries a chunk of a resumable upload
            case "PUT" -> path.contains("/upload/") ? CloudRequestOperation.MULTIPART_UPLOAD
                    : CloudRequestOperation.OTHER;
            case "DELETE" -> CloudRequestOperation.DELETE;
            default -> CloudRequestOperation.OTHER;
        };
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    @Deprecated
    public HttpParams getParams() {
        return delegate.getParams();
    }

    @Override
    @Deprecated
    public ClientConnectionManager getConnectionManager() {
        return delegate.getConnectionManager();
    }
}
