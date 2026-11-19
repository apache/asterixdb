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

import java.net.URL;

import org.apache.asterix.cloud.clients.metrics.CloudRequestFailure;
import org.apache.asterix.cloud.clients.metrics.CloudRequestListeners;
import org.apache.asterix.cloud.clients.metrics.CloudRequestOperation;

import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelineNextSyncPolicy;
import com.azure.core.http.HttpPipelinePosition;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;

import reactor.core.publisher.Mono;

/**
 * Reports each Azure Blob Storage call to the {@link CloudRequestListeners}: its duration across all attempts, and why
 * it failed. Placed per call rather than per retry, so that a retried request is reported once, with its outcome.
 */
public final class AzRequestMetricsPolicy implements HttpPipelinePolicy {
    public static final AzRequestMetricsPolicy INSTANCE = new AzRequestMetricsPolicy();

    private AzRequestMetricsPolicy() {
    }

    @Override
    public HttpPipelinePosition getPipelinePosition() {
        return HttpPipelinePosition.PER_CALL;
    }

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        CloudRequestOperation operation = operation(context.getHttpRequest());
        long start = System.nanoTime();
        return next.process().doOnSuccess(response -> report(operation, start, response))
                .doOnError(th -> CloudRequestListeners.requestCompleted(operation, System.nanoTime() - start,
                        CloudRequestFailure.ofTransportFailure(th)));
    }

    @Override
    public HttpResponse processSync(HttpPipelineCallContext context, HttpPipelineNextSyncPolicy next) {
        CloudRequestOperation operation = operation(context.getHttpRequest());
        long start = System.nanoTime();
        HttpResponse response;
        try {
            response = next.processSync();
        } catch (RuntimeException e) {
            CloudRequestListeners.requestCompleted(operation, System.nanoTime() - start,
                    CloudRequestFailure.ofTransportFailure(e));
            throw e;
        }
        report(operation, start, response);
        return response;
    }

    private static void report(CloudRequestOperation operation, long start, HttpResponse response) {
        int status = response == null ? 0 : response.getStatusCode();
        CloudRequestListeners.requestCompleted(operation, System.nanoTime() - start,
                status >= 400 ? CloudRequestFailure.ofStatus(status) : null);
    }

    static CloudRequestOperation operation(HttpRequest request) {
        HttpMethod method = request.getHttpMethod();
        URL url = request.getUrl();
        String query = url.getQuery() == null ? "" : url.getQuery();
        if (method == HttpMethod.GET) {
            return query.contains("comp=list") ? CloudRequestOperation.LIST : CloudRequestOperation.GET;
        } else if (method == HttpMethod.HEAD) {
            return CloudRequestOperation.HEAD;
        } else if (method == HttpMethod.PUT) {
            if (request.getHeaders().getValue("x-ms-copy-source") != null) {
                return CloudRequestOperation.COPY;
            }
            return query.contains("comp=block") ? CloudRequestOperation.MULTIPART_UPLOAD : CloudRequestOperation.WRITE;
        } else if (method == HttpMethod.DELETE) {
            return CloudRequestOperation.DELETE;
        } else if (method == HttpMethod.POST && query.contains("comp=batch")) {
            return CloudRequestOperation.DELETE;
        }
        return CloudRequestOperation.OTHER;
    }
}
