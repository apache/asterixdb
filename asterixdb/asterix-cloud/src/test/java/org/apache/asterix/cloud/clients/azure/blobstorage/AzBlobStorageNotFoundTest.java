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

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.junit.Assert;
import org.junit.Test;

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobStorageException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The object-not-found predicate decides whether a failure is retried. Getting it wrong either retries a permanent
 * 404 for the whole retry budget, or — when the check itself throws — discards the real cloud failure and surfaces
 * its own exception instead, which additionally escapes the retry loop because it is not a cloud SDK exception.
 */
public class AzBlobStorageNotFoundTest {

    @Test
    public void blobNotFoundIsNotRetried() {
        Assert.assertTrue(AzBlobStorageCloudClient.isNotFound(ex(HttpURLConnection.HTTP_NOT_FOUND, "BlobNotFound")));
    }

    /** A missing container answers 404 under a different error code, and is just as permanent. */
    @Test
    public void containerNotFoundIsNotRetried() {
        Assert.assertTrue(
                AzBlobStorageCloudClient.isNotFound(ex(HttpURLConnection.HTTP_NOT_FOUND, "ContainerNotFound")));
    }

    /** The service does not always send x-ms-error-code, and getErrorCode() is null when it does not. */
    @Test
    public void notFoundWithoutErrorCodeHeaderIsNotRetried() {
        Assert.assertTrue(AzBlobStorageCloudClient.isNotFound(ex(HttpURLConnection.HTTP_NOT_FOUND, null)));
    }

    @Test
    public void retryableStatusWithoutErrorCodeHeaderIsRetried() {
        Assert.assertFalse(AzBlobStorageCloudClient.isNotFound(ex(HttpURLConnection.HTTP_UNAVAILABLE, null)));
    }

    @Test
    public void forbiddenIsRetried() {
        Assert.assertFalse(
                AzBlobStorageCloudClient.isNotFound(ex(HttpURLConnection.HTTP_FORBIDDEN, "AuthorizationFailure")));
    }

    @Test
    public void nonAzureFailureIsRetried() {
        Assert.assertFalse(AzBlobStorageCloudClient.isNotFound(new IllegalStateException("not an SDK failure")));
    }

    private static BlobStorageException ex(int status, String errorCode) {
        return new BlobStorageException("simulated", new FakeResponse(status, errorCode), null);
    }

    /** BlobStorageException derives both its status and its error code from the response, so one is required. */
    private static final class FakeResponse extends HttpResponse {
        private final int status;
        private final HttpHeaders headers;

        private FakeResponse(int status, String errorCode) {
            super(new HttpRequest(HttpMethod.GET, "https://example.invalid/container/blob"));
            this.status = status;
            this.headers = new HttpHeaders();
            if (errorCode != null) {
                headers.set(HttpHeaderName.fromString("x-ms-error-code"), errorCode);
            }
        }

        @Override
        public int getStatusCode() {
            return status;
        }

        @Deprecated
        @Override
        public String getHeaderValue(String name) {
            return headers.getValue(HttpHeaderName.fromString(name));
        }

        @Override
        public HttpHeaders getHeaders() {
            return headers;
        }

        @Override
        public Flux<ByteBuffer> getBody() {
            return Flux.empty();
        }

        @Override
        public Mono<byte[]> getBodyAsByteArray() {
            return Mono.empty();
        }

        @Override
        public Mono<String> getBodyAsString() {
            return Mono.empty();
        }

        @Override
        public Mono<String> getBodyAsString(Charset charset) {
            return Mono.empty();
        }
    }
}
