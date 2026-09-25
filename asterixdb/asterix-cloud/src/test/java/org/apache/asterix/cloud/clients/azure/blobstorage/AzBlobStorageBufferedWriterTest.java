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

import java.io.ByteArrayOutputStream;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.cloud.CloudResettableInputStream;
import org.apache.asterix.cloud.WriterSingleBufferProvider;
import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.profiler.NoOpRequestProfilerLimiter;
import org.junit.Assert;
import org.junit.Test;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A block whose staging is retried must be committed exactly once. Each attempt stages the bytes under a fresh block
 * id, so an id kept from a failed attempt ends up in the committed block list: if that block never reached the
 * service the commit is rejected, and if it did and only the reply was lost the blob silently holds the bytes twice.
 */
public class AzBlobStorageBufferedWriterTest {
    private static final int BLOCK_SIZE = 8;
    private static final String DATA = "AAAAAAAABBBBBBBBCCCCCCCC";

    @Test
    public void blockThatNeverReachedTheServiceIsCommittedOnce() throws Exception {
        assertWrittenOnce(new FakeBlobService(false));
    }

    @Test
    public void blockWhoseReplyWasLostIsCommittedOnce() throws Exception {
        assertWrittenOnce(new FakeBlobService(true));
    }

    private static void assertWrittenOnce(FakeBlobService service) throws Exception {
        // a single SDK attempt, so the injected failure reaches CloudRetryableRequestUtil rather than the SDK's retry
        BlobContainerClient container =
                new BlobServiceClientBuilder().endpoint("http://127.0.0.1:1/account").httpClient(service)
                        .retryOptions(
                                new RequestRetryOptions(RetryPolicyType.FIXED, 1, (Integer) null, null, null, null))
                        .buildClient().getBlobContainerClient("container");
        AzBlobStorageBufferedWriter bufferedWriter =
                new AzBlobStorageBufferedWriter(container, NoOpRequestProfilerLimiter.INSTANCE,
                        ICloudGuardian.NoOpCloudGuardian.INSTANCE, "container", "blob", AccessTier.HOT);
        CloudResettableInputStream writer =
                new CloudResettableInputStream(bufferedWriter, new WriterSingleBufferProvider(BLOCK_SIZE));

        byte[] data = DATA.getBytes(StandardCharsets.UTF_8);
        writer.write(data, 0, data.length);
        writer.finish();

        Assert.assertEquals("one retried staging on top of one per block", DATA.length() / BLOCK_SIZE + 1,
                service.stageRequests);
        Assert.assertEquals(DATA, service.committed);
    }

    /**
     * Keeps staged blocks by id and assembles the blob from the committed block list, rejecting the commit when the
     * list names a block that was never staged, as the service does. The first staging request fails with a 503,
     * either before the block is stored or after.
     */
    private static final class FakeBlobService implements HttpClient {
        private static final Pattern BLOCK_ID = Pattern.compile("[?&]blockid=([^&]+)");
        private static final Pattern LATEST = Pattern.compile("<Latest>([^<]+)</Latest>");

        private final boolean storeBeforeFailing;
        private final Map<String, byte[]> staged = new HashMap<>();
        private int stageRequests;
        private String committed;

        private FakeBlobService(boolean storeBeforeFailing) {
            this.storeBeforeFailing = storeBeforeFailing;
        }

        @Override
        public Mono<HttpResponse> send(HttpRequest request) {
            String query = request.getUrl().getQuery();
            if (query != null && query.contains("comp=blocklist")) {
                return Mono.just(commit(request));
            }
            if (query != null && query.contains("comp=block")) {
                return Mono.just(stage(request));
            }
            return Mono.just(new FakeResponse(request, 400, "UnexpectedRequest"));
        }

        private HttpResponse stage(HttpRequest request) {
            boolean first = stageRequests++ == 0;
            if (first && !storeBeforeFailing) {
                return new FakeResponse(request, 503, "ServerBusy");
            }
            Matcher id = BLOCK_ID.matcher(request.getUrl().getQuery());
            Assert.assertTrue(id.find());
            staged.put(URLDecoder.decode(id.group(1), StandardCharsets.UTF_8), request.getBodyAsBinaryData().toBytes());
            return first ? new FakeResponse(request, 503, "ServerBusy") : new FakeResponse(request, 201, null);
        }

        private HttpResponse commit(HttpRequest request) {
            ByteArrayOutputStream blob = new ByteArrayOutputStream();
            Matcher ids = LATEST.matcher(request.getBodyAsBinaryData().toString());
            while (ids.find()) {
                byte[] block = staged.get(ids.group(1));
                if (block == null) {
                    return new FakeResponse(request, 400, "InvalidBlockList");
                }
                blob.writeBytes(block);
            }
            committed = blob.toString(StandardCharsets.UTF_8);
            return new FakeResponse(request, 201, null);
        }
    }

    private static final class FakeResponse extends HttpResponse {
        private final int status;
        private final HttpHeaders headers = new HttpHeaders();

        private FakeResponse(HttpRequest request, int status, String errorCode) {
            super(request);
            this.status = status;
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
