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
package org.apache.asterix.cloud.clients.aws.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hyracks.cloud.io.S3ChecksumBehavior;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.hyracks.util.annotations.AiProvenance.Agent;
import org.apache.hyracks.util.annotations.AiProvenance.ContributionKind;
import org.apache.hyracks.util.annotations.AiProvenance.Tool;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.DockerClientFactory;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Regression guard for the S3 checksum behavior in {@code when_required} mode. It exercises the real
 * {@link S3CloudClient#buildClient} path (including {@code LegacyMd5Plugin}) against a live Adobe
 * S3Mock container, with an in-process capturing reverse proxy in front so the exact request headers
 * on the wire are asserted while the request still round-trips to a real S3-compatible store. If a
 * future AWS SDK default change (as in 2.30.0) alters the checksum/transfer headers, the golden
 * snapshots below fail here instead of the change regressing silently in production.
 *
 * <p>Primary guard (the observed Huawei OBS failure): the SDK 2.30+ default (WHEN_SUPPORTED) turns
 * ordinary uploads into {@code aws-chunked} streaming-trailer PUTs
 * ({@code x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER}), which OBS rejects with
 * 400 XAmzContentSHA256Mismatch and Adobe S3Mock silently truncates. In when_required an upload must
 * stay a plain PUT, so it carries none of those headers and the body is stored intact.
 *
 * <p>Secondary guard: the bulk DeleteObjects (which OBS rejected with "Missing required header:
 * Content-MD5") carries the legacy {@code Content-MD5} in when_required. The SDK also emits a
 * flexible {@code x-amz-checksum-crc32} header that {@code LegacyMd5Plugin} cannot suppress (it is
 * added by a late signed pre-signing stage); OBS and every store validated tolerate it as long as
 * Content-MD5 is present, so the snapshot records both.
 */
@AiProvenance(agent = Agent.CLAUDE_OPUS_4_7, tool = Tool.ANTHROPIC_CLI, contributionKind = ContributionKind.TEST_GENERATED, notes = "ASTERIXDB-3791: asserts plain uploads and Content-MD5 on bulk delete in when_required against Adobe S3Mock; generated via Claude Code")
public class S3ChecksumHeaderTest {

    private static final String REGION = "us-east-1";
    private static final String BUCKET = "checksum-test-bucket";
    // @locked to match the s3mock-testcontainers library version managed in the parent pom.
    private static final String S3MOCK_VERSION = "4.12.4";
    private static final int UPLOAD_SIZE = 512 * 1024; // multi-chunk sized, to catch aws-chunked framing

    // Request headers that decide how the body is transferred and integrity-checked. A golden
    // snapshot of these is the wire contract; any SDK change to them breaks the test.
    private static final String[] INTEGRITY_HEADERS = { "Content-Encoding", "Content-MD5", "x-amz-checksum-crc32",
            "x-amz-checksum-crc32c", "x-amz-checksum-sha1", "x-amz-checksum-sha256", "x-amz-content-sha256",
            "x-amz-decoded-content-length", "x-amz-sdk-checksum-algorithm", "x-amz-trailer" };

    // Golden wire snapshots captured against AWS SDK 2.44.14 in when_required mode. If the SDK
    // changes checksum behavior these must be re-reviewed against real S3-compatible stores (OBS,
    // OCI) before being updated. A plain PUT carries no integrity/transfer headers at all.
    private static final String PUT_GOLDEN = "";
    private static final String DELETE_GOLDEN = "content-md5=<present>\n" + "x-amz-checksum-crc32=<present>\n"
            + "x-amz-content-sha256=UNSIGNED-PAYLOAD\n" + "x-amz-sdk-checksum-algorithm=CRC32\n";

    // Hop-by-hop / restricted headers the JDK HttpClient forbids setting; the proxy must not forward
    // them (the client re-derives Content-Length/Host from the forwarded body and target).
    private static final Set<String> UNFORWARDABLE = Set.of("connection", "content-length", "date", "expect", "from",
            "host", "upgrade", "via", "warning", "transfer-encoding", "keep-alive", "te", "trailer");

    private static final HttpClient FORWARDER = HttpClient.newHttpClient();

    private static S3MockContainer s3Mock;
    private static HttpServer proxy;
    private static S3Client s3Client;

    // Captured request state of the most recent exchange (tests read it right after the op).
    private static final AtomicReference<CapturedRequest> LAST = new AtomicReference<>();

    private static final class CapturedRequest {
        final String method;
        final String query;
        final Map<String, List<String>> headers;
        final byte[] body;

        CapturedRequest(String method, String query, Map<String, List<String>> headers, byte[] body) {
            this.method = method;
            this.query = query;
            this.headers = headers;
            this.body = body;
        }

        String header(String name) {
            for (Map.Entry<String, List<String>> e : headers.entrySet()) {
                if (e.getKey().equalsIgnoreCase(name)) {
                    return e.getValue().isEmpty() ? null : e.getValue().get(0);
                }
            }
            return null;
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Assume.assumeTrue("Docker not available; skipping S3Mock-backed checksum test",
                DockerClientFactory.instance().isDockerAvailable());
        // Testcontainers 1.21.x defaults to an old Docker API version; newer Docker Desktop needs >=1.44.
        if (System.getProperty("api.version") == null) {
            System.setProperty("api.version", "1.44");
        }
        s3Mock = new S3MockContainer(S3MOCK_VERSION).withInitialBuckets(BUCKET);
        s3Mock.start();

        proxy = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        URI target = URI.create(s3Mock.getHttpEndpoint());
        proxy.createContext("/", ex -> forward(ex, target));
        proxy.start();

        int writeBufferSize = StorageUtil.getIntSizeInBytes(5, StorageUtil.StorageUnit.MEGABYTE);
        S3ClientConfig config = new S3ClientConfig(REGION, "http://127.0.0.1:" + proxy.getAddress().getPort(), "", true,
                Collections.emptyList(), 0, writeBufferSize, S3ClientConfig.S3ParallelDownloaderClientType.ASYNC,
                false);
        // The public ctor cannot set these; the production path drives them from cloud properties.
        setField(config, "checksumBehavior", S3ChecksumBehavior.WHEN_REQUIRED);
        setField(config, "forcePathStyle", true);

        s3Client = (S3Client) S3CloudClient.buildClient(config).getConsumingClient();
    }

    @AfterClass
    public static void tearDown() {
        if (s3Client != null) {
            s3Client.close();
        }
        if (proxy != null) {
            proxy.stop(0);
        }
        if (s3Mock != null) {
            s3Mock.stop();
        }
    }

    /**
     * The observed OBS regression: in when_required an upload must stay a plain PUT (no aws-chunked,
     * no streaming content-sha256, no trailer, no flexible checksum), and the body must be stored
     * intact on the real store.
     */
    @Test
    public void uploadStaysPlainAndStoresIntact() {
        byte[] data = new byte[UPLOAD_SIZE];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xff);
        }
        s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET).key("obj").build(), RequestBody.fromBytes(data));

        CapturedRequest req = LAST.get();
        assertNotNull("no request captured", req);
        assertEquals("PUT", req.method);
        // Exact wire contract: a plain upload carries none of the integrity/transfer headers.
        assertEquals(
                "upload integrity headers changed - a plain PUT must send none; an aws-chunked/"
                        + "streaming upload breaks OBS and truncates on Adobe S3Mock",
                PUT_GOLDEN, integritySnapshot(req));

        // Real round-trip: the store must hold the full body (aws-chunked would truncate/corrupt it).
        // Read the size back via listing (a HEAD would lose Content-Length through the buffering proxy).
        var listed = s3Client.listObjectsV2(b -> b.bucket(BUCKET).prefix("obj"));
        assertEquals("object should be stored exactly once", 1, listed.keyCount().intValue());
        assertEquals("stored object size mismatch (aws-chunked truncation?)", UPLOAD_SIZE,
                listed.contents().get(0).size().longValue());
    }

    /**
     * The bulk DeleteObjects OBS rejected with "Missing required header: Content-MD5". In
     * when_required it must carry a correct Content-MD5, and the delete must actually succeed on the
     * real store.
     */
    @Test
    public void bulkDeleteCarriesContentMd5AndSucceeds() throws Exception {
        s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET).key("del/a").build(), RequestBody.fromString("a"));
        s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET).key("del/b").build(), RequestBody.fromString("b"));

        DeleteObjectsRequest delete =
                DeleteObjectsRequest
                        .builder().bucket(
                                BUCKET)
                        .delete(Delete.builder().quiet(true).objects(ObjectIdentifier.builder().key("del/a").build(),
                                ObjectIdentifier.builder().key("del/b").build()).build())
                        .build();
        s3Client.deleteObjects(delete);

        CapturedRequest req = LAST.get();
        assertNotNull("no request captured", req);
        assertEquals("POST", req.method);
        assertTrue("expected the ?delete sub-resource", req.query != null && req.query.contains("delete"));

        String md5 = req.header("Content-MD5");
        assertNotNull("bulk DeleteObjects must carry Content-MD5 in when_required mode", md5);
        assertEquals("Content-MD5 does not match the request body", base64Md5(req.body), md5);
        // Exact wire contract (see class javadoc: Content-MD5 required by OBS, crc32 tolerated).
        assertEquals("DeleteObjects integrity headers changed - re-verify against S3-compatible stores", DELETE_GOLDEN,
                integritySnapshot(req));

        int remaining = s3Client.listObjectsV2(b -> b.bucket(BUCKET).prefix("del/")).keyCount();
        assertEquals("objects should have been deleted", 0, remaining);
    }

    // --------------------------- capturing reverse proxy ---------------------------

    /** Captures the incoming request headers/body, forwards to the real S3Mock, relays the response. */
    private static void forward(HttpExchange ex, URI target) throws IOException {
        byte[] body = ex.getRequestBody().readAllBytes();
        // Deep-copy: the exchange's header map is recycled once the exchange closes.
        Map<String, List<String>> headers = new java.util.HashMap<>();
        for (Map.Entry<String, List<String>> e : ex.getRequestHeaders().entrySet()) {
            headers.put(e.getKey(), List.copyOf(e.getValue()));
        }
        LAST.set(new CapturedRequest(ex.getRequestMethod(), ex.getRequestURI().getQuery(), headers, body));

        URI dest = target.resolve(ex.getRequestURI().getRawPath()
                + (ex.getRequestURI().getRawQuery() == null ? "" : "?" + ex.getRequestURI().getRawQuery()));
        HttpRequest.Builder rb = HttpRequest.newBuilder(dest).method(ex.getRequestMethod(),
                body.length == 0 ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body));
        for (Map.Entry<String, List<String>> e : headers.entrySet()) {
            if (UNFORWARDABLE.contains(e.getKey().toLowerCase(Locale.ROOT))) {
                continue;
            }
            for (String v : e.getValue()) {
                rb.header(e.getKey(), v);
            }
        }
        HttpResponse<byte[]> resp;
        try {
            resp = FORWARDER.send(rb.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException(ie);
        }
        resp.headers().map().forEach((k, vs) -> {
            if (!UNFORWARDABLE.contains(k.toLowerCase(Locale.ROOT))) {
                vs.forEach(v -> ex.getResponseHeaders().add(k, v));
            }
        });
        byte[] respBody = resp.body();
        if (respBody.length == 0) {
            ex.sendResponseHeaders(resp.statusCode(), -1);
        } else {
            ex.sendResponseHeaders(resp.statusCode(), respBody.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(respBody);
            }
        }
        ex.close();
    }

    // --------------------------- helpers ---------------------------

    /**
     * Canonical, newline-joined view of the integrity-relevant headers present, in a fixed order.
     * Body-dependent values (Content-MD5, x-amz-checksum-*) are normalized to {@code <present>};
     * deterministic ones keep their literal value; absent headers are omitted.
     */
    private static String integritySnapshot(CapturedRequest req) {
        StringBuilder sb = new StringBuilder();
        for (String h : INTEGRITY_HEADERS) {
            String v = req.header(h);
            if (v == null) {
                continue;
            }
            String lh = h.toLowerCase(Locale.ROOT);
            boolean varies = lh.equals("content-md5") || lh.startsWith("x-amz-checksum-");
            sb.append(lh).append('=').append(varies ? "<present>" : v).append('\n');
        }
        return sb.toString();
    }

    private static String base64Md5(byte[] data) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        return Base64.getEncoder().encodeToString(md.digest(data));
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }
}
