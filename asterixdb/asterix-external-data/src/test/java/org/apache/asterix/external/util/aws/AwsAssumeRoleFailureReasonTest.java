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
package org.apache.asterix.external.util.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.external.AwsAssumeRoleFailureReason;
import org.apache.asterix.common.external.IExternalStatsTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.net.httpserver.HttpServer;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;

/**
 * Drives the real AssumeRole credentials provider against a local server answering with STS errors, so the failure
 * reason is classified from exactly what the SDK hands the interceptor.
 */
public class AwsAssumeRoleFailureReasonTest {

    private static final String STS_ENDPOINT_PROPERTY = "aws.endpointUrlSts";
    private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/reader";

    private HttpServer server;
    private volatile int status;
    private volatile String code;

    @Before
    public void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            byte[] body =
                    ("<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><Error><Type>Sender</Type>"
                            + "<Code>" + code + "</Code><Message>refused " + ROLE_ARN + "</Message></Error>"
                            + "<RequestId>r</RequestId></ErrorResponse>").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/xml");
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
        System.setProperty(STS_ENDPOINT_PROPERTY, "http://127.0.0.1:" + server.getAddress().getPort());
    }

    @After
    public void tearDown() {
        System.clearProperty(STS_ENDPOINT_PROPERTY);
        server.stop(0);
    }

    @Test
    public void testStsErrorsAreClassified() throws Exception {
        assertStsError(403, "AccessDenied", AwsAssumeRoleFailureReason.ACCESS_DENIED);
        assertStsError(403, "InvalidClientTokenId", AwsAssumeRoleFailureReason.INVALID_CREDENTIALS);
        assertStsError(403, "UnrecognizedClientException", AwsAssumeRoleFailureReason.INVALID_CREDENTIALS);
        assertStsError(403, "SignatureDoesNotMatch", AwsAssumeRoleFailureReason.INVALID_CREDENTIALS);
        assertStsError(400, "ExpiredToken", AwsAssumeRoleFailureReason.EXPIRED);
        assertStsError(403, "ExpiredTokenException", AwsAssumeRoleFailureReason.EXPIRED);
        assertStsError(400, "ValidationError", AwsAssumeRoleFailureReason.INVALID_REQUEST);
        assertStsError(400, "MalformedPolicyDocument", AwsAssumeRoleFailureReason.INVALID_REQUEST);
        assertStsError(403, "RegionDisabledException", AwsAssumeRoleFailureReason.REGION_DISABLED);
        assertStsError(400, "Throttling", AwsAssumeRoleFailureReason.THROTTLED);
        assertStsError(429, "SomethingNew", AwsAssumeRoleFailureReason.THROTTLED);
        assertStsError(503, "ServiceUnavailable", AwsAssumeRoleFailureReason.UNAVAILABLE);
        assertStsError(500, "SomethingNew", AwsAssumeRoleFailureReason.UNAVAILABLE);
        assertStsError(400, "SomethingNew", AwsAssumeRoleFailureReason.OTHER);
        assertStsError(403, "OptInRequired", AwsAssumeRoleFailureReason.OTHER);
    }

    @Test
    public void testUnreachableStsIsAClientError() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            closedPort = socket.getLocalPort();
        }
        System.setProperty(STS_ENDPOINT_PROPERTY, "http://127.0.0.1:" + closedPort);
        assertEquals(Collections.singletonList(AwsAssumeRoleFailureReason.CLIENT_ERROR), assumeRole());
    }

    @Test
    public void testFailureIsFoundAnywhereInTheCauseChain() {
        SdkClientException clientError = SdkClientException.create("unreachable");
        assertEquals(AwsAssumeRoleFailureReason.CLIENT_ERROR,
                AwsUtils.getAssumeRoleFailureReason(new CompletionException(clientError)));
        assertEquals(AwsAssumeRoleFailureReason.OTHER,
                AwsUtils.getAssumeRoleFailureReason(new IllegalStateException("not from the SDK")));
        assertEquals(AwsAssumeRoleFailureReason.OTHER, AwsUtils.getAssumeRoleFailureReason(null));
    }

    private void assertStsError(int status, String code, AwsAssumeRoleFailureReason expected) throws Exception {
        this.status = status;
        this.code = code;
        // the interceptor runs once per call, after the SDK's own retries
        assertEquals(status + " " + code, Collections.singletonList(expected), assumeRole());
    }

    private List<AwsAssumeRoleFailureReason> assumeRole() throws Exception {
        List<String> names = Collections.synchronizedList(new ArrayList<>());
        List<AwsAssumeRoleFailureReason> reasons = Collections.synchronizedList(new ArrayList<>());
        IExternalStatsTracker tracker = new IExternalStatsTracker() {
            @Override
            public String resolveName(Map<String, String> configuration) {
                return "s3link";
            }

            @Override
            public void incrementAwsAssumeRoleFailure(String name, AwsAssumeRoleFailureReason reason) {
                names.add(name);
                reasons.add(reason);
            }
        };
        ExternalProperties properties = mock(ExternalProperties.class);
        when(properties.getAwsAssumeRoleDuration()).thenReturn(900);
        IApplicationContext appCtx = mock(IApplicationContext.class);
        when(appCtx.getExternalStatsTracker()).thenReturn(tracker);
        when(appCtx.getExternalProperties()).thenReturn(properties);

        Map<String, String> configuration = new HashMap<>();
        configuration.put(AwsConstants.ACCESS_KEY_ID_FIELD_NAME, "AKIAEXAMPLE");
        configuration.put(AwsConstants.SECRET_ACCESS_KEY_FIELD_NAME, "secret");
        configuration.put(AwsConstants.ROLE_ARN_FIELD_NAME, ROLE_ARN);
        configuration.put(AwsConstants.REGION_FIELD_NAME, "us-east-1");
        AwsUtils.CloseableAwsClients clients = new AwsUtils.CloseableAwsClients();
        try {
            AwsCredentialsProvider provider = AwsUtils.getTrustAccountCredentials(appCtx, configuration, clients);
            assertThrows(RuntimeException.class, provider::resolveCredentials);
        } finally {
            AwsUtils.closeClients(clients);
        }
        for (String name : names) {
            assertEquals("s3link", name);
        }
        return new ArrayList<>(reasons);
    }
}
