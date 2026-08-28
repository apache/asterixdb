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
package org.apache.asterix.api.http.server;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ConcurrentMap;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Same-origin reverse proxy that forwards the dashboard's MCP panel traffic to
 * the local AsterixDB MCP gateway. The browser talks only to the cluster
 * controller it is already served from ({@code /mcp} on the dashboard web
 * server), so there is no second network-reachable port and no cross-origin
 * (CORS) surface to widen.
 * <p>
 * Security boundary:
 * <ul>
 * <li>The gateway is expected to bind loopback only; it is never exposed to the
 * browser or the network directly.</li>
 * <li>The bearer token is held by the CC and injected here, server-side. The
 * browser never receives or sends the gateway token.</li>
 * <li>The inbound {@code Origin} header is not forwarded, so the gateway's
 * DNS-rebinding guard sees a same-process server call.</li>
 * <li>The feature is off unless {@code asterix.mcp.enabled} is set — a
 * gateway-less cluster behaves exactly as before.</li>
 * </ul>
 * Configuration (JVM system properties on the CC):
 * {@code asterix.mcp.enabled}, {@code asterix.mcp.gateway.url},
 * {@code asterix.mcp.gateway.token}.
 */
public class MCPProxyServlet extends AbstractServlet {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String PROP_ENABLED = "asterix.mcp.enabled";
    private static final String PROP_GATEWAY_URL = "asterix.mcp.gateway.url";
    private static final String PROP_GATEWAY_TOKEN = "asterix.mcp.gateway.token";
    private static final String DEFAULT_GATEWAY_URL = "http://127.0.0.1:19200/mcp";
    private static final Duration UPSTREAM_TIMEOUT = Duration.ofSeconds(60);

    // Headers carried in both directions for the MCP Streamable HTTP transport.
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String ACCEPT = "Accept";
    private static final String SESSION_HEADER = "Mcp-Session-Id";

    private final boolean enabled;
    private final URI gatewayUri;
    private final String token;
    private final HttpClient httpClient;

    public MCPProxyServlet(ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
        this.enabled = Boolean.parseBoolean(System.getProperty(PROP_ENABLED, "false"));
        this.gatewayUri = URI.create(System.getProperty(PROP_GATEWAY_URL, DEFAULT_GATEWAY_URL));
        this.token = System.getProperty(PROP_GATEWAY_TOKEN, "");
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws IOException {
        forward(request, response, "GET");
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) throws IOException {
        forward(request, response, "POST");
    }

    private void forward(IServletRequest request, IServletResponse response, String method) throws IOException {
        if (!enabled) {
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            return;
        }
        try {
            HttpResponse<byte[]> upstream =
                    httpClient.send(buildUpstreamRequest(request, method), HttpResponse.BodyHandlers.ofByteArray());
            writeResponse(upstream, response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            response.setStatus(HttpResponseStatus.GATEWAY_TIMEOUT);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "MCP gateway proxy failure", e);
            response.setStatus(HttpResponseStatus.BAD_GATEWAY);
        }
    }

    private HttpRequest buildUpstreamRequest(IServletRequest request, String method) {
        FullHttpRequest inbound = request.getHttpRequest();
        byte[] body = ByteBufUtil.getBytes(inbound.content());
        HttpRequest.BodyPublisher publisher = "POST".equals(method) ? HttpRequest.BodyPublishers.ofByteArray(body)
                : HttpRequest.BodyPublishers.noBody();

        HttpRequest.Builder builder =
                HttpRequest.newBuilder(gatewayUri).timeout(UPSTREAM_TIMEOUT).method(method, publisher);
        copyHeader(request, builder, CONTENT_TYPE);
        copyHeader(request, builder, ACCEPT);
        copyHeader(request, builder, SESSION_HEADER);
        // Inject the gateway token server-side; never trust a browser-supplied one.
        if (!token.isEmpty()) {
            builder.header("Authorization", "Bearer " + token);
        }
        // Deliberately do NOT forward the inbound Origin header.
        return builder.build();
    }

    private void writeResponse(HttpResponse<byte[]> upstream, IServletResponse response) throws IOException {
        response.setStatus(HttpResponseStatus.valueOf(upstream.statusCode()));
        upstream.headers().firstValue("content-type")
                .ifPresent(value -> setHeaderQuietly(response, CONTENT_TYPE, value));
        upstream.headers().firstValue(SESSION_HEADER.toLowerCase())
                .ifPresent(value -> setHeaderQuietly(response, SESSION_HEADER, value));
        response.outputStream().write(upstream.body());
    }

    private static void copyHeader(IServletRequest request, HttpRequest.Builder builder, String name) {
        String value = request.getHeader(name);
        if (value != null && !value.isEmpty()) {
            builder.header(name, value);
        }
    }

    private static void setHeaderQuietly(IServletResponse response, String name, String value) {
        try {
            response.setHeader(name, value);
        } catch (IOException e) {
            LOGGER.log(Level.WARN, "Failed setting response header {}", name, e);
        }
    }
}
