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

import static com.fasterxml.jackson.databind.MapperFeature.SORT_PROPERTIES_ALPHABETICALLY;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;

import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public abstract class AbstractServlet implements IServlet {
    private static final Logger LOGGER = LogManager.getLogger();
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        OBJECT_MAPPER.configure(SORT_PROPERTIES_ALPHABETICALLY, true);
        OBJECT_MAPPER.configure(ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    protected final String[] paths;
    protected final ConcurrentMap<String, Object> ctx;
    protected final int[] servletPathLengths;

    public AbstractServlet(ConcurrentMap<String, Object> ctx, String... paths) {
        this.paths = paths;
        this.ctx = ctx;
        servletPathLengths = new int[paths.length];
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];
            if (path.endsWith("/*")) {
                servletPathLengths[i] = path.indexOf("/*");
            } else if (path.endsWith("/")) {
                servletPathLengths[i] = path.length() - 1;
            } else {
                servletPathLengths[i] = path.length();
            }
        }
    }

    @Override
    public String[] getPaths() {
        return paths;
    }

    @Override
    public ConcurrentMap<String, Object> ctx() {
        return ctx;
    }

    @Override
    public void handle(IServletRequest request, IServletResponse response) {
        try {
            final HttpMethod method = request.getHttpRequest().method();
            if (HttpMethod.GET.equals(method)) {
                get(request, response);
            } else if (HttpMethod.HEAD.equals(method)) {
                head(request, response);
            } else if (HttpMethod.POST.equals(method)) {
                post(request, response);
            } else if (HttpMethod.PUT.equals(method)) {
                put(request, response);
            } else if (HttpMethod.DELETE.equals(method)) {
                delete(request, response);
            } else if (HttpMethod.OPTIONS.equals(method)) {
                options(request, response);
            } else {
                methodNotAllowed(method, response);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Unhandled exception", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (Throwable th) { //NOSONAR Just logging and then throwing again
            try {
                LOGGER.log(Level.WARN, "Unhandled throwable", th);
            } catch (Throwable loggingFailure) {// NOSONAR... swallow logging failure
            }
            throw th;
        }
    }

    protected void sendError(IServletResponse response, HttpResponseStatus status, String message) throws IOException {
        sendError(response, HttpUtil.ContentType.TEXT_PLAIN, status, message);
    }

    protected void sendError(IServletResponse response, String contentType, HttpResponseStatus status, String message)
            throws IOException {
        response.setStatus(status);
        HttpUtil.setContentType(response, contentType, StandardCharsets.UTF_8);
        if (message != null) {
            response.writer().println(message);
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("sendError: status={}, message={}", status, message);
        }
    }

    protected void sendError(IServletResponse response, HttpResponseStatus status, IFormattedException ex)
            throws IOException {
        sendError(response, status, ex != null ? ex.getMessage() : null);
    }

    protected void sendError(IServletResponse response, HttpResponseStatus status) throws IOException {
        sendError(response, status, status.reasonPhrase());
    }

    protected void methodNotAllowed(HttpMethod method, IServletResponse response) throws IOException {
        sendError(response, HttpResponseStatus.METHOD_NOT_ALLOWED,
                "Method " + method + " not allowed for the requested resource.");
    }

    @SuppressWarnings("squid:S1172")
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        // designed to be extended but an error in standard case
        methodNotAllowed(HttpMethod.GET, response);
    }

    @SuppressWarnings("squid:S1172")
    protected void head(IServletRequest request, IServletResponse response) throws Exception {
        // designed to be extended but an error in standard case
        methodNotAllowed(HttpMethod.HEAD, response);
    }

    @SuppressWarnings("squid:S1172")
    protected void post(IServletRequest request, IServletResponse response) throws Exception {
        // designed to be extended but an error in standard case
        methodNotAllowed(HttpMethod.POST, response);
    }

    @SuppressWarnings("squid:S1172")
    protected void put(IServletRequest request, IServletResponse response) throws Exception {
        // designed to be extended but an error in standard case
        methodNotAllowed(HttpMethod.PUT, response);
    }

    @SuppressWarnings("squid:S1172")
    protected void delete(IServletRequest request, IServletResponse response) throws Exception {
        // designed to be extended but an error in standard case
        methodNotAllowed(HttpMethod.DELETE, response);
    }

    @SuppressWarnings("squid:S1172")
    protected void options(IServletRequest request, IServletResponse response) throws Exception {
        // designed to be extended but an error in standard case
        methodNotAllowed(HttpMethod.OPTIONS, response);
    }

    public String host(IServletRequest request) {
        return request.getHttpRequest().headers().get(HttpHeaderNames.HOST);
    }

    public String localPath(IServletRequest request) {
        final String uri = request.getHttpRequest().uri();
        int queryStart = uri.indexOf('?');
        return queryStart == -1 ? uri.substring(servletLength(uri)) : uri.substring(servletLength(uri), queryStart);
    }

    public String servletPath(IServletRequest request) {
        final String uri = request.getHttpRequest().uri();
        return uri.substring(0, servletLength(uri));
    }

    protected int servletLength(final String uri) {
        int trim = -1;
        if (paths.length > 1) {
            for (int i = 0; i < paths.length; i++) {
                int wildCardIdx = paths[i].indexOf("/*");
                String path = wildCardIdx >= 0 ? paths[i].substring(0, wildCardIdx) : paths[i];
                if (uri.indexOf(path) == 0) {
                    trim = servletPathLengths[i];
                    break;
                }
            }
        } else {
            trim = servletPathLengths[0];
        }
        return trim;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + Arrays.toString(paths);
    }

    @Override
    public boolean ignoresQueryParameters(HttpMethod method) {
        return false;
    }
}
