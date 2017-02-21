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

import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public abstract class AbstractServlet implements IServlet {
    private static final Logger LOGGER = Logger.getLogger(AbstractServlet.class.getName());

    protected final String[] paths;
    protected final ConcurrentMap<String, Object> ctx;
    private final int[] trims;

    public AbstractServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        this.paths = paths;
        this.ctx = ctx;
        trims = new int[paths.length];
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];
            if (path.endsWith("/*")) {
                trims[i] = path.indexOf("/*");
            } else if (path.endsWith("/")) {
                trims[i] = path.length() - 1;
            } else {
                trims[i] = path.length();
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
                response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Unhandled exception", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @SuppressWarnings("squid:S1172")
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        // designed to be extended but an error in standard case
        response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    @SuppressWarnings("squid:S1172")
    protected void head(IServletRequest request, IServletResponse response) throws Exception {
        // designed to be extended but an error in standard case
        response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    @SuppressWarnings("squid:S1172")
    protected void post(IServletRequest request, IServletResponse response) throws Exception {
        // designed to be extended but an error in standard case
        response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    @SuppressWarnings("squid:S1172")
    protected void put(IServletRequest request, IServletResponse response) throws Exception {
        // designed to be extended but an error in standard case
        response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    @SuppressWarnings("squid:S1172")
    protected void delete(IServletRequest request, IServletResponse response) throws Exception {
        // designed to be extended but an error in standard case
        response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    @SuppressWarnings("squid:S1172")
    protected void options(IServletRequest request, IServletResponse response) throws Exception {
        // designed to be extended but an error in standard case
        response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    public String path(IServletRequest request) {
        int trim = -1;
        if (paths.length > 1) {
            for (int i = 0; i < paths.length; i++) {
                String path = paths[i].indexOf('*') >= 0 ? paths[i].substring(0, paths[i].indexOf('*')) : paths[0];
                if (request.getHttpRequest().uri().indexOf(path) == 0) {
                    trim = trims[i];
                    break;
                }
            }
        } else {
            trim = trims[0];
        }
        return request.getHttpRequest().uri().substring(trim);
    }
}
