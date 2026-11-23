/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.databind.node.ArrayNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public abstract class AbstractRequestsServlet extends AbstractServlet {

    public static final String REDACT_PARAM = "redact";
    protected final ICcApplicationContext appCtx;

    public AbstractRequestsServlet(ConcurrentMap<String, Object> ctx, ICcApplicationContext appCtx, String... paths) {
        super(ctx, paths);
        this.appCtx = appCtx;
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        setAccessControlHeaders(request, response);
        ArrayNode requestsJson = JSONUtil.createArray();
        Collection<IClientRequest> requests = getRequests();
        String redact = request.getParameter(REDACT_PARAM);
        if (Boolean.parseBoolean(redact)) {
            for (IClientRequest req : requests) {
                requestsJson.add(req.asRedactedJson());
            }
        } else {
            for (IClientRequest req : requests) {
                requestsJson.add(req.asJson());
            }
        }
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
        response.setStatus(HttpResponseStatus.OK);
        JSONUtil.writeNode(response.writer(), requestsJson);
        response.writer().flush();
    }

    @Override
    protected void options(IServletRequest request, IServletResponse response) throws Exception {
        setAccessControlHeaders(request, response);
        response.setStatus(HttpResponseStatus.OK);
    }

    protected void setAccessControlHeaders(IServletRequest request, IServletResponse response) throws IOException {
        if (request.getHeader("Origin") != null) {
            response.setHeader("Access-Control-Allow-Origin", request.getHeader("Origin"));
            response.setHeader("Access-Control-Allow-Credentials", "true");
            response.setHeader("Vary", "Origin");
        }
        response.setHeader("Access-Control-Allow-Headers",
                "Origin, X-Requested-With, Content-Type, Accept, Authorization");
        response.setHeader("Access-Control-Allow-Methods", "GET, DELETE, OPTIONS");
        response.setHeader("Access-Control-Max-Age", "600");
    }

    abstract Collection<IClientRequest> getRequests();

}
