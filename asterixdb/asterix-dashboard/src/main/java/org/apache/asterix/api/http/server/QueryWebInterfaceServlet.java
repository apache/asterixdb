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
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.StaticResourceServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryWebInterfaceServlet extends StaticResourceServlet {
    private static final Logger LOGGER = LogManager.getLogger();
    private ICcApplicationContext appCtx;

    QueryWebInterfaceServlet(ICcApplicationContext appCtx, ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
        this.appCtx = appCtx;
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws IOException {
        String requestURI = request.getHttpRequest().uri();
        if ("/".equals(requestURI)) {
            HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML);
            // Dashboard Administration Console
            deliverResource("/dashboard/index.html", response);
        } else {
            deliverResource(requestURI, response);
        }
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) throws IOException {
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
        ExternalProperties externalProperties = appCtx.getExternalProperties();
        response.setStatus(HttpResponseStatus.OK);
        ObjectNode obj = OBJECT_MAPPER.createObjectNode();
        try {
            PrintWriter out = response.writer();
            obj.put("api_port", String.valueOf(externalProperties.getAPIServerPort()));
            out.println(obj.toString());
            return;
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Failure writing response", e);
        }
        try {
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Failure setting response status", e);
        }
    }
}
