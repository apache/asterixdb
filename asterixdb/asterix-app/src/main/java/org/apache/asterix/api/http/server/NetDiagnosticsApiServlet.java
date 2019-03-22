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

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.application.NCServiceContext;
import org.apache.hyracks.control.nc.net.NetworkManager;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.databind.JsonNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class NetDiagnosticsApiServlet extends AbstractServlet {

    private final INcApplicationContext appCtx;

    public NetDiagnosticsApiServlet(ConcurrentMap<String, Object> ctx, INcApplicationContext appCtx, String... paths) {
        super(ctx, paths);
        this.appCtx = appCtx;
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws IOException {
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
        response.setStatus(HttpResponseStatus.OK);
        final JsonNode netDiagnostics = getNetDiagnostics();
        final PrintWriter responseWriter = response.writer();
        JSONUtil.writeNode(responseWriter, netDiagnostics);
    }

    private JsonNode getNetDiagnostics() {
        final NCServiceContext serviceContext = (NCServiceContext) appCtx.getServiceContext();
        final NodeControllerService controllerService = (NodeControllerService) serviceContext.getControllerService();
        final NetworkManager networkManager = controllerService.getNetworkManager();
        return networkManager.getMuxDemux().getState();
    }
}
