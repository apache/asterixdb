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

import static org.apache.asterix.api.http.server.NodeControllerDetailsHelper.fixupKeys;
import static org.apache.asterix.api.http.server.ServletConstants.HYRACKS_CONNECTION_ATTR;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.JSONUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class DiagnosticsApiServlet extends NodeControllerDetailsApiServlet {
    private static final Logger LOGGER = LogManager.getLogger();
    protected final IHyracksClientConnection hcc;
    protected final ExecutorService executor;

    public DiagnosticsApiServlet(ICcApplicationContext appCtx, ConcurrentMap<String, Object> ctx, String... paths) {
        super(appCtx, ctx, paths);
        this.hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
        this.executor = (ExecutorService) ctx.get(ServletConstants.EXECUTOR_SERVICE_ATTR);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws IOException {
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
        PrintWriter responseWriter = response.writer();
        response.setStatus(HttpResponseStatus.OK);
        try {
            if (!"".equals(localPath(request))) {
                throw new IllegalArgumentException();
            }
            JSONUtil.writeNode(responseWriter, getClusterDiagnosticsJSON());
        } catch (IllegalStateException e) { // NOSONAR - exception not logged or rethrown
            response.setStatus(HttpResponseStatus.SERVICE_UNAVAILABLE);
        } catch (IllegalArgumentException e) { // NOSONAR - exception not logged or rethrown
            response.setStatus(HttpResponseStatus.NOT_FOUND);
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "exception thrown for " + request, e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            responseWriter.write(e.toString());
        }
        responseWriter.flush();
    }

    protected ObjectNode getClusterDiagnosticsJSON() throws Exception {
        Map<String, Future<JsonNode>> ccFutureData;
        ccFutureData = getCcDiagosticsFutures();
        IClusterStateManager csm = appCtx.getClusterStateManager();
        Map<String, Map<String, Future<JsonNode>>> ncDataMap = new HashMap<>();
        for (String nc : csm.getParticipantNodes()) {
            ncDataMap.put(nc, getNcDiagnosticFutures(nc));
        }
        ObjectNode result = OBJECT_MAPPER.createObjectNode();
        if (!ccFutureData.isEmpty()) {
            result.putPOJO("cc", resolveFutures(ccFutureData));
        }
        List<Map<String, ?>> ncList = new ArrayList<>();
        for (Map.Entry<String, Map<String, Future<JsonNode>>> entry : ncDataMap.entrySet()) {
            final Map<String, JsonNode> ncMap = resolveFutures(entry.getValue());
            ncMap.put("node_id", new TextNode(entry.getKey()));
            ncList.add(ncMap);
        }
        result.putPOJO("ncs", ncList);
        result.put("date", String.valueOf(new Date()));
        return result;
    }

    protected Map<String, Future<JsonNode>> getNcDiagnosticFutures(String nc) {
        Map<String, Future<JsonNode>> ncData;
        ncData = new HashMap<>();
        ncData.put("threaddump",
                executor.submit(() -> fixupKeys((ObjectNode) OBJECT_MAPPER.readTree(processThreadDump(nc)))));
        ncData.put("config", executor
                .submit(() -> fixupKeys((ObjectNode) OBJECT_MAPPER.readTree(processNodeDetails(nc, false, true)))));
        ncData.put("stats", executor.submit(() -> fixupKeys(processNodeStats(hcc, nc))));
        return ncData;
    }

    protected Map<String, Future<JsonNode>> getCcDiagosticsFutures() {
        Map<String, Future<JsonNode>> ccFutureData;
        ccFutureData = new HashMap<>();
        ccFutureData.put("threaddump",
                executor.submit(() -> fixupKeys((ObjectNode) OBJECT_MAPPER.readTree(processThreadDump(null)))));
        ccFutureData.put("config", executor
                .submit(() -> fixupKeys((ObjectNode) OBJECT_MAPPER.readTree(processNodeDetails(null, false, true)))));
        ccFutureData.put("stats", executor
                .submit(() -> fixupKeys((ObjectNode) OBJECT_MAPPER.readTree(processNodeDetails(null, true, false)))));
        return ccFutureData;
    }

    protected Map<String, JsonNode> resolveFutures(Map<String, Future<JsonNode>> futureMap)
            throws InterruptedException {
        Map<String, JsonNode> result = new HashMap<>();
        resolveFutures(futureMap, result, result);
        return result;
    }

    public static void resolveFutures(Map<String, Future<JsonNode>> futureMap, Map<String, JsonNode> outputMap,
            Map<String, JsonNode> errorMap) throws InterruptedException {
        for (Map.Entry<String, Future<JsonNode>> entry : futureMap.entrySet()) {
            try {
                outputMap.put(entry.getKey(), entry.getValue().get());
            } catch (ExecutionException e) {
                LOGGER.log(Level.WARN, "unexpected exception obtaining value for " + entry.getKey(), e);
                errorMap.put(entry.getKey(), new TextNode(String.valueOf(e)));
            }
        }
    }

    protected String processNodeDetails(String node, boolean includeStats, boolean includeConfig) throws Exception {
        return checkNullDetail(node, hcc.getNodeDetailsJSON(node, includeStats, includeConfig));
    }

    protected String processThreadDump(String node) throws Exception {
        return checkNullDetail(node, hcc.getThreadDump(node));
    }

}
