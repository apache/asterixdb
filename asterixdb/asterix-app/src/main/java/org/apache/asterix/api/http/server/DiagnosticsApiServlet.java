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

import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_CONNECTION_ATTR;

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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.api.http.servlet.ServletConstants;
import org.apache.asterix.runtime.utils.AppContextInfo;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.util.ServletUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class DiagnosticsApiServlet extends NodeControllerDetailsApiServlet {
    private static final Logger LOGGER = Logger.getLogger(DiagnosticsApiServlet.class.getName());

    public DiagnosticsApiServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    protected void getUnsafe(IServletRequest request, IServletResponse response) throws IOException {
        ServletUtils.setContentType(response, IServlet.ContentType.APPLICATION_JSON, IServlet.Encoding.UTF8);
        PrintWriter responseWriter = response.writer();
        ObjectNode json;
        ObjectMapper om = new ObjectMapper();
        response.setStatus(HttpResponseStatus.OK);
        om.enable(SerializationFeature.INDENT_OUTPUT);
        try {
            if (!"".equals(path(request))) {
                throw new IllegalArgumentException();
            }
            json = getClusterDiagnosticsJSON();
            responseWriter.write(om.writerWithDefaultPrettyPrinter().writeValueAsString(json));
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

    private ObjectNode getClusterDiagnosticsJSON() throws Exception {
        ObjectMapper om = new ObjectMapper();
        IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
        ExecutorService executor = (ExecutorService) ctx.get(ServletConstants.EXECUTOR_SERVICE);
        Map<String, Future<ObjectNode>> ccFutureData = new HashMap<>();
        ccFutureData.put("threaddump",
                executor.submit(() -> fixupKeys((ObjectNode) om.readTree(hcc.getThreadDump(null)))));
        ccFutureData.put("config",
                executor.submit(() -> fixupKeys((ObjectNode) om.readTree(hcc.getNodeDetailsJSON(null, false, true)))));
        ccFutureData.put("stats",
                executor.submit(() -> fixupKeys((ObjectNode) om.readTree(hcc.getNodeDetailsJSON(null, true, false)))));

        Map<String, Map<String, Future<ObjectNode>>> ncDataMap = new HashMap<>();
        for (String nc : AppContextInfo.INSTANCE.getMetadataProperties().getNodeNames()) {
            Map<String, Future<ObjectNode>> ncData = new HashMap<>();
            ncData.put("threaddump",
                    executor.submit(() -> fixupKeys((ObjectNode) om.readTree(hcc.getThreadDump(nc)))));
            ncData.put("config", executor
                    .submit(() -> fixupKeys((ObjectNode) om.readTree(hcc.getNodeDetailsJSON(nc, false, true)))));
            ncData.put("stats", executor.submit(() -> fixupKeys(processNodeStats(hcc, nc))));
            ncDataMap.put(nc, ncData);
        }
        ObjectNode result = om.createObjectNode();
        result.putPOJO("cc", resolveFutures(ccFutureData));
        List<Map<String, ?>> ncList = new ArrayList<>();
        for (Map.Entry<String, Map<String, Future<ObjectNode>>> entry : ncDataMap.entrySet()) {
            final Map<String, Object> ncMap = resolveFutures(entry.getValue());
            ncMap.put("node_id", entry.getKey());
            ncList.add(ncMap);
        }
        result.putPOJO("ncs", ncList);
        result.putPOJO("date", new Date());
        return result;
    }

    private Map<String, Object> resolveFutures(Map<String, Future<ObjectNode>> futureMap)
            throws ExecutionException, InterruptedException {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Future<ObjectNode>> entry : futureMap.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }
}
