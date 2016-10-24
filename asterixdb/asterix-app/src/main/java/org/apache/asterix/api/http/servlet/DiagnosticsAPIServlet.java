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
package org.apache.asterix.api.http.servlet;

import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_CONNECTION_ATTR;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.runtime.util.AsterixAppContextInfo;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.json.JSONObject;

public class DiagnosticsAPIServlet extends ClusterNodeDetailsAPIServlet {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(DiagnosticsAPIServlet.class.getName());

    @Override
    protected void getUnsafe(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("utf-8");
        PrintWriter responseWriter = response.getWriter();
        JSONObject json;
        try {
            if (request.getPathInfo() != null) {
                throw new IllegalArgumentException();
            }
            json = getClusterDiagnosticsJSON();
            response.setStatus(HttpServletResponse.SC_OK);
            responseWriter.write(json.toString(4));
        } catch (IllegalStateException e) { // NOSONAR - exception not logged or rethrown
            response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        } catch (IllegalArgumentException e) { // NOSONAR - exception not logged or rethrown
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "exception thrown for " + request, e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.toString());
        }
        responseWriter.flush();
    }

    private JSONObject getClusterDiagnosticsJSON() throws Exception {
        final ServletContext context = getServletContext();
        IHyracksClientConnection hcc =
                (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
        ExecutorService executor = (ExecutorService) context.getAttribute(ServletConstants.EXECUTOR_SERVICE);

        Map<String, Future<JSONObject>> ccFutureData = new HashMap<>();
        ccFutureData.put("threaddump", executor.submit(() -> fixupKeys(new JSONObject(hcc.getThreadDump(null)))));
        ccFutureData.put("config", executor.submit(() ->
                fixupKeys(new JSONObject(hcc.getNodeDetailsJSON(null, false, true)))));
        ccFutureData.put("stats", executor.submit(() ->
                fixupKeys(new JSONObject(hcc.getNodeDetailsJSON(null, true, false)))));

        Map<String, Map<String, Future<JSONObject>>> ncDataMap = new HashMap<>();
        for (String nc : AsterixAppContextInfo.INSTANCE.getMetadataProperties().getNodeNames()) {
            Map<String, Future<JSONObject>> ncData = new HashMap<>();
            ncData.put("threaddump", executor.submit(() ->
                    fixupKeys(new JSONObject(hcc.getThreadDump(nc)))));
            ncData.put("config", executor.submit(() ->
                    fixupKeys(new JSONObject(hcc.getNodeDetailsJSON(nc, false, true)))));
            ncData.put("stats", executor.submit(() ->
                    fixupKeys(processNodeStats(hcc, nc))));
            ncDataMap.put(nc, ncData);
        }
        JSONObject result = new JSONObject();
        result.put("cc", resolveFutures(ccFutureData));
        List<Map<String, ?>> ncList = new ArrayList<>();
        for (Map.Entry<String, Map<String, Future<JSONObject>>> entry : ncDataMap.entrySet()) {
            final Map<String, Object> ncMap = resolveFutures(entry.getValue());
            ncMap.put("node_id", entry.getKey());
            ncList.add(ncMap);
        }
        result.put("ncs", ncList);
        result.put("date", new Date());
        return result;
    }

    private Map<String, Object> resolveFutures(Map<String, Future<JSONObject>> futureMap)
            throws ExecutionException, InterruptedException {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Future<JSONObject>> entry : futureMap.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }
}
