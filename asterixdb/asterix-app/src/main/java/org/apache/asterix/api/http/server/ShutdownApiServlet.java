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

import static org.apache.asterix.api.http.server.ServletConstants.HYRACKS_CONNECTION_ATTR;
import static org.apache.asterix.common.api.IClusterManagementWork.ClusterState.SHUTTING_DOWN;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.JSONUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class ShutdownApiServlet extends AbstractServlet {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final String NODE_ID_KEY = "node_id";
    public static final String NCSERVICE_PID = "ncservice_pid";
    public static final String INI = "ini";
    public static final String PID = "pid";
    private final ICcApplicationContext appCtx;

    public ShutdownApiServlet(ICcApplicationContext appCtx, ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
        this.appCtx = appCtx;
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) {
        IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
        IClusterStateManager csm = appCtx.getClusterStateManager();
        boolean terminateNCServices = "true".equalsIgnoreCase(request.getParameter("all"));
        Thread t = new Thread(() -> {
            try {
                hcc.stopCluster(terminateNCServices);
            } catch (Exception e) {
                GlobalConfig.ASTERIX_LOGGER.log(Level.ERROR, "Exception stopping cluster", e);
            }
        }, "Shutdown Servlet Worker");

        try {
            HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
        } catch (IOException e) {
            LOGGER.log(Level.WARN, "Failure handling request", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }
        response.setStatus(HttpResponseStatus.ACCEPTED);
        ObjectNode jsonObject = OBJECT_MAPPER.createObjectNode();
        try {
            jsonObject.put("status", "SHUTTING_DOWN");
            jsonObject.put("date", new Date().toString());
            ObjectNode clusterState = csm.getClusterStateDescription();
            ArrayNode ncs = (ArrayNode) clusterState.get("ncs");
            for (int i = 0; i < ncs.size(); i++) {
                ObjectNode nc = (ObjectNode) ncs.get(i);
                String node = nc.get(NODE_ID_KEY).asText();
                final String detailsString = hcc.getNodeDetailsJSON(node, false, true);
                if (detailsString != null) {
                    ObjectNode details = (ObjectNode) OBJECT_MAPPER.readTree(detailsString);
                    nc.set(PID, details.get(PID));
                    if (details.has(INI) && details.get(INI).has(NCSERVICE_PID)) {
                        nc.put(NCSERVICE_PID, details.get(INI).get(NCSERVICE_PID).asInt());
                    }
                } else {
                    LOGGER.warn("Unable to get node details for " + node + " from hcc");
                }
            }
            jsonObject.set("cluster", clusterState);
            final PrintWriter writer = response.writer();
            JSONUtil.writeNode(writer, jsonObject);
            // accept no further queries once this servlet returns
            csm.setState(SHUTTING_DOWN);
            writer.close();
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.INFO, "Exception writing response", e);
        }
        t.start();
    }

}
