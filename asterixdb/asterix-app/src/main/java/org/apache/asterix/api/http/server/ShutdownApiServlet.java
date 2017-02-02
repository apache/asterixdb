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
import java.util.Date;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.runtime.utils.ClusterStateManager;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.util.ServletUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ShutdownApiServlet extends AbstractServlet {
    private static final Logger LOGGER = Logger.getLogger(ShutdownApiServlet.class.getName());
    public static final String NODE_ID_KEY = "node_id";
    public static final String NCSERVICE_PID = "ncservice_pid";
    public static final String INI = "ini";
    public static final String PID = "pid";

    public ShutdownApiServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    public void handle(IServletRequest request, IServletResponse response) {
        if (request.getHttpRequest().method() != HttpMethod.POST) {
            response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
            return;
        }
        IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
        boolean terminateNCServices = "true".equalsIgnoreCase(request.getParameter("all"));
        Thread t = new Thread(() -> {
            try {
                hcc.stopCluster(terminateNCServices);
            } catch (Exception e) {
                GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, "Exception stopping cluster", e);
            }
        }, "Shutdown Servlet Worker");

        try {
            ServletUtils.setContentType(response, IServlet.ContentType.APPLICATION_JSON, IServlet.Encoding.UTF8);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failure handling request", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }
        response.setStatus(HttpResponseStatus.ACCEPTED);
        ObjectMapper om = new ObjectMapper();
        ObjectNode jsonObject = om.createObjectNode();
        try {
            jsonObject.put("status", "SHUTTING_DOWN");
            jsonObject.putPOJO("date", new Date());
            ObjectNode clusterState = ClusterStateManager.INSTANCE.getClusterStateDescription();
            ArrayNode ncs = (ArrayNode) clusterState.get("ncs");
            for (int i = 0; i < ncs.size(); i++) {
                ObjectNode nc = (ObjectNode) ncs.get(i);
                String node = nc.get(NODE_ID_KEY).asText();
                ObjectNode details = (ObjectNode) om.readTree(hcc.getNodeDetailsJSON(node, false, true));
                nc.set(PID, details.get(PID));
                if (details.has(INI) && details.get(INI).has(NCSERVICE_PID)) {
                    nc.put(NCSERVICE_PID, details.get(INI).get(NCSERVICE_PID).asInt());
                }
            }
            jsonObject.set("cluster", clusterState);
            final PrintWriter writer = response.writer();
            writer.print(om.writeValueAsString(jsonObject));
            writer.close();
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.INFO, "Exception writing response", e);
        }
        t.start();
    }

}
