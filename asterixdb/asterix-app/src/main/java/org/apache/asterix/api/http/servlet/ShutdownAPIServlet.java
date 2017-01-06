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
import java.util.Date;
import java.util.logging.Level;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.runtime.util.ClusterStateManager;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ShutdownAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    public static final String NODE_ID_KEY = "node_id";
    public static final String NCSERVICE_PID = "ncservice_pid";
    public static final String INI = "ini";
    public static final String PID = "pid";

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        ServletContext context = getServletContext();
        IHyracksClientConnection hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
        boolean terminateNCServices = "true".equalsIgnoreCase(request.getParameter("all"));
        Thread t = new Thread(() -> {
            try {
                hcc.stopCluster(terminateNCServices);
            } catch (Exception e) {
                GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, "Exception stopping cluster", e);
            }
        }, "Shutdown Servlet Worker");

        response.setContentType("application/json");
        response.setCharacterEncoding("utf-8");
        response.setStatus(HttpServletResponse.SC_ACCEPTED);
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
            final PrintWriter writer = response.getWriter();
            writer.print(om.writeValueAsString(jsonObject));
            writer.close();
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.INFO, "Exception writing response", e);
        }
        t.start();
    }
}
