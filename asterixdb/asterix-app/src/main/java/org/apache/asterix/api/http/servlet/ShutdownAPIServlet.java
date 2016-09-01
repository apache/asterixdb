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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.logging.Level;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.json.JSONObject;

import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_CONNECTION_ATTR;

public class ShutdownAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        ServletContext context = getServletContext();
        IHyracksClientConnection hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
        Thread t = new Thread(() -> {
            try {
                hcc.stopCluster();
            } catch (Exception e) {
                GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, "Exception stopping cluster", e);
            }
        }, "Shutdown Servlet Worker");

        response.setContentType("application/json");
        response.setCharacterEncoding("utf-8");
        response.setStatus(HttpServletResponse.SC_ACCEPTED);
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("status", "SHUTTING_DOWN");
            jsonObject.put("date", new Date());
            jsonObject.put("cluster" , AsterixClusterProperties.INSTANCE.getClusterStateDescription());

            final PrintWriter writer = response.getWriter();
            writer.print(jsonObject.toString(4));
            writer.close();
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.INFO, "Exception writing response", e);
        }
        t.start();
    }
}
