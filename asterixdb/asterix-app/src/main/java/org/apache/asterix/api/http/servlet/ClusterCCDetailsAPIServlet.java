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
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ClusterCCDetailsAPIServlet extends ClusterAPIServlet {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(ClusterCCDetailsAPIServlet.class.getName());
    private final ObjectMapper om = new ObjectMapper();

    @Override
    protected void getUnsafe(HttpServletRequest request, HttpServletResponse response) throws IOException {
        PrintWriter responseWriter = response.getWriter();
        ServletContext context = getServletContext();
        IHyracksClientConnection hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
        try {
            ObjectNode json;
            if (request.getPathInfo() == null) {
                json = (ObjectNode) getClusterStateJSON(request, "../").get("cc");
            } else {
                json = processNode(request, hcc);
            }
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json");
            response.setCharacterEncoding("utf-8");
            responseWriter.write(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(json));
        } catch (IllegalArgumentException e) { // NOSONAR - exception not logged or rethrown
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "exception thrown for " + request, e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.toString());
        }
        responseWriter.flush();
    }

    private ObjectNode processNode(HttpServletRequest request, IHyracksClientConnection hcc) throws Exception {
        String pathInfo = request.getPathInfo();
        if (pathInfo.endsWith("/")) {
            throw new IllegalArgumentException();
        }
        String[] parts = pathInfo.substring(1).split("/");

        if (request.getPathInfo() == null) {
            return (ObjectNode) getClusterStateJSON(request, "../../").get("cc");
        } else if (parts.length == 1) {
            switch (parts[0]) {
                case "config":
                    return om.readValue(hcc.getNodeDetailsJSON(null, false, true), ObjectNode.class);
                case "stats":
                    return om.readValue(hcc.getNodeDetailsJSON(null, true, false), ObjectNode.class);
                case "threaddump":
                    return processCCThreadDump(hcc);

                default:
                    throw new IllegalArgumentException();
            }

        } else {
            throw new IllegalArgumentException();
        }
    }

    private ObjectNode processCCThreadDump(IHyracksClientConnection hcc) throws Exception {
        String dump = hcc.getThreadDump(null);
        if (dump == null) {
            throw new IllegalArgumentException();
        }
        return (ObjectNode) om.readTree(dump);
    }

}
