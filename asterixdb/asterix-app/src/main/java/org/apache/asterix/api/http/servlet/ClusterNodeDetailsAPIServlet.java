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
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.asterix.runtime.util.ClusterStateManager;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import com.fasterxml.jackson.databind.node.ArrayNode;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ClusterNodeDetailsAPIServlet extends ClusterAPIServlet {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(ClusterNodeDetailsAPIServlet.class.getName());
    private final ObjectMapper om = new ObjectMapper();

    @Override
    public void init() throws ServletException{
        om.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @Override
    protected void getUnsafe(HttpServletRequest request, HttpServletResponse response) throws IOException {
        PrintWriter responseWriter = response.getWriter();
        ServletContext context = getServletContext();
        IHyracksClientConnection hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
        try {
            ObjectNode json;
            if (request.getPathInfo() == null) {
                json = om.createObjectNode();
                json.set("ncs", getClusterStateJSON(request, "../").get("ncs"));
            } else {
                json = processNode(request, hcc);
            }
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json");
            response.setCharacterEncoding("utf-8");
            responseWriter.write(om.writerWithDefaultPrettyPrinter().writeValueAsString(json));
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

    private ObjectNode processNode(HttpServletRequest request, IHyracksClientConnection hcc)
            throws Exception {
        String pathInfo = request.getPathInfo();
        if (pathInfo.endsWith("/")) {
            throw new IllegalArgumentException();
        }
        String[] parts = pathInfo.substring(1).split("/");
        final String node = parts[0];

        if (parts.length == 1) {
            ArrayNode ncs = (ArrayNode) getClusterStateJSON(request, "../../").get("ncs");
            for (int i = 0; i < ncs.size(); i++) {
                if (node.equals(ncs.get(i).get("node_id").asText())) {
                    return (ObjectNode) ncs.get(i);
                }
            }
            if ("cc".equals(node)) {
                return om.createObjectNode();
            }
            throw new IllegalArgumentException();
        } else if (parts.length == 2) {
            ObjectNode json;

            switch (parts[1]) {
                case "config":
                    json = processNodeConfig(hcc, node);
                    break;

                case "stats":
                    json = processNodeStats(hcc, node);
                    break;

                case "threaddump":
                    return processNodeThreadDump(hcc, node);

                default:
                    throw new IllegalArgumentException();
            }
            fixupKeys(json);

            return json;
        } else {
            throw new IllegalArgumentException();
        }
    }

    protected ObjectNode fixupKeys(ObjectNode json)  {
        // TODO (mblow): generate the keys with _ to begin with
        List<String> keys = new ArrayList<>();
        for (Iterator iter = json.fieldNames(); iter.hasNext(); ) {
            keys.add((String) iter.next());
        }
        for (String key : keys) {
            String newKey = key.replace('-', '_');
            if (!newKey.equals(key)) {
                json.set(newKey, json.remove(key));
            }
        }
        return json;
    }

    protected ObjectNode processNodeStats(IHyracksClientConnection hcc, String node) throws Exception {
        final String details = hcc.getNodeDetailsJSON(node, true, false);
        if (details == null) {
            throw new IllegalArgumentException();
        }
        ObjectNode json = (ObjectNode) om.readTree(details);
        int index = json.get("rrd-ptr").asInt() - 1;
        json.remove("rrd-ptr");

        List<String> keys = new ArrayList<>();
        for (Iterator iter = json.fieldNames(); iter.hasNext(); ) {
            keys.add((String) iter.next());
        }

        final ArrayNode gcNames = (ArrayNode) json.get("gc-names");
        final ArrayNode gcCollectionTimes  = (ArrayNode) json.get("gc-collection-times");
        final ArrayNode gcCollectionCounts = (ArrayNode) json.get("gc-collection-counts");

        for (String key : keys) {
            if (key.startsWith("gc-")) {
                json.remove(key);
            } else if (json.get(key) instanceof ArrayNode) {
                final ArrayNode valueArray = (ArrayNode) json.get(key);
                // fixup an index of -1 to the final element in the array (i.e. RRD_SIZE)
                if (index == -1) {
                    index = valueArray.size() - 1;
                }
                final Object value = valueArray.get(index);
                json.remove(key);
                json.putPOJO(key.replaceAll("s$",""), value);
            }
        }
        List<ObjectNode> gcs = new ArrayList<>();

        for (int i = 0; i < gcNames.size(); i++) {
            ObjectNode gc = om.createObjectNode();
            gc.set("name", gcNames.get(i));
            gc.set("collection-time", ((ArrayNode)gcCollectionTimes.get(i)).get(index));
            gc.set("collection-count", ((ArrayNode)gcCollectionCounts.get(i)).get(index));
            gcs.add(gc);
        }
        json.putPOJO("gcs", gcs);

        return json;
    }

    private ObjectNode processNodeConfig(IHyracksClientConnection hcc, String node) throws Exception {
        String config = hcc.getNodeDetailsJSON(node, false, true);
        if (config == null) {
            throw new IllegalArgumentException();
        }
        return (ObjectNode) om.readTree(config);
    }

    private ObjectNode processNodeThreadDump(IHyracksClientConnection hcc, String node) throws Exception {
        if ("cc".equals(node)) {
            return om.createObjectNode();
        }
        String dump = hcc.getThreadDump(node);
        if (dump == null) {
            // check to see if this is a node that is simply down
            throw ClusterStateManager.INSTANCE.getNodePartitions(node) != null
                    ? new IllegalStateException()
                    : new IllegalArgumentException();
        }
        return (ObjectNode) om.readTree(dump);
    }

}
