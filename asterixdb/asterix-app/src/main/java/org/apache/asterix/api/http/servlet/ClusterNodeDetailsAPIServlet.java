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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.runtime.util.ClusterStateManager;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ClusterNodeDetailsAPIServlet extends ClusterAPIServlet {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(ClusterNodeDetailsAPIServlet.class.getName());

    @Override
    protected void getUnsafe(HttpServletRequest request, HttpServletResponse response) throws IOException {
        PrintWriter responseWriter = response.getWriter();
        ServletContext context = getServletContext();
        IHyracksClientConnection hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
        JSONObject json;

        try {
            if (request.getPathInfo() == null) {
                json = new JSONObject();
                json.put("ncs", getClusterStateJSON(request, "../").getJSONArray("ncs"));
            } else {
                json = processNode(request, hcc);
            }
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json");
            response.setCharacterEncoding("utf-8");
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

    private JSONObject processNode(HttpServletRequest request, IHyracksClientConnection hcc)
            throws Exception {
        String pathInfo = request.getPathInfo();
        if (pathInfo.endsWith("/")) {
            throw new IllegalArgumentException();
        }
        String[] parts = pathInfo.substring(1).split("/");
        final String node = parts[0];

        if (parts.length == 1) {
            JSONArray ncs = getClusterStateJSON(request, "../../").getJSONArray("ncs");
            for (int i = 0; i < ncs.length(); i++) {
                JSONObject json = ncs.getJSONObject(i);
                if (node.equals(json.getString("node_id"))) {
                    return json;
                }
            }
            if ("cc".equals(node)) {
                return new JSONObject();
            }
            throw new IllegalArgumentException();
        } else if (parts.length == 2) {
            JSONObject json;

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

    private void fixupKeys(JSONObject json) throws JSONException {
        // TODO (mblow): generate the keys with _ to begin with
        List<String> keys = new ArrayList<>();
        for (Iterator iter = json.keys(); iter.hasNext(); ) {
            keys.add((String) iter.next());
        }
        for (String key : keys) {
            String newKey = key.replace('-', '_');
            if (!newKey.equals(key)) {
                json.put(newKey, json.remove(key));
            }
        }
    }

    private JSONObject processNodeStats(IHyracksClientConnection hcc, String node) throws Exception {
        final String details = hcc.getNodeDetailsJSON(node, true, false);
        if (details == null) {
            throw new IllegalArgumentException();
        }
        JSONObject json = new JSONObject(details);
        int index = json.getInt("rrd-ptr") - 1;
        json.remove("rrd-ptr");

        List<String> keys = new ArrayList<>();
        for (Iterator iter = json.keys(); iter.hasNext(); ) {
            keys.add((String) iter.next());
        }

        final JSONArray gcNames = json.getJSONArray("gc-names");
        final JSONArray gcCollectionTimes = json.getJSONArray("gc-collection-times");
        final JSONArray gcCollectionCounts = json.getJSONArray("gc-collection-counts");

        for (String key : keys) {
            if (key.startsWith("gc-")) {
                json.remove(key);
            } else if (json.get(key) instanceof JSONArray) {
                final JSONArray valueArray = json.getJSONArray(key);
                // fixup an index of -1 to the final element in the array (i.e. RRD_SIZE)
                if (index == -1) {
                    index = valueArray.length() - 1;
                }
                final Object value = valueArray.get(index);
                json.remove(key);
                json.put(key.replaceAll("s$",""), value);
            }
        }
        List<JSONObject> gcs = new ArrayList<>();

        for (int i = 0; i < gcNames.length(); i++) {
            JSONObject gc = new JSONObject();
            gc.put("name", gcNames.get(i));
            gc.put("collection-time", ((JSONArray)gcCollectionTimes.get(i)).get(index));
            gc.put("collection-count", ((JSONArray)gcCollectionCounts.get(i)).get(index));
            gcs.add(gc);
        }
        json.put("gcs", gcs);

        return json;
    }

    private JSONObject processNodeConfig(IHyracksClientConnection hcc, String node) throws Exception {
        String config = hcc.getNodeDetailsJSON(node, false, true);
        if (config == null) {
            throw new IllegalArgumentException();
        }
        return new JSONObject(config);
    }

    private JSONObject processNodeThreadDump(IHyracksClientConnection hcc, String node) throws Exception {
        if ("cc".equals(node)) {
            return new JSONObject();
        }
        String dump = hcc.getThreadDump(node);
        if (dump == null) {
            // check to see if this is a node that is simply down
            throw ClusterStateManager.INSTANCE.getNodePartitions(node) != null
                    ? new IllegalStateException()
                    : new IllegalArgumentException();
        }
        return new JSONObject(dump);
    }

}
