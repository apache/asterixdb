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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.app.result.ResultUtil;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_CONNECTION_ATTR;

public class ClusterNodeDetailsAPIServlet extends ClusterAPIServlet {
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        PrintWriter responseWriter = response.getWriter();
        ServletContext context = getServletContext();
        IHyracksClientConnection hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
        JSONObject json;

        try {
            if (request.getPathInfo() == null) {
                json = new JSONObject();
                json.put("ncs", getClusterStateJSON(request, "").getJSONArray("ncs"));
            } else {
                json = processNode(request, hcc);
            }
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json");
            response.setCharacterEncoding("utf-8");
            responseWriter.write(json.toString(4));
        } catch (IllegalArgumentException e) { //NOSONAR - exception not logged or rethrown
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        } catch (Exception e) {
            ResultUtil.apiErrorHandler(responseWriter, e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        responseWriter.flush();
    }

    private JSONObject processNode(HttpServletRequest request, IHyracksClientConnection hcc)
            throws Exception {
        String[] parts = request.getPathInfo().substring(1).replaceAll("/+", "/").split("/");
        final String node = parts[0];

        if (parts.length == 1) {
            JSONArray ncs = getClusterStateJSON(request, "../").getJSONArray("ncs");
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
        if ("cc".equals(node)) {
            return new JSONObject();
        }

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

        int gcNames = json.getJSONArray("gc-names").length();
        for (String key : keys) {
            if (key.startsWith("gc-collection-")) {
                final JSONArray gcArray = json.getJSONArray(key);
                for (int i = 0; i < gcNames; i++) {
                    gcArray.put(i, gcArray.getJSONArray(i).get(index));
                }
            } else if (!"node-id".equals(key) && !"gc-names".equals(key)) {
                json.put(key, json.getJSONArray(key).get(index));
            }
        }
        return json;
    }

    private JSONObject processNodeConfig(IHyracksClientConnection hcc, String node) throws Exception {
        if ("cc".equals(node)) {
            return new JSONObject();
        }
        String config = hcc.getNodeDetailsJSON(node, false, true);
        if (config == null) {
            throw new IllegalArgumentException();
        }
        return new JSONObject(config);
    }
}
