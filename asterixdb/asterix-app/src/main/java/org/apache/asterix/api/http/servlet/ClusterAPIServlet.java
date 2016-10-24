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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.common.config.AbstractAsterixProperties;
import org.apache.asterix.runtime.util.ClusterStateManager;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ClusterAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(ClusterAPIServlet.class.getName());

    public static final String NODE_ID_KEY = "node_id";
    public static final String CONFIG_URI_KEY = "configUri";
    public static final String STATS_URI_KEY = "statsUri";
    public static final String THREAD_DUMP_URI_KEY = "threadDumpUri";
    public static final String SHUTDOWN_URI_KEY = "shutdownUri";
    public static final String FULL_SHUTDOWN_URI_KEY = "fullShutdownUri";
    public static final String VERSION_URI_KEY = "versionUri";
    public static final String DIAGNOSTICS_URI_KEY = "diagnosticsUri";
    public static final Pattern PARENT_DIR = Pattern.compile("/[^./]+/\\.\\./");

    @Override
    public final void doGet(HttpServletRequest request, HttpServletResponse response) {
        try {
            getUnsafe(request, response);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Unhandled IOException thrown from " + getClass().getName() + " get impl", e);
        }
    }

    protected void getUnsafe(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("utf-8");
        PrintWriter responseWriter = response.getWriter();
        JSONObject json;

        try {
            json = getClusterStateJSON(request, "");
            response.setStatus(HttpServletResponse.SC_OK);
            responseWriter.write(json.toString(4));
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "exception thrown for " + request, e);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.toString());
        }
        responseWriter.flush();
    }

    protected Map<String, Object> getAllClusterProperties() {
        Map<String, Object> allProperties = new HashMap<>();
        for (AbstractAsterixProperties properties : getPropertiesInstances()) {
            allProperties.putAll(properties.getProperties());
        }
        return allProperties;
    }

    protected List<AbstractAsterixProperties> getPropertiesInstances() {
        return AbstractAsterixProperties.getImplementations();
    }

    protected JSONObject getClusterStateJSON(HttpServletRequest request, String pathToNode)
            throws JSONException {
        JSONObject json;
        json = ClusterStateManager.INSTANCE.getClusterStateDescription();
        Map<String, Object> allProperties = getAllClusterProperties();
        json.put("config", allProperties);

        JSONArray ncs = json.getJSONArray("ncs");
        final StringBuilder requestURL = new StringBuilder(request.getRequestURL());
        if (requestURL.charAt(requestURL.length() - 1) != '/') {
            requestURL.append('/');
        }
        requestURL.append(pathToNode);
        String clusterURL = canonicalize(requestURL);
        String analyticsURL = canonicalize(clusterURL + "../");
        String nodeURL = clusterURL + "node/";
        for (int i = 0; i < ncs.length(); i++) {
            JSONObject nc = ncs.getJSONObject(i);
            nc.put(CONFIG_URI_KEY, nodeURL + nc.getString(NODE_ID_KEY) + "/config");
            nc.put(STATS_URI_KEY, nodeURL + nc.getString(NODE_ID_KEY) + "/stats");
            nc.put(THREAD_DUMP_URI_KEY, nodeURL + nc.getString(NODE_ID_KEY) + "/threaddump");
        }
        JSONObject cc;
        if (json.has("cc")) {
            cc = json.getJSONObject("cc");
        } else {
            cc = new JSONObject();
            json.put("cc", cc);
        }
        cc.put(CONFIG_URI_KEY, clusterURL + "cc/config");
        cc.put(STATS_URI_KEY, clusterURL + "cc/stats");
        cc.put(THREAD_DUMP_URI_KEY, clusterURL + "cc/threaddump");
        json.put(SHUTDOWN_URI_KEY, analyticsURL + "shutdown");
        json.put(FULL_SHUTDOWN_URI_KEY, analyticsURL + "shutdown?all=true");
        json.put(VERSION_URI_KEY, analyticsURL + "version");
        json.put(DIAGNOSTICS_URI_KEY, analyticsURL + "diagnostics");
        return json;
    }

    private String canonicalize(CharSequence requestURL) {
        String clusterURL = "";
        String newClusterURL = requestURL.toString();
        while (!clusterURL.equals(newClusterURL)) {
            clusterURL = newClusterURL;
            newClusterURL = PARENT_DIR.matcher(clusterURL).replaceAll("/");
        }
        return clusterURL;
    }
}
