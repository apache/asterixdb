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

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.app.result.ResultUtil;
import org.apache.asterix.common.config.AbstractAsterixProperties;
import org.apache.asterix.runtime.util.AsterixClusterProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ClusterAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("utf-8");
        PrintWriter responseWriter = response.getWriter();
        JSONObject json;

        try {
            json = getClusterStateJSON(request, "node/");
            response.setStatus(HttpServletResponse.SC_OK);
            responseWriter.write(json.toString(4));
        } catch (IllegalArgumentException e) {
            ResultUtil.apiErrorHandler(responseWriter, e);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        } catch (Exception e) {
            ResultUtil.apiErrorHandler(responseWriter, e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
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
        json = AsterixClusterProperties.INSTANCE.getClusterStateDescription();
        Map<String, Object> allProperties = getAllClusterProperties();
        json.put("config", allProperties);

        JSONArray ncs = json.getJSONArray("ncs");
        final StringBuffer requestURL = request.getRequestURL();
        if (requestURL.charAt(requestURL.length() - 1) != '/') {
            requestURL.append('/');
        }
        requestURL.append(pathToNode);
        String nodeURL = requestURL.toString().replaceAll("/[^./]+/\\.\\./", "/");
        for (int i = 0; i < ncs.length(); i++) {
            JSONObject nc = ncs.getJSONObject(i);
            nc.put("configUri", nodeURL + nc.getString("node_id") + "/config");
            nc.put("statsUri", nodeURL + nc.getString("node_id") + "/stats");
        }
        return json;
    }
}
