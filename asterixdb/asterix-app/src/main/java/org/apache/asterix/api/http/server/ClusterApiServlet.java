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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.asterix.common.config.AbstractProperties;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.utils.JSONUtil;
import org.apache.asterix.runtime.utils.ClusterStateManager;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ClusterApiServlet extends AbstractServlet {

    private static final Logger LOGGER = Logger.getLogger(ClusterApiServlet.class.getName());
    private static final Pattern PARENT_DIR = Pattern.compile("/[^./]+/\\.\\./");
    private static final Pattern REPLICATION_PROPERTY = Pattern.compile("^replication\\.");
    protected static final String NODE_ID_KEY = "node_id";
    protected static final String CONFIG_URI_KEY = "configUri";
    protected static final String STATS_URI_KEY = "statsUri";
    protected static final String THREAD_DUMP_URI_KEY = "threadDumpUri";
    protected static final String SHUTDOWN_URI_KEY = "shutdownUri";
    protected static final String FULL_SHUTDOWN_URI_KEY = "fullShutdownUri";
    protected static final String VERSION_URI_KEY = "versionUri";
    protected static final String DIAGNOSTICS_URI_KEY = "diagnosticsUri";
    protected static final String REPLICATION_URI_KEY = "replicationUri";
    private final ObjectMapper om = new ObjectMapper();

    public ClusterApiServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    protected void getUnsafe(IServletRequest request, IServletResponse response) throws IOException {
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, HttpUtil.Encoding.UTF8);
        PrintWriter responseWriter = response.writer();
        try {
            ObjectNode json;
            response.setStatus(HttpResponseStatus.OK);
            switch (path(request)) {
                case "":
                    json = getClusterStateJSON(request, "");
                    break;
                case "/replication":
                    json = getReplicationJSON();
                    break;
                case "/summary":
                    json = getClusterStateSummaryJSON();
                    break;
                default:
                    throw new IllegalArgumentException();
            }
            responseWriter.write(JSONUtil.convertNode(json));
        } catch (IllegalArgumentException e) { // NOSONAR - exception not logged or rethrown
            response.setStatus(HttpResponseStatus.NOT_FOUND);
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "exception thrown for " + request, e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            responseWriter.write(e.toString());
        }
        responseWriter.flush();
    }

    protected ObjectNode getClusterStateSummaryJSON() {
        return ClusterStateManager.INSTANCE.getClusterStateSummary();
    }

    protected ObjectNode getReplicationJSON() {
        for (AbstractProperties props : getPropertiesInstances()) {
            if (props instanceof ReplicationProperties) {
                ObjectNode json = om.createObjectNode();
                json.putPOJO("config", props.getProperties(key -> REPLICATION_PROPERTY.matcher(key).replaceFirst("")));
                return json;
            }
        }
        throw new IllegalStateException("ERROR: replication properties not found");
    }

    protected Map<String, Object> getAllClusterProperties() {
        Map<String, Object> allProperties = new HashMap<>();
        for (AbstractProperties properties : getPropertiesInstances()) {
            if (!(properties instanceof ReplicationProperties)) {
                allProperties.putAll(properties.getProperties());
            }
        }
        return allProperties;
    }

    protected List<AbstractProperties> getPropertiesInstances() {
        return AbstractProperties.getImplementations();
    }

    protected ObjectNode getClusterStateJSON(IServletRequest request, String pathToNode) {
        ObjectNode json = ClusterStateManager.INSTANCE.getClusterStateDescription();
        Map<String, Object> allProperties = getAllClusterProperties();
        json.putPOJO("config", allProperties);

        ArrayNode ncs = (ArrayNode) json.get("ncs");
        final StringBuilder requestURL = new StringBuilder("http://");
        requestURL.append(request.getHeader(HttpHeaderNames.HOST));
        requestURL.append(request.getHttpRequest().uri());
        if (requestURL.charAt(requestURL.length() - 1) != '/') {
            requestURL.append('/');
        }
        requestURL.append(pathToNode);
        String clusterURL = canonicalize(requestURL);
        String adminURL = canonicalize(clusterURL + "../");
        String nodeURL = clusterURL + "node/";
        for (int i = 0; i < ncs.size(); i++) {
            ObjectNode nc = (ObjectNode) ncs.get(i);
            nc.put(CONFIG_URI_KEY, nodeURL + nc.get(NODE_ID_KEY).asText() + "/config");
            nc.put(STATS_URI_KEY, nodeURL + nc.get(NODE_ID_KEY).asText() + "/stats");
            nc.put(THREAD_DUMP_URI_KEY, nodeURL + nc.get(NODE_ID_KEY).asText() + "/threaddump");
        }
        ObjectNode cc;
        if (json.has("cc")) {
            cc = (ObjectNode) json.get("cc");
        } else {
            cc = om.createObjectNode();
            json.set("cc", cc);
        }
        cc.put(CONFIG_URI_KEY, clusterURL + "cc/config");
        cc.put(STATS_URI_KEY, clusterURL + "cc/stats");
        cc.put(THREAD_DUMP_URI_KEY, clusterURL + "cc/threaddump");
        json.put(REPLICATION_URI_KEY, clusterURL + "replication");
        json.put(SHUTDOWN_URI_KEY, adminURL + "shutdown");
        json.put(FULL_SHUTDOWN_URI_KEY, adminURL + "shutdown?all=true");
        json.put(VERSION_URI_KEY, adminURL + "version");
        json.put(DIAGNOSTICS_URI_KEY, adminURL + "diagnostics");
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

    @Override
    public void handle(IServletRequest request, IServletResponse response) {
        if (request.getHttpRequest().method() != HttpMethod.GET) {
            response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
            return;
        }
        try {
            getUnsafe(request, response);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Unhandled IOException thrown from " + getClass().getName() + " get impl", e);
        }
    }

}
