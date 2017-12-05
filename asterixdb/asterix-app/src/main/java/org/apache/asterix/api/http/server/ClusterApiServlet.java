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

import static org.apache.asterix.api.http.server.ServletConstants.ASTERIX_APP_CONTEXT_INFO_ATTR;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.common.config.ConfigUtils;
import org.apache.hyracks.control.common.controllers.ControllerConfig;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ClusterApiServlet extends AbstractServlet {

    private static final Logger LOGGER = Logger.getLogger(ClusterApiServlet.class.getName());
    protected static final String NODE_ID_KEY = "node_id";
    protected static final String CONFIG_URI_KEY = "configUri";
    protected static final String STATS_URI_KEY = "statsUri";
    protected static final String THREAD_DUMP_URI_KEY = "threadDumpUri";
    protected static final String SHUTDOWN_URI_KEY = "shutdownUri";
    protected static final String FULL_SHUTDOWN_URI_KEY = "fullShutdownUri";
    protected static final String VERSION_URI_KEY = "versionUri";
    protected static final String DIAGNOSTICS_URI_KEY = "diagnosticsUri";
    protected final ICcApplicationContext appCtx;

    public ClusterApiServlet(ICcApplicationContext appCtx, ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
        this.appCtx = appCtx;
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws IOException {
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, HttpUtil.Encoding.UTF8);
        PrintWriter responseWriter = response.writer();
        try {
            ObjectNode json;
            response.setStatus(HttpResponseStatus.OK);
            switch (localPath(request)) {
                case "":
                    json = getClusterStateJSON(request, "");
                    break;
                case "/summary":
                    json = getClusterStateSummaryJSON();
                    break;
                default:
                    throw new IllegalArgumentException();
            }
            JSONUtil.writeNode(responseWriter, json);
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
        return appCtx.getClusterStateManager().getClusterStateSummary();
    }

    protected ObjectNode getClusterStateJSON(IServletRequest request, String pathToNode) {
        ObjectNode json = appCtx.getClusterStateManager().getClusterStateDescription();
        ICcApplicationContext appConfig = (ICcApplicationContext) ctx.get(ASTERIX_APP_CONTEXT_INFO_ATTR);
        json.putPOJO("config", ConfigUtils.getSectionOptionsForJSON(appConfig.getServiceContext().getAppConfig(),
                Section.COMMON, getConfigSelector()));

        ArrayNode ncs = (ArrayNode) json.get("ncs");
        String clusterURL = resolveClusterUrl(request, pathToNode);
        String adminURL = HttpUtil.canonicalize(clusterURL + "../");
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
            cc = OBJECT_MAPPER.createObjectNode();
            json.set("cc", cc);
        }
        cc.put(CONFIG_URI_KEY, clusterURL + "cc/config");
        cc.put(STATS_URI_KEY, clusterURL + "cc/stats");
        cc.put(THREAD_DUMP_URI_KEY, clusterURL + "cc/threaddump");
        json.put(SHUTDOWN_URI_KEY, adminURL + "shutdown");
        json.put(FULL_SHUTDOWN_URI_KEY, adminURL + "shutdown?all=true");
        json.put(VERSION_URI_KEY, adminURL + "version");
        json.put(DIAGNOSTICS_URI_KEY, adminURL + "diagnostics");
        return json;
    }

    protected String resolveClusterUrl(IServletRequest request, String pathToNode) {
        final StringBuilder requestURL = new StringBuilder("http://");
        requestURL.append(request.getHeader(HttpHeaderNames.HOST));
        requestURL.append(request.getHttpRequest().uri());
        if (requestURL.charAt(requestURL.length() - 1) != '/') {
            requestURL.append('/');
        }
        requestURL.append(pathToNode);
        return HttpUtil.canonicalize(requestURL);
    }

    protected Predicate<IOption> getConfigSelector() {
        return option -> !option.hidden() && option != ControllerConfig.Option.CONFIG_FILE
                && option != ControllerConfig.Option.CONFIG_FILE_URL;
    }

}
