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
package org.apache.hyracks.control.cc.web;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.web.util.IJSONOutputFunction;
import org.apache.hyracks.control.cc.web.util.JSONOutputRequestHandler;
import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.server.BasicAuthServlet;
import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.HttpServerConfigBuilder;
import org.apache.hyracks.http.server.StaticResourceServlet;
import org.apache.hyracks.http.server.WebManager;

public class WebServer {
    private final ClusterControllerService ccs;
    private final int listeningPort;
    private final ConcurrentMap<String, Object> ctx;
    private final WebManager webMgr;
    private final HttpServer server;
    private final Map<String, String> credentials;
    private final Map<String, String> systemCredentials;
    private final String systemAuthHeader;

    /**
     * @param credentials
     *            the credentials the endpoints require, keyed by user name, each a bcrypt hash of the user's password;
     *            or {@code null} to leave them unauthenticated
     */
    public WebServer(ClusterControllerService ccs, int port, Map<String, String> credentials) throws Exception {
        this.ccs = ccs;
        listeningPort = port;
        this.credentials = credentials;
        if (credentials != null) {
            Pair<String, Map<String, String>> systemCredential = BasicAuthServlet.generateSystemCredential(credentials);
            systemAuthHeader = systemCredential.getLeft();
            systemCredentials = systemCredential.getRight();
        } else {
            systemAuthHeader = null;
            systemCredentials = null;
        }
        ctx = new ConcurrentHashMap<>();
        webMgr = new WebManager();
        server = new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), listeningPort,
                HttpServerConfigBuilder.createDefault());
        addHandlers();
        webMgr.add(server);
    }

    private void addHandlers() {
        addJSONHandler("/rest/jobs/*", new JobsRESTAPIFunction(ccs));
        addJSONHandler("/rest/nodes/*", new NodesRESTAPIFunction(ccs));
        addJSONHandler("/rest/statedump", new StateDumpRESTAPIFunction(ccs));
        addAuthenticatedServlet(new StartNodeApiServlet(ctx, new String[] { "/rest/startnode" }, ccs));
        server.addServlet(new StaticResourceServlet(ctx, new String[] { "/static/*" }));
        addAuthenticatedServlet(new ApplicationInstallationHandler(ctx, new String[] { "/applications/*" }, ccs));
    }

    private void addJSONHandler(String path, IJSONOutputFunction fn) {
        addAuthenticatedServlet(new JSONOutputRequestHandler(ctx, new String[] { path }, fn));
    }

    private void addAuthenticatedServlet(IServlet servlet) {
        server.addServlet(
                credentials == null ? servlet : new BasicAuthServlet(servlet, credentials, systemCredentials));
    }

    /**
     * @return the {@code Authorization} header with which the node controllers fetch deployed binaries from this
     *         server, or {@code null} if it is unauthenticated
     */
    public String getSystemAuthHeader() {
        return systemAuthHeader;
    }

    public int getListeningPort() {
        return listeningPort;
    }

    public void start() throws Exception {
        webMgr.start();
    }

    public void stop() throws Exception {
        webMgr.stop();
    }
}
