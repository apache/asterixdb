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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.web.util.IJSONOutputFunction;
import org.apache.hyracks.control.cc.web.util.JSONOutputRequestHandler;
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

    public WebServer(ClusterControllerService ccs, int port) throws Exception {
        this.ccs = ccs;
        listeningPort = port;
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
        server.addServlet(new StartNodeApiServlet(ctx, new String[] { "/rest/startnode" }, ccs));
        server.addServlet(new StaticResourceServlet(ctx, new String[] { "/static/*" }));
        server.addServlet(new ApplicationInstallationHandler(ctx, new String[] { "/applications/*" }, ccs));
    }

    private void addJSONHandler(String path, IJSONOutputFunction fn) {
        server.addServlet(new JSONOutputRequestHandler(ctx, new String[] { path }, fn));
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
