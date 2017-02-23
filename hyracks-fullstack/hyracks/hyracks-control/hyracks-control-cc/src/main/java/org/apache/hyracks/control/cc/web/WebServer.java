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

import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.DispatcherType;

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.adminconsole.HyracksAdminConsoleApplication;
import org.apache.hyracks.control.cc.web.util.IJSONOutputFunction;
import org.apache.hyracks.control.cc.web.util.JSONOutputRequestHandler;
import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.StaticResourceServlet;
import org.apache.hyracks.http.server.WebManager;
import org.apache.wicket.Application;
import org.apache.wicket.RuntimeConfigurationType;
import org.apache.wicket.protocol.http.ContextParamWebApplicationFactory;
import org.apache.wicket.protocol.http.WicketFilter;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;

public class WebServer {
    private final ClusterControllerService ccs;
    private final int listeningPort;
    private final ConcurrentMap<String, Object> ctx;
    private final WebManager webMgr;
    private final HttpServer server;

    public WebServer(ClusterControllerService ccs, int port) throws Exception {
        this.ccs = ccs;
        listeningPort = port;
        ctx = new ConcurrentHashMap<String, Object>();
        webMgr = new WebManager();
        server = new HttpServer(webMgr.getBosses(), webMgr.getWorkers(), listeningPort);
        addHandlers();
        webMgr.add(server);
    }

    private void addHandlers() {
        addJSONHandler("/rest/jobs/*", new JobsRESTAPIFunction(ccs));
        addJSONHandler("/rest/nodes/*", new NodesRESTAPIFunction(ccs));
        addJSONHandler("/rest/statedump", new StateDumpRESTAPIFunction(ccs));
        // TODO(tillw) addHandler(createAdminConsoleHandler());
        server.addServlet(new StaticResourceServlet(ctx, new String[] { "/static/*" }));
        server.addServlet(new ApplicationInstallationHandler(ctx, new String[] { "/applications/*" }, ccs));
    }

    private void addJSONHandler(String path, IJSONOutputFunction fn) {
        server.addServlet(new JSONOutputRequestHandler(ctx, new String[] { path }, fn));
    }

    private Handler createAdminConsoleHandler() {
        FilterHolder filter = new FilterHolder(WicketFilter.class);
        filter.setInitParameter(ContextParamWebApplicationFactory.APP_CLASS_PARAM,
                HyracksAdminConsoleApplication.class.getName());
        filter.setInitParameter(WicketFilter.FILTER_MAPPING_PARAM, "/*");
        filter.setInitParameter(Application.CONFIGURATION, RuntimeConfigurationType.DEPLOYMENT.toString());

        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        handler.setContextPath("/adminconsole");
        handler.setAttribute(ClusterControllerService.class.getName(), ccs);
        handler.addFilter(filter, "/*", EnumSet.of(DispatcherType.REQUEST, DispatcherType.ERROR));
        handler.addServlet(DefaultServlet.class, "/");
        return handler;
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
