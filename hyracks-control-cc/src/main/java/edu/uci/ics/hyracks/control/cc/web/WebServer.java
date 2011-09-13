/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.cc.web;

import java.io.File;
import java.util.logging.Logger;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.webapp.WebAppContext;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.web.util.JSONOutputRequestHandler;
import edu.uci.ics.hyracks.control.cc.web.util.RoutingHandler;

public class WebServer {
    private final static Logger LOGGER = Logger.getLogger(WebServer.class.getName());

    private final ClusterControllerService ccs;
    private final Server server;
    private final SelectChannelConnector connector;
    private final HandlerCollection handlerCollection;

    public WebServer(ClusterControllerService ccs) throws Exception {
        this.ccs = ccs;
        server = new Server();

        connector = new SelectChannelConnector();

        server.setConnectors(new Connector[] { connector });

        handlerCollection = new ContextHandlerCollection();
        server.setHandler(handlerCollection);
        addHandlers();
    }

    private void addHandlers() {
        ContextHandler handler = new ContextHandler("/rest");
        RoutingHandler rh = new RoutingHandler();
        rh.addHandler("jobs", new JSONOutputRequestHandler(new JobsRESTAPIFunction(ccs)));
        rh.addHandler("nodes", new JSONOutputRequestHandler(new NodesRESTAPIFunction(ccs)));
        handler.setHandler(rh);
        addHandler(handler);

        handler = new ContextHandler("/adminconsole");
        handler.setHandler(new AdminConsoleHandler(ccs));
        addHandler(handler);

        handler = new ContextHandler("/applications");
        handler.setHandler(new ApplicationInstallationHandler(ccs));
        addHandler(handler);

        String basedir = System.getProperty("basedir");
        if (basedir != null) {
            File warFile = new File(basedir, "console/hyracks-admin-console.war");
            LOGGER.info("Looking for admin console binary in: " + warFile.getAbsolutePath());
            if (warFile.exists()) {
                WebAppContext waCtx = new WebAppContext();
                waCtx.setContextPath("/console");
                waCtx.setWar(warFile.getAbsolutePath());
                addHandler(waCtx);
            }
        }
    }

    public void setPort(int port) {
        connector.setPort(port);
    }

    public int getListeningPort() {
        return connector.getLocalPort();
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        server.stop();
    }

    public void addHandler(Handler handler) {
        handlerCollection.addHandler(handler);
    }
}