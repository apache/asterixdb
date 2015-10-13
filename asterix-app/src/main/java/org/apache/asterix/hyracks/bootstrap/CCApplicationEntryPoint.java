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
package org.apache.asterix.hyracks.bootstrap;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.api.http.servlet.APIServlet;
import org.apache.asterix.api.http.servlet.AQLAPIServlet;
import org.apache.asterix.api.http.servlet.ConnectorAPIServlet;
import org.apache.asterix.api.http.servlet.DDLAPIServlet;
import org.apache.asterix.api.http.servlet.FeedServlet;
import org.apache.asterix.api.http.servlet.QueryAPIServlet;
import org.apache.asterix.api.http.servlet.QueryResultAPIServlet;
import org.apache.asterix.api.http.servlet.QueryStatusAPIServlet;
import org.apache.asterix.api.http.servlet.ShutdownAPIServlet;
import org.apache.asterix.api.http.servlet.UpdateAPIServlet;
import org.apache.asterix.common.api.AsterixThreadFactory;
import org.apache.asterix.common.config.AsterixExternalProperties;
import org.apache.asterix.common.config.AsterixMetadataProperties;
import org.apache.asterix.common.feeds.api.ICentralActiveManager;
import org.apache.asterix.feeds.ActiveJobLifecycleListener;
import org.apache.asterix.feeds.CentralActiveManager;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.api.IAsterixStateProxy;
import org.apache.asterix.metadata.bootstrap.AsterixStateProxy;
import org.apache.asterix.metadata.cluster.ClusterManager;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.hyracks.api.application.ICCApplicationContext;
import org.apache.hyracks.api.application.ICCApplicationEntryPoint;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.lifecycle.LifeCycleComponentManager;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.AbstractLifeCycle;

public class CCApplicationEntryPoint implements ICCApplicationEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(CCApplicationEntryPoint.class.getName());

    private static final String HYRACKS_CONNECTION_ATTR = "org.apache.asterix.HYRACKS_CONNECTION";

    private Server webServer;
    private Server jsonAPIServer;
    private Server feedServer;
    private ICentralActiveManager centralActiveManager;

    private static IAsterixStateProxy proxy;
    private ICCApplicationContext appCtx;

    @Override
    public void start(ICCApplicationContext ccAppCtx, String[] args) throws Exception {
        this.appCtx = ccAppCtx;

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting Asterix cluster controller");
        }

        appCtx.setThreadFactory(new AsterixThreadFactory(new LifeCycleComponentManager()));
        AsterixAppContextInfo.initialize(appCtx, getNewHyracksClientConnection());

        proxy = AsterixStateProxy.registerRemoteObject();
        appCtx.setDistributedState(proxy);

        AsterixMetadataProperties metadataProperties = AsterixAppContextInfo.getInstance().getMetadataProperties();
        MetadataManager.INSTANCE = new MetadataManager(proxy, metadataProperties);

        AsterixAppContextInfo.getInstance().getCCApplicationContext()
                .addJobLifecycleListener(ActiveJobLifecycleListener.INSTANCE);

        AsterixExternalProperties externalProperties = AsterixAppContextInfo.getInstance().getExternalProperties();
        setupWebServer(externalProperties);
        webServer.start();

        setupJSONAPIServer(externalProperties);
        jsonAPIServer.start();

        setupFeedServer(externalProperties);
        feedServer.start();

        waitUntilServerStart(webServer);
        waitUntilServerStart(jsonAPIServer);
        waitUntilServerStart(feedServer);

        ExternalLibraryBootstrap.setUpExternaLibraries(false);
        centralActiveManager = CentralActiveManager.getInstance();
        centralActiveManager.start();

        AsterixGlobalRecoveryManager.INSTANCE = new AsterixGlobalRecoveryManager(
                (HyracksConnection) getNewHyracksClientConnection());
        ClusterManager.INSTANCE.registerSubscriber(AsterixGlobalRecoveryManager.INSTANCE);

        ccAppCtx.addClusterLifecycleListener(ClusterLifecycleListener.INSTANCE);
    }

    private void waitUntilServerStart(AbstractLifeCycle webServer) throws Exception {
        while (!webServer.isStarted()) {
            if (webServer.isFailed()) {
                throw new Exception("Server failed to start");
            }
            wait(1000);
        }
    }

    @Override
    public void stop() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Stopping Asterix cluster controller");
        }
        AsterixStateProxy.unregisterRemoteObject();

        webServer.stop();
        jsonAPIServer.stop();
        feedServer.stop();
    }

    private IHyracksClientConnection getNewHyracksClientConnection() throws Exception {
        String strIP = appCtx.getCCContext().getClusterControllerInfo().getClientNetAddress();
        int port = appCtx.getCCContext().getClusterControllerInfo().getClientNetPort();
        return new HyracksConnection(strIP, port);
    }

    private void setupWebServer(AsterixExternalProperties externalProperties) throws Exception {

        webServer = new Server(externalProperties.getWebInterfacePort());

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        IHyracksClientConnection hcc = getNewHyracksClientConnection();
        context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);

        webServer.setHandler(context);
        context.addServlet(new ServletHolder(new APIServlet()), "/*");
    }

    private void setupJSONAPIServer(AsterixExternalProperties externalProperties) throws Exception {
        jsonAPIServer = new Server(externalProperties.getAPIServerPort());

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        IHyracksClientConnection hcc = getNewHyracksClientConnection();
        context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);

        jsonAPIServer.setHandler(context);
        context.addServlet(new ServletHolder(new QueryAPIServlet()), "/query");
        context.addServlet(new ServletHolder(new QueryStatusAPIServlet()), "/query/status");
        context.addServlet(new ServletHolder(new QueryResultAPIServlet()), "/query/result");
        context.addServlet(new ServletHolder(new UpdateAPIServlet()), "/update");
        context.addServlet(new ServletHolder(new DDLAPIServlet()), "/ddl");
        context.addServlet(new ServletHolder(new AQLAPIServlet()), "/aql");
        context.addServlet(new ServletHolder(new ConnectorAPIServlet()), "/connector");
        context.addServlet(new ServletHolder(new ShutdownAPIServlet()), "/admin/shutdown");
    }

    private void setupFeedServer(AsterixExternalProperties externalProperties) throws Exception {
        feedServer = new Server(externalProperties.getFeedServerPort());

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        IHyracksClientConnection hcc = getNewHyracksClientConnection();
        context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);

        feedServer.setHandler(context);
        context.addServlet(new ServletHolder(new FeedServlet()), "/");

        // add paths here
    }
}