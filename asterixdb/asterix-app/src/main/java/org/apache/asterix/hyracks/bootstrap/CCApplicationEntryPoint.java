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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.Servlet;

import org.apache.asterix.api.http.servlet.APIServlet;
import org.apache.asterix.api.http.servlet.AQLAPIServlet;
import org.apache.asterix.api.http.servlet.ClusterAPIServlet;
import org.apache.asterix.api.http.servlet.ConnectorAPIServlet;
import org.apache.asterix.api.http.servlet.DDLAPIServlet;
import org.apache.asterix.api.http.servlet.FeedServlet;
import org.apache.asterix.api.http.servlet.QueryAPIServlet;
import org.apache.asterix.api.http.servlet.QueryResultAPIServlet;
import org.apache.asterix.api.http.servlet.QueryServiceServlet;
import org.apache.asterix.api.http.servlet.QueryStatusAPIServlet;
import org.apache.asterix.api.http.servlet.ShutdownAPIServlet;
import org.apache.asterix.api.http.servlet.UpdateAPIServlet;
import org.apache.asterix.api.http.servlet.VersionAPIServlet;
import org.apache.asterix.app.external.ActiveLifecycleListener;
import org.apache.asterix.app.external.ExternalLibraryUtils;
import org.apache.asterix.common.api.AsterixThreadFactory;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.config.AsterixExternalProperties;
import org.apache.asterix.common.config.AsterixMetadataProperties;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.utils.ServletUtil.Servlets;
import org.apache.asterix.compiler.provider.AqlCompilationProvider;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.event.service.ILookupService;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.api.IAsterixStateProxy;
import org.apache.asterix.metadata.bootstrap.AsterixStateProxy;
import org.apache.asterix.metadata.cluster.ClusterManager;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.api.application.ICCApplicationContext;
import org.apache.hyracks.api.application.ICCApplicationEntryPoint;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.lifecycle.LifeCycleComponentManager;
import org.apache.hyracks.api.messages.IMessageBroker;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import static org.apache.asterix.api.http.servlet.ServletConstants.ASTERIX_BUILD_PROP_ATTR;
import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_CONNECTION_ATTR;

public class CCApplicationEntryPoint implements ICCApplicationEntryPoint {

    private static final Logger LOGGER = Logger.getLogger(CCApplicationEntryPoint.class.getName());

    private List<Server> servers;

    private static IAsterixStateProxy proxy;
    protected ICCApplicationContext appCtx;

    @Override
    public void start(ICCApplicationContext ccAppCtx, String[] args) throws Exception {
        IMessageBroker messageBroker = new CCMessageBroker((ClusterControllerService) ccAppCtx.getControllerService());
        this.appCtx = ccAppCtx;

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting Asterix cluster controller");
        }

        appCtx.setThreadFactory(new AsterixThreadFactory(new LifeCycleComponentManager()));
        GlobalRecoveryManager.INSTANCE = new GlobalRecoveryManager((HyracksConnection) getNewHyracksClientConnection());
        ILibraryManager libraryManager = new ExternalLibraryManager();
        ExternalLibraryUtils.setUpExternaLibraries(libraryManager, false);
        AsterixAppContextInfo.initialize(appCtx, getNewHyracksClientConnection(), GlobalRecoveryManager.INSTANCE,
                libraryManager);

        if (System.getProperty("java.rmi.server.hostname") == null) {
            System.setProperty("java.rmi.server.hostname",
                    ((ClusterControllerService) ccAppCtx.getControllerService()).getCCConfig().clusterNetIpAddress);
        }

        proxy = AsterixStateProxy.registerRemoteObject();
        appCtx.setDistributedState(proxy);

        AsterixMetadataProperties metadataProperties = AsterixAppContextInfo.getInstance().getMetadataProperties();
        MetadataManager.INSTANCE = new MetadataManager(proxy, metadataProperties);

        AsterixAppContextInfo.getInstance().getCCApplicationContext()
                .addJobLifecycleListener(ActiveLifecycleListener.INSTANCE);

        servers = configureServers();

        for (Server server : servers) {
            server.start();
        }

        ClusterManager.INSTANCE.registerSubscriber(GlobalRecoveryManager.INSTANCE);

        ccAppCtx.addClusterLifecycleListener(ClusterLifecycleListener.INSTANCE);
        ccAppCtx.setMessageBroker(messageBroker);
    }

    protected List<Server> configureServers() throws Exception {
        AsterixExternalProperties externalProperties = AsterixAppContextInfo.getInstance().getExternalProperties();

        List<Server> serverList = new ArrayList<>();
        serverList.add(setupWebServer(externalProperties));
        serverList.add(setupJSONAPIServer(externalProperties));
        serverList.add(setupFeedServer(externalProperties));
        return serverList;
    }

    @Override
    public void stop() throws Exception {
        ActiveLifecycleListener.INSTANCE.stop();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Stopping Asterix cluster controller");
        }
        AsterixStateProxy.unregisterRemoteObject();
        // Stop servers
        for (Server server : servers) {
            server.stop();
        }
        // Make sure servers are stopped before proceeding
        for (Server server : servers) {
            server.join();
        }
    }

    protected IHyracksClientConnection getNewHyracksClientConnection() throws Exception {
        String strIP = appCtx.getCCContext().getClusterControllerInfo().getClientNetAddress();
        int port = appCtx.getCCContext().getClusterControllerInfo().getClientNetPort();
        return new HyracksConnection(strIP, port);
    }

    protected Server setupWebServer(AsterixExternalProperties externalProperties) throws Exception {

        Server webServer = new Server(externalProperties.getWebInterfacePort());

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        IHyracksClientConnection hcc = getNewHyracksClientConnection();
        context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);

        webServer.setHandler(context);
        context.addServlet(new ServletHolder(new APIServlet()), "/*");

        return webServer;
    }

    protected Server setupJSONAPIServer(AsterixExternalProperties externalProperties) throws Exception {
        Server jsonAPIServer = new Server(externalProperties.getAPIServerPort());

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        IHyracksClientConnection hcc = getNewHyracksClientConnection();
        context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
        context.setAttribute(ASTERIX_BUILD_PROP_ATTR, AsterixAppContextInfo.getInstance());

        jsonAPIServer.setHandler(context);

        // AQL rest APIs.
        addServlet(context, Servlets.AQL_QUERY);
        addServlet(context, Servlets.AQL_UPDATE);
        addServlet(context, Servlets.AQL_DDL);
        addServlet(context, Servlets.AQL);

        // SQL++ rest APIs.
        addServlet(context, Servlets.SQLPP_QUERY);
        addServlet(context, Servlets.SQLPP_UPDATE);
        addServlet(context, Servlets.SQLPP_DDL);
        addServlet(context, Servlets.SQLPP);

        // Other APIs.
        addServlet(context, Servlets.QUERY_STATUS);
        addServlet(context, Servlets.QUERY_RESULT);
        addServlet(context, Servlets.QUERY_SERVICE);
        addServlet(context, Servlets.CONNECTOR);
        addServlet(context, Servlets.SHUTDOWN);
        addServlet(context, Servlets.VERSION);
        addServlet(context, Servlets.CLUSTER_STATE);

        return jsonAPIServer;
    }

    protected void addServlet(ServletContextHandler context, Servlet servlet, String path) {
        context.addServlet(new ServletHolder(servlet), path);
    }

    protected void addServlet(ServletContextHandler context, Servlets key) {
        addServlet(context, createServlet(key), key.getPath());
    }

    protected Servlet createServlet(Servlets key) {
        switch (key) {
            case AQL:
                return new AQLAPIServlet(new AqlCompilationProvider());
            case AQL_QUERY:
                return new QueryAPIServlet(new AqlCompilationProvider());
            case AQL_UPDATE:
                return new UpdateAPIServlet(new AqlCompilationProvider());
            case AQL_DDL:
                return new DDLAPIServlet(new AqlCompilationProvider());
            case SQLPP:
                return new AQLAPIServlet(new SqlppCompilationProvider());
            case SQLPP_QUERY:
                return new QueryAPIServlet(new SqlppCompilationProvider());
            case SQLPP_UPDATE:
                return new UpdateAPIServlet(new SqlppCompilationProvider());
            case SQLPP_DDL:
                return new DDLAPIServlet(new SqlppCompilationProvider());
            case QUERY_STATUS:
                return new QueryStatusAPIServlet();
            case QUERY_RESULT:
                return new QueryResultAPIServlet();
            case QUERY_SERVICE:
                return new QueryServiceServlet(new SqlppCompilationProvider());
            case CONNECTOR:
                return new ConnectorAPIServlet();
            case SHUTDOWN:
                return new ShutdownAPIServlet();
            case VERSION:
                return new VersionAPIServlet();
            case CLUSTER_STATE:
                return new ClusterAPIServlet();
            default:
                throw new IllegalStateException(String.valueOf(key));
        }
    }

    protected Server setupFeedServer(AsterixExternalProperties externalProperties) throws Exception {
        Server feedServer = new Server(externalProperties.getFeedServerPort());

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        IHyracksClientConnection hcc = getNewHyracksClientConnection();
        context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);

        feedServer.setHandler(context);
        context.addServlet(new ServletHolder(new FeedServlet()), "/");

        return feedServer;
    }

    @Override
    public void startupCompleted() throws Exception {
        // Notify Zookeeper that the startup is complete
        ILookupService zookeeperService = ClusterManager.getLookupService();
        if (zookeeperService != null) {
            // Our asterix app runtimes tests don't use zookeeper
            zookeeperService.reportClusterState(AsterixClusterProperties.INSTANCE.getCluster().getInstanceName(),
                    ClusterState.ACTIVE);
        }
    }
}
