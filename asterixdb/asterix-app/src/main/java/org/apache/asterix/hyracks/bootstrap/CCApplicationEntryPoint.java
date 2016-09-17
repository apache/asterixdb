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

import static org.apache.asterix.api.http.servlet.ServletConstants.ASTERIX_BUILD_PROP_ATTR;
import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_CONNECTION_ATTR;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.Servlet;

import org.apache.asterix.active.ActiveLifecycleListener;
import org.apache.asterix.api.http.servlet.APIServlet;
import org.apache.asterix.api.http.servlet.AQLAPIServlet;
import org.apache.asterix.api.http.servlet.ClusterAPIServlet;
import org.apache.asterix.api.http.servlet.ClusterCCDetailsAPIServlet;
import org.apache.asterix.api.http.servlet.ClusterNodeDetailsAPIServlet;
import org.apache.asterix.api.http.servlet.ConnectorAPIServlet;
import org.apache.asterix.api.http.servlet.DDLAPIServlet;
import org.apache.asterix.api.http.servlet.FeedServlet;
import org.apache.asterix.api.http.servlet.QueryAPIServlet;
import org.apache.asterix.api.http.servlet.QueryResultAPIServlet;
import org.apache.asterix.api.http.servlet.QueryServiceServlet;
import org.apache.asterix.api.http.servlet.QueryStatusAPIServlet;
import org.apache.asterix.api.http.servlet.QueryWebInterfaceServlet;
import org.apache.asterix.api.http.servlet.ShutdownAPIServlet;
import org.apache.asterix.api.http.servlet.UpdateAPIServlet;
import org.apache.asterix.api.http.servlet.VersionAPIServlet;
import org.apache.asterix.app.cc.AsterixResourceIdManager;
import org.apache.asterix.app.cc.CompilerExtensionManager;
import org.apache.asterix.app.external.ExternalLibraryUtils;
import org.apache.asterix.common.api.AsterixThreadFactory;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.config.AsterixExtension;
import org.apache.asterix.common.config.AsterixExternalProperties;
import org.apache.asterix.common.config.AsterixMetadataProperties;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.utils.ServletUtil.Servlets;
import org.apache.asterix.event.service.ILookupService;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.api.IAsterixStateProxy;
import org.apache.asterix.metadata.bootstrap.AsterixStateProxy;
import org.apache.asterix.metadata.cluster.ClusterManager;
import org.apache.asterix.runtime.util.AsterixAppContextInfo;
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
import org.eclipse.jetty.servlet.ServletMapping;

public class CCApplicationEntryPoint implements ICCApplicationEntryPoint {

    private static final Logger LOGGER = Logger.getLogger(CCApplicationEntryPoint.class.getName());

    private List<Server> servers;

    private static IAsterixStateProxy proxy;
    protected ICCApplicationContext appCtx;
    protected CompilerExtensionManager ccExtensionManager;

    @Override
    public void start(ICCApplicationContext ccAppCtx, String[] args) throws Exception {
        IMessageBroker messageBroker = new CCMessageBroker((ClusterControllerService) ccAppCtx.getControllerService());
        this.appCtx = ccAppCtx;

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting Asterix cluster controller");
        }

        appCtx.setThreadFactory(new AsterixThreadFactory(new LifeCycleComponentManager()));
        GlobalRecoveryManager.instantiate((HyracksConnection) getNewHyracksClientConnection());
        ILibraryManager libraryManager = new ExternalLibraryManager();
        AsterixResourceIdManager resourceIdManager = new AsterixResourceIdManager();
        ExternalLibraryUtils.setUpExternaLibraries(libraryManager, false);
        AsterixAppContextInfo.initialize(appCtx, getNewHyracksClientConnection(), GlobalRecoveryManager.instance(),
                libraryManager, resourceIdManager);
        ccExtensionManager = new CompilerExtensionManager(getExtensions());
        AsterixAppContextInfo.INSTANCE.setExtensionManager(ccExtensionManager);

        if (System.getProperty("java.rmi.server.hostname") == null) {
            System.setProperty("java.rmi.server.hostname",
                    ((ClusterControllerService) ccAppCtx.getControllerService()).getCCConfig().clusterNetIpAddress);
        }

        setAsterixStateProxy(AsterixStateProxy.registerRemoteObject());
        appCtx.setDistributedState(proxy);

        AsterixMetadataProperties metadataProperties = AsterixAppContextInfo.INSTANCE.getMetadataProperties();
        MetadataManager.instantiate(new MetadataManager(proxy, metadataProperties));

        AsterixAppContextInfo.INSTANCE.getCCApplicationContext()
                .addJobLifecycleListener(ActiveLifecycleListener.INSTANCE);

        servers = configureServers();

        for (Server server : servers) {
            server.start();
        }

        ClusterManager.INSTANCE.registerSubscriber(GlobalRecoveryManager.instance());

        ccAppCtx.addClusterLifecycleListener(ClusterLifecycleListener.INSTANCE);
        ccAppCtx.setMessageBroker(messageBroker);
    }

    protected List<AsterixExtension> getExtensions() {
        return AsterixAppContextInfo.INSTANCE.getExtensionProperties().getExtensions();
    }

    protected List<Server> configureServers() throws Exception {
        AsterixExternalProperties externalProperties = AsterixAppContextInfo.INSTANCE.getExternalProperties();

        List<Server> serverList = new ArrayList<>();
        serverList.add(setupWebServer(externalProperties));
        serverList.add(setupJSONAPIServer(externalProperties));
        serverList.add(setupFeedServer(externalProperties));
        serverList.add(setupQueryWebServer(externalProperties));
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
        context.addServlet(new ServletHolder(new APIServlet(ccExtensionManager.getAqlCompilationProvider(),
                ccExtensionManager.getSqlppCompilationProvider(), ccExtensionManager.getQueryTranslatorFactory())),
                "/*");

        return webServer;
    }

    protected Server setupJSONAPIServer(AsterixExternalProperties externalProperties) throws Exception {
        Server jsonAPIServer = new Server(externalProperties.getAPIServerPort());

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        IHyracksClientConnection hcc = getNewHyracksClientConnection();
        context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
        context.setAttribute(ASTERIX_BUILD_PROP_ATTR, AsterixAppContextInfo.INSTANCE);

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
        addServlet(context, Servlets.CLUSTER_STATE_NODE_DETAIL);
        addServlet(context, Servlets.CLUSTER_STATE_CC_DETAIL);

        return jsonAPIServer;
    }

    protected Server setupQueryWebServer(AsterixExternalProperties externalProperties) throws Exception {

        Server queryWebServer = new Server(externalProperties.getQueryWebInterfacePort());

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        IHyracksClientConnection hcc = getNewHyracksClientConnection();
        context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);

        queryWebServer.setHandler(context);
        context.addServlet(new ServletHolder(new QueryWebInterfaceServlet()), "/*");
        return queryWebServer;
    }

    protected void addServlet(ServletContextHandler context, Servlet servlet, String... paths) {
        final ServletHolder holder = new ServletHolder(servlet);
        context.getServletHandler().addServlet(holder);
        ServletMapping mapping = new ServletMapping();
        mapping.setServletName(holder.getName());
        mapping.setPathSpecs(paths);
        context.getServletHandler().addServletMapping(mapping);
    }

    protected void addServlet(ServletContextHandler context, Servlets key) {
        addServlet(context, createServlet(key), key.getPath());
    }

    protected Servlet createServlet(Servlets key) {
        switch (key) {
            case AQL:
                return new AQLAPIServlet(ccExtensionManager.getAqlCompilationProvider(),
                        ccExtensionManager.getQueryTranslatorFactory());
            case AQL_QUERY:
                return new QueryAPIServlet(ccExtensionManager.getAqlCompilationProvider(),
                        ccExtensionManager.getQueryTranslatorFactory());
            case AQL_UPDATE:
                return new UpdateAPIServlet(ccExtensionManager.getAqlCompilationProvider(),
                        ccExtensionManager.getQueryTranslatorFactory());
            case AQL_DDL:
                return new DDLAPIServlet(ccExtensionManager.getAqlCompilationProvider(),
                        ccExtensionManager.getQueryTranslatorFactory());
            case SQLPP:
                return new AQLAPIServlet(ccExtensionManager.getSqlppCompilationProvider(),
                        ccExtensionManager.getQueryTranslatorFactory());
            case SQLPP_QUERY:
                return new QueryAPIServlet(ccExtensionManager.getSqlppCompilationProvider(),
                        ccExtensionManager.getQueryTranslatorFactory());
            case SQLPP_UPDATE:
                return new UpdateAPIServlet(ccExtensionManager.getSqlppCompilationProvider(),
                        ccExtensionManager.getQueryTranslatorFactory());
            case SQLPP_DDL:
                return new DDLAPIServlet(ccExtensionManager.getSqlppCompilationProvider(),
                        ccExtensionManager.getQueryTranslatorFactory());
            case QUERY_STATUS:
                return new QueryStatusAPIServlet();
            case QUERY_RESULT:
                return new QueryResultAPIServlet();
            case QUERY_SERVICE:
                return new QueryServiceServlet(ccExtensionManager.getSqlppCompilationProvider(),
                        ccExtensionManager.getQueryTranslatorFactory());
            case CONNECTOR:
                return new ConnectorAPIServlet();
            case SHUTDOWN:
                return new ShutdownAPIServlet();
            case VERSION:
                return new VersionAPIServlet();
            case CLUSTER_STATE:
                return new ClusterAPIServlet();
            case CLUSTER_STATE_NODE_DETAIL:
                return new ClusterNodeDetailsAPIServlet();
            case CLUSTER_STATE_CC_DETAIL:
                return new ClusterCCDetailsAPIServlet();
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
            zookeeperService.reportClusterState(ClusterProperties.INSTANCE.getCluster().getInstanceName(),
                    ClusterState.ACTIVE);
        }
    }

    public static synchronized void setAsterixStateProxy(IAsterixStateProxy proxy) {
        CCApplicationEntryPoint.proxy = proxy;
    }
}
