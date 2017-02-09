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

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveLifecycleListener;
import org.apache.asterix.api.http.server.ApiServlet;
import org.apache.asterix.api.http.server.ClusterApiServlet;
import org.apache.asterix.api.http.server.ClusterControllerDetailsApiServlet;
import org.apache.asterix.api.http.server.ConnectorApiServlet;
import org.apache.asterix.api.http.server.DdlApiServlet;
import org.apache.asterix.api.http.server.DiagnosticsApiServlet;
import org.apache.asterix.api.http.server.FeedServlet;
import org.apache.asterix.api.http.server.FullApiServlet;
import org.apache.asterix.api.http.server.NodeControllerDetailsApiServlet;
import org.apache.asterix.api.http.server.QueryApiServlet;
import org.apache.asterix.api.http.server.QueryResultApiServlet;
import org.apache.asterix.api.http.server.QueryServiceServlet;
import org.apache.asterix.api.http.server.QueryStatusApiServlet;
import org.apache.asterix.api.http.server.QueryWebInterfaceServlet;
import org.apache.asterix.api.http.server.ShutdownApiServlet;
import org.apache.asterix.api.http.server.UpdateApiServlet;
import org.apache.asterix.api.http.server.VersionApiServlet;
import org.apache.asterix.api.http.servlet.ServletConstants;
import org.apache.asterix.app.cc.CCExtensionManager;
import org.apache.asterix.app.cc.ResourceIdManager;
import org.apache.asterix.app.external.ExternalLibraryUtils;
import org.apache.asterix.common.api.AsterixThreadFactory;
import org.apache.asterix.common.config.AsterixExtension;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.utils.LetUtil.Lets;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.api.IAsterixStateProxy;
import org.apache.asterix.metadata.bootstrap.AsterixStateProxy;
import org.apache.asterix.metadata.cluster.ClusterManagerProvider;
import org.apache.asterix.runtime.job.resource.JobCapacityController;
import org.apache.asterix.runtime.utils.AppContextInfo;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.hyracks.api.application.ICCApplicationContext;
import org.apache.hyracks.api.application.ICCApplicationEntryPoint;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.lifecycle.LifeCycleComponentManager;
import org.apache.hyracks.api.messages.IMessageBroker;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.WebManager;

public class CCApplicationEntryPoint implements ICCApplicationEntryPoint {

    private static final Logger LOGGER = Logger.getLogger(CCApplicationEntryPoint.class.getName());
    private static IAsterixStateProxy proxy;
    protected ICCApplicationContext appCtx;
    protected CCExtensionManager ccExtensionManager;
    protected IStorageComponentProvider componentProvider;
    private IJobCapacityController jobCapacityController;
    protected WebManager webManager;

    @Override
    public void start(ICCApplicationContext ccAppCtx, String[] args) throws Exception {
        final ClusterControllerService controllerService = (ClusterControllerService) ccAppCtx.getControllerService();
        IMessageBroker messageBroker = new CCMessageBroker(controllerService);
        this.appCtx = ccAppCtx;

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting Asterix cluster controller");
        }

        appCtx.setThreadFactory(new AsterixThreadFactory(appCtx.getThreadFactory(), new LifeCycleComponentManager()));
        ILibraryManager libraryManager = new ExternalLibraryManager();
        ResourceIdManager resourceIdManager = new ResourceIdManager();
        ExternalLibraryUtils.setUpExternaLibraries(libraryManager, false);
        componentProvider = new StorageComponentProvider();
        GlobalRecoveryManager.instantiate((HyracksConnection) getNewHyracksClientConnection(), componentProvider);
        AppContextInfo.initialize(appCtx, getNewHyracksClientConnection(), libraryManager, resourceIdManager,
                () -> MetadataManager.INSTANCE, GlobalRecoveryManager.instance());
        ccExtensionManager = new CCExtensionManager(getExtensions());
        AppContextInfo.INSTANCE.setExtensionManager(ccExtensionManager);
        final CCConfig ccConfig = controllerService.getCCConfig();
        if (System.getProperty("java.rmi.server.hostname") == null) {
            System.setProperty("java.rmi.server.hostname", ccConfig.clusterNetIpAddress);
        }
        MetadataProperties metadataProperties = AppContextInfo.INSTANCE.getMetadataProperties();

        setAsterixStateProxy(AsterixStateProxy.registerRemoteObject(metadataProperties.getMetadataCallbackPort()));
        appCtx.setDistributedState(proxy);

        MetadataManager.initialize(proxy, metadataProperties);

        AppContextInfo.INSTANCE.getCCApplicationContext().addJobLifecycleListener(ActiveLifecycleListener.INSTANCE);

        // create event loop groups
        webManager = new WebManager();
        configureServers();
        webManager.start();
        ClusterManagerProvider.getClusterManager().registerSubscriber(GlobalRecoveryManager.instance());
        ccAppCtx.addClusterLifecycleListener(ClusterLifecycleListener.INSTANCE);
        ccAppCtx.setMessageBroker(messageBroker);

        jobCapacityController = new JobCapacityController(controllerService.getResourceManager());
    }

    protected List<AsterixExtension> getExtensions() {
        return AppContextInfo.INSTANCE.getExtensionProperties().getExtensions();
    }

    protected void configureServers() throws Exception {
        webManager.add(setupWebServer(AppContextInfo.INSTANCE.getExternalProperties()));
        webManager.add(setupJSONAPIServer(AppContextInfo.INSTANCE.getExternalProperties()));
        webManager.add(setupFeedServer(AppContextInfo.INSTANCE.getExternalProperties()));
        webManager.add(setupQueryWebServer(AppContextInfo.INSTANCE.getExternalProperties()));
    }

    @Override
    public void stop() throws Exception {
        ActiveLifecycleListener.INSTANCE.stop();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Stopping Asterix cluster controller");
        }
        AsterixStateProxy.unregisterRemoteObject();
        webManager.stop();
    }

    protected IHyracksClientConnection getNewHyracksClientConnection() throws Exception {
        String strIP = appCtx.getCCContext().getClusterControllerInfo().getClientNetAddress();
        int port = appCtx.getCCContext().getClusterControllerInfo().getClientNetPort();
        return new HyracksConnection(strIP, port);
    }

    protected HttpServer setupWebServer(ExternalProperties externalProperties) throws Exception {
        HttpServer webServer = new HttpServer(webManager.getBosses(), webManager.getWorkers(),
                externalProperties.getWebInterfacePort());
        IHyracksClientConnection hcc = getNewHyracksClientConnection();
        webServer.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
        webServer.addLet(new ApiServlet(webServer.ctx(), new String[] { "/*" },
                ccExtensionManager.getAqlCompilationProvider(), ccExtensionManager.getSqlppCompilationProvider(),
                getStatementExecutorFactory(), componentProvider));
        return webServer;
    }

    protected HttpServer setupJSONAPIServer(ExternalProperties externalProperties) throws Exception {
        HttpServer jsonAPIServer =
                new HttpServer(webManager.getBosses(), webManager.getWorkers(), externalProperties.getAPIServerPort());
        IHyracksClientConnection hcc = getNewHyracksClientConnection();
        jsonAPIServer.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
        jsonAPIServer.setAttribute(ASTERIX_BUILD_PROP_ATTR, AppContextInfo.INSTANCE);
        jsonAPIServer.setAttribute(ServletConstants.EXECUTOR_SERVICE,
                ((ClusterControllerService) appCtx.getControllerService()).getExecutor());

        // AQL rest APIs.
        addLet(jsonAPIServer, Lets.AQL_QUERY);
        addLet(jsonAPIServer, Lets.AQL_UPDATE);
        addLet(jsonAPIServer, Lets.AQL_DDL);
        addLet(jsonAPIServer, Lets.AQL);

        // SQL+x+ rest APIs.
        addLet(jsonAPIServer, Lets.SQLPP_QUERY);
        addLet(jsonAPIServer, Lets.SQLPP_UPDATE);
        addLet(jsonAPIServer, Lets.SQLPP_DDL);
        addLet(jsonAPIServer, Lets.SQLPP);

        // Other APIs.
        addLet(jsonAPIServer, Lets.QUERY_STATUS);
        addLet(jsonAPIServer, Lets.QUERY_RESULT);
        addLet(jsonAPIServer, Lets.QUERY_SERVICE);
        addLet(jsonAPIServer, Lets.CONNECTOR);
        addLet(jsonAPIServer, Lets.SHUTDOWN);
        addLet(jsonAPIServer, Lets.VERSION);
        addLet(jsonAPIServer, Lets.CLUSTER_STATE);
        addLet(jsonAPIServer, Lets.CLUSTER_STATE_NODE_DETAIL); // this must not precede add of CLUSTER_STATE
        addLet(jsonAPIServer, Lets.CLUSTER_STATE_CC_DETAIL); // this must not precede add of CLUSTER_STATE
        addLet(jsonAPIServer, Lets.DIAGNOSTICS);
        return jsonAPIServer;
    }

    protected void addLet(HttpServer server, Lets let) {
        server.addLet(createServlet(server, let, let.getPath()));
    }

    protected HttpServer setupQueryWebServer(ExternalProperties externalProperties) throws Exception {
        HttpServer queryWebServer = new HttpServer(webManager.getBosses(), webManager.getWorkers(),
                externalProperties.getQueryWebInterfacePort());
        IHyracksClientConnection hcc = getNewHyracksClientConnection();
        queryWebServer.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
        queryWebServer.addLet(new QueryWebInterfaceServlet(queryWebServer.ctx(), new String[] { "/*" }));
        return queryWebServer;
    }

    protected HttpServer setupFeedServer(ExternalProperties externalProperties) throws Exception {
        HttpServer feedServer = new HttpServer(webManager.getBosses(), webManager.getWorkers(),
                externalProperties.getFeedServerPort());
        feedServer.setAttribute(HYRACKS_CONNECTION_ATTR, getNewHyracksClientConnection());
        feedServer.addLet(new FeedServlet(feedServer.ctx(), new String[] { "/" }));
        return feedServer;
    }

    protected IServlet createServlet(HttpServer server, Lets key, String... paths) {
        switch (key) {
            case AQL:
                return new FullApiServlet(server.ctx(), paths, ccExtensionManager.getAqlCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case AQL_QUERY:
                return new QueryApiServlet(server.ctx(), paths, ccExtensionManager.getAqlCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case AQL_UPDATE:
                return new UpdateApiServlet(server.ctx(), paths, ccExtensionManager.getAqlCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case AQL_DDL:
                return new DdlApiServlet(server.ctx(), paths, ccExtensionManager.getAqlCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case SQLPP:
                return new FullApiServlet(server.ctx(), paths, ccExtensionManager.getSqlppCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case SQLPP_QUERY:
                return new QueryApiServlet(server.ctx(), paths, ccExtensionManager.getSqlppCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case SQLPP_UPDATE:
                return new UpdateApiServlet(server.ctx(), paths, ccExtensionManager.getSqlppCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case SQLPP_DDL:
                return new DdlApiServlet(server.ctx(), paths, ccExtensionManager.getSqlppCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case QUERY_STATUS:
                return new QueryStatusApiServlet(server.ctx(), paths);
            case QUERY_RESULT:
                return new QueryResultApiServlet(server.ctx(), paths);
            case QUERY_SERVICE:
                return new QueryServiceServlet(server.ctx(), paths, ccExtensionManager.getSqlppCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case CONNECTOR:
                return new ConnectorApiServlet(server.ctx(), paths);
            case SHUTDOWN:
                return new ShutdownApiServlet(server.ctx(), paths);
            case VERSION:
                return new VersionApiServlet(server.ctx(), paths);
            case CLUSTER_STATE:
                return new ClusterApiServlet(server.ctx(), paths);
            case CLUSTER_STATE_NODE_DETAIL:
                return new NodeControllerDetailsApiServlet(server.ctx(), paths);
            case CLUSTER_STATE_CC_DETAIL:
                return new ClusterControllerDetailsApiServlet(server.ctx(), paths);
            case DIAGNOSTICS:
                return new DiagnosticsApiServlet(server.ctx(), paths);
            default:
                throw new IllegalStateException(String.valueOf(key));
        }
    }

    private IStatementExecutorFactory getStatementExecutorFactory() {
        return ccExtensionManager.getStatementExecutorFactory(
                ((ClusterControllerService) appCtx.getControllerService()).getExecutorService());
    }

    @Override
    public void startupCompleted() throws Exception {
        ClusterManagerProvider.getClusterManager().notifyStartupCompleted();
    }

    @Override
    public IJobCapacityController getJobCapacityController() {
        return jobCapacityController;
    }

    public static synchronized void setAsterixStateProxy(IAsterixStateProxy proxy) {
        CCApplicationEntryPoint.proxy = proxy;
    }
}
