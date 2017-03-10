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

import static org.apache.asterix.api.http.servlet.ServletConstants.ASTERIX_APP_CONTEXT_INFO_ATTR;
import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_CONNECTION_ATTR;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
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
import org.apache.asterix.app.replication.FaultToleranceStrategyFactory;
import org.apache.asterix.common.api.AsterixThreadFactory;
import org.apache.asterix.common.config.AsterixExtension;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.replication.IFaultToleranceStrategy;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.utils.Servlets;
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
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.config.IConfigManager;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.lifecycle.LifeCycleComponentManager;
import org.apache.hyracks.control.cc.BaseCCApplication;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.WebManager;

public class CCApplication extends BaseCCApplication {

    private static final Logger LOGGER = Logger.getLogger(CCApplication.class.getName());
    private static IAsterixStateProxy proxy;
    protected ICCServiceContext ccServiceCtx;
    protected CCExtensionManager ccExtensionManager;
    protected IStorageComponentProvider componentProvider;
    private IJobCapacityController jobCapacityController;
    protected WebManager webManager;

    @Override
    public void start(IServiceContext serviceCtx, String[] args) throws Exception {
        if (args.length > 0) {
            throw new IllegalArgumentException("Unrecognized argument(s): " + Arrays.toString(args));
        }
        final ClusterControllerService controllerService = (ClusterControllerService) serviceCtx.getControllerService();
        ICCMessageBroker messageBroker = new CCMessageBroker(controllerService);
        this.ccServiceCtx = (ICCServiceContext) serviceCtx;

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting Asterix cluster controller");
        }

        ccServiceCtx.setThreadFactory(
                new AsterixThreadFactory(ccServiceCtx.getThreadFactory(), new LifeCycleComponentManager()));
        ILibraryManager libraryManager = new ExternalLibraryManager();
        ResourceIdManager resourceIdManager = new ResourceIdManager();
        IReplicationStrategy repStrategy = ClusterProperties.INSTANCE.getReplicationStrategy();
        IFaultToleranceStrategy ftStrategy = FaultToleranceStrategyFactory
                .create(ClusterProperties.INSTANCE.getCluster(), repStrategy, messageBroker);
        ExternalLibraryUtils.setUpExternaLibraries(libraryManager, false);
        componentProvider = new StorageComponentProvider();
        GlobalRecoveryManager.instantiate((HyracksConnection) getHcc(), componentProvider);
        AppContextInfo.initialize(ccServiceCtx, getHcc(), libraryManager, resourceIdManager,
                () -> MetadataManager.INSTANCE, GlobalRecoveryManager.instance(), ftStrategy);
        ccExtensionManager = new CCExtensionManager(getExtensions());
        AppContextInfo.INSTANCE.setExtensionManager(ccExtensionManager);
        final CCConfig ccConfig = controllerService.getCCConfig();
        if (System.getProperty("java.rmi.server.hostname") == null) {
            System.setProperty("java.rmi.server.hostname", ccConfig.getClusterListenAddress());
        }
        MetadataProperties metadataProperties = AppContextInfo.INSTANCE.getMetadataProperties();

        setAsterixStateProxy(AsterixStateProxy.registerRemoteObject(metadataProperties.getMetadataCallbackPort()));
        ccServiceCtx.setDistributedState(proxy);

        MetadataManager.initialize(proxy, metadataProperties);

        AppContextInfo.INSTANCE.getCCServiceContext().addJobLifecycleListener(ActiveLifecycleListener.INSTANCE);

        // create event loop groups
        webManager = new WebManager();
        configureServers();
        webManager.start();
        ClusterManagerProvider.getClusterManager().registerSubscriber(GlobalRecoveryManager.instance());
        ccServiceCtx.addClusterLifecycleListener(ClusterLifecycleListener.INSTANCE);
        ccServiceCtx.setMessageBroker(messageBroker);

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

    protected HttpServer setupWebServer(ExternalProperties externalProperties) throws Exception {
        HttpServer webServer = new HttpServer(webManager.getBosses(), webManager.getWorkers(),
                externalProperties.getWebInterfacePort());
        IHyracksClientConnection hcc = getHcc();
        webServer.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
        webServer.addServlet(new ApiServlet(webServer.ctx(), new String[] { "/*" },
                ccExtensionManager.getAqlCompilationProvider(), ccExtensionManager.getSqlppCompilationProvider(),
                getStatementExecutorFactory(), componentProvider));
        return webServer;
    }

    protected HttpServer setupJSONAPIServer(ExternalProperties externalProperties) throws Exception {
        HttpServer jsonAPIServer =
                new HttpServer(webManager.getBosses(), webManager.getWorkers(), externalProperties.getAPIServerPort());
        IHyracksClientConnection hcc = getHcc();
        jsonAPIServer.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
        jsonAPIServer.setAttribute(ASTERIX_APP_CONTEXT_INFO_ATTR, AppContextInfo.INSTANCE);
        jsonAPIServer.setAttribute(ServletConstants.EXECUTOR_SERVICE,
                ((ClusterControllerService) ccServiceCtx.getControllerService()).getExecutor());

        // AQL rest APIs.
        addServlet(jsonAPIServer, Servlets.AQL_QUERY);
        addServlet(jsonAPIServer, Servlets.AQL_UPDATE);
        addServlet(jsonAPIServer, Servlets.AQL_DDL);
        addServlet(jsonAPIServer, Servlets.AQL);

        // SQL+x+ rest APIs.
        addServlet(jsonAPIServer, Servlets.SQLPP_QUERY);
        addServlet(jsonAPIServer, Servlets.SQLPP_UPDATE);
        addServlet(jsonAPIServer, Servlets.SQLPP_DDL);
        addServlet(jsonAPIServer, Servlets.SQLPP);

        // Other APIs.
        addServlet(jsonAPIServer, Servlets.QUERY_STATUS);
        addServlet(jsonAPIServer, Servlets.QUERY_RESULT);
        addServlet(jsonAPIServer, Servlets.QUERY_SERVICE);
        addServlet(jsonAPIServer, Servlets.CONNECTOR);
        addServlet(jsonAPIServer, Servlets.SHUTDOWN);
        addServlet(jsonAPIServer, Servlets.VERSION);
        addServlet(jsonAPIServer, Servlets.CLUSTER_STATE);
        addServlet(jsonAPIServer, Servlets.CLUSTER_STATE_NODE_DETAIL); // must not precede add of CLUSTER_STATE
        addServlet(jsonAPIServer, Servlets.CLUSTER_STATE_CC_DETAIL); // must not precede add of CLUSTER_STATE
        addServlet(jsonAPIServer, Servlets.DIAGNOSTICS);
        return jsonAPIServer;
    }

    protected void addServlet(HttpServer server, String path) {
        server.addServlet(createServlet(server.ctx(), path, path));
    }

    protected HttpServer setupQueryWebServer(ExternalProperties externalProperties) throws Exception {
        HttpServer queryWebServer = new HttpServer(webManager.getBosses(), webManager.getWorkers(),
                externalProperties.getQueryWebInterfacePort());
        IHyracksClientConnection hcc = getHcc();
        queryWebServer.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
        queryWebServer.addServlet(new QueryWebInterfaceServlet(queryWebServer.ctx(), new String[] { "/*" }));
        return queryWebServer;
    }

    protected HttpServer setupFeedServer(ExternalProperties externalProperties) throws Exception {
        HttpServer feedServer = new HttpServer(webManager.getBosses(), webManager.getWorkers(),
                externalProperties.getActiveServerPort());
        feedServer.setAttribute(HYRACKS_CONNECTION_ATTR, getHcc());
        feedServer.addServlet(new FeedServlet(feedServer.ctx(), new String[] { "/" }));
        return feedServer;
    }

    protected IServlet createServlet(ConcurrentMap<String, Object> ctx, String key, String... paths) {
        switch (key) {
            case Servlets.AQL:
                return new FullApiServlet(ctx, paths, ccExtensionManager.getAqlCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case Servlets.AQL_QUERY:
                return new QueryApiServlet(ctx, paths, ccExtensionManager.getAqlCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case Servlets.AQL_UPDATE:
                return new UpdateApiServlet(ctx, paths, ccExtensionManager.getAqlCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case Servlets.AQL_DDL:
                return new DdlApiServlet(ctx, paths, ccExtensionManager.getAqlCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case Servlets.SQLPP:
                return new FullApiServlet(ctx, paths, ccExtensionManager.getSqlppCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case Servlets.SQLPP_QUERY:
                return new QueryApiServlet(ctx, paths, ccExtensionManager.getSqlppCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case Servlets.SQLPP_UPDATE:
                return new UpdateApiServlet(ctx, paths, ccExtensionManager.getSqlppCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case Servlets.SQLPP_DDL:
                return new DdlApiServlet(ctx, paths, ccExtensionManager.getSqlppCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case Servlets.QUERY_STATUS:
                return new QueryStatusApiServlet(ctx, paths);
            case Servlets.QUERY_RESULT:
                return new QueryResultApiServlet(ctx, paths);
            case Servlets.QUERY_SERVICE:
                return new QueryServiceServlet(ctx, paths, ccExtensionManager.getSqlppCompilationProvider(),
                        getStatementExecutorFactory(), componentProvider);
            case Servlets.CONNECTOR:
                return new ConnectorApiServlet(ctx, paths);
            case Servlets.SHUTDOWN:
                return new ShutdownApiServlet(ctx, paths);
            case Servlets.VERSION:
                return new VersionApiServlet(ctx, paths);
            case Servlets.CLUSTER_STATE:
                return new ClusterApiServlet(ctx, paths);
            case Servlets.CLUSTER_STATE_NODE_DETAIL:
                return new NodeControllerDetailsApiServlet(ctx, paths);
            case Servlets.CLUSTER_STATE_CC_DETAIL:
                return new ClusterControllerDetailsApiServlet(ctx, paths);
            case Servlets.DIAGNOSTICS:
                return new DiagnosticsApiServlet(ctx, paths);
            default:
                throw new IllegalStateException(String.valueOf(key));
        }
    }

    private IStatementExecutorFactory getStatementExecutorFactory() {
        return ccExtensionManager.getStatementExecutorFactory(
                ((ClusterControllerService) ccServiceCtx.getControllerService()).getExecutorService());
    }

    @Override
    public void startupCompleted() throws Exception {
        ClusterManagerProvider.getClusterManager().notifyStartupCompleted();
    }

    @Override
    public IJobCapacityController getJobCapacityController() {
        return jobCapacityController;
    }

    @Override
    public void registerConfig(IConfigManager configManager) {
        super.registerConfig(configManager);
        ApplicationClassHelper.registerConfigOptions(configManager);
    }

    public static synchronized void setAsterixStateProxy(IAsterixStateProxy proxy) {
        CCApplication.proxy = proxy;
    }

    @Override
    public AppContextInfo getApplicationContext() {
        return AppContextInfo.INSTANCE;
    }

    @Override
    public IHyracksClientConnection getHcc() throws Exception {
        String strIP = ccServiceCtx.getCCContext().getClusterControllerInfo().getClientNetAddress();
        int port = ccServiceCtx.getCCContext().getClusterControllerInfo().getClientNetPort();
        return new HyracksConnection(strIP, port);
    }
}
