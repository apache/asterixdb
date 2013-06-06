/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.hyracks.bootstrap;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import edu.uci.ics.asterix.api.http.servlet.APIServlet;
import edu.uci.ics.asterix.api.http.servlet.DDLAPIServlet;
import edu.uci.ics.asterix.api.http.servlet.QueryAPIServlet;
import edu.uci.ics.asterix.api.http.servlet.QueryResultAPIServlet;
import edu.uci.ics.asterix.api.http.servlet.QueryStatusAPIServlet;
import edu.uci.ics.asterix.api.http.servlet.UpdateAPIServlet;
import edu.uci.ics.asterix.common.api.AsterixThreadFactory;
import edu.uci.ics.asterix.common.config.AsterixExternalProperties;
import edu.uci.ics.asterix.common.config.AsterixMetadataProperties;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.api.IAsterixStateProxy;
import edu.uci.ics.asterix.metadata.bootstrap.AsterixStateProxy;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.application.ICCApplicationEntryPoint;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;

public class CCApplicationEntryPoint implements ICCApplicationEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(CCApplicationEntryPoint.class.getName());

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";

    private Server webServer;
    private Server jsonAPIServer;
    private static IAsterixStateProxy proxy;
    private ICCApplicationContext appCtx;

    @Override
    public void start(ICCApplicationContext ccAppCtx, String[] args) throws Exception {
        this.appCtx = ccAppCtx;

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting Asterix cluster controller");
        }
        appCtx.setThreadFactory(AsterixThreadFactory.INSTANCE);
        AsterixAppContextInfo.initialize(appCtx);
        proxy = AsterixStateProxy.registerRemoteObject();
        appCtx.setDistributedState(proxy);

        AsterixMetadataProperties metadataProperties = AsterixAppContextInfo.getInstance().getMetadataProperties();
        MetadataManager.INSTANCE = new MetadataManager(proxy, metadataProperties);

        AsterixExternalProperties externalProperties = AsterixAppContextInfo.getInstance().getExternalProperties();
        setupWebServer(externalProperties);
        webServer.start();
        setupJSONAPIServer(externalProperties);
        jsonAPIServer.start();
        ccAppCtx.addClusterLifecycleListener(ClusterLifecycleListener.INSTANCE);
    }

    @Override
    public void stop() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Stopping Asterix cluster controller");
        }
        AsterixStateProxy.unregisterRemoteObject();

        webServer.stop();
        jsonAPIServer.stop();
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
    }
}