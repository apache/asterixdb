/*
 * Copyright 2009-2011 by The Regents of the University of California
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import edu.uci.ics.asterix.api.aqlj.server.APIClientThreadFactory;
import edu.uci.ics.asterix.api.aqlj.server.ThreadedServer;
import edu.uci.ics.asterix.api.http.servlet.APIServlet;
import edu.uci.ics.asterix.common.api.AsterixAppContextInfoImpl;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.api.IAsterixStateProxy;
import edu.uci.ics.asterix.metadata.bootstrap.AsterixProperties;
import edu.uci.ics.asterix.metadata.bootstrap.AsterixStateProxy;
import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.application.ICCBootstrap;

public class CCBootstrapImpl implements ICCBootstrap {
    private static final Logger LOGGER = Logger.getLogger(CCBootstrapImpl.class.getName());

    private static final int DEFAULT_WEB_SERVER_PORT = 19001;
    public static final int DEFAULT_API_SERVER_PORT = 14600;
    private static final int DEFAULT_API_NODEDATA_SERVER_PORT = 14601;

    private Server server;
    private static IAsterixStateProxy proxy;
    private ICCApplicationContext appCtx;
    private ThreadedServer apiServer;

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting Asterix CC Bootstrap");
        String portStr = System.getProperty(GlobalConfig.WEB_SERVER_PORT_PROPERTY);
        int port = DEFAULT_WEB_SERVER_PORT;
        if (portStr != null) {
            port = Integer.parseInt(portStr);
        }
        server = new Server(port);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        context.addServlet(new ServletHolder(new APIServlet()), "/*");
        server.start();
        proxy = AsterixStateProxy.registerRemoteObject();
        proxy.setAsterixProperties(AsterixProperties.INSTANCE);

        // set the APINodeDataServer ports
        int startPort = DEFAULT_API_NODEDATA_SERVER_PORT;
        Map<String, Set<String>> nodeNameMap = new HashMap<String, Set<String>>();
        try {
            appCtx.getCCContext().getIPAddressNodeMap(nodeNameMap);
        } catch (Exception e) {
            throw new IOException(" unable to obtain IP address node map", e);
        }
        AsterixAppContextInfoImpl.setNodeControllerInfo(nodeNameMap);
        for (Map.Entry<String, Set<String>> entry : nodeNameMap.entrySet()) {
            Set<String> nodeNames = entry.getValue();
            Iterator<String> it = nodeNames.iterator();
            while (it.hasNext()) {
                AsterixNodeState ns = new AsterixNodeState();
                ns.setAPINodeDataServerPort(startPort);
                proxy.setAsterixNodeState(it.next(), ns);
                startPort++;
            }
        }

        appCtx.setDistributedState(proxy);
        MetadataManager.INSTANCE = new MetadataManager(proxy);
        apiServer = new ThreadedServer(DEFAULT_API_SERVER_PORT, new APIClientThreadFactory(appCtx));
        apiServer.start();
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping Asterix CC Bootstrap");
        AsterixStateProxy.deRegisterRemoteObject();
        server.stop();
        apiServer.shutdown();
    }

    @Override
    public void setApplicationContext(ICCApplicationContext appCtx) {
        this.appCtx = appCtx;
    }

}