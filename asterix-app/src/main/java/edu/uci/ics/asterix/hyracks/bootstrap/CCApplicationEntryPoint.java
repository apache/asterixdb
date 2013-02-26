package edu.uci.ics.asterix.hyracks.bootstrap;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import edu.uci.ics.asterix.api.http.servlet.APIServlet;
import edu.uci.ics.asterix.common.api.AsterixAppContextInfoImpl;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.api.IAsterixStateProxy;
import edu.uci.ics.asterix.metadata.bootstrap.AsterixProperties;
import edu.uci.ics.asterix.metadata.bootstrap.AsterixStateProxy;
import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.application.ICCApplicationEntryPoint;

public class CCApplicationEntryPoint implements ICCApplicationEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(CCApplicationEntryPoint.class.getName());

    private static final int DEFAULT_WEB_SERVER_PORT = 19001;

    private Server webServer;
    private static IAsterixStateProxy proxy;
    private ICCApplicationContext appCtx;

    @Override
    public void start(ICCApplicationContext ccAppCtx, String[] args) throws Exception {
        this.appCtx = ccAppCtx;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting Asterix cluster controller");
        }

        proxy = AsterixStateProxy.registerRemoteObject();
        proxy.setAsterixProperties(AsterixProperties.INSTANCE);
        appCtx.setDistributedState(proxy);

        MetadataManager.INSTANCE = new MetadataManager(proxy);

        setupWebServer();
        webServer.start();

        AsterixAppContextInfoImpl.initialize(appCtx);
    }

    @Override
    public void stop() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Stopping Asterix cluster controller");
        }
        AsterixStateProxy.unregisterRemoteObject();

        webServer.stop();
    }

    private void setupWebServer() throws Exception {
        String portStr = System.getProperty(GlobalConfig.WEB_SERVER_PORT_PROPERTY);
        int port = DEFAULT_WEB_SERVER_PORT;
        if (portStr != null) {
            port = Integer.parseInt(portStr);
        }
        webServer = new Server(port);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        webServer.setHandler(context);
        context.addServlet(new ServletHolder(new APIServlet()), "/*");
    }
}