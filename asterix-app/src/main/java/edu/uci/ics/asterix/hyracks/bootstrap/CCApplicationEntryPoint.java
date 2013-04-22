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
import edu.uci.ics.asterix.common.api.AsterixAppContextInfoImpl;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.api.IAsterixStateProxy;
import edu.uci.ics.asterix.metadata.bootstrap.AsterixProperties;
import edu.uci.ics.asterix.metadata.bootstrap.AsterixStateProxy;
import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.application.ICCApplicationEntryPoint;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;

public class CCApplicationEntryPoint implements ICCApplicationEntryPoint {
    private static final Logger LOGGER = Logger.getLogger(CCApplicationEntryPoint.class.getName());

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";

    private static final int DEFAULT_WEB_SERVER_PORT = 19001;

    private static final int DEFAULT_JSON_API_SERVER_PORT = 19101;

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

        proxy = AsterixStateProxy.registerRemoteObject();
        proxy.setAsterixProperties(AsterixProperties.INSTANCE);
        appCtx.setDistributedState(proxy);

        MetadataManager.INSTANCE = new MetadataManager(proxy);

        setupWebServer();
        webServer.start();
        System.out.println("STARTED WEB SERVER");

        // Setup and start the web interface
        setupJSONAPIServer();
        jsonAPIServer.start();

        AsterixAppContextInfoImpl.initialize(appCtx);
        ExternalLibraryBootstrap.setUpExternaLibraries(false);
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

    private void setupWebServer() throws Exception {
        String portStr = System.getProperty(GlobalConfig.WEB_SERVER_PORT_PROPERTY);
        int port = DEFAULT_WEB_SERVER_PORT;
        if (portStr != null) {
            port = Integer.parseInt(portStr);
        }
        webServer = new Server(port);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        IHyracksClientConnection hcc = getNewHyracksClientConnection();
        context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);

        webServer.setHandler(context);
        context.addServlet(new ServletHolder(new APIServlet()), "/*");
    }

    private void setupJSONAPIServer() throws Exception {
        String portStr = System.getProperty(GlobalConfig.JSON_API_SERVER_PORT_PROPERTY);
        int port = DEFAULT_JSON_API_SERVER_PORT;
        if (portStr != null) {
            port = Integer.parseInt(portStr);
        }
        jsonAPIServer = new Server(port);

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