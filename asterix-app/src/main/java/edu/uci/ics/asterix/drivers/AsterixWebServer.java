package edu.uci.ics.asterix.drivers;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import edu.uci.ics.asterix.api.http.servlet.APIServlet;

public class AsterixWebServer {
    public static void main(String args[]) throws Exception {
        Server server = new Server(8080);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        context.addServlet(new ServletHolder(new APIServlet()), "/*");
        server.start();
        server.join();
    }
}