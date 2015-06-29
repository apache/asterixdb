package edu.uci.ics.asterix.api.http.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;

import edu.uci.ics.asterix.api.common.SessionConfig.OutputFormat;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.result.ResultUtils;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;

public class ShutdownAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";

    private static final String HYRACKS_DATASET_ATTR = "edu.uci.ics.asterix.HYRACKS_DATASET";

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException {

        response.setContentType("application/json");
        response.setCharacterEncoding("utf-8");

        PrintWriter out = response.getWriter();
        OutputFormat format = OutputFormat.JSON;
        String accept = request.getHeader("Accept");
        if ((accept == null) || (accept.contains("application/x-adm"))) {
            format = OutputFormat.ADM;
        } else if (accept.contains("application/json")) {
            format = OutputFormat.JSON;
        }
        StringWriter sw = new StringWriter();
        IOUtils.copy(request.getInputStream(), sw, StandardCharsets.UTF_8.name());

        ServletContext context = getServletContext();
        IHyracksClientConnection hcc;
        try {
            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
                response.setStatus(HttpServletResponse.SC_ACCEPTED);
                hcc.stopCluster();
            }
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            ResultUtils.apiErrorHandler(out, e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    }
}
