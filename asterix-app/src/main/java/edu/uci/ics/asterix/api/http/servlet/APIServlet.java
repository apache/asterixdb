package edu.uci.ics.asterix.api.http.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.logging.Level;
import java.awt.image.BufferedImage;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.activation.MimetypesFileTypeMap;
import javax.imageio.ImageIO;

import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.aql.parser.TokenMgrError;
import edu.uci.ics.asterix.aql.translator.AqlTranslator;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.result.ResultReader;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.client.dataset.HyracksDataset;

public class APIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";

    private static final String HYRACKS_DATASET_ATTR = "edu.uci.ics.asterix.HYRACKS_DATASET";

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        DisplayFormat format = DisplayFormat.HTML;
        if (request.getContentType().equals("application/json")) {
            format = DisplayFormat.JSON;
        } else if (request.getContentType().equals("text/plain")) {
            format = DisplayFormat.TEXT;
        }

        String query = request.getParameter("query");
        String printExprParam = request.getParameter("print-expr-tree");
        String printRewrittenExprParam = request.getParameter("print-rewritten-expr-tree");
        String printLogicalPlanParam = request.getParameter("print-logical-plan");
        String printOptimizedLogicalPlanParam = request.getParameter("print-optimized-logical-plan");
        String printJob = request.getParameter("print-job");
        response.setCharacterEncoding("utf-8");
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        ServletContext context = getServletContext();
        IHyracksClientConnection hcc;
        IHyracksDataset hds;

        try {
            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);

                hds = (IHyracksDataset) context.getAttribute(HYRACKS_DATASET_ATTR);
                if (hds == null) {
                    hds = new HyracksDataset(hcc, ResultReader.FRAME_SIZE, ResultReader.NUM_READERS);
                    context.setAttribute(HYRACKS_DATASET_ATTR, hds);
                }
            }
            AQLParser parser = new AQLParser(query);
            List<Statement> aqlStatements = parser.Statement();
            SessionConfig sessionConfig = new SessionConfig(true, isSet(printExprParam),
                    isSet(printRewrittenExprParam), isSet(printLogicalPlanParam),
                    isSet(printOptimizedLogicalPlanParam), false, true, isSet(printJob));
            MetadataManager.INSTANCE.init();
            AqlTranslator aqlTranslator = new AqlTranslator(aqlStatements, out, sessionConfig, format);
            double duration = 0;
            long startTime = System.currentTimeMillis();
            aqlTranslator.compileAndExecute(hcc, hds, false);
            long endTime = System.currentTimeMillis();
            duration = (endTime - startTime) / 1000.00;
            out.println("<PRE>Duration of all jobs: " + duration + " sec</PRE>");
        } catch (ParseException | TokenMgrError | edu.uci.ics.asterix.aqlplus.parser.TokenMgrError pe) {
            out.println("<pre class=\"error\">");
            String message = pe.getMessage();
            message = message.replace("<", "&lt");
            message = message.replace(">", "&gt");
            out.println("SyntaxError:" + message);
            int pos = message.indexOf("line");
            if (pos > 0) {
                int columnPos = message.indexOf(",", pos + 1 + "line".length());
                int lineNo = Integer.parseInt(message.substring(pos + "line".length() + 1, columnPos));
                String[] lines = query.split("\n");
                if (lineNo >= lines.length) {
                    out.println("===> &ltBLANK LINE&gt");
                } else {
                    String line = lines[lineNo - 1];
                    out.println("==> " + line);
                }
            }
            out.println("</pre>");
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            out.println("<pre class=\"error\">");
            out.println(e.getMessage());
            out.println("</pre>");
        }
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String resourcePath = null;
        String requestURI = request.getRequestURI();

        if (requestURI.equals("/")) {
            response.setContentType("text/html");
            resourcePath = "/webui/querytemplate.html";
        } else {
            resourcePath = requestURI;
        }
                
        InputStream is = APIServlet.class.getResourceAsStream(resourcePath);
        if (is == null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        // Special handler for font files and .png resources
        if (resourcePath.endsWith(".png")) { 
            
            BufferedImage img = ImageIO.read(is);
            OutputStream outputStream = response.getOutputStream();
            String formatName = "png";            
            response.setContentType("image/png");
            ImageIO.write(img, formatName, outputStream);
            outputStream.close();
            return;

        }
        
        response.setCharacterEncoding("utf-8");
        InputStreamReader isr = new InputStreamReader(is);
        StringBuilder sb = new StringBuilder();
        BufferedReader br = new BufferedReader(isr);
        String line = br.readLine();

        while (line != null) {
            sb.append(line);
            line = br.readLine();
        }

        PrintWriter out = response.getWriter();
        out.println(sb.toString());
    }

    private static boolean isSet(String requestParameter) {
        return (requestParameter != null && requestParameter.equals("true"));
    }
}
