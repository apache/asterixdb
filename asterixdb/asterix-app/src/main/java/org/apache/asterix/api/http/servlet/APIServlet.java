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
package org.apache.asterix.api.http.servlet;

import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_CONNECTION_ATTR;
import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_DATASET_ATTR;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.ImageIO;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.result.ResultUtil;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.aql.parser.TokenMgrError;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionConfig.OutputFormat;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.client.dataset.HyracksDataset;

public class APIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(APIServlet.class.getName());
    public static final String HTML_STATEMENT_SEPARATOR = "<!-- BEGIN -->";

    private final ILangCompilationProvider aqlCompilationProvider;
    private final ILangCompilationProvider sqlppCompilationProvider;
    private final transient IStatementExecutorFactory statementExectorFactory;

    public APIServlet(ILangCompilationProvider aqlCompilationProvider,
            ILangCompilationProvider sqlppCompilationProvider, IStatementExecutorFactory statementExecutorFactory) {
        this.aqlCompilationProvider = aqlCompilationProvider;
        this.sqlppCompilationProvider = sqlppCompilationProvider;
        this.statementExectorFactory = statementExecutorFactory;
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        // Query language
        ILangCompilationProvider compilationProvider = "AQL".equals(request.getParameter("query-language"))
                ? aqlCompilationProvider : sqlppCompilationProvider;
        IParserFactory parserFactory = compilationProvider.getParserFactory();

        // Output format.
        OutputFormat format;
        boolean csv_and_header = false;
        String output = request.getParameter("output-format");
        try {
            format = OutputFormat.valueOf(output);
        } catch (IllegalArgumentException e) {
            LOGGER.info(output + ": unsupported output-format, using " + OutputFormat.CLEAN_JSON + " instead");
            // Default output format
            format = OutputFormat.CLEAN_JSON;
        }

        String query = request.getParameter("query");
        String wrapperArray = request.getParameter("wrapper-array");
        String printExprParam = request.getParameter("print-expr-tree");
        String printRewrittenExprParam = request.getParameter("print-rewritten-expr-tree");
        String printLogicalPlanParam = request.getParameter("print-logical-plan");
        String printOptimizedLogicalPlanParam = request.getParameter("print-optimized-logical-plan");
        String printJob = request.getParameter("print-job");
        String executeQuery = request.getParameter("execute-query");
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
            IParser parser = parserFactory.createParser(query);
            List<Statement> aqlStatements = parser.parse();
            SessionConfig sessionConfig = new SessionConfig(out, format, true, isSet(executeQuery), true);
            sessionConfig.set(SessionConfig.FORMAT_HTML, true);
            sessionConfig.set(SessionConfig.FORMAT_CSV_HEADER, csv_and_header);
            sessionConfig.set(SessionConfig.FORMAT_WRAPPER_ARRAY, isSet(wrapperArray));
            sessionConfig.setOOBData(isSet(printExprParam), isSet(printRewrittenExprParam),
                    isSet(printLogicalPlanParam), isSet(printOptimizedLogicalPlanParam), isSet(printJob));
            MetadataManager.INSTANCE.init();
            IStatementExecutor translator =
                    statementExectorFactory.create(aqlStatements, sessionConfig, compilationProvider);
            double duration = 0;
            long startTime = System.currentTimeMillis();
            translator.compileAndExecute(hcc, hds, IStatementExecutor.ResultDelivery.SYNC);
            long endTime = System.currentTimeMillis();
            duration = (endTime - startTime) / 1000.00;
            out.println(HTML_STATEMENT_SEPARATOR);
            out.println("<PRE>Duration of all jobs: " + duration + " sec</PRE>");
        } catch (AsterixException | TokenMgrError | org.apache.asterix.aqlplus.parser.TokenMgrError pe) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.INFO, pe.toString(), pe);
            ResultUtil.webUIParseExceptionHandler(out, pe, query);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            ResultUtil.webUIErrorHandler(out, e);
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

        try (InputStream is = APIServlet.class.getResourceAsStream(resourcePath)) {
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
            String line;
            try {
                line = br.readLine();
            } catch (NullPointerException e) {
                LOGGER.log(Level.WARNING, "NPE reading resource " + resourcePath
                        + ", assuming JDK-8080094; returning 404", e);
                // workaround lame JDK bug where a broken InputStream is returned in case the resourcePath is a
                // directory; see https://bugs.openjdk.java.net/browse/JDK-8080094
                response.sendError(HttpServletResponse.SC_NOT_FOUND);
                return;
            }

            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }

            PrintWriter out = response.getWriter();
            out.println(sb.toString());
        }
    }

    private static boolean isSet(String requestParameter) {
        return (requestParameter != null && requestParameter.equals("true"));
    }
}
