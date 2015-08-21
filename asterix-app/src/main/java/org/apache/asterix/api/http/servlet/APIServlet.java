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

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.logging.Level;

import javax.imageio.ImageIO;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.api.common.APIFramework;
import org.apache.asterix.api.common.SessionConfig;
import org.apache.asterix.api.common.SessionConfig.OutputFormat;
import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.parser.AQLParser;
import org.apache.asterix.aql.parser.ParseException;
import org.apache.asterix.aql.parser.TokenMgrError;
import org.apache.asterix.aql.translator.AqlTranslator;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.result.ResultReader;
import org.apache.asterix.result.ResultUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.client.dataset.HyracksDataset;

public class APIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "org.apache.asterix.HYRACKS_CONNECTION";

    private static final String HYRACKS_DATASET_ATTR = "org.apache.asterix.HYRACKS_DATASET";

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        OutputFormat format;
        boolean csv_and_header = false;
        String output = request.getParameter("output-format");
        if (output.equals("ADM")) {
            format = OutputFormat.ADM;
        } else if (output.equals("CSV")) {
            format = OutputFormat.CSV;
        } else if (output.equals("CSV-Header")) {
            format = OutputFormat.CSV;
            csv_and_header = true;
        } else if (output.equals("LOSSLESS_JSON")) {
            format = OutputFormat.LOSSLESS_JSON;
        } else {
            // Default output format
            format = OutputFormat.CLEAN_JSON;
        }

        String query = request.getParameter("query");
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
            AQLParser parser = new AQLParser(query);
            List<Statement> aqlStatements = parser.parse();
            SessionConfig sessionConfig = new SessionConfig(out, format, true, isSet(executeQuery), true);
            sessionConfig.set(SessionConfig.FORMAT_HTML, true);
            sessionConfig.set(SessionConfig.FORMAT_CSV_HEADER, csv_and_header);
            sessionConfig.setOOBData(isSet(printExprParam), isSet(printRewrittenExprParam),
                    isSet(printLogicalPlanParam), isSet(printOptimizedLogicalPlanParam), isSet(printJob));
            MetadataManager.INSTANCE.init();
            AqlTranslator aqlTranslator = new AqlTranslator(aqlStatements, sessionConfig);
            double duration = 0;
            long startTime = System.currentTimeMillis();
            aqlTranslator.compileAndExecute(hcc, hds, AqlTranslator.ResultDelivery.SYNC);
            long endTime = System.currentTimeMillis();
            duration = (endTime - startTime) / 1000.00;
            out.println(APIFramework.HTML_STATEMENT_SEPARATOR);
            out.println("<PRE>Duration of all jobs: " + duration + " sec</PRE>");
        } catch (ParseException | TokenMgrError | org.apache.asterix.aqlplus.parser.TokenMgrError pe) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.INFO, pe.toString(), pe);
            ResultUtils.webUIParseExceptionHandler(out, pe, query);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            ResultUtils.webUIErrorHandler(out, e);
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
