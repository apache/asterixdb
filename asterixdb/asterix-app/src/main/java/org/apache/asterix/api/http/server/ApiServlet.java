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
package org.apache.asterix.api.http.server;

import static org.apache.asterix.api.http.server.ServletConstants.HYRACKS_CONNECTION_ATTR;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import javax.imageio.ImageIO;

import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.aql.parser.TokenMgrError;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.ResultProperties;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionConfig.OutputFormat;
import org.apache.asterix.translator.SessionConfig.PlanFormat;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.StaticResourceServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.http.server.utils.HttpUtil.ContentType;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpResponseStatus;

public class ApiServlet extends AbstractServlet {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final String HTML_STATEMENT_SEPARATOR = "<!-- BEGIN -->";

    private final ICcApplicationContext appCtx;
    private final ILangCompilationProvider aqlCompilationProvider;
    private final ILangCompilationProvider sqlppCompilationProvider;
    private final IStatementExecutorFactory statementExectorFactory;
    private final IStorageComponentProvider componentProvider;

    public ApiServlet(ConcurrentMap<String, Object> ctx, String[] paths, ICcApplicationContext appCtx,
            ILangCompilationProvider aqlCompilationProvider, ILangCompilationProvider sqlppCompilationProvider,
            IStatementExecutorFactory statementExecutorFactory, IStorageComponentProvider componentProvider) {
        super(ctx, paths);
        this.appCtx = appCtx;
        this.aqlCompilationProvider = aqlCompilationProvider;
        this.sqlppCompilationProvider = sqlppCompilationProvider;
        this.statementExectorFactory = statementExecutorFactory;
        this.componentProvider = componentProvider;
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) {
        final IRequestReference requestReference = appCtx.getReceptionist().welcome(request);
        // Query language
        ILangCompilationProvider compilationProvider = "AQL".equals(request.getParameter("query-language"))
                ? aqlCompilationProvider : sqlppCompilationProvider;
        IParserFactory parserFactory = compilationProvider.getParserFactory();

        try {
            HttpUtil.setContentType(response, ContentType.TEXT_HTML, request);
        } catch (IOException e) {
            LOGGER.log(Level.WARN, "Failure setting content type", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }
        // Output format.
        PrintWriter out = response.writer();
        OutputFormat format;

        boolean csvAndHeader = false;
        String output = request.getParameter("output-format");

        if ("CSV-Header".equals(output)) {
            output = "CSV";
            csvAndHeader = true;
        }
        try {
            format = OutputFormat.valueOf(output);
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.INFO,
                    output + ": unsupported output-format, using " + OutputFormat.CLEAN_JSON + " instead", e);
            // Default output format
            format = OutputFormat.CLEAN_JSON;
        }
        PlanFormat planFormat =
                PlanFormat.get(request.getParameter("plan-format"), "plan format", PlanFormat.STRING, LOGGER);

        String query = request.getParameter("query");
        String wrapperArray = request.getParameter("wrapper-array");
        String printExprParam = request.getParameter("print-expr-tree");
        String printRewrittenExprParam = request.getParameter("print-rewritten-expr-tree");
        String printLogicalPlanParam = request.getParameter("print-logical-plan");
        String printOptimizedLogicalPlanParam = request.getParameter("print-optimized-logical-plan");
        String printJob = request.getParameter("print-job");
        String executeQuery = request.getParameter("execute-query");
        response.setStatus(HttpResponseStatus.OK);
        try {
            // TODO: warnings should be retrieved from warnings collectors
            IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
            IResultSet resultSet = ServletUtil.getResultSet(hcc, appCtx, ctx);
            IParser parser = parserFactory.createParser(query);
            List<Statement> statements = parser.parse();
            SessionConfig sessionConfig = new SessionConfig(format, true, isSet(executeQuery), true, planFormat);
            sessionConfig.set(SessionConfig.FORMAT_HTML, true);
            sessionConfig.set(SessionConfig.FORMAT_CSV_HEADER, csvAndHeader);
            sessionConfig.set(SessionConfig.FORMAT_WRAPPER_ARRAY, isSet(wrapperArray));
            sessionConfig.setOOBData(isSet(printExprParam), isSet(printRewrittenExprParam),
                    isSet(printLogicalPlanParam), isSet(printOptimizedLogicalPlanParam), isSet(printJob));
            SessionOutput sessionOutput = new SessionOutput(sessionConfig, out);
            MetadataManager.INSTANCE.init();
            IStatementExecutor translator = statementExectorFactory.create(appCtx, statements, sessionOutput,
                    compilationProvider, componentProvider, new ResponsePrinter(sessionOutput));
            double duration;
            long startTime = System.currentTimeMillis();
            final IRequestParameters requestParameters = new RequestParameters(requestReference, query, resultSet,
                    new ResultProperties(IStatementExecutor.ResultDelivery.IMMEDIATE), new IStatementExecutor.Stats(),
                    null, null, null, null, true);
            translator.compileAndExecute(hcc, requestParameters);
            long endTime = System.currentTimeMillis();
            duration = (endTime - startTime) / 1000.00;
            out.println(HTML_STATEMENT_SEPARATOR);
            out.println("<PRE>Duration of all jobs: " + duration + " sec</PRE>");
        } catch (AsterixException | TokenMgrError | org.apache.asterix.aqlplus.parser.TokenMgrError pe) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.INFO, pe.toString(), pe);
            ResultUtil.webUIParseExceptionHandler(out, pe, query);
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.ERROR, e.getMessage(), e);
            ResultUtil.webUIErrorHandler(out, e);
        }
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) {
        String resourcePath = null;
        String requestURI = request.getHttpRequest().uri();
        response.setStatus(HttpResponseStatus.OK);
        if ("/".equals(requestURI)) {
            try {
                HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML, request);
            } catch (IOException e) {
                LOGGER.log(Level.WARN, "Failure setting content type", e);
                response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
                return;
            }
            resourcePath = "/webui/querytemplate.html";
        } else {
            resourcePath = requestURI;
        }

        try (InputStream is = ApiServlet.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                response.setStatus(HttpResponseStatus.NOT_FOUND);
                return;
            }
            // Special handler for font files and .png resources
            if (resourcePath.endsWith(".png")) {
                BufferedImage img = ImageIO.read(is);
                HttpUtil.setContentType(response, HttpUtil.ContentType.IMG_PNG);
                OutputStream outputStream = response.outputStream();
                String formatName = "png";
                ImageIO.write(img, formatName, outputStream);
                outputStream.close();
                return;
            }
            String type = HttpUtil.mime(StaticResourceServlet.extension(resourcePath));
            HttpUtil.setContentType(response, "".equals(type) ? HttpUtil.ContentType.TEXT_PLAIN : type,
                    HttpUtil.Encoding.UTF8);
            writeOutput(response, is, resourcePath);
        } catch (IOException e) {
            LOGGER.log(Level.WARN, "Failure handling request", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void writeOutput(IServletResponse response, InputStream is, String resourcePath) throws IOException {
        try (InputStreamReader isr = new InputStreamReader(is); BufferedReader br = new BufferedReader(isr)) {
            StringBuilder sb = new StringBuilder();
            String line;
            try {
                line = br.readLine();
            } catch (NullPointerException e) {
                LOGGER.log(Level.WARN, "NPE reading resource " + resourcePath + ", assuming JDK-8080094; returning 404",
                        e);
                // workaround lame JDK bug where a broken InputStream is returned in case the resourcePath is a
                // directory; see https://bugs.openjdk.java.net/browse/JDK-8080094
                response.setStatus(HttpResponseStatus.NOT_FOUND);
                return;
            }
            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            PrintWriter out = response.writer();
            out.println(sb.toString());
        }
    }

    private static boolean isSet(String requestParameter) {
        return "true".equals(requestParameter);
    }
}
