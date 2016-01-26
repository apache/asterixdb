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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.api.common.SessionConfig;
import org.apache.asterix.aql.translator.QueryTranslator;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.lang.aql.parser.TokenMgrError;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.result.ResultReader;
import org.apache.asterix.result.ResultUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.client.dataset.HyracksDataset;

public class QueryServiceServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(QueryServiceServlet.class.getName());

    public static final String HYRACKS_CONNECTION_ATTR = "org.apache.asterix.HYRACKS_CONNECTION";
    public static final String HYRACKS_DATASET_ATTR = "org.apache.asterix.HYRACKS_DATASET";

    public enum Parameter {
        // Standard
        statement,
        format,
        // Asterix
        header
    }

    public enum Header {
        Accept("Accept"),
        ContentLength("Content-Length");

        private final String str;

        Header(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    public enum MediaType {
        CSV("text/csv"),
        JSON("application/json");

        private final String str;

        MediaType(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    public enum ResultFields {
        requestID,
        signature,
        status,
        results,
        errors,
        metrics
    }

    public enum ResultStatus {
        success,
        timeout,
        errors,
        fatal
    }

    public enum ErrorField {
        code,
        msg,
        stack
    }

    public enum Metrics {
        elapsedTime,
        executionTime,
        resultCount,
        resultSize
    }

    private final ILangCompilationProvider compilationProvider = new SqlppCompilationProvider();

    static SessionConfig.OutputFormat getFormat(HttpServletRequest request) {
        // First check the "format" parameter.
        String format = request.getParameter(Parameter.format.name());
        if (format != null && format.equals("CSV")) {
            return SessionConfig.OutputFormat.CSV;
        }
        // Second check the Accept: HTTP header.
        String accept = request.getHeader(Header.Accept.str());
        if (accept != null && accept.contains(MediaType.CSV.str())) {
            return SessionConfig.OutputFormat.CSV;
        }
        return SessionConfig.OutputFormat.CLEAN_JSON;
    }

    /**
     * Construct a SessionConfig with the appropriate output writer and
     * output-format based on the Accept: header and other servlet parameters.
     */
    static SessionConfig createSessionConfig(HttpServletRequest request, PrintWriter resultWriter) {
        SessionConfig.ResultDecorator resultPrefix = new SessionConfig.ResultDecorator() {
            @Override
            public PrintWriter print(PrintWriter pw) {
                pw.print("\t\"");
                pw.print(ResultFields.results.name());
                pw.print("\": ");
                return pw;
            }
        };

        SessionConfig.ResultDecorator resultPostfix = new SessionConfig.ResultDecorator() {
            @Override
            public PrintWriter print(PrintWriter pw) {
                pw.print(",\n");
                return pw;
            }
        };

        SessionConfig.OutputFormat format = getFormat(request);
        SessionConfig sessionConfig = new SessionConfig(resultWriter, format, resultPrefix, resultPostfix);
        sessionConfig.set(SessionConfig.FORMAT_WRAPPER_ARRAY, (format == SessionConfig.OutputFormat.CLEAN_JSON));

        if (format == SessionConfig.OutputFormat.CSV && ("present".equals(request.getParameter(Parameter.header.name()))
                || request.getHeader(Header.Accept.str()).contains("header=present"))) {
            sessionConfig.set(SessionConfig.FORMAT_CSV_HEADER, true);
        }
        return sessionConfig;
    }

    /**
     * Initialize the Content-Type of the response based on a SessionConfig.
     */
    static void initResponse(HttpServletResponse response, SessionConfig sessionConfig) throws IOException {
        response.setCharacterEncoding("utf-8");
        switch (sessionConfig.fmt()) {
            case CLEAN_JSON:
                response.setContentType(MediaType.JSON.str());
                break;
            case CSV:
                String contentType = MediaType.CSV.str() + "; header="
                        + (sessionConfig.is(SessionConfig.FORMAT_CSV_HEADER) ? "present" : "absent");
                response.setContentType(contentType);
                break;
        }
    }

    static void printField(PrintWriter pw, String name, String value) {
        printField(pw, name, value, true);
    }

    static void printField(PrintWriter pw, String name, String value, boolean comma) {
        pw.print("\t\"");
        pw.print(name);
        pw.print("\": \"");
        pw.print(value);
        pw.print('"');
        if (comma) {
            pw.print(',');
        }
        pw.print('\n');
    }

    static UUID printRequestId(PrintWriter pw) {
        UUID requestId = UUID.randomUUID();
        printField(pw, ResultFields.requestID.name(), requestId.toString());
        return requestId;
    }

    static void printSignature(PrintWriter pw) {
        printField(pw, ResultFields.signature.name(), "*");
    }

    static void printStatus(PrintWriter pw, ResultStatus rs) {
        printField(pw, ResultFields.status.name(), rs.name());
    }

    static void printError(PrintWriter pw, Throwable e) {
        final boolean addStack = false;
        pw.print("\t\"");
        pw.print(ResultFields.errors.name());
        pw.print("\": [{ \n");
        printField(pw, ErrorField.code.name(), "1");
        printField(pw, ErrorField.msg.name(), JSONUtil.escape(e.getMessage()), addStack);
        if (addStack) {
            StringWriter sw = new StringWriter();
            PrintWriter stackWriter = new PrintWriter(sw);
            e.printStackTrace(stackWriter);
            stackWriter.close();
            printField(pw, ErrorField.stack.name(), JSONUtil.escape(sw.toString()), false);
        }
        pw.print("\t}],\n");
    }

    static void printMetrics(PrintWriter pw, long elapsedTime, long executionTime, long resultCount, long resultSize) {
        pw.print("\t\"");
        pw.print(ResultFields.metrics.name());
        pw.print("\": {\n");
        pw.print("\t");
        printField(pw, Metrics.elapsedTime.name(), String.valueOf(elapsedTime));
        pw.print("\t");
        printField(pw, Metrics.executionTime.name(), String.valueOf(executionTime));
        pw.print("\t");
        printField(pw, Metrics.resultCount.name(), String.valueOf(resultCount));
        pw.print("\t");
        printField(pw, Metrics.resultSize.name(), String.valueOf(resultSize), false);
        pw.print("\t}\n");
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String query = request.getParameter(Parameter.statement.name());
        if (query == null) {
            StringWriter sw = new StringWriter();
            IOUtils.copy(request.getInputStream(), sw, StandardCharsets.UTF_8.name());
            query = sw.toString();
        }
        handleRequest(request, response, query);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String query = request.getParameter(Parameter.statement.name());
        handleRequest(request, response, query);
    }

    public void handleRequest(HttpServletRequest request, HttpServletResponse response, String query)
            throws IOException {
        long elapsedStart = System.nanoTime();

        query = query + ";";

        final StringWriter stringWriter = new StringWriter();
        final PrintWriter resultWriter = new PrintWriter(stringWriter);

        SessionConfig sessionConfig = createSessionConfig(request, resultWriter);
        initResponse(response, sessionConfig);

        int respCode = HttpServletResponse.SC_OK;
        ResultUtils.Stats stats = new ResultUtils.Stats();
        long execStart = 0, execEnd = 0;

        resultWriter.print("{\n");
        UUID requestId = printRequestId(resultWriter);
        printSignature(resultWriter);
        try {
            IHyracksClientConnection hcc;
            IHyracksDataset hds;
            ServletContext context = getServletContext();
            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
                hds = (IHyracksDataset) context.getAttribute(HYRACKS_DATASET_ATTR);
                if (hds == null) {
                    hds = new HyracksDataset(hcc, ResultReader.FRAME_SIZE, ResultReader.NUM_READERS);
                    context.setAttribute(HYRACKS_DATASET_ATTR, hds);
                }
            }
            IParser parser = compilationProvider.getParserFactory().createParser(query);
            List<Statement> aqlStatements = parser.parse();
            MetadataManager.INSTANCE.init();
            QueryTranslator translator = new QueryTranslator(aqlStatements, sessionConfig, compilationProvider);
            execStart = System.nanoTime();
            translator.compileAndExecute(hcc, hds, QueryTranslator.ResultDelivery.SYNC, stats);
            execEnd = System.nanoTime();
            printStatus(resultWriter, ResultStatus.success);
        } catch (AsterixException | TokenMgrError | org.apache.asterix.aqlplus.parser.TokenMgrError pe) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, pe.getMessage(), pe);
            printError(resultWriter, pe);
            printStatus(resultWriter, ResultStatus.fatal);
            respCode = HttpServletResponse.SC_BAD_REQUEST;
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            printError(resultWriter, e);
            printStatus(resultWriter, ResultStatus.fatal);
            respCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
        }
        printMetrics(resultWriter, (System.nanoTime() - elapsedStart) / 1000, (execEnd - execStart) / 1000, stats.count,
                stats.size);
        resultWriter.print("}\n");
        resultWriter.flush();
        String result = stringWriter.toString();

        GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, result);
        //result = JSONUtil.indent(result);

        response.setIntHeader(Header.ContentLength.str(), result.length());
        response.getWriter().print(result);
        if (response.getWriter().checkError()) {
            LOGGER.warning("Error flushing output writer");
        }
        response.setStatus(respCode);
    }

}
