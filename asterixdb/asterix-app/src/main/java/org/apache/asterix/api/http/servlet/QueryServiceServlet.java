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

import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.app.result.ResultUtil;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.utils.JSONUtil;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.aql.parser.TokenMgrError;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.runtime.util.ClusterStateManager;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.SessionConfig;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksAppendable;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.json.JSONException;
import org.json.JSONObject;

public class QueryServiceServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(QueryServiceServlet.class.getName());
    private final transient ILangCompilationProvider compilationProvider;
    private final transient IStatementExecutorFactory statementExecutorFactory;

    public QueryServiceServlet(ILangCompilationProvider compilationProvider,
            IStatementExecutorFactory statementExecutorFactory) {
        this.compilationProvider = compilationProvider;
        this.statementExecutorFactory = statementExecutorFactory;
    }

    public enum Parameter {
        STATEMENT("statement"),
        FORMAT("format"),
        CLIENT_ID("client_context_id"),
        PRETTY("pretty");

        private final String str;

        Parameter(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    private enum MediaType {
        CSV("text/csv"),
        JSON("application/json"),
        ADM("application/x-adm");

        private final String str;

        MediaType(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    private enum Attribute {
        HEADER("header"),
        LOSSLESS("lossless");

        private final String str;

        Attribute(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    public enum ResultFields {
        REQUEST_ID("requestID"),
        CLIENT_ID("clientContextID"),
        SIGNATURE("signature"),
        TYPE("type"),
        STATUS("status"),
        RESULTS("results"),
        ERRORS("errors"),
        METRICS("metrics");

        private final String str;

        ResultFields(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    public enum ResultStatus {
        SUCCESS("success"),
        TIMEOUT("timeout"),
        ERRORS("errors"),
        FATAL("fatal");

        private final String str;

        ResultStatus(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    private enum ErrorField {
        CODE("code"),
        MSG("msg"),
        STACK("stack");

        private final String str;

        ErrorField(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    private enum Metrics {
        ELAPSED_TIME("elapsedTime"),
        EXECUTION_TIME("executionTime"),
        RESULT_COUNT("resultCount"),
        RESULT_SIZE("resultSize");

        private final String str;

        Metrics(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    enum TimeUnit {
        SEC("s", 9),
        MILLI("ms", 6),
        MICRO("µs", 3),
        NANO("ns", 0);

        String unit;
        int nanoDigits;

        TimeUnit(String unit, int nanoDigits) {
            this.unit = unit;
            this.nanoDigits = nanoDigits;
        }

        static String formatNanos(long nanoTime) {
            final String strTime = String.valueOf(nanoTime);
            final int len = strTime.length();
            for (TimeUnit tu : TimeUnit.values()) {
                if (len > tu.nanoDigits) {
                    final String integer = strTime.substring(0, len - tu.nanoDigits);
                    final String fractional = strTime.substring(len - tu.nanoDigits);
                    return integer + (fractional.length() > 0 ? "." + fractional : "") + tu.unit;
                }
            }
            return "illegal string value: " + strTime;
        }
    }

    static class RequestParameters {
        String statement;
        String format;
        boolean pretty;
        String clientContextID;

        @Override
        public String toString() {
            return append(new StringBuilder()).toString();
        }

        public StringBuilder append(final StringBuilder sb) {
            sb.append("{ ");
            sb.append("\"statement\": \"");
            JSONUtil.escape(sb, statement);
            sb.append("\", ");
            sb.append("\"format\": \"").append(format).append("\", ");
            sb.append("\"pretty\": ").append(pretty).append(", ");
            sb.append("\"clientContextID\": \"").append(clientContextID).append("\" ");
            sb.append('}');
            return sb;
        }
    }

    private static String getParameterValue(String content, String attribute) {
        if (content == null || attribute == null) {
            return null;
        }
        int sc = content.indexOf(';');
        if (sc < 0) {
            return null;
        }
        int eq = content.indexOf('=', sc + 1);
        if (eq < 0) {
            return null;
        }
        if (content.substring(sc + 1, eq).trim().equalsIgnoreCase(attribute)) {
            return content.substring(eq + 1).trim().toLowerCase();
        }
        return null;
    }

    private static String toLower(String s) {
        return s != null ? s.toLowerCase() : s;
    }

    private static SessionConfig.OutputFormat getFormat(String format) {
        if (format != null) {
            if (format.startsWith(MediaType.CSV.str())) {
                return SessionConfig.OutputFormat.CSV;
            }
            if (format.equals(MediaType.ADM.str())) {
                return SessionConfig.OutputFormat.ADM;
            }
            if (format.startsWith(MediaType.JSON.str())) {
                return Boolean.parseBoolean(getParameterValue(format, "lossless"))
                        ? SessionConfig.OutputFormat.LOSSLESS_JSON : SessionConfig.OutputFormat.CLEAN_JSON;
            }
        }
        return SessionConfig.OutputFormat.CLEAN_JSON;
    }

    private static SessionConfig createSessionConfig(RequestParameters param, PrintWriter resultWriter) {
        SessionConfig.ResultDecorator resultPrefix = new SessionConfig.ResultDecorator() {
            int resultNo = -1;
            @Override
            public AlgebricksAppendable append(AlgebricksAppendable app) throws AlgebricksException {
                app.append("\t\"");
                app.append(ResultFields.RESULTS.str());
                if (resultNo >= 0) {
                    app.append('-').append(String.valueOf(resultNo));
                }
                ++resultNo;
                app.append("\": ");
                return app;
            }
        };

        SessionConfig.ResultDecorator resultPostfix = (AlgebricksAppendable app) -> {
            app.append("\t,\n");
            return app;
        };

        SessionConfig.OutputFormat format = getFormat(param.format);
        SessionConfig sessionConfig = new SessionConfig(resultWriter, format, resultPrefix, resultPostfix);
        sessionConfig.set(SessionConfig.FORMAT_WRAPPER_ARRAY, true);
        sessionConfig.set(SessionConfig.FORMAT_INDENT_JSON, param.pretty);
        sessionConfig.set(SessionConfig.FORMAT_QUOTE_RECORD,
                format != SessionConfig.OutputFormat.CLEAN_JSON && format != SessionConfig.OutputFormat.LOSSLESS_JSON);
        sessionConfig.set(SessionConfig.FORMAT_CSV_HEADER, format == SessionConfig.OutputFormat.CSV
                && "present".equals(getParameterValue(param.format, "header")));
        return sessionConfig;
    }

    private static void printField(PrintWriter pw, String name, String value) {
        printField(pw, name, value, true);
    }

    private static void printField(PrintWriter pw, String name, String value, boolean comma) {
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

    private static UUID printRequestId(PrintWriter pw) {
        UUID requestId = UUID.randomUUID();
        printField(pw, ResultFields.REQUEST_ID.str(), requestId.toString());
        return requestId;
    }

    private static void printClientContextID(PrintWriter pw, RequestParameters params) {
        if (params.clientContextID != null && !params.clientContextID.isEmpty()) {
            printField(pw, ResultFields.CLIENT_ID.str(), params.clientContextID);
        }
    }

    private static void printSignature(PrintWriter pw) {
        printField(pw, ResultFields.SIGNATURE.str(), "*");
    }

    private static void printType(PrintWriter pw, SessionConfig sessionConfig) {
        switch (sessionConfig.fmt()) {
            case ADM:
                printField(pw, ResultFields.TYPE.str(), MediaType.ADM.str());
                break;
            case CSV:
                String contentType = MediaType.CSV.str() + "; header="
                        + (sessionConfig.is(SessionConfig.FORMAT_CSV_HEADER) ? "present" : "absent");
                printField(pw, ResultFields.TYPE.str(), contentType);
                break;
            default:
                break;
        }
    }

    private static void printStatus(PrintWriter pw, ResultStatus rs) {
        printField(pw, ResultFields.STATUS.str(), rs.str());
    }

    private static void printError(PrintWriter pw, Throwable e) {
        Throwable rootCause = ResultUtil.getRootCause(e);
        if (rootCause == null) {
            rootCause = e;
        }
        final boolean addStack = false;
        pw.print("\t\"");
        pw.print(ResultFields.ERRORS.str());
        pw.print("\": [{ \n");
        printField(pw, ErrorField.CODE.str(), "1");
        final String msg = rootCause.getMessage();
        printField(pw, ErrorField.MSG.str(), JSONUtil.escape(msg != null ? msg : rootCause.getClass().getSimpleName()),
                addStack);
        if (addStack) {
            StringWriter sw = new StringWriter();
            PrintWriter stackWriter = new PrintWriter(sw);
            LOGGER.info(stackWriter.toString());
            stackWriter.close();
            printField(pw, ErrorField.STACK.str(), JSONUtil.escape(sw.toString()), false);
        }
        pw.print("\t}],\n");
    }

    private static void printMetrics(PrintWriter pw, long elapsedTime, long executionTime, long resultCount,
            long resultSize) {
        pw.print("\t\"");
        pw.print(ResultFields.METRICS.str());
        pw.print("\": {\n");
        pw.print("\t");
        printField(pw, Metrics.ELAPSED_TIME.str(), TimeUnit.formatNanos(elapsedTime));
        pw.print("\t");
        printField(pw, Metrics.EXECUTION_TIME.str(), TimeUnit.formatNanos(executionTime));
        pw.print("\t");
        printField(pw, Metrics.RESULT_COUNT.str(), String.valueOf(resultCount));
        pw.print("\t");
        printField(pw, Metrics.RESULT_SIZE.str(), String.valueOf(resultSize), false);
        pw.print("\t}\n");
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException {
        try {
            handleRequest(getRequestParameters(request), response);
        } catch (IOException e) {
            // Servlet methods should not throw exceptions
            // http://cwe.mitre.org/data/definitions/600.html
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    private RequestParameters getRequestParameters(HttpServletRequest request) throws IOException {
        final String contentTypeParam = request.getContentType();
        int sep = contentTypeParam.indexOf(';');
        final String contentType = sep < 0 ? contentTypeParam.trim() : contentTypeParam.substring(0, sep).trim();
        RequestParameters param = new RequestParameters();
        if (MediaType.JSON.str().equals(contentType)) {
            try {
                JSONObject jsonRequest = new JSONObject(getRequestBody(request));
                param.statement = jsonRequest.getString(Parameter.STATEMENT.str());
                param.format = toLower(jsonRequest.optString(Parameter.FORMAT.str()));
                param.pretty = jsonRequest.optBoolean(Parameter.PRETTY.str());
                param.clientContextID = jsonRequest.optString(Parameter.CLIENT_ID.str());
            } catch (JSONException e) {
                // if the JSON parsing fails, the statement is empty and we get an empty statement error
                GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        } else {
            param.statement = request.getParameter(Parameter.STATEMENT.str());
            if (param.statement == null) {
                param.statement = getRequestBody(request);
            }
            param.format = toLower(request.getParameter(Parameter.FORMAT.str()));
            param.pretty = Boolean.parseBoolean(request.getParameter(Parameter.PRETTY.str()));
            param.clientContextID = request.getParameter(Parameter.CLIENT_ID.str());
        }
        return param;
    }

    private static String getRequestBody(HttpServletRequest request) throws IOException {
        StringWriter sw = new StringWriter();
        IOUtils.copy(request.getInputStream(), sw, StandardCharsets.UTF_8.name());
        return sw.toString();
    }

    private void handleRequest(RequestParameters param, HttpServletResponse response) throws IOException {
        LOGGER.info(param.toString());
        long elapsedStart = System.nanoTime();
        final StringWriter stringWriter = new StringWriter();
        final PrintWriter resultWriter = new PrintWriter(stringWriter);

        SessionConfig sessionConfig = createSessionConfig(param, resultWriter);
        response.setCharacterEncoding("utf-8");
        response.setContentType(MediaType.JSON.str());

        int respCode = HttpServletResponse.SC_OK;
        Stats stats = new Stats();
        long execStart = -1;
        long execEnd = -1;

        resultWriter.print("{\n");
        printRequestId(resultWriter);
        printClientContextID(resultWriter, param);
        printSignature(resultWriter);
        printType(resultWriter, sessionConfig);
        try {
            final IClusterManagementWork.ClusterState clusterState = ClusterStateManager.INSTANCE.getState();
            if (clusterState != IClusterManagementWork.ClusterState.ACTIVE) {
                // using a plain IllegalStateException here to get into the right catch clause for a 500
                throw new IllegalStateException("Cannot execute request, cluster is " + clusterState);
            }
            if (param.statement == null || param.statement.isEmpty()) {
                throw new AsterixException("Empty request, no statement provided");
            }
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
            IParser parser = compilationProvider.getParserFactory().createParser(param.statement);
            List<Statement> aqlStatements = parser.parse();
            MetadataManager.INSTANCE.init();
            IStatementExecutor translator = statementExecutorFactory.create(aqlStatements, sessionConfig,
                    compilationProvider);
            execStart = System.nanoTime();
            translator.compileAndExecute(hcc, hds, QueryTranslator.ResultDelivery.SYNC, stats);
            execEnd = System.nanoTime();
            printStatus(resultWriter, ResultStatus.SUCCESS);
        } catch (AsterixException | TokenMgrError | org.apache.asterix.aqlplus.parser.TokenMgrError pe) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, pe.getMessage(), pe);
            printError(resultWriter, pe);
            printStatus(resultWriter, ResultStatus.FATAL);
            respCode = HttpServletResponse.SC_BAD_REQUEST;
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            printError(resultWriter, e);
            printStatus(resultWriter, ResultStatus.FATAL);
            respCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
        } finally {
            if (execStart == -1) {
                execEnd = -1;
            } else if (execEnd == -1) {
                execEnd = System.nanoTime();
            }
        }
        printMetrics(resultWriter, System.nanoTime() - elapsedStart, execEnd - execStart, stats.getCount(),
                stats.getSize());
        resultWriter.print("}\n");
        resultWriter.flush();
        String result = stringWriter.toString();

        GlobalConfig.ASTERIX_LOGGER.log(Level.FINE, result);

        response.setStatus(respCode);
        response.getWriter().print(result);
        if (response.getWriter().checkError()) {
            LOGGER.warning("Error flushing output writer");
        }
    }
}
