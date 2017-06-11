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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.api.http.servlet.ServletConstants;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.aql.parser.TokenMgrError;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.runtime.utils.ClusterStateManager;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.IStatementExecutorContext;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryServiceServlet extends AbstractQueryApiServlet {
    private static final Logger LOGGER = Logger.getLogger(QueryServiceServlet.class.getName());
    protected final ILangExtension.Language queryLanguage;
    private final ILangCompilationProvider compilationProvider;
    private final IStatementExecutorFactory statementExecutorFactory;
    private final IStorageComponentProvider componentProvider;
    private final IStatementExecutorContext queryCtx;
    protected final IServiceContext serviceCtx;

    public QueryServiceServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
            ILangExtension.Language queryLanguage, ILangCompilationProvider compilationProvider,
            IStatementExecutorFactory statementExecutorFactory, IStorageComponentProvider componentProvider) {
        super(appCtx, ctx, paths);
        this.queryLanguage = queryLanguage;
        this.compilationProvider = compilationProvider;
        this.statementExecutorFactory = statementExecutorFactory;
        this.componentProvider = componentProvider;
        this.queryCtx = (IStatementExecutorContext) ctx.get(ServletConstants.RUNNING_QUERIES_ATTR);
        this.serviceCtx = (IServiceContext) ctx.get(ServletConstants.SERVICE_CONTEXT_ATTR);
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) {
        try {
            handleRequest(getRequestParameters(request), response);
        } catch (IOException e) {
            // Servlet methods should not throw exceptions
            // http://cwe.mitre.org/data/definitions/600.html
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public enum Parameter {
        STATEMENT("statement"),
        FORMAT("format"),
        CLIENT_ID("client_context_id"),
        PRETTY("pretty"),
        MODE("mode");

        private final String str;

        Parameter(String str) {
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

    private enum Metrics {
        ELAPSED_TIME("elapsedTime"),
        EXECUTION_TIME("executionTime"),
        RESULT_COUNT("resultCount"),
        RESULT_SIZE("resultSize"),
        ERROR_COUNT("errorCount");

        private final String str;

        Metrics(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    public enum TimeUnit {
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

        public static String formatNanos(long nanoTime) {
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
        String host;
        String path;
        String statement;
        String format;
        boolean pretty;
        String clientContextID;
        String mode;

        @Override
        public String toString() {
            try {
                ObjectMapper om = new ObjectMapper();
                ObjectNode on = om.createObjectNode();
                on.put("host", host);
                on.put("path", path);
                on.put("statement", JSONUtil.escape(new StringBuilder(), statement).toString());
                on.put("pretty", pretty);
                on.put("mode", mode);
                on.put("clientContextID", clientContextID);
                return om.writer(new MinimalPrettyPrinter()).writeValueAsString(on);
            } catch (JsonProcessingException e) { // NOSONAR
                return e.getMessage();
            }
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
            if (format.startsWith(HttpUtil.ContentType.CSV)) {
                return SessionConfig.OutputFormat.CSV;
            }
            if (format.equals(HttpUtil.ContentType.APPLICATION_ADM)) {
                return SessionConfig.OutputFormat.ADM;
            }
            if (format.startsWith(HttpUtil.ContentType.APPLICATION_JSON)) {
                return Boolean.parseBoolean(getParameterValue(format, Attribute.LOSSLESS.str()))
                        ? SessionConfig.OutputFormat.LOSSLESS_JSON : SessionConfig.OutputFormat.CLEAN_JSON;
            }
        }
        return SessionConfig.OutputFormat.CLEAN_JSON;
    }

    private static SessionOutput createSessionOutput(RequestParameters param, String handleUrl,
            PrintWriter resultWriter) {
        SessionOutput.ResultDecorator resultPrefix = ResultUtil.createPreResultDecorator();
        SessionOutput.ResultDecorator resultPostfix = ResultUtil.createPostResultDecorator();
        SessionOutput.ResultAppender appendHandle = ResultUtil.createResultHandleAppender(handleUrl);
        SessionOutput.ResultAppender appendStatus = ResultUtil.createResultStatusAppender();

        SessionConfig.OutputFormat format = getFormat(param.format);
        SessionConfig sessionConfig = new SessionConfig(format);
        sessionConfig.set(SessionConfig.FORMAT_WRAPPER_ARRAY, true);
        sessionConfig.set(SessionConfig.FORMAT_INDENT_JSON, param.pretty);
        sessionConfig.set(SessionConfig.FORMAT_QUOTE_RECORD,
                format != SessionConfig.OutputFormat.CLEAN_JSON && format != SessionConfig.OutputFormat.LOSSLESS_JSON);
        sessionConfig.set(SessionConfig.FORMAT_CSV_HEADER, format == SessionConfig.OutputFormat.CSV
                && "present".equals(getParameterValue(param.format, Attribute.HEADER.str())));
        return new SessionOutput(sessionConfig, resultWriter, resultPrefix, resultPostfix, appendHandle, appendStatus);
    }

    private static void printClientContextID(PrintWriter pw, RequestParameters params) {
        if (params.clientContextID != null && !params.clientContextID.isEmpty()) {
            ResultUtil.printField(pw, ResultFields.CLIENT_ID.str(), params.clientContextID);
        }
    }

    private static void printSignature(PrintWriter pw) {
        ResultUtil.printField(pw, ResultFields.SIGNATURE.str(), "*");
    }

    private static void printType(PrintWriter pw, SessionConfig sessionConfig) {
        switch (sessionConfig.fmt()) {
            case ADM:
                ResultUtil.printField(pw, ResultFields.TYPE.str(), HttpUtil.ContentType.APPLICATION_ADM);
                break;
            case CSV:
                String contentType = HttpUtil.ContentType.CSV + "; header="
                        + (sessionConfig.is(SessionConfig.FORMAT_CSV_HEADER) ? "present" : "absent");
                ResultUtil.printField(pw, ResultFields.TYPE.str(), contentType);
                break;
            default:
                break;
        }
    }

    private static void printMetrics(PrintWriter pw, long elapsedTime, long executionTime, long resultCount,
            long resultSize, long errorCount) {
        boolean hasErrors = errorCount != 0;
        pw.print("\t\"");
        pw.print(ResultFields.METRICS.str());
        pw.print("\": {\n");
        pw.print("\t");
        ResultUtil.printField(pw, Metrics.ELAPSED_TIME.str(), TimeUnit.formatNanos(elapsedTime));
        pw.print("\t");
        ResultUtil.printField(pw, Metrics.EXECUTION_TIME.str(), TimeUnit.formatNanos(executionTime));
        pw.print("\t");
        ResultUtil.printField(pw, Metrics.RESULT_COUNT.str(), resultCount, true);
        pw.print("\t");
        ResultUtil.printField(pw, Metrics.RESULT_SIZE.str(), resultSize, hasErrors);
        if (hasErrors) {
            pw.print("\t");
            ResultUtil.printField(pw, Metrics.ERROR_COUNT.str(), errorCount, false);
        }
        pw.print("\t}\n");
    }

    private String getOptText(JsonNode node, String fieldName) {
        final JsonNode value = node.get(fieldName);
        return value != null ? value.asText() : null;
    }

    private boolean getOptBoolean(JsonNode node, String fieldName, boolean defaultValue) {
        final JsonNode value = node.get(fieldName);
        return value != null ? value.asBoolean() : defaultValue;
    }

    private RequestParameters getRequestParameters(IServletRequest request) throws IOException {
        final String contentTypeParam = request.getHttpRequest().headers().get(HttpHeaderNames.CONTENT_TYPE);
        int sep = contentTypeParam.indexOf(';');
        final String contentType = sep < 0 ? contentTypeParam.trim() : contentTypeParam.substring(0, sep).trim();
        RequestParameters param = new RequestParameters();
        param.host = host(request);
        param.path = servletPath(request);
        if (HttpUtil.ContentType.APPLICATION_JSON.equals(contentType)) {
            try {
                JsonNode jsonRequest = new ObjectMapper().readTree(HttpUtil.getRequestBody(request));
                param.statement = jsonRequest.get(Parameter.STATEMENT.str()).asText();
                param.format = toLower(getOptText(jsonRequest, Parameter.FORMAT.str()));
                param.pretty = getOptBoolean(jsonRequest, Parameter.PRETTY.str(), false);
                param.mode = toLower(getOptText(jsonRequest, Parameter.MODE.str()));
                param.clientContextID = getOptText(jsonRequest, Parameter.CLIENT_ID.str());
            } catch (JsonParseException | JsonMappingException e) {
                // if the JSON parsing fails, the statement is empty and we get an empty statement error
                GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        } else {
            param.statement = request.getParameter(Parameter.STATEMENT.str());
            if (param.statement == null) {
                param.statement = HttpUtil.getRequestBody(request);
            }
            param.format = toLower(request.getParameter(Parameter.FORMAT.str()));
            param.pretty = Boolean.parseBoolean(request.getParameter(Parameter.PRETTY.str()));
            param.mode = toLower(request.getParameter(Parameter.MODE.str()));
            param.clientContextID = request.getParameter(Parameter.CLIENT_ID.str());
        }
        return param;
    }

    private static ResultDelivery parseResultDelivery(String mode) {
        if ("async".equals(mode)) {
            return ResultDelivery.ASYNC;
        } else if ("deferred".equals(mode)) {
            return ResultDelivery.DEFERRED;
        } else {
            return ResultDelivery.IMMEDIATE;
        }
    }

    private static String handlePath(ResultDelivery delivery) {
        switch (delivery) {
            case ASYNC:
                return "/status/";
            case DEFERRED:
                return "/result/";
            case IMMEDIATE:
            default:
                return "";
        }
    }

    /**
     * Determines the URL for a result handle based on the host and the path of the incoming request and the result
     * delivery mode. Usually there will be a "status" endpoint for ASYNC requests that exposes the status of the
     * execution and a "result" endpoint for DEFERRED requests that will deliver the result for a successful execution.
     *
     * @param host
     *            hostname used for this request
     * @param path
     *            servlet path for this request
     * @param delivery
     *            ResultDelivery mode for this request
     * @return a handle (URL) that allows a client to access further information for this request
     */
    protected String getHandleUrl(String host, String path, ResultDelivery delivery) {
        return "http://" + host + path + handlePath(delivery);
    }

    private void handleRequest(RequestParameters param, IServletResponse response) throws IOException {
        LOGGER.info(param.toString());
        long elapsedStart = System.nanoTime();
        final StringWriter stringWriter = new StringWriter();
        final PrintWriter resultWriter = new PrintWriter(stringWriter);

        ResultDelivery delivery = parseResultDelivery(param.mode);

        String handleUrl = getHandleUrl(param.host, param.path, delivery);
        SessionOutput sessionOutput = createSessionOutput(param, handleUrl, resultWriter);
        SessionConfig sessionConfig = sessionOutput.config();
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, HttpUtil.Encoding.UTF8);

        HttpResponseStatus status = HttpResponseStatus.OK;
        Stats stats = new Stats();
        long[] execStartEnd = new long[] { -1, -1 };

        resultWriter.print("{\n");
        printRequestId(resultWriter);
        printClientContextID(resultWriter, param);
        printSignature(resultWriter);
        printType(resultWriter, sessionConfig);
        long errorCount = 1; // so far we just return 1 error
        try {
            if (param.statement == null || param.statement.isEmpty()) {
                throw new AsterixException("Empty request, no statement provided");
            }
            String statementsText = param.statement + ";";
            executeStatement(statementsText, sessionOutput, delivery, stats, param, handleUrl, execStartEnd);
            if (ResultDelivery.IMMEDIATE == delivery || ResultDelivery.DEFERRED == delivery) {
                ResultUtil.printStatus(sessionOutput, ResultStatus.SUCCESS);
            }
            errorCount = 0;
        } catch (AlgebricksException | TokenMgrError | org.apache.asterix.aqlplus.parser.TokenMgrError pe) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.INFO, pe.getMessage(), pe);
            ResultUtil.printError(resultWriter, pe);
            ResultUtil.printStatus(sessionOutput, ResultStatus.FATAL);
            status = HttpResponseStatus.BAD_REQUEST;
        } catch (HyracksException pe) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.WARNING, pe.getMessage(), pe);
            ResultUtil.printError(resultWriter, pe);
            ResultUtil.printStatus(sessionOutput, ResultStatus.FATAL);
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        } catch (Exception e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, "Unexpected exception", e);
            ResultUtil.printError(resultWriter, e);
            ResultUtil.printStatus(sessionOutput, ResultStatus.FATAL);
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        } finally {
            if (execStartEnd[0] == -1) {
                execStartEnd[1] = -1;
            } else if (execStartEnd[1] == -1) {
                execStartEnd[1] = System.nanoTime();
            }
        }
        printMetrics(resultWriter, System.nanoTime() - elapsedStart, execStartEnd[1] - execStartEnd[0],
                stats.getCount(), stats.getSize(), errorCount);
        resultWriter.print("}\n");
        resultWriter.flush();
        String result = stringWriter.toString();

        GlobalConfig.ASTERIX_LOGGER.log(Level.FINE, result);

        response.setStatus(status);
        response.writer().print(result);
        if (response.writer().checkError()) {
            LOGGER.warning("Error flushing output writer");
        }
    }

    protected void executeStatement(String statementsText, SessionOutput sessionOutput, ResultDelivery delivery,
            IStatementExecutor.Stats stats, RequestParameters param, String handleUrl, long[] outExecStartEnd)
            throws Exception {
        IClusterManagementWork.ClusterState clusterState = ClusterStateManager.INSTANCE.getState();
        if (clusterState != IClusterManagementWork.ClusterState.ACTIVE) {
            // using a plain IllegalStateException here to get into the right catch clause for a 500
            throw new IllegalStateException("Cannot execute request, cluster is " + clusterState);
        }
        IParser parser = compilationProvider.getParserFactory().createParser(statementsText);
        List<Statement> statements = parser.parse();
        MetadataManager.INSTANCE.init();
        IStatementExecutor translator = statementExecutorFactory.create((ICcApplicationContext) appCtx, statements,
                sessionOutput, compilationProvider, componentProvider);
        outExecStartEnd[0] = System.nanoTime();
        translator.compileAndExecute(getHyracksClientConnection(), getHyracksDataset(), delivery, null, stats,
                param.clientContextID, queryCtx);
        outExecStartEnd[1] = System.nanoTime();
    }
}