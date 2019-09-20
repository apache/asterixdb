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

import static org.apache.asterix.common.exceptions.ErrorCode.ASTERIX;
import static org.apache.asterix.common.exceptions.ErrorCode.NO_STATEMENT_PROVIDED;
import static org.apache.asterix.common.exceptions.ErrorCode.REJECT_BAD_CLUSTER_STATE;
import static org.apache.asterix.common.exceptions.ErrorCode.REJECT_NODE_UNREGISTERED;
import static org.apache.asterix.common.exceptions.ErrorCode.REQUEST_TIMEOUT;
import static org.apache.hyracks.api.exceptions.ErrorCode.HYRACKS;
import static org.apache.hyracks.api.exceptions.ErrorCode.JOB_REQUIREMENTS_EXCEED_CAPACITY;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.app.result.ExecutionError;
import org.apache.asterix.app.result.ExecutionWarning;
import org.apache.asterix.app.result.ResponseMetrics;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.result.fields.ClientContextIdPrinter;
import org.apache.asterix.app.result.fields.ErrorsPrinter;
import org.apache.asterix.app.result.fields.MetricsPrinter;
import org.apache.asterix.app.result.fields.ParseOnlyResultPrinter;
import org.apache.asterix.app.result.fields.PlansPrinter;
import org.apache.asterix.app.result.fields.ProfilePrinter;
import org.apache.asterix.app.result.fields.RequestIdPrinter;
import org.apache.asterix.app.result.fields.SignaturePrinter;
import org.apache.asterix.app.result.fields.StatusPrinter;
import org.apache.asterix.app.result.fields.TypePrinter;
import org.apache.asterix.app.result.fields.WarningsPrinter;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.ICodedMessage;
import org.apache.asterix.common.api.IReceptionist;
import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.lang.aql.parser.TokenMgrError;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.translator.ExecutionPlans;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.asterix.translator.ResultProperties;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryServiceServlet extends AbstractQueryApiServlet {
    protected static final Logger LOGGER = LogManager.getLogger();
    protected final ILangExtension.Language queryLanguage;
    private final ILangCompilationProvider compilationProvider;
    private final IStatementExecutorFactory statementExecutorFactory;
    private final IStorageComponentProvider componentProvider;
    private final IReceptionist receptionist;
    protected final IServiceContext serviceCtx;
    protected final Function<IServletRequest, Map<String, String>> optionalParamProvider;
    protected String hostName;

    public QueryServiceServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
            ILangExtension.Language queryLanguage, ILangCompilationProvider compilationProvider,
            IStatementExecutorFactory statementExecutorFactory, IStorageComponentProvider componentProvider,
            Function<IServletRequest, Map<String, String>> optionalParamProvider) {
        super(appCtx, ctx, paths);
        this.queryLanguage = queryLanguage;
        this.compilationProvider = compilationProvider;
        this.statementExecutorFactory = statementExecutorFactory;
        this.componentProvider = componentProvider;
        receptionist = appCtx.getReceptionist();
        this.serviceCtx = (IServiceContext) ctx.get(ServletConstants.SERVICE_CONTEXT_ATTR);
        this.optionalParamProvider = optionalParamProvider;
        try {
            this.hostName =
                    InetAddress.getByName(serviceCtx.getAppConfig().getString(CCConfig.Option.CLUSTER_PUBLIC_ADDRESS))
                            .getHostName();
        } catch (UnknownHostException e) {
            LOGGER.warn("Reverse DNS not properly configured, CORS defaulting to localhost", e);
            this.hostName = "localhost";
        }
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) throws IOException {
        handleRequest(request, response);
    }

    @Override
    protected void options(IServletRequest request, IServletResponse response) throws Exception {
        if (request.getHeader("Origin") != null) {
            response.setHeader("Access-Control-Allow-Origin", request.getHeader("Origin"));
        }
        response.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
        response.setStatus(HttpResponseStatus.OK);
    }

    public enum Parameter {
        ARGS("args"),
        STATEMENT("statement"),
        FORMAT("format"),
        CLIENT_ID("client_context_id"),
        PRETTY("pretty"),
        MODE("mode"),
        TIMEOUT("timeout"),
        PLAN_FORMAT("plan-format"),
        MAX_RESULT_READS("max-result-reads"),
        EXPRESSION_TREE("expression-tree"),
        REWRITTEN_EXPRESSION_TREE("rewritten-expression-tree"),
        LOGICAL_PLAN("logical-plan"),
        OPTIMIZED_LOGICAL_PLAN("optimized-logical-plan"),
        PARSE_ONLY("parse-only"),
        READ_ONLY("readonly"),
        JOB("job"),
        PROFILE("profile"),
        SIGNATURE("signature"),
        MULTI_STATEMENT("multi-statement"),
        MAX_WARNINGS("max-warnings");

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

    protected static final class RequestExecutionState {
        private long execStart = -1;
        private long execEnd = -1;
        private ResultStatus resultStatus = ResultStatus.SUCCESS;
        private HttpResponseStatus httpResponseStatus = HttpResponseStatus.OK;

        public void setStatus(ResultStatus resultStatus, HttpResponseStatus httpResponseStatus) {
            this.resultStatus = resultStatus;
            this.httpResponseStatus = httpResponseStatus;
        }

        ResultStatus getResultStatus() {
            return resultStatus;
        }

        HttpResponseStatus getHttpStatus() {
            return httpResponseStatus;
        }

        void start() {
            execStart = System.nanoTime();
        }

        void end() {
            execEnd = System.nanoTime();
        }

        void finish() {
            if (execStart == -1) {
                execEnd = -1;
            } else if (execEnd == -1) {
                execEnd = System.nanoTime();
            }
        }

        long duration() {
            return execEnd - execStart;
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
            if (isJsonFormat(format)) {
                return Boolean.parseBoolean(getParameterValue(format, Attribute.LOSSLESS.str()))
                        ? SessionConfig.OutputFormat.LOSSLESS_JSON : SessionConfig.OutputFormat.CLEAN_JSON;
            }
        }
        return SessionConfig.OutputFormat.CLEAN_JSON;
    }

    private static SessionOutput createSessionOutput(PrintWriter resultWriter) {
        SessionOutput.ResultDecorator resultPrefix = ResultUtil.createPreResultDecorator();
        SessionOutput.ResultDecorator resultPostfix = ResultUtil.createPostResultDecorator();
        SessionOutput.ResultAppender appendStatus = ResultUtil.createResultStatusAppender();
        SessionConfig sessionConfig = new SessionConfig(SessionConfig.OutputFormat.CLEAN_JSON);
        return new SessionOutput(sessionConfig, resultWriter, resultPrefix, resultPostfix, null, appendStatus);
    }

    protected String getOptText(JsonNode node, Parameter parameter) {
        return getOptText(node, parameter.str());
    }

    protected String getOptText(JsonNode node, String fieldName) {
        final JsonNode value = node.get(fieldName);
        return value != null ? value.asText() : null;
    }

    protected boolean getOptBoolean(JsonNode node, Parameter parameter, boolean defaultValue) {
        return getOptBoolean(node, parameter.str(), defaultValue);
    }

    protected boolean getOptBoolean(JsonNode node, String fieldName, boolean defaultValue) {
        final JsonNode value = node.get(fieldName);
        return value != null ? value.asBoolean() : defaultValue;
    }

    protected long getOptLong(JsonNode node, Parameter parameter, long defaultValue) {
        final JsonNode value = node.get(parameter.str);
        return value != null ? Integer.parseInt(value.asText()) : defaultValue;
    }

    protected long getOptLong(IServletRequest request, Parameter parameter, long defaultValue) {
        String value = getParameter(request, parameter);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    protected boolean getOptBoolean(IServletRequest request, Parameter parameter, boolean defaultValue) {
        String value = getParameter(request, parameter);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    protected String getParameter(IServletRequest request, Parameter parameter) {
        return request.getParameter(parameter.str());
    }

    @FunctionalInterface
    interface CheckedFunction<I, O> {
        O apply(I requestParamValue) throws IOException;
    }

    private <R, P> Map<String, JsonNode> getOptStatementParameters(R request, Iterator<String> paramNameIter,
            BiFunction<R, String, P> paramValueAccessor, CheckedFunction<P, JsonNode> paramValueParser)
            throws IOException {
        Map<String, JsonNode> result = null;
        while (paramNameIter.hasNext()) {
            String paramName = paramNameIter.next();
            String stmtParamName = extractStatementParameterName(paramName);
            if (stmtParamName != null) {
                if (result == null) {
                    result = new HashMap<>();
                }
                P paramValue = paramValueAccessor.apply(request, paramName);
                JsonNode stmtParamValue = paramValueParser.apply(paramValue);
                result.put(stmtParamName, stmtParamValue);
            } else if (Parameter.ARGS.str().equals(paramName)) {
                if (result == null) {
                    result = new HashMap<>();
                }
                P paramValue = paramValueAccessor.apply(request, paramName);
                JsonNode stmtParamValue = paramValueParser.apply(paramValue);
                if (stmtParamValue.isArray()) {
                    for (int i = 0, ln = stmtParamValue.size(); i < ln; i++) {
                        result.put(String.valueOf(i + 1), stmtParamValue.get(i));
                    }
                }
            }
        }
        return result;
    }

    protected void setRequestParam(IServletRequest request, QueryServiceRequestParameters param,
            Map<String, String> optionalParams) throws IOException, AlgebricksException {
        param.setHost(host(request));
        param.setPath(servletPath(request));
        String contentType = HttpUtil.getContentTypeOnly(request);
        if (HttpUtil.ContentType.APPLICATION_JSON.equals(contentType)) {
            try {
                setParamFromJSON(request, param, optionalParams);
            } catch (JsonParseException | JsonMappingException e) {
                // if the JSON parsing fails, the statement is empty and we get an empty statement error
                GlobalConfig.ASTERIX_LOGGER.log(Level.ERROR, e.getMessage(), e);
            }
        } else {
            setParamFromRequest(request, param, optionalParams);
        }
    }

    private void setParamFromJSON(IServletRequest request, QueryServiceRequestParameters param,
            Map<String, String> optionalParameters) throws IOException {
        JsonNode jsonRequest = OBJECT_MAPPER.readTree(HttpUtil.getRequestBody(request));
        param.setStatement(getOptText(jsonRequest, Parameter.STATEMENT));
        param.setFormat(toLower(getOptText(jsonRequest, Parameter.FORMAT)));
        param.setPretty(getOptBoolean(jsonRequest, Parameter.PRETTY, false));
        param.setMode(toLower(getOptText(jsonRequest, Parameter.MODE)));
        param.setClientContextID(getOptText(jsonRequest, Parameter.CLIENT_ID));
        param.setTimeout(getOptText(jsonRequest, Parameter.TIMEOUT));
        param.setMaxResultReads(getOptText(jsonRequest, Parameter.MAX_RESULT_READS));
        param.setPlanFormat(getOptText(jsonRequest, Parameter.PLAN_FORMAT));
        param.setExpressionTree(getOptBoolean(jsonRequest, Parameter.EXPRESSION_TREE, false));
        param.setRewrittenExpressionTree(getOptBoolean(jsonRequest, Parameter.REWRITTEN_EXPRESSION_TREE, false));
        param.setLogicalPlan(getOptBoolean(jsonRequest, Parameter.LOGICAL_PLAN, false));
        param.setParseOnly(getOptBoolean(jsonRequest, Parameter.PARSE_ONLY, false));
        param.setReadOnly(getOptBoolean(jsonRequest, Parameter.READ_ONLY, false));
        param.setOptimizedLogicalPlan(getOptBoolean(jsonRequest, Parameter.OPTIMIZED_LOGICAL_PLAN, false));
        param.setJob(getOptBoolean(jsonRequest, Parameter.JOB, false));
        param.setProfile(getOptBoolean(jsonRequest, Parameter.PROFILE, false));
        param.setSignature(getOptBoolean(jsonRequest, Parameter.SIGNATURE, true));
        param.setStatementParams(
                getOptStatementParameters(jsonRequest, jsonRequest.fieldNames(), JsonNode::get, v -> v));
        param.setMultiStatement(getOptBoolean(jsonRequest, Parameter.MULTI_STATEMENT, true));
        param.setMaxWarnings(
                getOptLong(jsonRequest, Parameter.MAX_WARNINGS, QueryServiceRequestParameters.DEFAULT_MAX_WARNINGS));
        setJsonOptionalParameters(jsonRequest, param, optionalParameters);
    }

    protected void setJsonOptionalParameters(JsonNode jsonRequest, QueryServiceRequestParameters param,
            Map<String, String> optionalParameters) {
        // allows extensions to set extra parameters
    }

    private void setParamFromRequest(IServletRequest request, QueryServiceRequestParameters param,
            Map<String, String> optionalParameters) throws IOException {
        param.setStatement(getParameter(request, Parameter.STATEMENT));
        param.setFormat(toLower(getParameter(request, Parameter.FORMAT)));
        param.setPretty(Boolean.parseBoolean(getParameter(request, Parameter.PRETTY)));
        param.setMode(toLower(getParameter(request, Parameter.MODE)));
        param.setClientContextID(getParameter(request, Parameter.CLIENT_ID));
        param.setTimeout(getParameter(request, Parameter.TIMEOUT));
        param.setMaxResultReads(getParameter(request, Parameter.MAX_RESULT_READS));
        param.setPlanFormat(getParameter(request, Parameter.PLAN_FORMAT));
        param.setExpressionTree(getOptBoolean(request, Parameter.EXPRESSION_TREE, false));
        param.setRewrittenExpressionTree(getOptBoolean(request, Parameter.REWRITTEN_EXPRESSION_TREE, false));
        param.setLogicalPlan(getOptBoolean(request, Parameter.LOGICAL_PLAN, false));
        param.setParseOnly(getOptBoolean(request, Parameter.PARSE_ONLY, false));
        param.setReadOnly(getOptBoolean(request, Parameter.READ_ONLY, false));
        param.setOptimizedLogicalPlan(getOptBoolean(request, Parameter.OPTIMIZED_LOGICAL_PLAN, false));
        param.setJob(getOptBoolean(request, Parameter.JOB, false));
        param.setProfile(getOptBoolean(request, Parameter.PROFILE, false));
        param.setSignature(getOptBoolean(request, Parameter.SIGNATURE, true));
        param.setMultiStatement(getOptBoolean(request, Parameter.MULTI_STATEMENT, true));
        param.setMaxWarnings(
                getOptLong(request, Parameter.MAX_WARNINGS, QueryServiceRequestParameters.DEFAULT_MAX_WARNINGS));
        try {
            param.setStatementParams(getOptStatementParameters(request, request.getParameterNames().iterator(),
                    IServletRequest::getParameter, OBJECT_MAPPER::readTree));
        } catch (JsonParseException | JsonMappingException e) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.ERROR, e.getMessage(), e);
        }
        setOptionalParameters(request, param, optionalParameters);
    }

    protected void setOptionalParameters(IServletRequest request, QueryServiceRequestParameters param,
            Map<String, String> optionalParameters) {
        // allows extensions to set extra parameters
    }

    private void setAccessControlHeaders(IServletRequest request, IServletResponse response) throws IOException {
        //CORS
        if (request.getHeader("Origin") != null) {
            response.setHeader("Access-Control-Allow-Origin", request.getHeader("Origin"));
        }
        response.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
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

    private void handleRequest(IServletRequest request, IServletResponse response) throws IOException {
        final IRequestReference requestRef = receptionist.welcome(request);
        long elapsedStart = System.nanoTime();
        long errorCount = 1;
        Stats stats = new Stats();
        RequestExecutionState execution = new RequestExecutionState();
        List<Warning> warnings = new ArrayList<>();
        Charset resultCharset = HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
        PrintWriter httpWriter = response.writer();
        SessionOutput sessionOutput = createSessionOutput(httpWriter);
        QueryServiceRequestParameters param = newRequestParameters();
        ResponsePrinter responsePrinter = new ResponsePrinter(sessionOutput);
        ResultDelivery delivery = ResultDelivery.IMMEDIATE;
        try {
            // buffer the output until we are ready to set the status of the response message correctly
            responsePrinter.begin();
            Map<String, String> optionalParams = null;
            if (optionalParamProvider != null) {
                optionalParams = optionalParamProvider.apply(request);
            }
            setRequestParam(request, param, optionalParams);
            LOGGER.info(() -> "handleRequest: " + LogRedactionUtil.userData(param.toString()));
            delivery = parseResultDelivery(param.getMode());
            setSessionConfig(sessionOutput, param, delivery);
            final ResultProperties resultProperties = param.getMaxResultReads() == null ? new ResultProperties(delivery)
                    : new ResultProperties(delivery, Long.parseLong(param.getMaxResultReads()));
            buildResponseHeaders(requestRef, sessionOutput, param, responsePrinter, delivery);
            responsePrinter.printHeaders();
            validateStatement(param.getStatement());
            String statementsText = param.getStatement() + ";";
            if (param.isParseOnly()) {
                ResultUtil.ParseOnlyResult parseOnlyResult = parseStatement(statementsText);
                setAccessControlHeaders(request, response);
                response.setStatus(execution.getHttpStatus());
                responsePrinter.addResultPrinter(new ParseOnlyResultPrinter(parseOnlyResult));
            } else {
                Map<String, byte[]> statementParams = org.apache.asterix.app.translator.RequestParameters
                        .serializeParameterValues(param.getStatementParams());
                setAccessControlHeaders(request, response);
                response.setStatus(execution.getHttpStatus());
                stats.setType(param.isProfile() ? Stats.ProfileType.FULL : Stats.ProfileType.COUNTS);
                executeStatement(requestRef, statementsText, sessionOutput, resultProperties, stats, param, execution,
                        optionalParams, statementParams, responsePrinter, warnings);
            }
            errorCount = 0;
        } catch (Exception | TokenMgrError | org.apache.asterix.aqlplus.parser.TokenMgrError e) {
            handleExecuteStatementException(e, execution, param);
            response.setStatus(execution.getHttpStatus());
            requestFailed(e, responsePrinter);
        } finally {
            execution.finish();
        }
        responsePrinter.printResults();
        buildResponseFooters(elapsedStart, errorCount, stats, execution, resultCharset, responsePrinter, delivery);
        responsePrinter.printFooters();
        responsePrinter.end();
        if (sessionOutput.out().checkError()) {
            LOGGER.warn("Error flushing output writer");
        }
    }

    protected void buildResponseHeaders(IRequestReference requestRef, SessionOutput sessionOutput,
            QueryServiceRequestParameters param, ResponsePrinter responsePrinter, ResultDelivery delivery) {
        responsePrinter.addHeaderPrinter(new RequestIdPrinter(requestRef.getUuid()));
        if (param.getClientContextID() != null && !param.getClientContextID().isEmpty()) {
            responsePrinter.addHeaderPrinter(new ClientContextIdPrinter(param.getClientContextID()));
        }
        if (param.isSignature() && delivery != ResultDelivery.ASYNC && !param.isParseOnly()) {
            responsePrinter.addHeaderPrinter(SignaturePrinter.INSTANCE);
        }
        if (sessionOutput.config().fmt() == SessionConfig.OutputFormat.ADM
                || sessionOutput.config().fmt() == SessionConfig.OutputFormat.CSV) {
            responsePrinter.addHeaderPrinter(new TypePrinter(sessionOutput.config()));
        }
    }

    protected void buildResponseResults(ResponsePrinter responsePrinter, SessionOutput sessionOutput,
            ExecutionPlans plans, List<Warning> warnings) {
        responsePrinter.addResultPrinter(new PlansPrinter(plans, sessionOutput.config().getPlanFormat()));
        if (!warnings.isEmpty()) {
            List<ICodedMessage> codedWarnings = new ArrayList<>();
            warnings.forEach(warn -> codedWarnings.add(ExecutionWarning.of(warn)));
            responsePrinter.addResultPrinter(new WarningsPrinter(codedWarnings));
        }
    }

    protected void buildResponseFooters(long elapsedStart, long errorCount, Stats stats,
            RequestExecutionState execution, Charset resultCharset, ResponsePrinter responsePrinter,
            ResultDelivery delivery) {
        if (ResultDelivery.ASYNC != delivery) {
            // in case of ASYNC delivery, the status is printed by query translator
            responsePrinter.addFooterPrinter(new StatusPrinter(execution.getResultStatus()));
        }
        final ResponseMetrics metrics = ResponseMetrics.of(System.nanoTime() - elapsedStart, execution.duration(),
                stats.getCount(), stats.getSize(), stats.getProcessedObjects(), errorCount,
                stats.getTotalWarningsCount(), stats.getDiskIoCount());
        responsePrinter.addFooterPrinter(new MetricsPrinter(metrics, resultCharset));
        if (isPrintingProfile(stats)) {
            responsePrinter.addFooterPrinter(new ProfilePrinter(stats.getJobProfile()));
        }
    }

    protected void validateStatement(String statement) throws RuntimeDataException {
        if (statement == null || statement.isEmpty()) {
            throw new RuntimeDataException(NO_STATEMENT_PROVIDED);
        }
    }

    protected ResultUtil.ParseOnlyResult parseStatement(String statementsText) throws CompilationException {
        IParserFactory factory = compilationProvider.getParserFactory();
        IParser parser = factory.createParser(statementsText);
        List<Statement> stmts = parser.parse();
        QueryTranslator.validateStatements(stmts, true, RequestParameters.NO_CATEGORY_RESTRICTION_MASK);
        Query query = (Query) stmts.get(stmts.size() - 1);
        Set<VariableExpr> extVars =
                compilationProvider.getRewriterFactory().createQueryRewriter().getExternalVariables(query.getBody());
        return new ResultUtil.ParseOnlyResult(extVars);
    }

    protected void executeStatement(IRequestReference requestReference, String statementsText,
            SessionOutput sessionOutput, ResultProperties resultProperties, Stats stats,
            QueryServiceRequestParameters param, RequestExecutionState execution,
            Map<String, String> optionalParameters, Map<String, byte[]> statementParameters,
            ResponsePrinter responsePrinter, List<Warning> warnings) throws Exception {
        IClusterManagementWork.ClusterState clusterState =
                ((ICcApplicationContext) appCtx).getClusterStateManager().getState();
        if (clusterState != IClusterManagementWork.ClusterState.ACTIVE) {
            // using a plain IllegalStateException here to get into the right catch clause for a 500
            throw new IllegalStateException("Cannot execute request, cluster is " + clusterState);
        }
        IParser parser = compilationProvider.getParserFactory().createParser(statementsText);
        List<Statement> statements = parser.parse();
        long maxWarnings = sessionOutput.config().getMaxWarnings();
        parser.getWarnings(warnings, maxWarnings);
        long parserTotalWarningsCount = parser.getTotalWarningsCount();
        MetadataManager.INSTANCE.init();
        IStatementExecutor translator = statementExecutorFactory.create((ICcApplicationContext) appCtx, statements,
                sessionOutput, compilationProvider, componentProvider, responsePrinter);
        execution.start();
        Map<String, IAObject> stmtParams =
                org.apache.asterix.app.translator.RequestParameters.deserializeParameterValues(statementParameters);
        int stmtCategoryRestriction = org.apache.asterix.app.translator.RequestParameters
                .getStatementCategoryRestrictionMask(param.isReadOnly());
        IRequestParameters requestParameters = new org.apache.asterix.app.translator.RequestParameters(requestReference,
                statementsText, getResultSet(), resultProperties, stats, null, param.getClientContextID(),
                optionalParameters, stmtParams, param.isMultiStatement(), stmtCategoryRestriction);
        translator.compileAndExecute(getHyracksClientConnection(), requestParameters);
        execution.end();
        translator.getWarnings(warnings, maxWarnings - warnings.size());
        stats.updateTotalWarningsCount(parserTotalWarningsCount);
        buildResponseResults(responsePrinter, sessionOutput, translator.getExecutionPlans(), warnings);
    }

    protected void handleExecuteStatementException(Throwable t, RequestExecutionState state,
            QueryServiceRequestParameters param) {
        if (t instanceof org.apache.asterix.aqlplus.parser.TokenMgrError || t instanceof TokenMgrError
                || t instanceof AlgebricksException) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("handleException: {}: {}", t.getMessage(), LogRedactionUtil.userData(param.toString()), t);
            } else {
                LOGGER.info(() -> "handleException: " + t.getMessage() + ": "
                        + LogRedactionUtil.userData(param.toString()));
            }
            state.setStatus(ResultStatus.FATAL, HttpResponseStatus.BAD_REQUEST);
        } else if (t instanceof HyracksException) {
            HyracksException he = (HyracksException) t;
            switch (he.getComponent() + he.getErrorCode()) {
                case ASTERIX + REQUEST_TIMEOUT:
                    LOGGER.info(() -> "handleException: request execution timed out: "
                            + LogRedactionUtil.userData(param.toString()));
                    state.setStatus(ResultStatus.TIMEOUT, HttpResponseStatus.OK);
                    break;
                case ASTERIX + REJECT_BAD_CLUSTER_STATE:
                case ASTERIX + REJECT_NODE_UNREGISTERED:
                    LOGGER.warn(() -> "handleException: " + he.getMessage() + ": "
                            + LogRedactionUtil.userData(param.toString()));
                    state.setStatus(ResultStatus.FATAL, HttpResponseStatus.SERVICE_UNAVAILABLE);
                    break;
                case ASTERIX + NO_STATEMENT_PROVIDED:
                case HYRACKS + JOB_REQUIREMENTS_EXCEED_CAPACITY:
                    state.setStatus(ResultStatus.FATAL, HttpResponseStatus.BAD_REQUEST);
                    break;
                default:
                    LOGGER.warn(() -> "handleException: unexpected exception " + he.getMessage() + ": "
                            + LogRedactionUtil.userData(param.toString()), he);
                    state.setStatus(ResultStatus.FATAL, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    break;
            }
        } else {
            LOGGER.warn(() -> "handleException: unexpected exception: " + LogRedactionUtil.userData(param.toString()),
                    t);
            state.setStatus(ResultStatus.FATAL, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void setSessionConfig(SessionOutput sessionOutput, QueryServiceRequestParameters param,
            ResultDelivery delivery) {
        String handleUrl = getHandleUrl(param.getHost(), param.getPath(), delivery);
        sessionOutput.setHandleAppender(ResultUtil.createResultHandleAppender(handleUrl));
        SessionConfig sessionConfig = sessionOutput.config();
        SessionConfig.OutputFormat format = getFormat(param.getFormat());
        SessionConfig.PlanFormat planFormat = SessionConfig.PlanFormat.get(param.getPlanFormat(), param.getPlanFormat(),
                SessionConfig.PlanFormat.JSON, LOGGER);
        sessionConfig.setFmt(format);
        sessionConfig.setPlanFormat(planFormat);
        sessionConfig.setMaxWarnings(param.getMaxWarnings());
        sessionConfig.set(SessionConfig.FORMAT_WRAPPER_ARRAY, true);
        sessionConfig.set(SessionConfig.OOB_EXPR_TREE, param.isExpressionTree());
        sessionConfig.set(SessionConfig.OOB_REWRITTEN_EXPR_TREE, param.isRewrittenExpressionTree());
        sessionConfig.set(SessionConfig.OOB_LOGICAL_PLAN, param.isLogicalPlan());
        sessionConfig.set(SessionConfig.OOB_OPTIMIZED_LOGICAL_PLAN, param.isOptimizedLogicalPlan());
        sessionConfig.set(SessionConfig.OOB_HYRACKS_JOB, param.isJob());
        sessionConfig.set(SessionConfig.FORMAT_INDENT_JSON, param.isPretty());
        sessionConfig.set(SessionConfig.FORMAT_QUOTE_RECORD,
                format != SessionConfig.OutputFormat.CLEAN_JSON && format != SessionConfig.OutputFormat.LOSSLESS_JSON);
        sessionConfig.set(SessionConfig.FORMAT_CSV_HEADER, format == SessionConfig.OutputFormat.CSV
                && "present".equals(getParameterValue(param.getFormat(), Attribute.HEADER.str())));
    }

    protected void requestFailed(Throwable throwable, ResponsePrinter responsePrinter) {
        final ExecutionError executionError = ExecutionError.of(throwable);
        responsePrinter.addResultPrinter(new ErrorsPrinter(Collections.singletonList(executionError)));
    }

    protected QueryServiceRequestParameters newRequestParameters() {
        return new QueryServiceRequestParameters();
    }

    private static boolean isJsonFormat(String format) {
        return format.startsWith(HttpUtil.ContentType.APPLICATION_JSON)
                || format.equalsIgnoreCase(HttpUtil.ContentType.JSON);
    }

    public static String extractStatementParameterName(String name) {
        int ln = name.length();
        if ((ln == 2 || isStatementParameterNameRest(name, 2)) && name.charAt(0) == '$'
                && Character.isLetter(name.charAt(1))) {
            return name.substring(1);
        }
        return null;
    }

    private static boolean isStatementParameterNameRest(CharSequence input, int startIndex) {
        int i = startIndex;
        for (int ln = input.length(); i < ln; i++) {
            char c = input.charAt(i);
            boolean ok = c == '_' || Character.isLetterOrDigit(c);
            if (!ok) {
                return false;
            }
        }
        return i > startIndex;
    }

    private static boolean isPrintingProfile(IStatementExecutor.Stats stats) {
        return stats.getType() == Stats.ProfileType.FULL && stats.getJobProfile() != null;
    }
}
