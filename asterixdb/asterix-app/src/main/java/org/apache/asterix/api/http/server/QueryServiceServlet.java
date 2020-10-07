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
import static org.apache.asterix.common.exceptions.ErrorCode.INVALID_REQ_JSON_VAL;
import static org.apache.asterix.common.exceptions.ErrorCode.INVALID_REQ_PARAM_VAL;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
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
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
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
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    protected void get(IServletRequest request, IServletResponse response) throws IOException {
        handleRequest(request, response, true);
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) throws IOException {
        handleRequest(request, response, false);
    }

    @Override
    protected void options(IServletRequest request, IServletResponse response) throws Exception {
        if (request.getHeader("Origin") != null) {
            response.setHeader("Access-Control-Allow-Origin", request.getHeader("Origin"));
        }
        response.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
        response.setStatus(HttpResponseStatus.OK);
    }

    protected static class RequestExecutionState {
        private long execStart = -1;
        private long execEnd = -1;
        private ResultStatus resultStatus = ResultStatus.FATAL;
        private HttpResponseStatus httpResponseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;

        public void setStatus(ResultStatus resultStatus, HttpResponseStatus httpResponseStatus) {
            this.resultStatus = resultStatus;
            this.httpResponseStatus = httpResponseStatus;
        }

        public ResultStatus getResultStatus() {
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

        public long duration() {
            return execEnd - execStart;
        }

        protected StringBuilder append(StringBuilder sb) {
            return sb.append("ResultStatus: ").append(resultStatus.str()).append(" HTTPStatus: ")
                    .append(String.valueOf(httpResponseStatus));
        }

        @Override
        public String toString() {
            return append(new StringBuilder()).toString();
        }
    }

    private static SessionOutput createSessionOutput(PrintWriter resultWriter) {
        SessionOutput.ResultDecorator resultPrefix = ResultUtil.createPreResultDecorator();
        SessionOutput.ResultDecorator resultPostfix = ResultUtil.createPostResultDecorator();
        SessionOutput.ResultAppender appendStatus = ResultUtil.createResultStatusAppender();
        SessionConfig sessionConfig = new SessionConfig(SessionConfig.OutputFormat.CLEAN_JSON);
        return new SessionOutput(sessionConfig, resultWriter, resultPrefix, resultPostfix, null, appendStatus);
    }

    protected void setRequestParam(IServletRequest request, QueryServiceRequestParameters param,
            Function<IServletRequest, Map<String, String>> optionalParamProvider, RequestExecutionState executionState)
            throws IOException, AlgebricksException {
        Map<String, String> optionalParams = null;
        if (optionalParamProvider != null) {
            optionalParams = optionalParamProvider.apply(request);
        }
        param.setParameters(this, request, optionalParams);
    }

    private void setAccessControlHeaders(IServletRequest request, IServletResponse response) throws IOException {
        //CORS
        if (request.getHeader("Origin") != null) {
            response.setHeader("Access-Control-Allow-Origin", request.getHeader("Origin"));
        }
        response.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
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

    private void handleRequest(IServletRequest request, IServletResponse response, boolean forceReadOnly)
            throws IOException {
        final IRequestReference requestRef = receptionist.welcome(request);
        long elapsedStart = System.nanoTime();
        long errorCount = 1;
        Stats stats = new Stats();
        List<Warning> warnings = new ArrayList<>();
        Charset resultCharset = HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
        PrintWriter httpWriter = response.writer();
        SessionOutput sessionOutput = createSessionOutput(httpWriter);
        ResponsePrinter responsePrinter = new ResponsePrinter(sessionOutput);
        ResultDelivery delivery = ResultDelivery.IMMEDIATE;
        QueryServiceRequestParameters param = newRequestParameters();
        RequestExecutionState executionState = newRequestExecutionState();
        try {
            // buffer the output until we are ready to set the status of the response message correctly
            responsePrinter.begin();
            setRequestParam(request, param, optionalParamProvider, executionState);
            if (forceReadOnly) {
                param.setReadOnly(true);
            }
            LOGGER.info(() -> "handleRequest: " + LogRedactionUtil.statement(param.toString()));
            delivery = param.getMode();
            setSessionConfig(sessionOutput, param, delivery);
            final ResultProperties resultProperties = new ResultProperties(delivery, param.getMaxResultReads());
            buildResponseHeaders(requestRef, sessionOutput, param, responsePrinter, delivery);
            responsePrinter.printHeaders();
            validateStatement(param.getStatement());
            String statementsText = param.getStatement() + ";";
            if (param.isParseOnly()) {
                ResultUtil.ParseOnlyResult parseOnlyResult = parseStatement(statementsText);
                setAccessControlHeaders(request, response);
                executionState.setStatus(ResultStatus.SUCCESS, HttpResponseStatus.OK);
                response.setStatus(executionState.getHttpStatus());
                responsePrinter.addResultPrinter(new ParseOnlyResultPrinter(parseOnlyResult));
            } else {
                Map<String, byte[]> statementParams = org.apache.asterix.app.translator.RequestParameters
                        .serializeParameterValues(param.getStatementParams());
                setAccessControlHeaders(request, response);
                stats.setProfileType(param.getProfileType());
                IStatementExecutor.StatementProperties statementProperties =
                        new IStatementExecutor.StatementProperties();
                response.setStatus(HttpResponseStatus.OK);
                executeStatement(requestRef, statementsText, sessionOutput, resultProperties, statementProperties,
                        stats, param, executionState, param.getOptionalParams(), statementParams, responsePrinter,
                        warnings);
                executionState.setStatus(ResultStatus.SUCCESS, HttpResponseStatus.OK);
            }
            errorCount = 0;
        } catch (Exception | org.apache.asterix.lang.sqlpp.parser.TokenMgrError e) {
            handleExecuteStatementException(e, executionState, param);
            response.setStatus(executionState.getHttpStatus());
            requestFailed(e, responsePrinter);
        } finally {
            executionState.finish();
        }
        responsePrinter.printResults();
        buildResponseFooters(elapsedStart, errorCount, stats, executionState, resultCharset, responsePrinter, delivery);
        responsePrinter.printFooters();
        responsePrinter.end();
        if (sessionOutput.out().checkError()) {
            LOGGER.warn("Error flushing output writer");
        }
    }

    protected RequestExecutionState newRequestExecutionState() throws HyracksDataException {
        return new RequestExecutionState();
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
            ExecutionPlans plans, List<Warning> warnings) throws HyracksDataException {
        responsePrinter.addResultPrinter(new PlansPrinter(plans, sessionOutput.config().getPlanFormat()));
        if (!warnings.isEmpty()) {
            List<ICodedMessage> codedWarnings = new ArrayList<>();
            warnings.forEach(warn -> codedWarnings.add(ExecutionWarning.of(warn)));
            responsePrinter.addResultPrinter(new WarningsPrinter(codedWarnings));
        }
    }

    protected ResponseMetrics buildResponseFooters(long elapsedStart, long errorCount, Stats stats,
            RequestExecutionState executionState, Charset resultCharset, ResponsePrinter responsePrinter,
            ResultDelivery delivery) {
        if (ResultDelivery.ASYNC != delivery) {
            // in case of ASYNC delivery, the status is printed by query translator
            responsePrinter.addFooterPrinter(new StatusPrinter(executionState.getResultStatus()));
        }
        final ResponseMetrics metrics =
                ResponseMetrics.of(System.nanoTime() - elapsedStart, executionState.duration(), stats.getCount(),
                        stats.getSize(), stats.getProcessedObjects(), errorCount, stats.getTotalWarningsCount());
        responsePrinter.addFooterPrinter(new MetricsPrinter(metrics, resultCharset));
        if (isPrintingProfile(stats)) {
            responsePrinter.addFooterPrinter(new ProfilePrinter(stats.getJobProfile()));
        }
        return metrics;
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
            SessionOutput sessionOutput, ResultProperties resultProperties,
            IStatementExecutor.StatementProperties statementProperties, Stats stats,
            QueryServiceRequestParameters param, RequestExecutionState executionState,
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
        executionState.start();
        Map<String, IAObject> stmtParams =
                org.apache.asterix.app.translator.RequestParameters.deserializeParameterValues(statementParameters);
        int stmtCategoryRestriction = org.apache.asterix.app.translator.RequestParameters
                .getStatementCategoryRestrictionMask(param.isReadOnly());
        IRequestParameters requestParameters =
                new org.apache.asterix.app.translator.RequestParameters(requestReference, statementsText,
                        getResultSet(), resultProperties, stats, statementProperties, null, param.getClientContextID(),
                        optionalParameters, stmtParams, param.isMultiStatement(), stmtCategoryRestriction);
        translator.compileAndExecute(getHyracksClientConnection(), requestParameters);
        executionState.end();
        translator.getWarnings(warnings, maxWarnings - warnings.size());
        stats.updateTotalWarningsCount(parserTotalWarningsCount);
        buildResponseResults(responsePrinter, sessionOutput, translator.getExecutionPlans(), warnings);
    }

    protected void handleExecuteStatementException(Throwable t, RequestExecutionState executionState,
            QueryServiceRequestParameters param) {
        if (t instanceof org.apache.asterix.lang.sqlpp.parser.TokenMgrError || t instanceof AlgebricksException) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("handleException: {}: {}", t.getMessage(), LogRedactionUtil.statement(param.toString()),
                        t);
            } else {
                LOGGER.info(() -> "handleException: " + t.getMessage() + ": "
                        + LogRedactionUtil.statement(param.toString()));
            }
            executionState.setStatus(ResultStatus.FATAL, HttpResponseStatus.BAD_REQUEST);
        } else if (t instanceof HyracksException) {
            HyracksException he = (HyracksException) t;
            switch (he.getComponent() + he.getErrorCode()) {
                case ASTERIX + REQUEST_TIMEOUT:
                    LOGGER.info(() -> "handleException: request execution timed out: "
                            + LogRedactionUtil.userData(param.toString()));
                    executionState.setStatus(ResultStatus.TIMEOUT, HttpResponseStatus.OK);
                    break;
                case ASTERIX + REJECT_BAD_CLUSTER_STATE:
                case ASTERIX + REJECT_NODE_UNREGISTERED:
                    LOGGER.warn(() -> "handleException: " + he.getMessage() + ": "
                            + LogRedactionUtil.userData(param.toString()));
                    executionState.setStatus(ResultStatus.FATAL, HttpResponseStatus.SERVICE_UNAVAILABLE);
                    break;
                case ASTERIX + INVALID_REQ_PARAM_VAL:
                case ASTERIX + INVALID_REQ_JSON_VAL:
                case ASTERIX + NO_STATEMENT_PROVIDED:
                case HYRACKS + JOB_REQUIREMENTS_EXCEED_CAPACITY:
                    executionState.setStatus(ResultStatus.FATAL, HttpResponseStatus.BAD_REQUEST);
                    break;
                default:
                    LOGGER.warn(() -> "handleException: unexpected exception " + he.getMessage() + ": "
                            + LogRedactionUtil.userData(param.toString()), he);
                    executionState.setStatus(ResultStatus.FATAL, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    break;
            }
        } else {
            LOGGER.warn(() -> "handleException: unexpected exception: " + LogRedactionUtil.userData(param.toString()),
                    t);
            executionState.setStatus(ResultStatus.FATAL, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void setSessionConfig(SessionOutput sessionOutput, QueryServiceRequestParameters param,
            ResultDelivery delivery) {
        String handleUrl = getHandleUrl(param.getHost(), param.getPath(), delivery);
        sessionOutput.setHandleAppender(ResultUtil.createResultHandleAppender(handleUrl));
        SessionConfig sessionConfig = sessionOutput.config();
        SessionConfig.OutputFormat format = param.getFormat();
        SessionConfig.PlanFormat planFormat = param.getPlanFormat();
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
        sessionConfig.set(SessionConfig.FORMAT_CSV_HEADER, param.isCSVWithHeader());
    }

    protected void requestFailed(Throwable throwable, ResponsePrinter responsePrinter) {
        final ExecutionError executionError = ExecutionError.of(throwable);
        responsePrinter.addResultPrinter(new ErrorsPrinter(Collections.singletonList(executionError)));
    }

    protected QueryServiceRequestParameters newRequestParameters() {
        return new QueryServiceRequestParameters();
    }

    protected static boolean isPrintingProfile(IStatementExecutor.Stats stats) {
        return stats.getProfileType() == Stats.ProfileType.FULL && stats.getJobProfile() != null;
    }
}
