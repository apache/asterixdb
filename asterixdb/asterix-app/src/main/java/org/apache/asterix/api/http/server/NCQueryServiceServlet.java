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

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.app.message.CancelQueryRequest;
import org.apache.asterix.app.message.ExecuteStatementRequestMessage;
import org.apache.asterix.app.message.ExecuteStatementResponseMessage;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.result.fields.MetricsPrinter;
import org.apache.asterix.app.result.fields.NcResultPrinter;
import org.apache.asterix.app.result.fields.NcStatementsPrinter;
import org.apache.asterix.app.result.fields.SignaturePrinter;
import org.apache.asterix.app.translator.QueryTranslator;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.ResultProperties;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.http.api.IChannelClosedHandler;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.InterruptOnCloseHandler;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.logging.log4j.Level;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Query service servlet that can run on NC nodes.
 * Delegates query execution to CC, then serves the result.
 */
public class NCQueryServiceServlet extends QueryServiceServlet {

    /** What a request reports for itself where its statements report the rest. */
    private static final Set<MetricsPrinter.Metrics> REQUEST_METRICS = EnumSet.of(MetricsPrinter.Metrics.ELAPSED_TIME,
            MetricsPrinter.Metrics.EXECUTION_TIME, MetricsPrinter.Metrics.ERROR_COUNT);

    public NCQueryServiceServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
            ILangExtension.Language queryLanguage, ILangCompilationProvider compilationProvider,
            Function<IServletRequest, Map<String, String>> optionalParamProvider) {
        super(ctx, paths, appCtx, queryLanguage, compilationProvider, null, null, optionalParamProvider);
    }

    @Override
    protected void executeStatement(IServletRequest request, IRequestReference requestReference, String statementsText,
            SessionOutput sessionOutput, ResultProperties resultProperties,
            IStatementExecutor.StatementProperties statementProperties, IStatementExecutor.Stats stats,
            QueryServiceRequestParameters param, RequestExecutionState executionState,
            Map<String, String> optionalParameters, Map<String, byte[]> statementParameters,
            ResponsePrinter responsePrinter, List<Warning> warnings) throws Exception {
        ensureOptionalParameters(optionalParameters);
        // Running on NC -> send 'execute' message to CC
        INCServiceContext ncCtx = (INCServiceContext) serviceCtx;
        INCMessageBroker ncMb = (INCMessageBroker) ncCtx.getMessageBroker();
        final IStatementExecutor.ResultDelivery delivery = resultProperties.getDelivery();
        ExecuteStatementResponseMessage responseMsg;
        MessageFuture responseFuture = ncMb.registerMessageFuture();
        final String handleUrl;
        if (!param.isIncludeHost()) {
            handleUrl = getHandleUrl(param.getPath(), delivery);
        } else {
            handleUrl = getHandleUrl(param.getHost(), param.getPath(), delivery);
        }
        try {
            long timeout = param.getTimeout();
            int stmtCategoryRestrictionMask = org.apache.asterix.app.translator.RequestParameters
                    .getStatementCategoryRestrictionMask(param.isReadOnly());
            ExecuteStatementRequestMessage requestMsg = createRequestMessage(request, requestReference, statementsText,
                    sessionOutput, resultProperties, param, optionalParameters, statementParameters, ncCtx,
                    responseFuture, queryLanguage, handleUrl, stmtCategoryRestrictionMask, false);
            executionState.start();
            ncMb.sendMessageToPrimaryCC(requestMsg);
            try {
                responseMsg = (ExecuteStatementResponseMessage) responseFuture.get(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                cancelQuery(ncMb, ncCtx.getNodeId(), requestReference.getUuid(), param.getClientContextID(), e, false,
                        "interrupt");
                throw e;
            } catch (TimeoutException exception) {
                RuntimeDataException hde = new RuntimeDataException(ErrorCode.REQUEST_TIMEOUT);
                hde.addSuppressed(exception);
                // cancel query
                cancelQuery(ncMb, ncCtx.getNodeId(), requestReference.getUuid(), param.getClientContextID(), hde, true,
                        "timeout");
                throw hde;
            }
            executionState.end();
        } finally {
            ncMb.deregisterMessageFuture(responseFuture.getFutureId());
        }

        updatePropertiesFromCC(statementProperties, responseMsg);
        boolean perStatement = useMultiStatementResponse(responseMsg);
        Throwable err = responseMsg.getError();
        // reported one by one, a failure belongs to its statement, so long as another statement did run
        if (err != null && (!perStatement || !anyStatementSucceeded(responseMsg))) {
            if (err instanceof Error) {
                throw (Error) err;
            } else if (err instanceof Exception) {
                throw (Exception) err;
            } else {
                throw new Exception(err.toString(), err);
            }
        }
        // if the was no error, we can set the result status to success
        if (delivery == IStatementExecutor.ResultDelivery.ASYNC && !isOldApi(request)) {
            executionState.setStatus(ResultStatus.SUCCESS, HttpResponseStatus.ACCEPTED);
        } else {
            executionState.setStatus(ResultStatus.SUCCESS, HttpResponseStatus.OK);
        }
        updateStatsFromCC(stats, responseMsg);
        if (perStatement) {
            responsePrinter.addResultPrinter(newStatementsPrinter(responseMsg, delivery, sessionOutput,
                    HttpUtil.getPreferredCharset(request), param.isSignature() && !param.isParseOnly(),
                    param.isIncludeHost() ? null : requestReference.getUuid()));
            // kept for the footers, which report what belongs to the request rather than to a statement
            executionState.setStatements(statementsOf(responseMsg));
        } else {
            if (param.isSignature() && delivery != IStatementExecutor.ResultDelivery.ASYNC && !param.isParseOnly()) {
                responsePrinter.addResultPrinter(SignaturePrinter.newInstance(responseMsg.getExecutionPlans()));
            }
            if (hasResult(responseMsg)) {
                responsePrinter.addResultPrinter(
                        new NcResultPrinter(appCtx, responseMsg, getResultSet(), delivery, sessionOutput, stats));
            }
            warnings.addAll(responseMsg.getWarnings());
            buildResponseResults(responsePrinter, sessionOutput, responseMsg.getExecutionPlans(), warnings,
                    executionState);
        }
    }

    /** Where each statement reports what it produced, the request reports only what is its own. */
    @Override
    protected Set<MetricsPrinter.Metrics> requestMetrics(RequestExecutionState executionState) {
        return executionState.getStatements().isEmpty() ? super.requestMetrics(executionState) : REQUEST_METRICS;
    }

    /**
     * Whether to report this request statement by statement. Off here deliberately: this service has always accepted
     * several statements, so clients rely on the response it already returns. A deployment that rejected them can turn
     * it on with {@link #hasMultipleStatements}.
     */
    protected boolean useMultiStatementResponse(ExecuteStatementResponseMessage responseMsg) {
        return false;
    }

    /**
     * Whether the request carried more than one statement of the client's own. {@code USE}, {@code SET} and
     * {@code DECLARE FUNCTION} get an entry of their own but are not counted, as in the rejection path.
     */
    protected static boolean hasMultipleStatements(ExecuteStatementResponseMessage responseMsg) {
        return statementsOf(responseMsg).stream().filter(statement -> statement.getKind() != null
                && QueryTranslator.isNotAllowedMultiStatement(statement.getKind())).count() > 1;
    }

    /** Overridable so that a deployment can report the statements in a form of its own. */
    protected IResponseFieldPrinter newStatementsPrinter(ExecuteStatementResponseMessage responseMsg,
            IStatementExecutor.ResultDelivery delivery, SessionOutput sessionOutput, Charset resultCharset,
            boolean printSignature, String requestId) throws Exception {
        return new NcStatementsPrinter(appCtx, statementsOf(responseMsg), getResultSet(), delivery, sessionOutput,
                resultCharset, printSignature, requestId);
    }

    /** The client's statements as the CC reported them; the request's own dataverse declaration is not one. */
    protected static List<IStatementExecutor.StatementInfo> statementsOf(ExecuteStatementResponseMessage responseMsg) {
        IStatementExecutor.ResultMetadata metadata = responseMsg.getMetadata();
        if (metadata == null) {
            return Collections.emptyList();
        }
        return metadata.getStatements().stream().filter(statement -> statement.getPosition() > 0).toList();
    }

    /** Whether any statement ran, which decides whether the request reports a failure of its own. */
    private static boolean anyStatementSucceeded(ExecuteStatementResponseMessage responseMsg) {
        return statementsOf(responseMsg).stream().anyMatch(statement -> statement.getError() == null);
    }

    protected void ensureOptionalParameters(Map<String, String> optionalParameters) throws HyracksDataException {

    }

    protected ExecuteStatementRequestMessage createRequestMessage(IServletRequest request,
            IRequestReference requestReference, String statementsText, SessionOutput sessionOutput,
            ResultProperties resultProperties, QueryServiceRequestParameters param,
            Map<String, String> optionalParameters, Map<String, byte[]> statementParameters, INCServiceContext ncCtx,
            MessageFuture responseFuture, ILangExtension.Language queryLanguage, String handleUrl,
            int stmtCategoryRestrictionMask, boolean forceDropDataset) {
        ExecuteStatementRequestMessage requestMessage = new ExecuteStatementRequestMessage(ncCtx.getNodeId(),
                responseFuture.getFutureId(), queryLanguage, statementsText, sessionOutput.config(),
                resultProperties.getNcToCcResultProperties(), param.getClientContextID(), param.getDataverse(),
                handleUrl, optionalParameters, statementParameters, param.isMultiStatement(), param.getProfileType(),
                stmtCategoryRestrictionMask, requestReference, forceDropDataset);
        requestMessage.setSQLCompatMode(param.isSQLCompatMode());
        requestMessage.setSkipQueryPlanCache(param.isSkipQueryPlanCache());
        return requestMessage;
    }

    private void cancelQuery(INCMessageBroker messageBroker, String nodeId, String uuid, String clientContextID,
            Exception exception, boolean wait, String reason) {
        if (uuid == null && clientContextID == null) {
            return;
        }
        MessageFuture cancelQueryFuture = messageBroker.registerMessageFuture();
        try {
            CancelQueryRequest cancelQueryMessage =
                    new CancelQueryRequest(nodeId, cancelQueryFuture.getFutureId(), uuid, clientContextID);
            // TODO(mblow): multicc -- need to send cancellation to the correct cc
            LOGGER.info("Cancelling query with uuid:{}, clientContextID:{} due to {}", uuid, clientContextID, reason);
            messageBroker.sendMessageToPrimaryCC(cancelQueryMessage);
            if (wait) {
                cancelQueryFuture.get(ExecuteStatementRequestMessage.DEFAULT_QUERY_CANCELLATION_WAIT_MILLIS,
                        TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            exception.addSuppressed(e);
        } finally {
            messageBroker.deregisterMessageFuture(cancelQueryFuture.getFutureId());
        }
    }

    @Override
    protected void handleExecuteStatementException(Throwable t, RequestExecutionState executionState,
            QueryServiceRequestParameters param, IServletResponse response) {
        if (t instanceof TimeoutException // TODO(mblow): I don't think t can ever been an instance of TimeoutException
                || ExceptionUtils.matchingCause(t, candidate -> candidate instanceof IPCException)) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.WARN, t.toString(), t);
            executionState.setStatus(ResultStatus.FAILED, HttpResponseStatus.SERVICE_UNAVAILABLE);
        } else {
            super.handleExecuteStatementException(t, executionState, param, response);
        }
    }

    @Override
    public IChannelClosedHandler getChannelClosedHandler(HttpServer server) {
        return InterruptOnCloseHandler.INSTANCE;
    }

    private static boolean hasResult(ExecuteStatementResponseMessage responseMsg) {
        return !responseMsg.getMetadata().getResultSets().isEmpty() || !responseMsg.getResult().isEmpty();
    }

    private static void updateStatsFromCC(IStatementExecutor.Stats stats, ExecuteStatementResponseMessage responseMsg) {
        IStatementExecutor.Stats responseStats = responseMsg.getStats();
        stats.setJobProfile(responseStats.getJobProfile());
        stats.setProcessedObjects(responseStats.getProcessedObjects());
        stats.updateTotalWarningsCount(responseStats.getTotalWarningsCount());
        stats.setCompileTimeNanos(responseStats.getCompileTimeNanos());
        stats.setQueueWaitTimeNanos(responseStats.getQueueWaitTimeNanos());
        stats.setBufferCacheHitRatio(responseStats.getBufferCacheHitRatio());
        stats.setBufferCachePageReadCount(responseStats.getBufferCachePageReadCount());
        stats.setCloudReadRequestsCount(responseStats.getCloudReadRequestsCount());
        stats.setCloudPagesReadCount(responseStats.getCloudPagesReadCount());
        stats.setCloudPagesPersistedCount(responseStats.getCloudPagesPersistedCount());
        stats.setCachedPlan(responseStats.isCachedPlan());
    }

    private static void updatePropertiesFromCC(IStatementExecutor.StatementProperties statementProperties,
            ExecuteStatementResponseMessage responseMsg) {
        IStatementExecutor.StatementProperties responseStmtProps = responseMsg.getStatementProperties();
        if (responseStmtProps != null) {
            statementProperties.setKind(responseStmtProps.getKind());
            statementProperties.setName(responseStmtProps.getName());
        }
    }
}
