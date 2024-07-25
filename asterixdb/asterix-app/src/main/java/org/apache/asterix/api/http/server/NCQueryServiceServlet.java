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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.app.message.CancelQueryRequest;
import org.apache.asterix.app.message.ExecuteStatementRequestMessage;
import org.apache.asterix.app.message.ExecuteStatementResponseMessage;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.app.result.fields.NcResultPrinter;
import org.apache.asterix.app.result.fields.SignaturePrinter;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IRequestReference;
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
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.logging.log4j.Level;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Query service servlet that can run on NC nodes.
 * Delegates query execution to CC, then serves the result.
 */
public class NCQueryServiceServlet extends QueryServiceServlet {

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
        final String handleUrl = getHandleUrl(param.getHost(), param.getPath(), delivery);
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
                cancelQuery(ncMb, ncCtx.getNodeId(), requestReference.getUuid(), param.getClientContextID(), e, false);
                throw e;
            } catch (TimeoutException exception) {
                RuntimeDataException hde = new RuntimeDataException(ErrorCode.REQUEST_TIMEOUT);
                hde.addSuppressed(exception);
                // cancel query
                cancelQuery(ncMb, ncCtx.getNodeId(), requestReference.getUuid(), param.getClientContextID(), hde, true);
                throw hde;
            }
            executionState.end();
        } finally {
            ncMb.deregisterMessageFuture(responseFuture.getFutureId());
        }

        updatePropertiesFromCC(statementProperties, responseMsg);
        Throwable err = responseMsg.getError();
        if (err != null) {
            if (err instanceof Error) {
                throw (Error) err;
            } else if (err instanceof Exception) {
                throw (Exception) err;
            } else {
                throw new Exception(err.toString(), err);
            }
        }
        // if the was no error, we can set the result status to success
        executionState.setStatus(ResultStatus.SUCCESS, HttpResponseStatus.OK);
        updateStatsFromCC(stats, responseMsg);
        if (param.isSignature() && delivery != IStatementExecutor.ResultDelivery.ASYNC && !param.isParseOnly()) {
            responsePrinter.addResultPrinter(SignaturePrinter.newInstance(responseMsg.getExecutionPlans()));
        }
        if (hasResult(responseMsg)) {
            responsePrinter.addResultPrinter(
                    new NcResultPrinter(appCtx, responseMsg, getResultSet(), delivery, sessionOutput, stats));
        }
        warnings.addAll(responseMsg.getWarnings());
        buildResponseResults(responsePrinter, sessionOutput, responseMsg.getExecutionPlans(), warnings);
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
        return requestMessage;
    }

    private void cancelQuery(INCMessageBroker messageBroker, String nodeId, String uuid, String clientContextID,
            Exception exception, boolean wait) {
        if (uuid == null && clientContextID == null) {
            return;
        }
        MessageFuture cancelQueryFuture = messageBroker.registerMessageFuture();
        try {
            CancelQueryRequest cancelQueryMessage =
                    new CancelQueryRequest(nodeId, cancelQueryFuture.getFutureId(), uuid, clientContextID);
            // TODO(mblow): multicc -- need to send cancellation to the correct cc
            LOGGER.info("Cancelling query with uuid:{}, clientContextID:{} due to {}", uuid, clientContextID,
                    exception.getClass().getSimpleName());
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
        stats.setCompileTime(responseStats.getCompileTime());
        stats.setQueueWaitTime(responseStats.getQueueWaitTime());
        stats.setBufferCacheHitRatio(responseStats.getBufferCacheHitRatio());
        stats.setBufferCachePageReadCount(responseStats.getBufferCachePageReadCount());
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
