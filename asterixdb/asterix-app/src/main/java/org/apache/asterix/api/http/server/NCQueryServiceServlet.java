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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.app.message.CancelQueryRequest;
import org.apache.asterix.app.message.ExecuteStatementRequestMessage;
import org.apache.asterix.app.message.ExecuteStatementResponseMessage;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.api.Duration;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.ResultProperties;
import org.apache.asterix.translator.SessionOutput;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.http.api.IChannelClosedHandler;
import org.apache.hyracks.http.api.IServletRequest;
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
            ILangExtension.Language queryLanguage,
            Function<IServletRequest, Map<String, String>> optionalParamProvider) {
        super(ctx, paths, appCtx, queryLanguage, null, null, null, optionalParamProvider);
    }

    @Override
    protected void executeStatement(String statementsText, SessionOutput sessionOutput,
            ResultProperties resultProperties, IStatementExecutor.Stats stats, RequestParameters param,
            RequestExecutionState execution, Map<String, String> optionalParameters) throws Exception {
        // Running on NC -> send 'execute' message to CC
        INCServiceContext ncCtx = (INCServiceContext) serviceCtx;
        INCMessageBroker ncMb = (INCMessageBroker) ncCtx.getMessageBroker();
        final IStatementExecutor.ResultDelivery delivery = resultProperties.getDelivery();
        ExecuteStatementResponseMessage responseMsg;
        MessageFuture responseFuture = ncMb.registerMessageFuture();
        final String handleUrl = getHandleUrl(param.host, param.path, delivery);
        try {
            if (param.clientContextID == null) {
                param.clientContextID = UUID.randomUUID().toString();
            }
            long timeout = ExecuteStatementRequestMessage.DEFAULT_NC_TIMEOUT_MILLIS;
            if (param.timeout != null && !param.timeout.trim().isEmpty()) {
                timeout = TimeUnit.NANOSECONDS.toMillis(Duration.parseDurationStringToNanos(param.timeout));
            }
            ExecuteStatementRequestMessage requestMsg = new ExecuteStatementRequestMessage(ncCtx.getNodeId(),
                    responseFuture.getFutureId(), queryLanguage, statementsText, sessionOutput.config(),
                    resultProperties.getNcToCcResultProperties(), param.clientContextID, handleUrl, optionalParameters);
            execution.start();
            ncMb.sendMessageToPrimaryCC(requestMsg);
            try {
                responseMsg = (ExecuteStatementResponseMessage) responseFuture.get(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                cancelQuery(ncMb, ncCtx.getNodeId(), param.clientContextID, e, false);
                throw e;
            } catch (TimeoutException exception) {
                RuntimeDataException hde = new RuntimeDataException(ErrorCode.QUERY_TIMEOUT);
                hde.addSuppressed(exception);
                // cancel query
                cancelQuery(ncMb, ncCtx.getNodeId(), param.clientContextID, hde, true);
                throw hde;
            }
            execution.end();
        } finally {
            ncMb.deregisterMessageFuture(responseFuture.getFutureId());
        }

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
        // no errors - stop buffering and allow for streaming result delivery
        sessionOutput.release();

        IStatementExecutor.ResultMetadata resultMetadata = responseMsg.getMetadata();
        if (delivery == IStatementExecutor.ResultDelivery.IMMEDIATE && !resultMetadata.getResultSets().isEmpty()) {
            stats.setProcessedObjects(responseMsg.getStats().getProcessedObjects());
            for (Triple<JobId, ResultSetId, ARecordType> rsmd : resultMetadata.getResultSets()) {
                ResultReader resultReader = new ResultReader(getHyracksDataset(), rsmd.getLeft(), rsmd.getMiddle());
                ResultUtil.printResults(appCtx, resultReader, sessionOutput, stats, rsmd.getRight());
            }
        } else {
            sessionOutput.out().append(responseMsg.getResult());
        }
        printExecutionPlans(sessionOutput, responseMsg.getExecutionPlans());
    }

    private void cancelQuery(INCMessageBroker messageBroker, String nodeId, String clientContextID, Exception exception,
            boolean wait) {
        MessageFuture cancelQueryFuture = messageBroker.registerMessageFuture();
        try {
            CancelQueryRequest cancelQueryMessage =
                    new CancelQueryRequest(nodeId, cancelQueryFuture.getFutureId(), clientContextID);
            // TODO(mblow): multicc -- need to send cancellation to the correct cc
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
    protected void handleExecuteStatementException(Throwable t, RequestExecutionState state, RequestParameters param) {
        if (t instanceof TimeoutException // TODO(mblow): I don't think t can ever been an instance of TimeoutException
                || ExceptionUtils.matchingCause(t, candidate -> candidate instanceof IPCException)) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.WARN, t.toString(), t);
            state.setStatus(ResultStatus.FAILED, HttpResponseStatus.SERVICE_UNAVAILABLE);
        } else {
            super.handleExecuteStatementException(t, state, param);
        }
    }

    @Override
    public IChannelClosedHandler getChannelClosedHandler(HttpServer server) {
        return InterruptOnCloseHandler.INSTANCE;
    }
}
