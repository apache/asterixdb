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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.app.message.ExecuteStatementRequestMessage;
import org.apache.asterix.app.message.ExecuteStatementResponseMessage;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionOutput;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.ipc.exceptions.IPCException;

/**
 * Query service servlet that can run on NC nodes.
 * Delegates query execution to CC, then serves the result.
 */
public class NCQueryServiceServlet extends QueryServiceServlet {
    public NCQueryServiceServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
            ILangExtension.Language queryLanguage) {
        super(ctx, paths, appCtx, queryLanguage, null, null, null);
    }

    @Override
    protected void executeStatement(String statementsText, SessionOutput sessionOutput,
            IStatementExecutor.ResultDelivery delivery, IStatementExecutor.Stats stats, RequestParameters param,
            String handleUrl, long[] outExecStartEnd) throws Exception {
        // Running on NC -> send 'execute' message to CC
        INCServiceContext ncCtx = (INCServiceContext) serviceCtx;
        INCMessageBroker ncMb = (INCMessageBroker) ncCtx.getMessageBroker();
        IStatementExecutor.ResultDelivery ccDelivery = delivery == IStatementExecutor.ResultDelivery.IMMEDIATE
                ? IStatementExecutor.ResultDelivery.DEFERRED : delivery;
        ExecuteStatementResponseMessage responseMsg;
        MessageFuture responseFuture = ncMb.registerMessageFuture();
        try {
            ExecuteStatementRequestMessage requestMsg =
                    new ExecuteStatementRequestMessage(ncCtx.getNodeId(), responseFuture.getFutureId(), queryLanguage,
                            statementsText, sessionOutput.config(), ccDelivery, param.clientContextID, handleUrl);
            outExecStartEnd[0] = System.nanoTime();
            ncMb.sendMessageToCC(requestMsg);
            responseMsg = (ExecuteStatementResponseMessage) responseFuture.get(
                    ExecuteStatementResponseMessage.DEFAULT_TIMEOUT_MILLIS, java.util.concurrent.TimeUnit.MILLISECONDS);
            outExecStartEnd[1] = System.nanoTime();
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

        IStatementExecutor.ResultMetadata resultMetadata = responseMsg.getMetadata();
        if (delivery == IStatementExecutor.ResultDelivery.IMMEDIATE && !resultMetadata.getResultSets().isEmpty()) {
            for (Triple<JobId, ResultSetId, ARecordType> rsmd : resultMetadata.getResultSets()) {
                ResultReader resultReader = new ResultReader(getHyracksDataset(), rsmd.getLeft(), rsmd.getMiddle());
                ResultUtil.printResults(appCtx, resultReader, sessionOutput, stats, rsmd.getRight());
            }
        } else {
            sessionOutput.out().append(responseMsg.getResult());
        }
    }

    @Override
    protected HttpResponseStatus handleExecuteStatementException(Throwable t) {
        if (t instanceof IPCException || t instanceof TimeoutException) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.WARNING, t.toString(), t);
            return HttpResponseStatus.SERVICE_UNAVAILABLE;
        } else {
            return super.handleExecuteStatementException(t);
        }
    }
}
