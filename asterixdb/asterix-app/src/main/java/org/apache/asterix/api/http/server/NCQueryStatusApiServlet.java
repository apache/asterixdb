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

import static org.apache.asterix.utils.AsyncRequestsAPIUtil.NC_TIMEOUT_MILLIS;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.app.message.ClientInfoRequestMessage;
import org.apache.asterix.app.message.ClientInfoResponseMessage;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.utils.AsyncRequestsAPIUtil;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;

public class NCQueryStatusApiServlet extends QueryStatusApiServlet {

    public NCQueryStatusApiServlet(ConcurrentMap<String, Object> ctx, IApplicationContext appCtx, String... paths) {
        super(ctx, appCtx, paths);
    }

    protected boolean isValidRequest(String requestId, JobId jobId, IServletRequest request, IServletResponse response)
            throws HyracksDataException {
        return AsyncRequestsAPIUtil.isValidRequest(appCtx, requestId, jobId, response);
    }

    public void printMetricsWithoutResultMetadata(ResponsePrinter printer, IServletRequest request, String requestId,
            JobId jobId, ResultStatus status) throws HyracksDataException {
        INCServiceContext serviceCtx = (INCServiceContext) appCtx.getServiceContext();
        INCMessageBroker messageBroker = (INCMessageBroker) serviceCtx.getMessageBroker();
        MessageFuture messageFuture = messageBroker.registerMessageFuture();
        long futureId = messageFuture.getFutureId();
        ClientInfoRequestMessage clientInfoRequestMessage =
                new ClientInfoRequestMessage(serviceCtx.getNodeId(), futureId, jobId, requestId, true);
        try {
            messageBroker.sendMessageToPrimaryCC(clientInfoRequestMessage);
            ClientInfoResponseMessage responseMessage =
                    (ClientInfoResponseMessage) messageFuture.get(NC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (responseMessage == null || !responseMessage.isValidRequestId()) {
                return;
            }
            printMetrics(printer, request, status, responseMessage.getRequestCreateTime(),
                    responseMessage.getJobCreateTime(), responseMessage.getJobStartTime(),
                    responseMessage.getJobQueueWaitTime());
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }
}
