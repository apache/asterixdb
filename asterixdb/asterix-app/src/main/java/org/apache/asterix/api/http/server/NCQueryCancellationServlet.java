/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.api.http.server;

import static org.apache.asterix.api.http.server.CcQueryCancellationServlet.REQUEST_UUID_PARAM_NAME;
import static org.apache.asterix.app.message.ExecuteStatementRequestMessage.DEFAULT_NC_TIMEOUT_MILLIS;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.api.http.server.QueryServiceServlet.Parameter;
import org.apache.asterix.app.message.CancelQueryRequest;
import org.apache.asterix.app.message.CancelQueryResponse;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.common.utils.RequestStatus;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * The servlet provides a REST API on an NC for cancelling an on-going query.
 */
public class NCQueryCancellationServlet extends AbstractServlet {
    private static final Logger LOGGER = LogManager.getLogger();
    private final INCServiceContext serviceCtx;
    private final INCMessageBroker messageBroker;

    public NCQueryCancellationServlet(ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
        this.serviceCtx = (INCServiceContext) ctx.get(ServletConstants.SERVICE_CONTEXT_ATTR);
        messageBroker = (INCMessageBroker) serviceCtx.getMessageBroker();
    }

    @Override
    protected void delete(IServletRequest request, IServletResponse response) {
        String uuid = request.getParameter(REQUEST_UUID_PARAM_NAME);
        String clientContextId = request.getParameter(Parameter.CLIENT_ID.str());
        if (uuid == null && clientContextId == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        final MessageFuture cancelQueryFuture = messageBroker.registerMessageFuture();
        try {
            CancelQueryRequest cancelQueryMessage = new CancelQueryRequest(serviceCtx.getNodeId(),
                    cancelQueryFuture.getFutureId(), uuid, clientContextId);
            // TODO(mblow): multicc -- need to send cancellation to the correct cc
            messageBroker.sendMessageToPrimaryCC(cancelQueryMessage);
            CancelQueryResponse cancelResponse =
                    (CancelQueryResponse) cancelQueryFuture.get(DEFAULT_NC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            final RequestStatus status = cancelResponse.getStatus();
            switch (status) {
                case SUCCESS:
                case FAILED:
                case NOT_FOUND:
                    response.setStatus(status.toHttpResponse());
                    break;
                default:
                    throw new IllegalStateException("Unrecognized status: " + status);
            }
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Unexpected exception while canceling query", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            messageBroker.deregisterMessageFuture(cancelQueryFuture.getFutureId());
        }
    }
}
