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

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.api.http.server.QueryServiceRequestParameters.Parameter;
import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.api.IRequestTracker;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * The servlet provides a REST API for getting the running queries or cancelling an on-going one.
 */
public class ActiveRequestsServlet extends AbstractRequestsServlet {

    public static final String REQUEST_UUID_PARAM_NAME = "request_id";
    private static final Logger LOGGER = LogManager.getLogger();

    public ActiveRequestsServlet(ConcurrentMap<String, Object> ctx, ICcApplicationContext appCtx, String... paths) {
        super(ctx, appCtx, paths);
    }

    @Override
    public Collection<IClientRequest> getRequests() {
        return appCtx.getRequestTracker().getRunningRequests();
    }

    @Override
    protected void delete(IServletRequest request, IServletResponse response) throws IOException {
        String uuid = request.getParameter(REQUEST_UUID_PARAM_NAME);
        String clientCtxId = request.getParameter(Parameter.CLIENT_ID.str());
        LOGGER.debug("received cancel request, uuid={}, clientCtxId={}", uuid, clientCtxId);
        if (uuid == null && clientCtxId == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        final IRequestTracker requestTracker = appCtx.getRequestTracker();
        IClientRequest req = uuid != null ? requestTracker.get(uuid) : requestTracker.getByClientContextId(clientCtxId);
        if (req == null) {
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            return;
        }
        if (!req.isCancellable()) {
            response.setStatus(HttpResponseStatus.FORBIDDEN);
            return;
        }
        try {
            // Cancels the on-going job.
            requestTracker.cancel(req.getId());
            // response: OK
            response.setStatus(HttpResponseStatus.OK);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "unexpected exception thrown from cancel", e);
            // response: INTERNAL SERVER ERROR
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
