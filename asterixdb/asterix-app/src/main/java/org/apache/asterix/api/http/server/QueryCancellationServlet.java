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
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.translator.IStatementExecutorContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * The servlet provides a REST API for cancelling an on-going query.
 */
public class QueryCancellationServlet extends AbstractServlet {
    private static final Logger LOGGER = LogManager.getLogger();
    protected static final String CLIENT_CONTEXT_ID = "client_context_id";

    public QueryCancellationServlet(ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
    }

    @Override
    protected void delete(IServletRequest request, IServletResponse response) throws IOException {
        String clientContextId = request.getParameter(CLIENT_CONTEXT_ID);
        if (clientContextId == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }

        // Retrieves the corresponding Hyracks job id.
        IStatementExecutorContext runningQueries =
                (IStatementExecutorContext) ctx.get(ServletConstants.RUNNING_QUERIES_ATTR);
        IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(ServletConstants.HYRACKS_CONNECTION_ATTR);
        JobId jobId = runningQueries.getJobIdFromClientContextId(clientContextId);

        if (jobId == null) {
            // response: NOT FOUND
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            return;
        }
        try {
            // Cancels the on-going job.
            hcc.cancelJob(jobId);
            // Removes the cancelled query from the map activeQueries.
            runningQueries.removeJobIdFromClientContextId(clientContextId);
            // response: OK
            response.setStatus(HttpResponseStatus.OK);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "unexpected exception thrown from cancel", e);
            // response: INTERNAL SERVER ERROR
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }
}