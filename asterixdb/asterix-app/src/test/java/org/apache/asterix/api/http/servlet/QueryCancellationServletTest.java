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

package org.apache.asterix.api.http.servlet;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.api.http.ctx.StatementExecutorContext;
import org.apache.asterix.api.http.server.QueryCancellationServlet;
import org.apache.asterix.api.http.server.ServletConstants;
import org.apache.asterix.translator.IStatementExecutorContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.junit.Test;
import org.mockito.Mockito;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryCancellationServletTest {

    @Test
    public void testDelete() throws Exception {
        // Creates a query cancellation servlet.
        QueryCancellationServlet cancellationServlet = new QueryCancellationServlet(new ConcurrentHashMap<>(),
                new String[] { "/" });
        // Adds mocked Hyracks client connection into the servlet context.
        IHyracksClientConnection mockHcc = mock(IHyracksClientConnection.class);
        cancellationServlet.ctx().put(ServletConstants.HYRACKS_CONNECTION_ATTR, mockHcc);
        // Adds a query context into the servlet context.
        IStatementExecutorContext queryCtx = new StatementExecutorContext();
        cancellationServlet.ctx().put(ServletConstants.RUNNING_QUERIES_ATTR, queryCtx);

        // Tests the case that query is not in the map.
        IServletRequest mockRequest = mockRequest("1");
        IServletResponse mockResponse = mock(IServletResponse.class);
        cancellationServlet.handle(mockRequest, mockResponse);
        verify(mockResponse, times(1)).setStatus(HttpResponseStatus.NOT_FOUND);

        // Tests the case that query is in the map.
        queryCtx.put("1", new JobId(1));
        cancellationServlet.handle(mockRequest, mockResponse);
        verify(mockResponse, times(1)).setStatus(HttpResponseStatus.OK);

        // Tests the case the client_context_id is not provided.
        mockRequest = mockRequest(null);
        cancellationServlet.handle(mockRequest, mockResponse);
        verify(mockResponse, times(1)).setStatus(HttpResponseStatus.BAD_REQUEST);

        // Tests the case that the job cancellation hit some exception from Hyracks.
        queryCtx.put("2", new JobId(2));
        Mockito.doThrow(new Exception()).when(mockHcc).cancelJob(any());
        mockRequest = mockRequest("2");
        cancellationServlet.handle(mockRequest, mockResponse);
        verify(mockResponse, times(1)).setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    private IServletRequest mockRequest(String clientContextId) {
        IServletRequest mockRequest = mock(IServletRequest.class);
        FullHttpRequest mockHttpRequest = mock(FullHttpRequest.class);
        when(mockRequest.getHttpRequest()).thenReturn(mockHttpRequest);
        when(mockHttpRequest.method()).thenReturn(HttpMethod.DELETE);
        if (clientContextId != null) {
            when(mockRequest.getParameter("client_context_id")).thenReturn(clientContextId);
        }
        return mockRequest;
    }
}
