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

import org.apache.asterix.api.http.server.CcQueryCancellationServlet;
import org.apache.asterix.api.http.server.ServletConstants;
import org.apache.asterix.app.translator.RequestParameters;
import org.apache.asterix.common.api.RequestReference;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.runtime.utils.RequestTracker;
import org.apache.asterix.translator.ClientRequest;
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
        ICcApplicationContext appCtx = mock(ICcApplicationContext.class);
        ExternalProperties externalProperties = mock(ExternalProperties.class);
        Mockito.when(externalProperties.getRequestsArchiveSize()).thenReturn(50);
        Mockito.when(appCtx.getExternalProperties()).thenReturn(externalProperties);
        RequestTracker tracker = new RequestTracker(appCtx);
        Mockito.when(appCtx.getRequestTracker()).thenReturn(tracker);
        // Creates a query cancellation servlet.
        CcQueryCancellationServlet cancellationServlet =
                new CcQueryCancellationServlet(new ConcurrentHashMap<>(), appCtx, new String[] { "/" });
        // Adds mocked Hyracks client connection into the servlet context.
        IHyracksClientConnection mockHcc = mock(IHyracksClientConnection.class);
        cancellationServlet.ctx().put(ServletConstants.HYRACKS_CONNECTION_ATTR, mockHcc);
        Mockito.when(appCtx.getHcc()).thenReturn(mockHcc);
        // Tests the case that query is not in the map.
        IServletRequest mockRequest = mockRequest("1");
        IServletResponse mockResponse = mock(IServletResponse.class);
        cancellationServlet.handle(mockRequest, mockResponse);
        verify(mockResponse, times(1)).setStatus(HttpResponseStatus.NOT_FOUND);

        final RequestReference requestReference = RequestReference.of("1", "node1", System.currentTimeMillis());
        RequestParameters requestParameters =
                new RequestParameters(requestReference, "select 1", null, null, null, null, "1", null, null, true);
        ClientRequest request = new ClientRequest(requestParameters);
        request.setJobId(new JobId(1));
        request.markCancellable();
        tracker.track(request);
        // Tests the case that query is in the map.
        cancellationServlet.handle(mockRequest, mockResponse);
        verify(mockResponse, times(1)).setStatus(HttpResponseStatus.OK);

        // Tests the case the client_context_id is not provided.
        mockRequest = mockRequest(null);
        cancellationServlet.handle(mockRequest, mockResponse);
        verify(mockResponse, times(1)).setStatus(HttpResponseStatus.BAD_REQUEST);

        // Tests the case that the job cancellation hit some exception from Hyracks.
        final RequestReference requestReference2 = RequestReference.of("2", "node1", System.currentTimeMillis());
        requestParameters =
                new RequestParameters(requestReference2, "select 1", null, null, null, null, "2", null, null, true);
        ClientRequest request2 = new ClientRequest(requestParameters);
        request2.setJobId(new JobId(2));
        request2.markCancellable();
        tracker.track(request2);
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
