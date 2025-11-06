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

import org.apache.asterix.app.message.DiscardResultRequestMessage;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.utils.AsyncRequestsAPIUtil;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;

public class NCQueryResultApiServlet extends QueryResultApiServlet {

    public NCQueryResultApiServlet(ConcurrentMap<String, Object> ctx, IApplicationContext appCtx, String... paths) {
        super(ctx, appCtx, paths);
    }

    @Override
    protected boolean isValidRequest(String requestId, JobId jobId, IServletRequest request, IServletResponse response)
            throws HyracksDataException {
        return AsyncRequestsAPIUtil.isValidRequest(appCtx, requestId, jobId, response);
    }

    @Override
    protected void discardResult(String requestId, JobId jobId, ResultSetId resultSetId) throws HyracksDataException {
        INCServiceContext serviceCtx = (INCServiceContext) appCtx.getServiceContext();
        INCMessageBroker messageBroker = (INCMessageBroker) serviceCtx.getMessageBroker();
        DiscardResultRequestMessage request =
                new DiscardResultRequestMessage(serviceCtx.getNodeId(), jobId, resultSetId, requestId);
        try {
            messageBroker.sendMessageToPrimaryCC(request);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }
}
