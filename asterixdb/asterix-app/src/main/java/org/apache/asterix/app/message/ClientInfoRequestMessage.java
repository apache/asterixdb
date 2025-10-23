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
package org.apache.asterix.app.message;

import java.util.Optional;

import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.translator.ClientRequest;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClientInfoRequestMessage implements ICcAddressedMessage {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    private final String nodeId;
    private final JobId jobId;
    private final String requestId;
    private final long ncReqId;

    public ClientInfoRequestMessage(String nodeId, long ncReqId, JobId jobId, String requestId) {
        this.nodeId = nodeId;
        this.ncReqId = ncReqId;
        this.jobId = jobId;
        this.requestId = requestId;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException {
        CCMessageBroker messageBroker = (CCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        Optional<IClientRequest> clientRequest = appCtx.getRequestTracker().getAsyncOrDeferredRequest(requestId);
        ClientInfoResponseMessage response;
        if (clientRequest.isEmpty()) {
            response = new ClientInfoResponseMessage(ncReqId, false);
        } else {
            response = new ClientInfoResponseMessage(ncReqId,
                    jobId.equals(((ClientRequest) clientRequest.get()).getJobId()));
        }
        try {
            messageBroker.sendApplicationMessageToNC(response, nodeId);
        } catch (Exception e) {
            LOGGER.info("Failed to process request", e);
            throw HyracksDataException.create(e);
        }
    }
}
