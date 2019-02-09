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

import java.util.Collection;

import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ActiveRequestsRequest implements ICcAddressedMessage {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    private final String nodeId;
    private final long reqId;

    public ActiveRequestsRequest(String nodeId, long reqId) {
        this.nodeId = nodeId;
        this.reqId = reqId;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        final Collection<IClientRequest> runningRequests = appCtx.getRequestTracker().getRunningRequests();
        final String[] requests = runningRequests.stream().map(IClientRequest::toJson).toArray(String[]::new);
        ActiveRequestsResponse response = new ActiveRequestsResponse(reqId, requests);
        CCMessageBroker messageBroker = (CCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        try {
            messageBroker.sendApplicationMessageToNC(response, nodeId);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Failure sending response to nc", e);
        }
    }
}