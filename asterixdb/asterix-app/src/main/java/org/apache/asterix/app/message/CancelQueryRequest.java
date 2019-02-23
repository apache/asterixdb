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

import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.api.IRequestTracker;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.utils.RequestStatus;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CancelQueryRequest implements ICcAddressedMessage {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    private final String nodeId;
    private final long reqId;
    private final String uuid;
    private final String contextId;

    public CancelQueryRequest(String nodeId, long reqId, String uuid, String contextId) {
        this.nodeId = nodeId;
        this.reqId = reqId;
        this.uuid = uuid;
        this.contextId = contextId;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        final IRequestTracker requestTracker = appCtx.getRequestTracker();
        IClientRequest req = uuid != null ? requestTracker.get(uuid) : requestTracker.getByClientContextId(contextId);
        RequestStatus status;

        if (req == null) {
            LOGGER.log(Level.INFO, "No request found for uuid {} or context id {}", uuid, contextId);
            status = RequestStatus.NOT_FOUND;
        } else {
            if (!req.isCancellable()) {
                status = RequestStatus.REJECTED;
            } else {
                try {
                    requestTracker.cancel(req.getId());
                    status = RequestStatus.SUCCESS;
                } catch (Exception e) {
                    LOGGER.log(Level.WARN, "unexpected exception thrown from cancel", e);
                    status = RequestStatus.FAILED;
                }
            }
        }
        CancelQueryResponse response = new CancelQueryResponse(reqId, status);
        CCMessageBroker messageBroker = (CCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        try {
            messageBroker.sendApplicationMessageToNC(response, nodeId);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Failure sending response to nc", e);
        }
    }

}
