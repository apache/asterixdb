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

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.utils.RequestStatus;
import org.apache.asterix.hyracks.bootstrap.CCApplication;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.translator.IStatementExecutorContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CancelQueryRequest implements ICcAddressedMessage {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    private final String nodeId;
    private final long reqId;
    private final String contextId;

    public CancelQueryRequest(String nodeId, long reqId, String contextId) {
        this.nodeId = nodeId;
        this.reqId = reqId;
        this.contextId = contextId;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        ClusterControllerService ccs = (ClusterControllerService) appCtx.getServiceContext().getControllerService();
        CCApplication application = (CCApplication) ccs.getApplication();
        IStatementExecutorContext executorsCtx = application.getStatementExecutorContext();
        JobId jobId = executorsCtx.getJobIdFromClientContextId(contextId);
        RequestStatus status;

        if (jobId == null) {
            LOGGER.log(Level.WARN, "No job found for context id " + contextId);
            status = RequestStatus.NOT_FOUND;
        } else {
            try {
                IHyracksClientConnection hcc = application.getHcc();
                hcc.cancelJob(jobId);
                executorsCtx.removeJobIdFromClientContextId(contextId);
                status = RequestStatus.SUCCESS;
            } catch (Exception e) {
                LOGGER.log(Level.WARN, "unexpected exception thrown from cancel", e);
                status = RequestStatus.FAILED;
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
