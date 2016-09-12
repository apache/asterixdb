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
package org.apache.asterix.runtime.message;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.common.messaging.api.IApplicationMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.replication.IRemoteRecoveryManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.control.nc.NodeControllerService;

public class TakeoverPartitionsRequestMessage implements IApplicationMessage {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(TakeoverPartitionsRequestMessage.class.getName());
    private final Integer[] partitions;
    private final long requestId;
    private final String nodeId;

    public TakeoverPartitionsRequestMessage(long requestId, String nodeId, Integer[] partitionsToTakeover) {
        this.requestId = requestId;
        this.nodeId = nodeId;
        this.partitions = partitionsToTakeover;
    }

    public Integer[] getPartitions() {
        return partitions;
    }

    public long getRequestId() {
        return requestId;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(TakeoverPartitionsRequestMessage.class.getSimpleName());
        sb.append(" Request ID: " + requestId);
        sb.append(" Node ID: " + nodeId);
        sb.append(" Partitions: ");
        for (Integer partitionId : partitions) {
            sb.append(partitionId + ",");
        }
        //remove last comma
        sb.charAt(sb.length() - 1);
        return sb.toString();
    }

    @Override
    public void handle(IControllerService cs) throws HyracksDataException {
        NodeControllerService ncs = (NodeControllerService) cs;
        IAsterixAppRuntimeContext appContext =
                (IAsterixAppRuntimeContext) ncs.getApplicationContext().getApplicationObject();
        INCMessageBroker broker = (INCMessageBroker) ncs.getApplicationContext().getMessageBroker();
        //if the NC is shutting down, it should ignore takeover partitions request
        if (!appContext.isShuttingdown()) {
            HyracksDataException hde = null;
            try {
                IRemoteRecoveryManager remoteRecoeryManager = appContext.getRemoteRecoveryManager();
                remoteRecoeryManager.takeoverPartitons(partitions);
            } catch (IOException | ACIDException e) {
                LOGGER.log(Level.SEVERE, "Failure taking over partitions", e);
                hde = ExceptionUtils.suppressIntoHyracksDataException(hde, e);
            } finally {
                //send response after takeover is completed
                TakeoverPartitionsResponseMessage reponse = new TakeoverPartitionsResponseMessage(requestId,
                        appContext.getTransactionSubsystem().getId(), partitions);
                try {
                    broker.sendMessageToCC(reponse);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Failure taking over partitions", e);
                    hde = ExceptionUtils.suppressIntoHyracksDataException(hde, e);
                }
            }
            if (hde != null) {
                throw hde;
            }
        }
    }
}
