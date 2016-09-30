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
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.replication.IRemoteRecoveryManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.control.nc.NodeControllerService;

public class CompleteFailbackRequestMessage extends AbstractFailbackPlanMessage {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(CompleteFailbackRequestMessage.class.getName());
    private final Set<Integer> partitions;
    private final String nodeId;

    public CompleteFailbackRequestMessage(long planId, int requestId, String nodeId, Set<Integer> partitions) {
        super(planId, requestId);
        this.nodeId = nodeId;
        this.partitions = partitions;
    }

    public Set<Integer> getPartitions() {
        return partitions;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(CompleteFailbackRequestMessage.class.getSimpleName());
        sb.append(" Plan ID: " + planId);
        sb.append(" Node ID: " + nodeId);
        sb.append(" Partitions: " + partitions);
        return sb.toString();
    }

    @Override
    public void handle(IControllerService cs) throws HyracksDataException {
        NodeControllerService ncs = (NodeControllerService) cs;
        IAsterixAppRuntimeContext appContext =
                (IAsterixAppRuntimeContext) ncs.getApplicationContext().getApplicationObject();
        INCMessageBroker broker = (INCMessageBroker) ncs.getApplicationContext().getMessageBroker();
        HyracksDataException hde = null;
        try {
            IRemoteRecoveryManager remoteRecoeryManager = appContext.getRemoteRecoveryManager();
            remoteRecoeryManager.completeFailbackProcess();
        } catch (IOException | InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Failure during completion of failback process", e);
            hde = ExceptionUtils.convertToHyracksDataException(e);
        } finally {
            CompleteFailbackResponseMessage reponse = new CompleteFailbackResponseMessage(planId,
                    requestId, partitions);
            try {
                broker.sendMessageToCC(reponse);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failure sending message to CC", e);
                hde = ExceptionUtils.suppressIntoHyracksDataException(hde, e);
            }
        }
        if (hde != null) {
            throw hde;
        }
    }
}
