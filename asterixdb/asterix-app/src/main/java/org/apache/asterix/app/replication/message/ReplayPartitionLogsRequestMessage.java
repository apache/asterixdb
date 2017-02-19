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
package org.apache.asterix.app.replication.message;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IAppRuntimeContext;
import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.replication.INCLifecycleMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.control.nc.NodeControllerService;

public class ReplayPartitionLogsRequestMessage implements INCLifecycleMessage {

    private static final Logger LOGGER = Logger.getLogger(ReplayPartitionLogsRequestMessage.class.getName());
    private static final long serialVersionUID = 1L;
    private final Set<Integer> partitions;

    public ReplayPartitionLogsRequestMessage(Set<Integer> partitions) {
        this.partitions = partitions;
    }

    @Override
    public void handle(IControllerService cs) throws HyracksDataException, InterruptedException {
        NodeControllerService ncs = (NodeControllerService) cs;
        IAppRuntimeContext appContext = (IAppRuntimeContext) ncs.getApplicationContext().getApplicationObject();
        // Replay the logs for these partitions and flush any impacted dataset
        appContext.getRemoteRecoveryManager().replayReplicaPartitionLogs(partitions, true);

        INCMessageBroker broker = (INCMessageBroker) ncs.getApplicationContext().getMessageBroker();
        ReplayPartitionLogsResponseMessage reponse = new ReplayPartitionLogsResponseMessage(ncs.getId(), partitions);
        try {
            broker.sendMessageToCC(reponse);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed sending message to cc", e);
            throw ExceptionUtils.convertToHyracksDataException(e);
        }
    }

    @Override
    public MessageType getType() {
        return MessageType.REPLAY_LOGS_REQUEST;
    }
}
