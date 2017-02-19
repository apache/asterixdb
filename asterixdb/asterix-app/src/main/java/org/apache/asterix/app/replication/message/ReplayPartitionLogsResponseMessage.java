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

import org.apache.asterix.common.replication.INCLifecycleMessage;
import org.apache.asterix.runtime.utils.AppContextInfo;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;

import java.util.Set;

public class ReplayPartitionLogsResponseMessage implements INCLifecycleMessage {

    private static final long serialVersionUID = 1L;
    private final Set<Integer> partitions;
    private final String nodeId;

    public ReplayPartitionLogsResponseMessage(String nodeId, Set<Integer> partitions) {
        this.partitions = partitions;
        this.nodeId = nodeId;
    }

    @Override
    public void handle(IControllerService cs) throws HyracksDataException, InterruptedException {
        AppContextInfo.INSTANCE.getFaultToleranceStrategy().process(this);
    }

    public Set<Integer> getPartitions() {
        return partitions;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public MessageType getType() {
        return MessageType.REPLAY_LOGS_RESPONSE;
    }
}