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
package org.apache.asterix.common.messaging;

import java.util.Set;

public class CompleteFailbackRequestMessage extends AbstractFailbackPlanMessage {

    private static final long serialVersionUID = 1L;
    private final Set<Integer> partitions;
    private final String nodeId;

    public CompleteFailbackRequestMessage(long planId, int requestId, String nodeId, Set<Integer> partitions) {
        super(planId, requestId);
        this.nodeId = nodeId;
        this.partitions = partitions;
    }

    @Override
    public ApplicationMessageType getMessageType() {
        return ApplicationMessageType.COMPLETE_FAILBACK_REQUEST;
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
        sb.append("Plan ID: " + planId);
        sb.append(" Node ID: " + nodeId);
        sb.append(" Partitions: " + partitions);
        return sb.toString();
    }
}
