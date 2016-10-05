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

import org.apache.asterix.common.messaging.api.IApplicationMessage;
import org.apache.asterix.runtime.util.ClusterStateManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;

public class TakeoverPartitionsResponseMessage implements IApplicationMessage {

    private static final long serialVersionUID = 1L;
    private final Integer[] partitions;
    private final String nodeId;
    private final long requestId;

    public TakeoverPartitionsResponseMessage(long requestId, String nodeId, Integer[] partitionsToTakeover) {
        this.requestId = requestId;
        this.nodeId = nodeId;
        this.partitions = partitionsToTakeover;
    }

    public Integer[] getPartitions() {
        return partitions;
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getRequestId() {
        return requestId;
    }

    @Override
    public void handle(IControllerService cs) throws HyracksDataException, InterruptedException {
        ClusterStateManager.INSTANCE.processPartitionTakeoverResponse(this);
    }

    @Override
    public String toString() {
        return TakeoverPartitionsResponseMessage.class.getSimpleName();
    }
}
