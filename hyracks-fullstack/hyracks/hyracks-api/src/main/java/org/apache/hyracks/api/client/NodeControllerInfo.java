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
package org.apache.hyracks.api.client;

import java.io.Serializable;

import org.apache.hyracks.api.comm.NetworkAddress;

public class NodeControllerInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String nodeId;

    private final NodeStatus status;

    private final NetworkAddress netAddress;

    private final NetworkAddress resultNetworkAddress;

    private final NetworkAddress messagingNetworkAddress;

    private final int numAvailableCores;

    public NodeControllerInfo(String nodeId, NodeStatus status, NetworkAddress netAddress,
            NetworkAddress resultNetworkAddress, NetworkAddress messagingNetworkAddress, int numAvailableCores) {
        this.nodeId = nodeId;
        this.status = status;
        this.netAddress = netAddress;
        this.resultNetworkAddress = resultNetworkAddress;
        this.messagingNetworkAddress = messagingNetworkAddress;
        this.numAvailableCores = numAvailableCores;
    }

    public String getNodeId() {
        return nodeId;
    }

    public NodeStatus getStatus() {
        return status;
    }

    public NetworkAddress getNetworkAddress() {
        return netAddress;
    }

    public NetworkAddress getResultNetworkAddress() {
        return resultNetworkAddress;
    }

    public NetworkAddress getMessagingNetworkAddress() {
        return messagingNetworkAddress;
    }

    public int getNumAvailableCores() {
        return numAvailableCores;
    }
}
