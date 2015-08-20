/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.api.client;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;

public class NodeControllerInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String nodeId;

    private final NodeStatus status;

    private final NetworkAddress netAddress;

    private final NetworkAddress datasetNetworkAddress;

    public NodeControllerInfo(String nodeId, NodeStatus status, NetworkAddress netAddress,
            NetworkAddress datasetNetworkAddress) {
        this.nodeId = nodeId;
        this.status = status;
        this.netAddress = netAddress;
        this.datasetNetworkAddress = datasetNetworkAddress;
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

    public NetworkAddress getDatasetNetworkAddress() {
        return datasetNetworkAddress;
    }
}