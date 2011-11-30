/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.net.InetAddress;

public class NodeControllerInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String nodeId;

    private final InetAddress ipAddress;

    private final NodeStatus status;

    public NodeControllerInfo(String nodeId, InetAddress ipAddress, NodeStatus status) {
        this.nodeId = nodeId;
        this.ipAddress = ipAddress;
        this.status = status;
    }

    public String getNodeId() {
        return nodeId;
    }

    public InetAddress getIpAddress() {
        return ipAddress;
    }

    public NodeStatus getStatus() {
        return status;
    }
}