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
package edu.uci.ics.hyracks.control.common.controllers;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.control.common.base.INodeController;

public final class NodeRegistration implements Serializable {
    private static final long serialVersionUID = 1L;

    private final INodeController nc;

    private final String nodeId;

    private final NCConfig ncConfig;

    private final NetworkAddress dataPort;

    public NodeRegistration(INodeController nc, String nodeId, NCConfig ncConfig, NetworkAddress dataPort) {
        this.nc = nc;
        this.nodeId = nodeId;
        this.ncConfig = ncConfig;
        this.dataPort = dataPort;
    }

    public INodeController getNodeController() {
        return nc;
    }

    public String getNodeId() {
        return nodeId;
    }

    public NCConfig getNCConfig() {
        return ncConfig;
    }

    public NetworkAddress getDataPort() {
        return dataPort;
    }
}