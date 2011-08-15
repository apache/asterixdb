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
package edu.uci.ics.hyracks.control.cc;

import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;

public class NodeControllerState {
    private final INodeController nodeController;

    private final NCConfig ncConfig;

    private final NetworkAddress dataPort;

    private final Set<JobId> activeJobIds;

    private int lastHeartbeatDuration;

    public NodeControllerState(INodeController nodeController, NCConfig ncConfig, NetworkAddress dataPort) {
        this.nodeController = nodeController;
        this.ncConfig = ncConfig;
        this.dataPort = dataPort;
        activeJobIds = new HashSet<JobId>();
    }

    public void notifyHeartbeat() {
        lastHeartbeatDuration = 0;
    }

    public int incrementLastHeartbeatDuration() {
        return lastHeartbeatDuration++;
    }

    public int getLastHeartbeatDuration() {
        return lastHeartbeatDuration;
    }

    public INodeController getNodeController() {
        return nodeController;
    }

    public NCConfig getNCConfig() {
        return ncConfig;
    }

    public Set<JobId> getActiveJobIds() {
        return activeJobIds;
    }

    public NetworkAddress getDataPort() {
        return dataPort;
    }
}