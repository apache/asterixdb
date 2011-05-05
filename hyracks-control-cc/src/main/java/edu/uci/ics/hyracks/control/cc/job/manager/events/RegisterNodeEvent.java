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
package edu.uci.ics.hyracks.control.cc.job.manager.events;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.cc.jobqueue.SynchronizableEvent;

public class RegisterNodeEvent extends SynchronizableEvent {
    private final ClusterControllerService ccs;
    private final String nodeId;
    private final NodeControllerState state;

    public RegisterNodeEvent(ClusterControllerService ccs, String nodeId, NodeControllerState state) {
        this.ccs = ccs;
        this.nodeId = nodeId;
        this.state = state;
    }

    @Override
    protected void doRun() throws Exception {
        Map<String, NodeControllerState> nodeMap = ccs.getNodeMap();
        if (nodeMap.containsKey(nodeId)) {
            throw new Exception("Node with this name already registered.");
        }
        nodeMap.put(nodeId, state);
        Map<String, Set<String>> ipAddressNodeNameMap = ccs.getIPAddressNodeNameMap();
        String ipAddress = state.getNCConfig().dataIPAddress;
        Set<String> nodes = ipAddressNodeNameMap.get(ipAddress);
        if (nodes == null) {
            nodes = new HashSet<String>();
            ipAddressNodeNameMap.put(ipAddress, nodes);
        }
        nodes.add(nodeId);
    }
}