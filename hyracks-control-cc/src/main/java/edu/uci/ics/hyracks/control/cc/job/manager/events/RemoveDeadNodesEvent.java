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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.cc.jobqueue.AbstractEvent;

public class RemoveDeadNodesEvent extends AbstractEvent {
    private static Logger LOGGER = Logger.getLogger(RemoveDeadNodesEvent.class.getName());

    private final ClusterControllerService ccs;

    public RemoveDeadNodesEvent(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public void run() {
        Set<String> deadNodes = new HashSet<String>();
        Map<String, NodeControllerState> nodeMap = ccs.getNodeMap();
        for (Map.Entry<String, NodeControllerState> e : nodeMap.entrySet()) {
            NodeControllerState state = e.getValue();
            if (state.incrementLastHeartbeatDuration() >= ccs.getConfig().maxHeartbeatLapsePeriods) {
                deadNodes.add(e.getKey());
                LOGGER.info(e.getKey() + " considered dead");
            }
        }
        Map<String, Set<String>> ipAddressNodeNameMap = ccs.getIPAddressNodeNameMap();
        for (String deadNode : deadNodes) {
            NodeControllerState state = nodeMap.remove(deadNode);
            // Deal with dead tasks.
            String ipAddress = state.getNCConfig().dataIPAddress;
            Set<String> ipNodes = ipAddressNodeNameMap.get(ipAddress);
            if (ipNodes != null) {
                if (ipNodes.remove(deadNode) && ipNodes.isEmpty()) {
                    ipAddressNodeNameMap.remove(ipAddress);
                }
            }
        }
    }

    @Override
    public Level logLevel() {
        return Level.FINE;
    }
}