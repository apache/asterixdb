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

package edu.uci.ics.hyracks.control.cc.work;

import java.util.Map;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatData;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;

public abstract class AbstractHeartbeatWork extends SynchronizableWork {

    private final ClusterControllerService ccs;
    private final String nodeId;
    private final HeartbeatData hbData;

    public AbstractHeartbeatWork(ClusterControllerService ccs, String nodeId, HeartbeatData hbData) {
        this.ccs = ccs;
        this.nodeId = nodeId;
        this.hbData = hbData;
    }

    @Override
    public void doRun() {
        Map<String, NodeControllerState> nodeMap = ccs.getNodeMap();
        NodeControllerState state = nodeMap.get(nodeId);
        if (state != null) {
            state.notifyHeartbeat(hbData);
        }
        runWork();
    }

    public abstract void runWork();

}
