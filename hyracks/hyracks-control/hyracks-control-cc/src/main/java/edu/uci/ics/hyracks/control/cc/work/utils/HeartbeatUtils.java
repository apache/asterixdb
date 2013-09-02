package edu.uci.ics.hyracks.control.cc.work.utils;

import java.util.Map;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatData;

public class HeartbeatUtils {

    public static void notifyHeartbeat(ClusterControllerService ccs, String nodeId, HeartbeatData hbData) {
        Map<String, NodeControllerState> nodeMap = ccs.getNodeMap();
        NodeControllerState state = nodeMap.get(nodeId);
        if (state != null) {
            state.notifyHeartbeat(hbData);
        }
    }
}
