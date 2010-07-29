package edu.uci.ics.hyracks.controller.clustercontroller;

import edu.uci.ics.hyracks.controller.nodecontroller.INodeController;

public class NodeControllerState {
    private final INodeController nodeController;

    private int lastHeartbeatDuration;

    public NodeControllerState(INodeController nodeController) {
        this.nodeController = nodeController;
    }

    void notifyHeartbeat() {
        lastHeartbeatDuration = 0;
    }

    int incrementLastHeartbeatDuration() {
        return lastHeartbeatDuration++;
    }

    int getLastHeartbeatDuration() {
        return lastHeartbeatDuration;
    }

    public INodeController getNodeController() {
        return nodeController;
    }
}