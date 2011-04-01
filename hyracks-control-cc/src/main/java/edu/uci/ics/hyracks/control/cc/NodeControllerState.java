package edu.uci.ics.hyracks.control.cc;

import edu.uci.ics.hyracks.control.common.base.INodeController;

public class NodeControllerState {
    private final INodeController nodeController;

    private int lastHeartbeatDuration;

    public NodeControllerState(INodeController nodeController) {
        this.nodeController = nodeController;
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
}