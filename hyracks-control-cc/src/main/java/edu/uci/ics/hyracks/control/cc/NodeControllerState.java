package edu.uci.ics.hyracks.control.cc;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import edu.uci.ics.hyracks.api.control.INodeController;
import edu.uci.ics.hyracks.api.control.NCConfig;

public class NodeControllerState {
    private final INodeController nodeController;

    private final NCConfig ncConfig;

    private final Set<UUID> activeJobIds;

    private int lastHeartbeatDuration;

    public NodeControllerState(INodeController nodeController, NCConfig ncConfig) {
        this.nodeController = nodeController;
        this.ncConfig = ncConfig;
        activeJobIds = new HashSet<UUID>();
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

    public Set<UUID> getActiveJobIds() {
        return activeJobIds;
    }
}