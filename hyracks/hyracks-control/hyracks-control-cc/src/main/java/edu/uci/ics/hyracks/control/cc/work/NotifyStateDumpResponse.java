package edu.uci.ics.hyracks.control.cc.work;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.work.AbstractWork;

public class NotifyStateDumpResponse extends AbstractWork {

    private final ClusterControllerService ccs;

    private final String stateDumpId;

    private final String nodeId;

    private final String state;

    public NotifyStateDumpResponse(ClusterControllerService ccs, String nodeId, String stateDumpId, String state) {
        this.ccs = ccs;
        this.stateDumpId = stateDumpId;
        this.nodeId = nodeId;
        this.state = state;
    }

    @Override
    public void run() {
        ccs.getStateDumpRun(stateDumpId).notifyStateDumpReceived(nodeId, state);
    }
}
