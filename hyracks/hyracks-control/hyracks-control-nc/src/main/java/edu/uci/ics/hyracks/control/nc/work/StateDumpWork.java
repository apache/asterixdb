package edu.uci.ics.hyracks.control.nc.work;

import java.io.ByteArrayOutputStream;

import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;

public class StateDumpWork extends SynchronizableWork {
    private final NodeControllerService ncs;

    private final String stateDumpId;

    public StateDumpWork(NodeControllerService ncs, String stateDumpId) {
        this.ncs = ncs;
        this.stateDumpId = stateDumpId;
    }

    @Override
    protected void doRun() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ncs.getApplicationContext().getStateDumpHandler().dumpState(baos);
        ncs.getClusterController().notifyStateDump(ncs.getApplicationContext().getNodeId(), stateDumpId,
                baos.toString("UTF-8"));
    }
}
