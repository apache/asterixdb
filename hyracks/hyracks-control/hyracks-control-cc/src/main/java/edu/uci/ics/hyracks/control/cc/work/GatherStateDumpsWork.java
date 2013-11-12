package edu.uci.ics.hyracks.control.cc.work;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;

public class GatherStateDumpsWork extends SynchronizableWork {
    private final ClusterControllerService ccs;

    private final StateDumpRun sdr;

    public GatherStateDumpsWork(ClusterControllerService ccs) {
        this.ccs = ccs;
        this.sdr = new StateDumpRun(ccs);
    }

    @Override
    public void doRun() throws Exception {
        ccs.addStateDumpRun(sdr.stateDumpId, sdr);
        sdr.setNCs(new HashSet<>(ccs.getNodeMap().keySet()));
        for (NodeControllerState ncs : ccs.getNodeMap().values()) {
            ncs.getNodeController().dumpState(sdr.stateDumpId);
        }
    }

    public StateDumpRun getStateDumpRun() {
        return sdr;
    }

    public static class StateDumpRun {

        private final ClusterControllerService ccs;

        private final String stateDumpId;

        private final Map<String, String> ncStates;

        private Set<String> ncIds;

        private boolean complete;

        public StateDumpRun(ClusterControllerService ccs) {
            this.ccs = ccs;
            stateDumpId = UUID.randomUUID().toString();
            ncStates = new HashMap<>();
            complete = false;
        }

        public void setNCs(Set<String> ncIds) {
            this.ncIds = ncIds;
        }

        public Map<String, String> getStateDump() {
            return ncStates;
        }

        public synchronized void notifyStateDumpReceived(String nodeId, String state) {
            ncIds.remove(nodeId);
            ncStates.put(nodeId, state);
            if (ncIds.size() == 0) {
                complete = true;
                ccs.removeStateDumpRun(stateDumpId);
                notifyAll();
            }
        }

        public synchronized void waitForCompletion() throws InterruptedException {
            while (!complete) {
                wait();
            }
        }

        public String getStateDumpId() {
            return stateDumpId;
        }
    }

}
