package edu.uci.ics.hyracks.control.cc.partitions;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.api.util.Pair;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.job.PartitionDescriptor;
import edu.uci.ics.hyracks.control.common.job.PartitionRequest;

public class PartitionUtils {
    public static void reportPartitionMatch(ClusterControllerService ccs, final PartitionId pid,
            Pair<PartitionDescriptor, PartitionRequest> match) {
        PartitionDescriptor desc = match.first;
        PartitionRequest req = match.second;

        NodeControllerState producerNCS = ccs.getNodeMap().get(desc.getNodeId());
        NodeControllerState requestorNCS = ccs.getNodeMap().get(req.getNodeId());
        final NetworkAddress dataport = producerNCS.getDataPort();
        final INodeController requestorNC = requestorNCS.getNodeController();
        ccs.getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    requestorNC.reportPartitionAvailability(pid, dataport);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}