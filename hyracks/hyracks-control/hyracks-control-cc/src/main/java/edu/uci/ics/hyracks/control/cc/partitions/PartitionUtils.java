package edu.uci.ics.hyracks.control.cc.partitions;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.NodeControllerState;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.job.PartitionDescriptor;
import edu.uci.ics.hyracks.control.common.job.PartitionRequest;

public class PartitionUtils {
    public static void reportPartitionMatch(ClusterControllerService ccs, final PartitionId pid,
            Pair<PartitionDescriptor, PartitionRequest> match) throws Exception {
        PartitionDescriptor desc = match.getLeft();
        PartitionRequest req = match.getRight();

        NodeControllerState producerNCS = ccs.getNodeMap().get(desc.getNodeId());
        NodeControllerState requestorNCS = ccs.getNodeMap().get(req.getNodeId());
        final NetworkAddress dataport = producerNCS.getDataPort();
        final INodeController requestorNC = requestorNCS.getNodeController();
        requestorNC.reportPartitionAvailability(pid, dataport);
    }
}