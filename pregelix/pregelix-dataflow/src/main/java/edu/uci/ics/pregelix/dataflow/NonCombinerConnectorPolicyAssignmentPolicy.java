package edu.uci.ics.pregelix.dataflow;

import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.PipeliningConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.SendSideMaterializedPipeliningConnectorPolicy;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;

public class NonCombinerConnectorPolicyAssignmentPolicy implements IConnectorPolicyAssignmentPolicy {
    private static final long serialVersionUID = 1L;
    private IConnectorPolicy senderSideMaterializePolicy = new SendSideMaterializedPipeliningConnectorPolicy();
    private IConnectorPolicy pipeliningPolicy = new PipeliningConnectorPolicy();

    @Override
    public IConnectorPolicy getConnectorPolicyAssignment(IConnectorDescriptor c, int nProducers, int nConsumers,
            int[] fanouts) {
        if (c instanceof MToNPartitioningConnectorDescriptor) {
            return senderSideMaterializePolicy;
        } else {
            return pipeliningPolicy;
        }
    }
}
