/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hivesterix.runtime.jobgen;

import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.PipeliningConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.SendSideMaterializedBlockingConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.SendSideMaterializedPipeliningConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.SendSideMaterializedReceiveSideMaterializedBlockingConnectorPolicy;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;

public class HiveConnectorPolicyAssignmentPolicy implements IConnectorPolicyAssignmentPolicy {
    public enum Policy {
        PIPELINING,
        SEND_SIDE_MAT_PIPELINING,
        SEND_SIDE_MAT_BLOCKING,
        SEND_SIDE_MAT_RECEIVE_SIDE_MAT_BLOCKING;
    };

    private static final long serialVersionUID = 1L;

    private final IConnectorPolicy pipeliningPolicy = new PipeliningConnectorPolicy();
    private final IConnectorPolicy sendSideMatPipeliningPolicy = new SendSideMaterializedPipeliningConnectorPolicy();
    private final IConnectorPolicy sendSideMatBlockingPolicy = new SendSideMaterializedBlockingConnectorPolicy();
    private final IConnectorPolicy sendSideMatReceiveSideMatBlockingPolicy = new SendSideMaterializedReceiveSideMaterializedBlockingConnectorPolicy();
    private final Policy policy;

    public HiveConnectorPolicyAssignmentPolicy(Policy policy) {
        this.policy = policy;
    }

    @Override
    public IConnectorPolicy getConnectorPolicyAssignment(IConnectorDescriptor c, int nProducers, int nConsumers,
            int[] fanouts) {
        if (c instanceof MToNPartitioningMergingConnectorDescriptor) {
            // avoid deadlocks
            switch (policy) {
                case PIPELINING:
                case SEND_SIDE_MAT_PIPELINING:
                    return sendSideMatPipeliningPolicy;
                case SEND_SIDE_MAT_BLOCKING:
                    return sendSideMatBlockingPolicy;
                case SEND_SIDE_MAT_RECEIVE_SIDE_MAT_BLOCKING:
                    return sendSideMatReceiveSideMatBlockingPolicy;
                default:
                    return sendSideMatPipeliningPolicy;
            }
        } else if (c instanceof MToNPartitioningConnectorDescriptor) {
            // support different repartitioning policies
            switch (policy) {
                case PIPELINING:
                    return pipeliningPolicy;
                case SEND_SIDE_MAT_PIPELINING:
                    return sendSideMatPipeliningPolicy;
                case SEND_SIDE_MAT_BLOCKING:
                    return sendSideMatBlockingPolicy;
                case SEND_SIDE_MAT_RECEIVE_SIDE_MAT_BLOCKING:
                    return sendSideMatReceiveSideMatBlockingPolicy;
                default:
                    return pipeliningPolicy;
            }
        } else {
            // pipelining for other connectors
            return pipeliningPolicy;
        }
    }
}
