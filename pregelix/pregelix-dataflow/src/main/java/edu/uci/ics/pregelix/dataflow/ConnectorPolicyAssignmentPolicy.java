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

package edu.uci.ics.pregelix.dataflow;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.PipeliningConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.SendSideMaterializedBlockingConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.SendSideMaterializedPipeliningConnectorPolicy;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexInsertUpdateDeleteOperatorDescriptor;

public class ConnectorPolicyAssignmentPolicy implements IConnectorPolicyAssignmentPolicy {
    private static final long serialVersionUID = 1L;
    private IConnectorPolicy senderSideMatPipPolicy = new SendSideMaterializedPipeliningConnectorPolicy();
    private IConnectorPolicy senderSideMatBlkPolicy = new SendSideMaterializedBlockingConnectorPolicy();
    private IConnectorPolicy pipeliningPolicy = new PipeliningConnectorPolicy();
    private JobSpecification spec;

    public ConnectorPolicyAssignmentPolicy(JobSpecification spec) {
        this.spec = spec;
    }

    @Override
    public IConnectorPolicy getConnectorPolicyAssignment(IConnectorDescriptor c, int nProducers, int nConsumers,
            int[] fanouts) {
        if (c instanceof MToNPartitioningMergingConnectorDescriptor) {
            return senderSideMatPipPolicy;
        } else {
            Pair<Pair<IOperatorDescriptor, Integer>, Pair<IOperatorDescriptor, Integer>> endPoints = spec
                    .getConnectorOperatorMap().get(c.getConnectorId());
            IOperatorDescriptor consumer = endPoints.getRight().getLeft();
            if (consumer instanceof TreeIndexInsertUpdateDeleteOperatorDescriptor) {
                return senderSideMatBlkPolicy;
            } else {
                return pipeliningPolicy;
            }
        }
    }
}
