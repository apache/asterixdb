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
package edu.uci.ics.hyracks.algebricks.core.jobgen.impl;

import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.PipeliningConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.connectors.SendSideMaterializedPipeliningConnectorPolicy;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;

public class ConnectorPolicyAssignmentPolicy implements IConnectorPolicyAssignmentPolicy {
    private static final long serialVersionUID = 1L;
    private IConnectorPolicy senderSideMaterializePolicy = new SendSideMaterializedPipeliningConnectorPolicy();
    private IConnectorPolicy pipeliningPolicy = new PipeliningConnectorPolicy();

    @Override
    public IConnectorPolicy getConnectorPolicyAssignment(IConnectorDescriptor c, int nProducers, int nConsumers,
            int[] fanouts) {
        if (c instanceof MToNPartitioningMergingConnectorDescriptor) {
            return senderSideMaterializePolicy;
        } else {
            return pipeliningPolicy;
        }
    }
}
