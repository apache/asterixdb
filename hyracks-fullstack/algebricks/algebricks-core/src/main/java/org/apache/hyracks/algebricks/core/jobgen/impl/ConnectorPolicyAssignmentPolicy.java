/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.algebricks.core.jobgen.impl;

import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicy;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.api.dataflow.connectors.PipeliningConnectorPolicy;
import org.apache.hyracks.api.dataflow.connectors.SendSideMaterializedPipeliningConnectorPolicy;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.MToOneSequentialMergingConnectorDescriptor;

public class ConnectorPolicyAssignmentPolicy implements IConnectorPolicyAssignmentPolicy {
    private static final long serialVersionUID = 1L;
    private IConnectorPolicy senderSideMaterializePolicy = new SendSideMaterializedPipeliningConnectorPolicy();
    private IConnectorPolicy pipeliningPolicy = new PipeliningConnectorPolicy();

    @Override
    public IConnectorPolicy getConnectorPolicyAssignment(IConnectorDescriptor c, int nProducers, int nConsumers,
            int[] fanouts) {
        if (c instanceof MToNPartitioningMergingConnectorDescriptor
                || c instanceof MToOneSequentialMergingConnectorDescriptor) {
            return senderSideMaterializePolicy;
        } else {
            return pipeliningPolicy;
        }
    }
}
