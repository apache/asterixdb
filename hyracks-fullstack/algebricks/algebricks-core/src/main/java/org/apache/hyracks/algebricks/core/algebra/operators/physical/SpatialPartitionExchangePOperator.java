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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.Set;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.SpatialPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputerFactory;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.data.partition.SpatialPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartialBroadcastConnectorDescriptor;

public class SpatialPartitionExchangePOperator extends AbstractExchangePOperator {

    private Set<LogicalVariable> partitioningFields;
    private INodeDomain domain;

    public SpatialPartitionExchangePOperator(Set<LogicalVariable> partitioningFields, INodeDomain domain) {
        this.partitioningFields = partitioningFields;
        this.domain = domain;
    }

    @Override
    public Pair<IConnectorDescriptor, IHyracksJobBuilder.TargetConstraint> createConnectorDescriptor(
            IConnectorDescriptorRegistry spec, ILogicalOperator op, IOperatorSchema opSchema, JobGenContext context)
            throws AlgebricksException {
        int[] keys = new int[partitioningFields.size()];
        int i = 0;
        for (LogicalVariable v : partitioningFields) {
            keys[i] = opSchema.findVariable(v);
            ++i;
        }

        // TODO: parse the partitioning parameters from user inputs
        int numRows = 3;
        int numColumns = 3;
        int numPartitions = numRows * numColumns;
        ITupleMultiPartitionComputerFactory tpcf =
                new SpatialPartitionComputerFactory(keys, -180.0, -83.0, 180.0, 90.0, numRows, numColumns);
        IConnectorDescriptor conn = new MToNPartialBroadcastConnectorDescriptor(spec, tpcf);
        return new Pair<>(conn, null);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.SPATIAL_PARTITION_EXCHANGE;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + " " + partitioningFields;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) throws AlgebricksException {
        return emptyUnaryRequirements();
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        IPartitioningProperty p =
                new SpatialPartitionedProperty(new ListSet<LogicalVariable>(partitioningFields), domain);
        this.deliveredProperties = new StructuralPropertiesVector(p, null);
    }

    public Set<LogicalVariable> getPartitioningFields() {
        return partitioningFields;
    }

    public INodeDomain getDomain() {
        return domain;
    }
}
