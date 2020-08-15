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

import java.util.Collections;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.PartialBroadcastOrderedFollowingProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputerFactory;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.data.partition.range.FieldRangeFollowingPartitionComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.dataflow.std.connectors.MToNPartialBroadcastConnectorDescriptor;

/**
 * This exchange operator delivers {@link IPartitioningProperty.PartitioningType#PARTIAL_BROADCAST_ORDERED_FOLLOWING}
 * structural property
 */
public final class PartialBroadcastRangeFollowingExchangePOperator extends AbstractRangeExchangePOperator {

    public PartialBroadcastRangeFollowingExchangePOperator(List<OrderColumn> partitioningFields, INodeDomain domain,
            RangeMap rangeMap) {
        super(partitioningFields, domain, rangeMap);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.PARTIAL_BROADCAST_RANGE_FOLLOWING_EXCHANGE;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        IPartitioningProperty pp = new PartialBroadcastOrderedFollowingProperty(partitioningFields, domain, rangeMap);
        // Broadcasts destroy input's local properties.
        this.deliveredProperties = new StructuralPropertiesVector(pp, Collections.emptyList());
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        return emptyUnaryRequirements();
    }

    @Override
    public Pair<IConnectorDescriptor, IHyracksJobBuilder.TargetConstraint> createConnectorDescriptor(
            IConnectorDescriptorRegistry spec, ILogicalOperator op, IOperatorSchema opSchema, JobGenContext context)
            throws AlgebricksException {
        Pair<int[], IBinaryComparatorFactory[]> pOrderColumns = createOrderColumnsAndComparators(op, opSchema, context);
        ITupleMultiPartitionComputerFactory tpcf = new FieldRangeFollowingPartitionComputerFactory(pOrderColumns.first,
                pOrderColumns.second, crateRangeMapSupplier(), op.getSourceLocation());
        IConnectorDescriptor conn = new MToNPartialBroadcastConnectorDescriptor(spec, tpcf);
        return new Pair<>(conn, null);
    }
}
