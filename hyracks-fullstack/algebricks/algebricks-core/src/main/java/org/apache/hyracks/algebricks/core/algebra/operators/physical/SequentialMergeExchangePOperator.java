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

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty.PartitioningType;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.connectors.MToOneSequentialMergingConnectorDescriptor;

/**
 * A merging connector that merges the tuples sequentially from the partitions starting from the partition at index 0.
 */
public class SequentialMergeExchangePOperator extends AbstractExchangePOperator {
    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.SEQUENTIAL_MERGE_EXCHANGE;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector requiredByParent, IOptimizationContext context) {
        return emptyUnaryRequirements();
    }

    /**
     * <Pre>
     * The local properties delivered by this connector are either:
     * 1. nothing if the child doesn't deliver any special property
     * 2. order property if the child is locally ordered and globally ordered on the same prefix
     *
     * The partitioning property is always UNPARTITIONED since it's a merging connector
     * </Pre>
     * @param op the logical operator of this physical operator
     * @param context optimization context, not used here
     */
    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator childOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        List<ILocalStructuralProperty> childLocalProps = childOp.getDeliveredPhysicalProperties().getLocalProperties();
        IPartitioningProperty childPartitioning = childOp.getDeliveredPhysicalProperties().getPartitioningProperty();
        List<ILocalStructuralProperty> outputLocalProp = new ArrayList<>(0);
        if (childLocalProps != null && !childLocalProps.isEmpty() && childPartitioning != null
                && childPartitioning.getPartitioningType() == PartitioningType.ORDERED_PARTITIONED) {
            // the child could have a local order property that matches its global order property
            propagateChildProperties((OrderedPartitionedProperty) childPartitioning, childLocalProps, outputLocalProp);
        }

        deliveredProperties = new StructuralPropertiesVector(IPartitioningProperty.UNPARTITIONED, outputLocalProp);
    }

    @Override
    public Pair<IConnectorDescriptor, IHyracksJobBuilder.TargetConstraint> createConnectorDescriptor(
            IConnectorDescriptorRegistry spec, ILogicalOperator op, IOperatorSchema opSchema, JobGenContext context) {
        IConnectorDescriptor connector = new MToOneSequentialMergingConnectorDescriptor(spec);
        return new Pair<>(connector, IHyracksJobBuilder.TargetConstraint.ONE);
    }

    /**
     * Matches prefix of the child's local order property & global order property. If a prefix is determined, the
     * local order property is propagated through this connector. In essence, the connector says it maintains the
     * order originally present in the child.
     * @param childPartitioning the global ordering property of the child made by ORDERED_PARTITIONED partitioning
     * @param childLocalProps the local properties inside the partitions
     * @param outputLocalProp the local property of the connector that will be modified if propagating prop. happens
     */
    private void propagateChildProperties(OrderedPartitionedProperty childPartitioning,
            List<ILocalStructuralProperty> childLocalProps, List<ILocalStructuralProperty> outputLocalProp) {
        ILocalStructuralProperty childLocalProp = childLocalProps.get(0);
        // skip if the first property is a grouping property
        if (childLocalProp.getPropertyType() == ILocalStructuralProperty.PropertyType.LOCAL_ORDER_PROPERTY) {
            OrderColumn localOrderColumn;
            List<OrderColumn> outputOrderColumns = new ArrayList<>();
            List<OrderColumn> globalOrderColumns = childPartitioning.getOrderColumns();
            List<OrderColumn> localOrderColumns = ((LocalOrderProperty) childLocalProp).getOrderColumns();
            // start matching the order columns
            for (int i = 0; i < localOrderColumns.size() && i < globalOrderColumns.size(); i++) {
                localOrderColumn = localOrderColumns.get(i);
                if (localOrderColumn.equals(globalOrderColumns.get(i))) {
                    outputOrderColumns.add(localOrderColumn);
                } else {
                    // stop whenever the matching fails, end of prefix matching
                    break;
                }
            }

            if (!outputOrderColumns.isEmpty()) {
                // found a prefix
                outputLocalProp.add(new LocalOrderProperty(outputOrderColumns));
            }
        }
    }
}
