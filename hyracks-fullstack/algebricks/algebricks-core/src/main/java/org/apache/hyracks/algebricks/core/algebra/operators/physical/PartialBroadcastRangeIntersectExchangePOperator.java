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
import java.util.Objects;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.IntervalColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.PartialBroadcastOrderedIntersectProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputerFactory;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.data.partition.range.FieldRangeIntersectPartitionComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.dataflow.common.data.partition.range.StaticRangeMapSupplier;
import org.apache.hyracks.dataflow.std.connectors.MToNPartialBroadcastConnectorDescriptor;

/**
 * This exchange operator delivers {@link IPartitioningProperty.PartitioningType#PARTIAL_BROADCAST_ORDERED_INTERSECT}
 * structural property
 */
public class PartialBroadcastRangeIntersectExchangePOperator extends AbstractExchangePOperator {

    private final List<IntervalColumn> intervalFields;

    private final INodeDomain domain;

    private final RangeMap rangeMap;

    public PartialBroadcastRangeIntersectExchangePOperator(List<IntervalColumn> intervalFields, INodeDomain domain,
            RangeMap rangeMap) {
        this.intervalFields = intervalFields;
        this.domain = domain;
        this.rangeMap = rangeMap;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.PARTIAL_BROADCAST_RANGE_INTERSECT_EXCHANGE;
    }

    public final List<IntervalColumn> getIntervalFields() {
        return intervalFields;
    }

    public final INodeDomain getDomain() {
        return domain;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        IPartitioningProperty pp = new PartialBroadcastOrderedIntersectProperty(intervalFields, domain, rangeMap);
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
        Triple<int[], int[], IBinaryComparatorFactory[]> pIntervalColumns =
                createIntervalColumnsAndComparators(op, opSchema, context);
        ITupleMultiPartitionComputerFactory tpcf =
                new FieldRangeIntersectPartitionComputerFactory(pIntervalColumns.first, pIntervalColumns.second,
                        pIntervalColumns.third, new StaticRangeMapSupplier(rangeMap), op.getSourceLocation());
        IConnectorDescriptor conn = new MToNPartialBroadcastConnectorDescriptor(spec, tpcf);
        return new Pair<>(conn, null);
    }

    private Triple<int[], int[], IBinaryComparatorFactory[]> createIntervalColumnsAndComparators(ILogicalOperator op,
            IOperatorSchema opSchema, JobGenContext context) throws AlgebricksException {
        int n = intervalFields.size();
        int[] startFields = new int[n];
        int[] endFields = new int[n];
        IBinaryComparatorFactory[] comps = new IBinaryComparatorFactory[n];
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        int i = 0;
        for (IntervalColumn ic : intervalFields) {
            LogicalVariable startVar = ic.getStartColumn();
            startFields[i] = opSchema.findVariable(startVar);
            Object startVarType = env.getVarType(startVar);
            LogicalVariable endEvar = ic.getEndColumn();
            endFields[i] = opSchema.findVariable(endEvar);
            Object endVarType = env.getVarType(endEvar);
            if (!Objects.equals(startVarType, endVarType)) {
                throw new IllegalStateException();
            }
            IBinaryComparatorFactoryProvider bcfp = context.getBinaryComparatorFactoryProvider();
            comps[i] =
                    bcfp.getBinaryComparatorFactory(startVarType, ic.getOrder() == OrderOperator.IOrder.OrderKind.ASC);
            i++;
        }
        return new Triple<>(startFields, endFields, comps);
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + " " + intervalFields + (rangeMap != null ? " RANGE_MAP:" + rangeMap : "");
    }
}
