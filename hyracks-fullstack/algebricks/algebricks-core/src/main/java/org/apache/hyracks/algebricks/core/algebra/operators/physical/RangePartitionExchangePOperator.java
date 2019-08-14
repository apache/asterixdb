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
import java.util.LinkedList;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder.TargetConstraint;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.data.partition.range.DynamicRangeMapSupplier;
import org.apache.hyracks.dataflow.common.data.partition.range.FieldRangePartitionComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMapSupplier;
import org.apache.hyracks.dataflow.common.data.partition.range.StaticRangeMapSupplier;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;

public class RangePartitionExchangePOperator extends AbstractExchangePOperator {

    private List<OrderColumn> partitioningFields;
    private INodeDomain domain;
    private RangeMap rangeMap;
    private final boolean rangeMapIsComputedAtRunTime;
    private final String rangeMapKeyInContext;

    private RangePartitionExchangePOperator(List<OrderColumn> partitioningFields, INodeDomain domain, RangeMap rangeMap,
            boolean rangeMapIsComputedAtRunTime, String rangeMapKeyInContext) {
        this.partitioningFields = partitioningFields;
        this.domain = domain;
        this.rangeMap = rangeMap;
        this.rangeMapIsComputedAtRunTime = rangeMapIsComputedAtRunTime;
        this.rangeMapKeyInContext = rangeMapKeyInContext;
    }

    public RangePartitionExchangePOperator(List<OrderColumn> partitioningFields, String rangeMapKeyInContext,
            INodeDomain domain) {
        this(partitioningFields, domain, null, true, rangeMapKeyInContext);
    }

    public RangePartitionExchangePOperator(List<OrderColumn> partitioningFields, INodeDomain domain,
            RangeMap rangeMap) {
        this(partitioningFields, domain, rangeMap, false, "");
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.RANGE_PARTITION_EXCHANGE;
    }

    public List<OrderColumn> getPartitioningFields() {
        return partitioningFields;
    }

    public INodeDomain getDomain() {
        return domain;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        IPartitioningProperty p = new OrderedPartitionedProperty(new ArrayList<>(partitioningFields), domain, rangeMap);
        this.deliveredProperties = new StructuralPropertiesVector(p, new LinkedList<ILocalStructuralProperty>());
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        return emptyUnaryRequirements();
    }

    @Override
    public Pair<IConnectorDescriptor, TargetConstraint> createConnectorDescriptor(IConnectorDescriptorRegistry spec,
            ILogicalOperator op, IOperatorSchema opSchema, JobGenContext context) throws AlgebricksException {
        int n = partitioningFields.size();
        int[] sortFields = new int[n];
        IBinaryComparatorFactory[] comps = new IBinaryComparatorFactory[n];
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        int i = 0;
        for (OrderColumn oc : partitioningFields) {
            LogicalVariable var = oc.getColumn();
            sortFields[i] = opSchema.findVariable(var);
            Object type = env.getVarType(var);
            IBinaryComparatorFactoryProvider bcfp = context.getBinaryComparatorFactoryProvider();
            comps[i] = bcfp.getBinaryComparatorFactory(type, oc.getOrder() == OrderKind.ASC);
            i++;
        }
        RangeMapSupplier rangeMapSupplier = rangeMapIsComputedAtRunTime
                ? new DynamicRangeMapSupplier(rangeMapKeyInContext) : new StaticRangeMapSupplier(rangeMap);
        FieldRangePartitionComputerFactory partitionerFactory =
                new FieldRangePartitionComputerFactory(sortFields, comps, rangeMapSupplier, op.getSourceLocation());
        IConnectorDescriptor conn = new MToNPartitioningConnectorDescriptor(spec, partitionerFactory);
        return new Pair<>(conn, null);
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + " " + partitioningFields
                + (rangeMap != null ? " RANGE_MAP:" + rangeMap : "");
    }
}
