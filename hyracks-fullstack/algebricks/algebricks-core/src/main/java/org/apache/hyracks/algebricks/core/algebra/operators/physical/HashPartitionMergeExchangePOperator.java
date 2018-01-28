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
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder.TargetConstraint;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty.PropertyType;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import org.apache.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;

public class HashPartitionMergeExchangePOperator extends AbstractExchangePOperator {

    private final List<OrderColumn> orderColumns;
    private final List<LogicalVariable> partitionFields;
    private final INodeDomain domain;

    public HashPartitionMergeExchangePOperator(List<OrderColumn> orderColumns, List<LogicalVariable> partitionFields,
            INodeDomain domain) {
        this.orderColumns = orderColumns;
        this.partitionFields = partitionFields;
        this.domain = domain;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.HASH_PARTITION_MERGE_EXCHANGE;
    }

    public List<OrderColumn> getOrderExpressions() {
        return orderColumns;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        IPartitioningProperty p =
                new UnorderedPartitionedProperty(new ListSet<LogicalVariable>(partitionFields), domain);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        List<ILocalStructuralProperty> op2Locals = op2.getDeliveredPhysicalProperties().getLocalProperties();
        List<ILocalStructuralProperty> locals = new ArrayList<ILocalStructuralProperty>();
        for (ILocalStructuralProperty prop : op2Locals) {
            if (prop.getPropertyType() == PropertyType.LOCAL_ORDER_PROPERTY) {
                locals.add(prop);
            } else {
                break;
            }
        }

        this.deliveredProperties = new StructuralPropertiesVector(p, locals);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        List<ILocalStructuralProperty> orderProps = new LinkedList<ILocalStructuralProperty>();
        List<OrderColumn> columns = new ArrayList<OrderColumn>();
        for (OrderColumn oc : orderColumns) {
            LogicalVariable var = oc.getColumn();
            columns.add(new OrderColumn(var, oc.getOrder()));
        }
        orderProps.add(new LocalOrderProperty(columns));
        StructuralPropertiesVector[] r =
                new StructuralPropertiesVector[] { new StructuralPropertiesVector(null, orderProps) };
        return new PhysicalRequirements(r, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + " MERGE:" + orderColumns + " HASH:" + partitionFields;
    }

    @Override
    public Pair<IConnectorDescriptor, TargetConstraint> createConnectorDescriptor(IConnectorDescriptorRegistry spec,
            ILogicalOperator op, IOperatorSchema opSchema, JobGenContext context) throws AlgebricksException {
        int[] keys = new int[partitionFields.size()];
        IBinaryHashFunctionFactory[] hashFunctionFactories = new IBinaryHashFunctionFactory[partitionFields.size()];
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        {
            int i = 0;
            IBinaryHashFunctionFactoryProvider hashFunProvider = context.getBinaryHashFunctionFactoryProvider();
            for (LogicalVariable v : partitionFields) {
                keys[i] = opSchema.findVariable(v);
                hashFunctionFactories[i] = hashFunProvider.getBinaryHashFunctionFactory(env.getVarType(v));
                ++i;
            }
        }
        ITuplePartitionComputerFactory tpcf = new FieldHashPartitionComputerFactory(keys, hashFunctionFactories);

        int n = orderColumns.size();
        int[] sortFields = new int[n];
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[n];

        INormalizedKeyComputerFactoryProvider nkcfProvider = context.getNormalizedKeyComputerFactoryProvider();
        INormalizedKeyComputerFactory nkcf = null;

        int j = 0;
        for (OrderColumn oc : orderColumns) {
            LogicalVariable var = oc.getColumn();
            sortFields[j] = opSchema.findVariable(var);
            Object type = env.getVarType(var);
            IBinaryComparatorFactoryProvider bcfp = context.getBinaryComparatorFactoryProvider();
            comparatorFactories[j] = bcfp.getBinaryComparatorFactory(type, oc.getOrder() == OrderKind.ASC);
            if (j == 0 && nkcfProvider != null && type != null) {
                nkcf = nkcfProvider.getNormalizedKeyComputerFactory(type, oc.getOrder() == OrderKind.ASC);
            }
            j++;
        }

        IConnectorDescriptor conn =
                new MToNPartitioningMergingConnectorDescriptor(spec, tpcf, sortFields, comparatorFactories, nkcf);
        return new Pair<IConnectorDescriptor, TargetConstraint>(conn, null);
    }

    public List<LogicalVariable> getPartitionFields() {
        return partitionFields;
    }

    public List<OrderColumn> getOrderColumns() {
        return orderColumns;
    }

}
