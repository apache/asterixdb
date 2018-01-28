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
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder.TargetConstraint;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty.PropertyType;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
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

public class SortMergeExchangePOperator extends AbstractExchangePOperator {

    private final OrderColumn[] sortColumns;

    public SortMergeExchangePOperator(OrderColumn[] sortColumns) {
        this.sortColumns = sortColumns;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.SORT_MERGE_EXCHANGE;
    }

    public OrderColumn[] getSortColumns() {
        return sortColumns;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getOperatorTag());
        sb.append(" [");
        sb.append(sortColumns[0]);
        for (int i = 1; i < sortColumns.length; i++) {
            sb.append(", " + sortColumns[i]);
        }
        sb.append(" ]");
        return sb.toString();
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator inp1 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        IPhysicalPropertiesVector pv1 = inp1.getDeliveredPhysicalProperties();
        if (pv1 == null) {
            inp1.computeDeliveredPhysicalProperties(context);
            pv1 = inp1.getDeliveredPhysicalProperties();
        }

        List<OrderColumn> orderColumns = new ArrayList<OrderColumn>();
        List<ILocalStructuralProperty> localProps = new ArrayList<ILocalStructuralProperty>(sortColumns.length);
        for (ILocalStructuralProperty prop : pv1.getLocalProperties()) {
            if (prop.getPropertyType() == PropertyType.LOCAL_ORDER_PROPERTY) {
                LocalOrderProperty lop = (LocalOrderProperty) prop;
                for (OrderColumn oc : lop.getOrderColumns()) {
                    if (oc.equals(sortColumns[orderColumns.size()])) {
                        orderColumns.add(oc);
                        if (orderColumns.size() == sortColumns.length) {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            } else {
                continue;
            }
        }
        if (orderColumns.size() > 0) {
            localProps.add(new LocalOrderProperty(orderColumns));
        }
        this.deliveredProperties = new StructuralPropertiesVector(IPartitioningProperty.UNPARTITIONED, localProps);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        List<ILocalStructuralProperty> localProps = new ArrayList<ILocalStructuralProperty>(sortColumns.length);
        localProps.add(new LocalOrderProperty(Arrays.asList(sortColumns)));
        StructuralPropertiesVector[] r =
                new StructuralPropertiesVector[] { new StructuralPropertiesVector(null, localProps) };
        return new PhysicalRequirements(r, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public Pair<IConnectorDescriptor, TargetConstraint> createConnectorDescriptor(IConnectorDescriptorRegistry spec,
            ILogicalOperator op, IOperatorSchema opSchema, JobGenContext context) throws AlgebricksException {
        int n = sortColumns.length;
        int[] sortFields = new int[n];
        IBinaryComparatorFactory[] comps = new IBinaryComparatorFactory[n];
        IBinaryHashFunctionFactory[] hashFuns = new IBinaryHashFunctionFactory[n];
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);

        INormalizedKeyComputerFactoryProvider nkcfProvider = context.getNormalizedKeyComputerFactoryProvider();
        INormalizedKeyComputerFactory nkcf = null;

        for (int i = 0; i < n; i++) {
            sortFields[i] = opSchema.findVariable(sortColumns[i].getColumn());
            Object type = env.getVarType(sortColumns[i].getColumn());
            IBinaryComparatorFactoryProvider bcfp = context.getBinaryComparatorFactoryProvider();
            comps[i] = bcfp.getBinaryComparatorFactory(type, sortColumns[i].getOrder() == OrderKind.ASC);
            IBinaryHashFunctionFactoryProvider bhffp = context.getBinaryHashFunctionFactoryProvider();
            hashFuns[i] = bhffp.getBinaryHashFunctionFactory(type);
            if (i == 0 && nkcfProvider != null && type != null) {
                nkcf = nkcfProvider.getNormalizedKeyComputerFactory(type, sortColumns[i].getOrder() == OrderKind.ASC);
            }
        }
        ITuplePartitionComputerFactory tpcf = new FieldHashPartitionComputerFactory(sortFields, hashFuns);
        IConnectorDescriptor conn = new MToNPartitioningMergingConnectorDescriptor(spec, tpcf, sortFields, comps, nkcf);
        return new Pair<IConnectorDescriptor, TargetConstraint>(conn, TargetConstraint.ONE);
    }

}
