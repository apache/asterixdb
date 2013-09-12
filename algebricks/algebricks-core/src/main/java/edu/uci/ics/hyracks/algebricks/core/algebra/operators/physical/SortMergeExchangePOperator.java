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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder.TargetConstraint;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty.PropertyType;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.OrderColumn;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.job.IConnectorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;

public class SortMergeExchangePOperator extends AbstractExchangePOperator {

    private OrderColumn[] sortColumns;

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
        int sortCol = 0;
        List<ILocalStructuralProperty> localProps = new ArrayList<ILocalStructuralProperty>(sortColumns.length);
        for (ILocalStructuralProperty prop : pv1.getLocalProperties()) {
            if (prop.getPropertyType() == PropertyType.LOCAL_ORDER_PROPERTY) {
                LocalOrderProperty lop = (LocalOrderProperty) prop;
                if (lop.getOrderColumn().equals(sortColumns[sortCol])) {
                    localProps.add(lop);
                    sortCol++;
                    if (sortCol == sortColumns.length) {
                        break;
                    }
                }
            } else {
                break;
            }
        }
        if (sortCol < sortColumns.length) {
            localProps = null;
        }
        this.deliveredProperties = new StructuralPropertiesVector(IPartitioningProperty.UNPARTITIONED, localProps);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        List<ILocalStructuralProperty> localProps = new ArrayList<ILocalStructuralProperty>(sortColumns.length);
        for (OrderColumn oc : sortColumns) {
            localProps.add(new LocalOrderProperty(oc));
        }
        StructuralPropertiesVector[] r = new StructuralPropertiesVector[] { new StructuralPropertiesVector(null,
                localProps) };
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
