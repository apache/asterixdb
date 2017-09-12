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

import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;

public abstract class AbstractPreSortedDistinctByPOperator extends AbstractPhysicalOperator {

    protected List<LogicalVariable> columnList;

    public AbstractPreSortedDistinctByPOperator(List<LogicalVariable> columnList) {
        this.columnList = columnList;
    }

    public void setDistinctByColumns(List<LogicalVariable> distinctByColumns) {
        this.columnList = distinctByColumns;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        IPartitioningProperty pp = op2.getDeliveredPhysicalProperties().getPartitioningProperty();
        List<ILocalStructuralProperty> propsLocal = op2.getDeliveredPhysicalProperties().getLocalProperties();
        deliveredProperties = new StructuralPropertiesVector(pp, propsLocal);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
        List<ILocalStructuralProperty> localProps = new ArrayList<>();
        List<OrderColumn> orderColumns = new ArrayList<>();
        for (LogicalVariable column : columnList) {
            orderColumns.add(new OrderColumn(column, OrderOperator.IOrder.OrderKind.ASC));
        }
        localProps.add(new LocalOrderProperty(orderColumns));
        IPartitioningProperty pp = null;
        AbstractLogicalOperator aop = (AbstractLogicalOperator) op;
        if (aop.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
            pp = new UnorderedPartitionedProperty(new ListSet<>(columnList), context.getComputationNodeDomain());
        }
        pv[0] = new StructuralPropertiesVector(pp, localProps);
        return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }

    protected int[] getKeysAndDecs(IOperatorSchema inputSchema) {
        int keys[] = JobGenHelper.variablesToFieldIndexes(columnList, inputSchema);
        int sz = inputSchema.getSize();
        int fdSz = sz - columnList.size();
        int[] fdColumns = new int[fdSz];
        int j = 0;
        for (LogicalVariable v : inputSchema) {
            if (!columnList.contains(v)) {
                fdColumns[j++] = inputSchema.findVariable(v);
            }
        }
        int[] keysAndDecs = new int[keys.length + fdColumns.length];
        for (int i = 0; i < keys.length; i++) {
            keysAndDecs[i] = keys[i];
        }
        for (int i = 0; i < fdColumns.length; i++) {
            keysAndDecs[i + keys.length] = fdColumns[i];
        }
        return keysAndDecs;
    }

}
