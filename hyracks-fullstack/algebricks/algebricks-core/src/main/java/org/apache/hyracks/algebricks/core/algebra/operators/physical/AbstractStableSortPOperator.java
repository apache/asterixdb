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
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;

public abstract class AbstractStableSortPOperator extends AbstractPhysicalOperator {

    protected OrderColumn[] sortColumns;
    protected ILocalStructuralProperty orderProp;

    public AbstractStableSortPOperator() {
    }

    public OrderColumn[] getSortColumns() {
        return sortColumns;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        // if (orderProps == null) { // to do caching, we need some mechanism to
        // invalidate cache
        computeLocalProperties(op);
        // }
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        StructuralPropertiesVector childProp = (StructuralPropertiesVector) op2.getDeliveredPhysicalProperties();
        deliveredProperties = new StructuralPropertiesVector(childProp.getPartitioningProperty(),
                Collections.singletonList(orderProp));
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator sortOp,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext ctx) {
        if (sortOp.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
            if (orderProp == null) {
                computeLocalProperties(sortOp);
            }
            StructuralPropertiesVector[] requiredProp = new StructuralPropertiesVector[1];
            IPartitioningProperty partitioning;
            INodeDomain targetNodeDomain = ctx.getComputationNodeDomain();
            if (isFullParallel((AbstractLogicalOperator) sortOp, targetNodeDomain, ctx)) {
                // partitioning requirement: input data is re-partitioned on sort columns (global ordering)
                // TODO(ali): static range map implementation should be fixed to require ORDERED_PARTITION and come here
                partitioning = new OrderedPartitionedProperty(Arrays.asList(sortColumns), targetNodeDomain);
            } else {
                // partitioning requirement: input data is unpartitioned (i.e. must be merged at one site)
                partitioning = IPartitioningProperty.UNPARTITIONED;
            }
            // local requirement: each partition must be locally ordered
            requiredProp[0] = new StructuralPropertiesVector(partitioning, Collections.singletonList(orderProp));
            return new PhysicalRequirements(requiredProp, IPartitioningRequirementsCoordinator.NO_COORDINATION);
        } else {
            return emptyUnaryRequirements();
        }
    }

    public void computeLocalProperties(ILogicalOperator op) {
        OrderOperator ord = (OrderOperator) op;
        List<OrderColumn> orderColumns = new ArrayList<OrderColumn>();
        for (Pair<IOrder, Mutable<ILogicalExpression>> p : ord.getOrderExpressions()) {
            ILogicalExpression expr = p.second.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
                LogicalVariable var = varRef.getVariableReference();
                orderColumns.add(new OrderColumn(var, p.first.getKind()));
            } else {
                throw new IllegalStateException();
            }
        }
        sortColumns = orderColumns.toArray(new OrderColumn[orderColumns.size()]);
        orderProp = new LocalOrderProperty(orderColumns);
    }

    public ILocalStructuralProperty getOrderProperty() {
        return orderProp;
    }

    @Override
    public String toString() {
        if (orderProp == null) {
            return getOperatorTag().toString();
        } else {
            return getOperatorTag().toString() + " " + orderProp;
        }
    }

    @Override
    public Pair<int[], int[]> getInputOutputDependencyLabels(ILogicalOperator op) {
        int[] inputDependencyLabels = new int[] { 0 };
        int[] outputDependencyLabels = new int[] { 1 };
        return new Pair<int[], int[]>(inputDependencyLabels, outputDependencyLabels);
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }

    /**
     * When true, the sort operator requires ORDERED_PARTITION (only applicable to dynamic version for now).
     * Conditions:
     * 1. Execution mode == partitioned
     * 2. Dynamic range map was not disabled by some checks
     * 3. User didn't disable it
     * 4. User didn't provide static range map
     * 5. Physical sort operator is not in-memory
     * 6. There are at least two partitions in the cluster
     * @param sortOp the sort operator
     * @param clusterDomain the partitions specification of the cluster
     * @param ctx optimization context
     * @return true if the sort operator should be full parallel sort, false otherwise.
     */
    private boolean isFullParallel(AbstractLogicalOperator sortOp, INodeDomain clusterDomain,
            IOptimizationContext ctx) {
        return sortOp.getAnnotations().get(OperatorAnnotations.USE_DYNAMIC_RANGE) != Boolean.FALSE
                && !sortOp.getAnnotations().containsKey(OperatorAnnotations.USE_STATIC_RANGE)
                && sortOp.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.STABLE_SORT
                && clusterDomain.cardinality() != null && clusterDomain.cardinality() > 1
                && ctx.getPhysicalOptimizationConfig().getSortParallel();
    }
}
