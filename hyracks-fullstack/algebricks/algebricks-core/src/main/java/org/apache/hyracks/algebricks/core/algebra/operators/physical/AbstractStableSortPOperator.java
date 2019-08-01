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
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

public abstract class AbstractStableSortPOperator extends AbstractPhysicalOperator {

    // variable memory, min 3 frames
    public static final int MIN_FRAME_LIMIT_FOR_SORT = 3;

    OrderColumn[] sortColumns;
    ILocalStructuralProperty orderProp;

    AbstractStableSortPOperator() {
    }

    public OrderColumn[] getSortColumns() {
        return sortColumns;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        OrderOperator sortOp = (OrderOperator) op;
        computeLocalProperties(sortOp);
        AbstractLogicalOperator childOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        StructuralPropertiesVector childProp = (StructuralPropertiesVector) childOp.getDeliveredPhysicalProperties();
        deliveredProperties = new StructuralPropertiesVector(childProp.getPartitioningProperty(),
                Collections.singletonList(orderProp));
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext ctx) {
        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
            OrderOperator sortOp = (OrderOperator) op;
            if (orderProp == null) {
                computeLocalProperties(sortOp);
            }
            StructuralPropertiesVector[] requiredProp = new StructuralPropertiesVector[1];
            IPartitioningProperty partitioning;
            INodeDomain targetNodeDomain = ctx.getComputationNodeDomain();
            String fullParallelAnnotation = getFullParallelAnnotation(sortOp, targetNodeDomain, ctx);
            if (fullParallelAnnotation != null) {
                // partitioning requirement: input data is re-partitioned on sort columns (global ordering)
                RangeMap rangeMap = getRangeMap(sortOp, fullParallelAnnotation);
                partitioning = new OrderedPartitionedProperty(Arrays.asList(sortColumns), targetNodeDomain, rangeMap);
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

    public void computeLocalProperties(OrderOperator ord) {
        List<OrderColumn> orderColumns = new ArrayList<>();
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
     * When a non-null value is returned, the sort operator requires ORDERED_PARTITION (applicable to both static and
     *  dynamic range versions).
     * Conditions:
     * 1. Execution mode == partitioned
     * 2. Dynamic range map that was not disabled by some checks, or a static range map
     * 3. User didn't disable it
     * 4. User didn't provide static range map
     * 5. Physical sort operator is not in-memory
     * 6. There are at least two partitions in the cluster
     * @param sortOp the sort operator
     * @param clusterDomain the partitions specification of the cluster
     * @param ctx optimization context
     * @return annotation name (non-null) if the sort operator should be full parallel sort, {@code null} otherwise.
     */
    private String getFullParallelAnnotation(AbstractLogicalOperator sortOp, INodeDomain clusterDomain,
            IOptimizationContext ctx) {
        if (sortOp.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.STABLE_SORT
                && clusterDomain.cardinality() != null && clusterDomain.cardinality() > 1
                && ctx.getPhysicalOptimizationConfig().getSortParallel()) {
            if (sortOp.getAnnotations().containsKey(OperatorAnnotations.USE_STATIC_RANGE)) {
                return OperatorAnnotations.USE_STATIC_RANGE;
            } else if (sortOp.getAnnotations().get(OperatorAnnotations.USE_DYNAMIC_RANGE) != Boolean.FALSE) {
                return OperatorAnnotations.USE_DYNAMIC_RANGE;
            }
        }
        return null;
    }

    private RangeMap getRangeMap(ILogicalOperator sortOp, String fullParallelAnnotation) {
        switch (fullParallelAnnotation) {
            case OperatorAnnotations.USE_STATIC_RANGE:
                return (RangeMap) sortOp.getAnnotations().get(fullParallelAnnotation);
            case OperatorAnnotations.USE_DYNAMIC_RANGE:
                return null;
            default:
                throw new IllegalStateException(fullParallelAnnotation);
        }
    }

    @Override
    public void createLocalMemoryRequirements(ILogicalOperator op) {
        localMemoryRequirements = LocalMemoryRequirements.variableMemoryBudget(MIN_FRAME_LIMIT_FOR_SORT);
    }
}
