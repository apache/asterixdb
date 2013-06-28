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

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.OrderColumn;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;

public abstract class AbstractStableSortPOperator extends AbstractPhysicalOperator {

    protected OrderColumn[] sortColumns;
    protected List<ILocalStructuralProperty> orderProps;

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
        deliveredProperties = new StructuralPropertiesVector(childProp.getPartitioningProperty(), orderProps);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator iop,
            IPhysicalPropertiesVector reqdByParent) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) iop;
        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
            if (orderProps == null) {
                computeLocalProperties(op);
            }
            StructuralPropertiesVector[] r = new StructuralPropertiesVector[] { new StructuralPropertiesVector(
                    IPartitioningProperty.UNPARTITIONED, orderProps) };
            return new PhysicalRequirements(r, IPartitioningRequirementsCoordinator.NO_COORDINATION);
        } else {
            return emptyUnaryRequirements();
        }
    }

    public void computeLocalProperties(ILogicalOperator op) {
        orderProps = new LinkedList<ILocalStructuralProperty>();

        OrderOperator ord = (OrderOperator) op;
        for (Pair<IOrder, Mutable<ILogicalExpression>> p : ord.getOrderExpressions()) {
            ILogicalExpression expr = p.second.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
                LogicalVariable var = varRef.getVariableReference();
                switch (p.first.getKind()) {
                    case ASC: {
                        orderProps.add(new LocalOrderProperty(new OrderColumn(var, OrderKind.ASC)));
                        break;
                    }
                    case DESC: {
                        orderProps.add(new LocalOrderProperty(new OrderColumn(var, OrderKind.DESC)));
                        break;
                    }
                    default: {
                        throw new NotImplementedException();
                    }
                }
            } else {
                throw new IllegalStateException();
            }
        }

        int n = orderProps.size();
        sortColumns = new OrderColumn[n];
        int i = 0;
        for (ILocalStructuralProperty prop : orderProps) {
            sortColumns[i++] = ((LocalOrderProperty) prop).getOrderColumn();
        }
    }

    public List<ILocalStructuralProperty> getOrderProperties() {
        return orderProps;
    }

    @Override
    public String toString() {
        if (orderProps == null) {
            return getOperatorTag().toString();
        } else {
            return getOperatorTag().toString() + " " + orderProps;
        }
    }

}
