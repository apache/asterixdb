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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StableSortPOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushNestedOrderByUnderPreSortedGroupByRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        if (op.getPhysicalOperator() == null) {
            return false;
        }
        AbstractPhysicalOperator pOp = (AbstractPhysicalOperator) op.getPhysicalOperator();
        if (pOp.getOperatorTag() != PhysicalOperatorTag.PRE_CLUSTERED_GROUP_BY) {
            return false;
        }
        GroupByOperator gby = (GroupByOperator) op;
        if (gby.getNestedPlans().isEmpty()) {
            return false;
        }
        ILogicalPlan plan = gby.getNestedPlans().get(0);
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) plan.getRoots().get(0).getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        Mutable<ILogicalOperator> opRef2 = op1.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.ORDER) {
            return false;
        }
        OrderOperator order1 = (OrderOperator) op2;
        if (!isIndependentFromChildren(order1)) {
            return false;
        }
        if (OperatorManipulationUtil.ancestorOfOperators(order1.getInputs().get(0).getValue(),
                EnumSet.of(LogicalOperatorTag.ORDER))) {
            return false;
        }
        AbstractPhysicalOperator pOrder1 = (AbstractPhysicalOperator) op2.getPhysicalOperator();
        if (pOrder1.getOperatorTag() != PhysicalOperatorTag.STABLE_SORT
                && pOrder1.getOperatorTag() != PhysicalOperatorTag.MICRO_STABLE_SORT) {
            return false;
        }
        // StableSortPOperator sort1 = (StableSortPOperator) pOrder1;
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        if (op3.getOperatorTag() != LogicalOperatorTag.ORDER) {
            return false;
        }
        AbstractPhysicalOperator pOp3 = (AbstractPhysicalOperator) op3.getPhysicalOperator();
        if (pOp3.getOperatorTag() != PhysicalOperatorTag.STABLE_SORT) {
            return false;
        }
        OrderOperator order2 = (OrderOperator) op3;
        StableSortPOperator sort2 = (StableSortPOperator) pOp3;
        // int n1 = sort1.getSortColumns().length;
        // int n2 = sort2.getSortColumns().length;
        // OrderColumn[] sortColumns = new OrderColumn[n2 + n1];
        // System.arraycopy(sort2.getSortColumns(), 0, sortColumns, 0, n2);
        // int k = 0;
        for (Pair<IOrder, Mutable<ILogicalExpression>> oe : order1.getOrderExpressions()) {
            order2.getOrderExpressions().add(oe);
            // sortColumns[n2 + k] = sort1.getSortColumns()[k];
            // ++k;
        }
        // sort2.setSortColumns(sortColumns);
        sort2.computeDeliveredProperties(order2, null);
        // remove order1
        ILogicalOperator underOrder1 = order1.getInputs().get(0).getValue();
        opRef2.setValue(underOrder1);
        return true;
    }

    private boolean isIndependentFromChildren(OrderOperator order1) throws AlgebricksException {
        Set<LogicalVariable> free = new HashSet<LogicalVariable>();
        OperatorPropertiesUtil.getFreeVariablesInSelfOrDesc(order1, free);
        Set<LogicalVariable> usedInOrder = new HashSet<LogicalVariable>();
        VariableUtilities.getUsedVariables(order1, usedInOrder);
        return free.containsAll(usedInOrder);
    }

}
