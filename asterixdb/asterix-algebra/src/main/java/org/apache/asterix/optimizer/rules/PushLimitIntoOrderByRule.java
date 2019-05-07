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
package org.apache.asterix.optimizer.rules;

import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StableSortPOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * If an ORDER operator is followed by LIMIT, then we can push LIMIT into ORDER operator.
 * Finally, ORDER operator use TopKSorterOperatorDescriptor that can efficiently
 * sort tuples and fetch top K results.
 * =================
 * matching pattern:
 * limit <- order
 * =
 * producing pattern:
 * limit <- new order (topK applied)
 */
public class PushLimitIntoOrderByRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        // The current operator should be LIMIT operator.
        if (op.getOperatorTag() != LogicalOperatorTag.LIMIT) {
            return false;
        }

        Mutable<ILogicalOperator> opRef2 = op.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();

        if (context.checkAndAddToAlreadyCompared(op, op2)) {
            return false;
        }

        // Should be ORDER operator
        if (op2.getOperatorTag() != LogicalOperatorTag.ORDER) {
            return false;
        } else {
            // ORDER operator is followed by LIMIT. Thus we can check whether we can apply this rule.
            boolean res = pushLimitIntoOrder(opRef, opRef2, context);
            if (res) {
                OperatorPropertiesUtil.typeOpRec(opRef, context);
            }
            return res;
        }
    }

    /**
     * Generate new ORDER operator that uses TopKSort module and replaces the old ORDER operator.
     */
    private boolean pushLimitIntoOrder(Mutable<ILogicalOperator> opRef, Mutable<ILogicalOperator> opRef2,
            IOptimizationContext context) throws AlgebricksException {
        LimitOperator limitOp = (LimitOperator) opRef.getValue();
        OrderOperator orderOp = (OrderOperator) opRef2.getValue();

        // We don't push-down LIMIT into in-memory sort.
        if (orderOp.getPhysicalOperator().getOperatorTag() != PhysicalOperatorTag.STABLE_SORT) {
            return false;
        }

        Integer topK = getOutputLimit(limitOp);
        if (topK == null) {
            return false;
        }

        // Create the new ORDER operator, set the topK value, and replace the current one.
        OrderOperator newOrderOp = new OrderOperator(orderOp.getOrderExpressions(), topK);
        newOrderOp.setSourceLocation(orderOp.getSourceLocation());
        newOrderOp.setPhysicalOperator(new StableSortPOperator(newOrderOp.getTopK()));
        newOrderOp.getInputs().addAll(orderOp.getInputs());
        newOrderOp.setExecutionMode(orderOp.getExecutionMode());
        newOrderOp.recomputeSchema();
        newOrderOp.computeDeliveredPhysicalProperties(context);
        opRef2.setValue(newOrderOp);
        context.computeAndSetTypeEnvironmentForOperator(newOrderOp);
        context.addToDontApplySet(this, limitOp);
        return true;
    }

    static Integer getOutputLimit(LimitOperator limitOp) {
        // Currently, we support LIMIT with a constant value.
        ILogicalExpression maxObjectsExpr = limitOp.getMaxObjects().getValue();
        IAObject maxObjectsValue = ConstantExpressionUtil.getConstantIaObject(maxObjectsExpr, ATypeTag.INTEGER);
        if (maxObjectsValue == null) {
            return null;
        }
        int topK = ((AInt32) maxObjectsValue).getIntegerValue();
        if (topK < 0) {
            topK = 0;
        }

        // Get the offset constant if there is one. If one presents, then topK = topK + offset.
        // This is because we can't apply offset to the external sort.
        // Final topK will be applied through LIMIT.
        ILogicalExpression offsetExpr = limitOp.getOffset().getValue();
        if (offsetExpr != null) {
            IAObject offsetValue = ConstantExpressionUtil.getConstantIaObject(offsetExpr, ATypeTag.INTEGER);
            if (offsetValue == null) {
                return null;
            }
            int offset = ((AInt32) offsetValue).getIntegerValue();
            if (offset < 0) {
                offset = 0;
            }
            // Check the overflow case.
            if (offset >= Integer.MAX_VALUE - topK) {
                return null;
            }
            topK += offset;
        }

        return topK;
    }
}
