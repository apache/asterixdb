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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.IsomorphismOperatorVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.IsomorphismUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Merges two adjacent window operators into one if their window specifications are compatible.
 * <pre>
 * window [$x] <- [f()] with nested plan (aggregate [$a] <- [agg_1()] - ... - nts )
 * window [$y] <- [g()] with nested plan (aggregate [$b] <- [agg_2()] - ... - nts )
 * -->
 * window [$x, $y] <- [f(), g()] with nested plan ( aggregate [$a, $b] <- [agg_1(), agg_2()] - ... - nts )
 * </pre>
 */
public class ConsolidateWindowOperatorsRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.WINDOW) {
            return false;
        }
        WindowOperator winOp1 = (WindowOperator) op1;

        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.WINDOW) {
            return false;
        }

        WindowOperator winOp2 = (WindowOperator) op2;

        if (!IsomorphismOperatorVisitor.compareWindowPartitionSpec(winOp1, winOp2)) {
            return false;
        }
        if (winOp1.hasNestedPlans() && winOp2.hasNestedPlans()
                && !IsomorphismOperatorVisitor.compareWindowFrameSpecExcludingMaxObjects(winOp1, winOp2)) {
            return false;
        }

        Set<LogicalVariable> used1 = new HashSet<>();
        VariableUtilities.getUsedVariables(winOp1, used1);
        if (!OperatorPropertiesUtil.disjoint(winOp2.getVariables(), used1)) {
            return false;
        }

        if (winOp2.hasNestedPlans() && !consolidateNestedPlans(winOp1, winOp2, context)) {
            return false;
        }

        winOp1.getVariables().addAll(winOp2.getVariables());
        winOp1.getExpressions().addAll(winOp2.getExpressions());

        winOp1.getInputs().clear();
        winOp1.getInputs().addAll(winOp2.getInputs());
        context.computeAndSetTypeEnvironmentForOperator(winOp1);

        return true;
    }

    private boolean consolidateNestedPlans(WindowOperator winOpTo, WindowOperator winOpFrom,
            IOptimizationContext context) throws AlgebricksException {
        if (winOpTo.hasNestedPlans()) {
            AggregateOperator aggTo = getAggregateRoot(winOpTo.getNestedPlans());
            if (aggTo == null) {
                return false;
            }
            AggregateOperator aggFrom = getAggregateRoot(winOpFrom.getNestedPlans());
            if (aggFrom == null) {
                return false;
            }
            if (!IsomorphismUtilities.isOperatorIsomorphicPlanSegment(aggTo.getInputs().get(0).getValue(),
                    aggFrom.getInputs().get(0).getValue())) {
                return false;
            }
            int winOpToMaxObjects = winOpTo.getFrameMaxObjects();
            int winOpFromMaxObjects = winOpFrom.getFrameMaxObjects();
            if (winOpToMaxObjects != winOpFromMaxObjects) {
                if (subsumeFrameMaxObjects(winOpFromMaxObjects, winOpToMaxObjects, aggTo)) {
                    winOpToMaxObjects = winOpFromMaxObjects;
                } else if (!subsumeFrameMaxObjects(winOpToMaxObjects, winOpFromMaxObjects, aggFrom)) {
                    return false;
                }
            }

            winOpTo.setFrameMaxObjects(winOpToMaxObjects);
            aggTo.getVariables().addAll(aggFrom.getVariables());
            aggTo.getExpressions().addAll(aggFrom.getExpressions());
            context.computeAndSetTypeEnvironmentForOperator(aggTo);
        } else {
            setAll(winOpTo.getNestedPlans(), winOpFrom.getNestedPlans());
            setAll(winOpTo.getFrameValueExpressions(), winOpFrom.getFrameValueExpressions());
            setAll(winOpTo.getFrameStartExpressions(), winOpFrom.getFrameStartExpressions());
            setAll(winOpTo.getFrameStartValidationExpressions(), winOpFrom.getFrameStartValidationExpressions());
            setAll(winOpTo.getFrameEndExpressions(), winOpFrom.getFrameEndExpressions());
            setAll(winOpTo.getFrameEndValidationExpressions(), winOpFrom.getFrameEndValidationExpressions());
            setAll(winOpTo.getFrameExcludeExpressions(), winOpFrom.getFrameExcludeExpressions());
            winOpTo.setFrameExcludeNegationStartIdx(winOpFrom.getFrameExcludeNegationStartIdx());
            winOpTo.getFrameExcludeUnaryExpression().setValue(winOpFrom.getFrameExcludeUnaryExpression().getValue());
            winOpTo.getFrameOffsetExpression().setValue(winOpFrom.getFrameOffsetExpression().getValue());
            winOpTo.setFrameMaxObjects(winOpFrom.getFrameMaxObjects());
        }
        return true;
    }

    private AggregateOperator getAggregateRoot(List<ILogicalPlan> nestedPlans) {
        if (nestedPlans.size() != 1) {
            return null;
        }
        List<Mutable<ILogicalOperator>> roots = nestedPlans.get(0).getRoots();
        if (roots.size() != 1) {
            return null;
        }
        ILogicalOperator rootOp = roots.get(0).getValue();
        if (rootOp.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return null;
        }
        return (AggregateOperator) rootOp;
    }

    private <T> void setAll(Collection<? super T> to, Collection<? extends T> from) {
        if (!to.isEmpty()) {
            throw new IllegalStateException(String.valueOf(to.size()));
        }
        to.addAll(from);
    }

    /**
     * Returns {@code true} if {@code maxObjects1} subsumes {@code maxObjects2}
     */
    protected boolean subsumeFrameMaxObjects(int maxObjects1, int maxObjects2, AggregateOperator aggOp2) {
        return false;
    }
}
