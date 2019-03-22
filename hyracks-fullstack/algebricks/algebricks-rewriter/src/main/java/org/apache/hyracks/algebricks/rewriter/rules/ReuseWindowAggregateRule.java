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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.IsomorphismOperatorVisitor;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * If two adjacent window operators compute the same running aggregate then replace the second computation with
 * the assign operator referring to the first operator's output variable:
 *
 * <pre>
 * window [$x] <- [f()] ...
 * window [$y] <- [f()] ...
 * -->
 * assign [$x] <- [$y]
 * window [] <- [] ...
 * window [$y] <- [f()] ...
 * </pre>
 *
 * Both window operators must have the same partitioning specification.
 *
 * This rule must be followed by {@link RemoveRedundantVariablesRule} to substitute {@code $x} references with
 * {@code $y} and then {@link RemoveUnusedAssignAndAggregateRule} to eliminate the new assign operator.
 */
public class ReuseWindowAggregateRule implements IAlgebraicRewriteRule {
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

        List<LogicalVariable> assignVars = new ArrayList<>();
        List<Mutable<ILogicalExpression>> assignExprs = new ArrayList<>();

        List<LogicalVariable> varsOp1 = winOp1.getVariables();
        List<LogicalVariable> varsOp2 = winOp2.getVariables();
        List<Mutable<ILogicalExpression>> exprsOp1 = winOp1.getExpressions();
        List<Mutable<ILogicalExpression>> exprsOp2 = winOp2.getExpressions();
        Iterator<LogicalVariable> varsOp1Iter = varsOp1.iterator();
        Iterator<Mutable<ILogicalExpression>> exprsOp1Iter = exprsOp1.iterator();
        while (varsOp1Iter.hasNext()) {
            LogicalVariable varOp1 = varsOp1Iter.next();
            Mutable<ILogicalExpression> exprOp1 = exprsOp1Iter.next();
            VariableReferenceExpression varOp2Ref =
                    OperatorManipulationUtil.findAssignedVariable(varsOp2, exprsOp2, exprOp1.getValue());
            if (varOp2Ref != null) {
                varsOp1Iter.remove();
                exprsOp1Iter.remove();
                assignVars.add(varOp1);
                assignExprs.add(new MutableObject<>(varOp2Ref));
            }
        }

        if (assignVars.isEmpty()) {
            return false;
        }

        AssignOperator assignOp = new AssignOperator(assignVars, assignExprs);
        assignOp.getInputs().add(new MutableObject<>(winOp1));
        assignOp.setSourceLocation(winOp1.getSourceLocation());
        context.computeAndSetTypeEnvironmentForOperator(assignOp);
        opRef.setValue(assignOp);
        return true;
    }
}
