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

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.AbstractConstVarFunVisitor;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class InlineAssignIntoAggregateRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.GROUP && op.getOperatorTag() != LogicalOperatorTag.WINDOW) {
            return false;
        }
        boolean changed = false;
        AbstractOperatorWithNestedPlans opWithNestedPlan = (AbstractOperatorWithNestedPlans) op;
        for (ILogicalPlan p : opWithNestedPlan.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                changed |= inlined(r.getValue(), opWithNestedPlan);
            }
        }
        return changed;
    }

    private boolean inlined(ILogicalOperator planRootOp, AbstractOperatorWithNestedPlans opWithNestedPlan)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) planRootOp;
        if (op1.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggOp = (AggregateOperator) op1;
        boolean inlined = inlineInputAssignIntoAgg(aggOp);
        if (opWithNestedPlan.getOperatorTag() == LogicalOperatorTag.WINDOW) {
            inlined |= inlineOuterInputAssignIntoAgg(aggOp, opWithNestedPlan);
        }
        return inlined;
    }

    private boolean inlineInputAssignIntoAgg(AggregateOperator aggOp) throws AlgebricksException {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) aggOp.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assignOp = (AssignOperator) op2;
        VarExprSubstitution ves = new VarExprSubstitution(assignOp.getVariables(), assignOp.getExpressions());
        inlineVariables(aggOp, ves);
        List<Mutable<ILogicalOperator>> op1InpList = aggOp.getInputs();
        op1InpList.clear();
        op1InpList.add(op2.getInputs().get(0));
        return true;
    }

    private boolean inlineOuterInputAssignIntoAgg(AggregateOperator aggOp,
            AbstractOperatorWithNestedPlans opWithNestedPlans) throws AlgebricksException {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opWithNestedPlans.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assignOp = (AssignOperator) op2;
        VarExprSubstitution ves = new VarExprSubstitution(assignOp.getVariables(), assignOp.getExpressions());
        return inlineVariables(aggOp, ves);
    }

    private boolean inlineVariables(AggregateOperator aggOp, VarExprSubstitution ves) throws AlgebricksException {
        boolean inlined = false;
        for (Mutable<ILogicalExpression> exprRef : aggOp.getExpressions()) {
            ILogicalExpression expr = exprRef.getValue();
            Pair<Boolean, ILogicalExpression> p = expr.accept(ves, null);
            if (p.first) {
                exprRef.setValue(p.second);
                inlined = true;
            }
        }
        return inlined;
    }

    private class VarExprSubstitution extends AbstractConstVarFunVisitor<Pair<Boolean, ILogicalExpression>, Void> {

        private List<LogicalVariable> variables;
        private List<Mutable<ILogicalExpression>> expressions;

        public VarExprSubstitution(List<LogicalVariable> variables, List<Mutable<ILogicalExpression>> expressions) {
            this.variables = variables;
            this.expressions = expressions;
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitConstantExpression(ConstantExpression expr, Void arg) {
            return new Pair<Boolean, ILogicalExpression>(false, expr);
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitFunctionCallExpression(AbstractFunctionCallExpression expr,
                Void arg) throws AlgebricksException {
            boolean changed = false;
            for (Mutable<ILogicalExpression> eRef : expr.getArguments()) {
                ILogicalExpression e = eRef.getValue();
                Pair<Boolean, ILogicalExpression> p = e.accept(this, arg);
                if (p.first) {
                    eRef.setValue(p.second);
                    changed = true;
                }
            }
            return new Pair<Boolean, ILogicalExpression>(changed, expr);
        }

        @Override
        public Pair<Boolean, ILogicalExpression> visitVariableReferenceExpression(VariableReferenceExpression expr,
                Void arg) {
            LogicalVariable v = expr.getVariableReference();
            int idx = variables.indexOf(v);
            if (idx < 0) {
                return new Pair<Boolean, ILogicalExpression>(false, expr);
            } else {
                return new Pair<Boolean, ILogicalExpression>(true, expressions.get(idx).getValue());
            }

        }

    }
}
