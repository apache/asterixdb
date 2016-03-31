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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
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
        if (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        boolean changed = false;
        GroupByOperator gbyOp = (GroupByOperator) op;
        for (ILogicalPlan p : gbyOp.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                if (inlined(r)) {
                    changed = true;
                }
            }
        }
        return changed;
    }

    private boolean inlined(Mutable<ILogicalOperator> r) throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) r.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AggregateOperator agg = (AggregateOperator) op1;
        AssignOperator assign = (AssignOperator) op2;
        VarExprSubstitution ves = new VarExprSubstitution(assign.getVariables(), assign.getExpressions());
        for (Mutable<ILogicalExpression> exprRef : agg.getExpressions()) {
            ILogicalExpression expr = exprRef.getValue();
            Pair<Boolean, ILogicalExpression> p = expr.accept(ves, null);
            if (p.first == true) {
                exprRef.setValue(p.second);
            }
            // AbstractLogicalExpression ale = (AbstractLogicalExpression) expr;
            // ale.accept(ves, null);
        }
        List<Mutable<ILogicalOperator>> op1InpList = op1.getInputs();
        op1InpList.clear();
        op1InpList.add(op2.getInputs().get(0));
        return true;
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
