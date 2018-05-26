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
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule normalizes an GroupBy operator. It extracts non-variable-reference decoration expressions
 * as a separate assign operator before the GroupBy operator.
 * The reason that we have this rule is that in various rules as well as the code generation for the
 * GroupBy operator we assumed that decoration expressions can only be variable reference expressions.
 */
public class ExtractGroupByDecorVariablesRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        GroupByOperator groupByOperator = (GroupByOperator) op;
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decorList = groupByOperator.getDecorList();

        // Returns immediately if there is no decoration entry.
        if (groupByOperator.getDecorList() == null || groupByOperator.getDecorList().isEmpty()) {
            return false;
        }

        // Goes over the decoration list and performs the rewrite.
        boolean changed = false;
        List<LogicalVariable> vars = new ArrayList<>();
        List<Mutable<ILogicalExpression>> exprs = new ArrayList<>();
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> decorVarExpr : decorList) {
            Mutable<ILogicalExpression> exprRef = decorVarExpr.second;
            ILogicalExpression expr = exprRef.getValue();
            if (expr == null || expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                continue;
            }
            // Rewrites the decoration entry if the decoration expression is not a variable reference expression.
            changed = true;
            LogicalVariable newVar = context.newVar();
            vars.add(newVar);
            exprs.add(exprRef);

            // Normalizes the decor entry -- expression be a variable reference
            VariableReferenceExpression newVarRef = new VariableReferenceExpression(newVar);
            newVarRef.setSourceLocation(expr.getSourceLocation());
            decorVarExpr.second = new MutableObject<>(newVarRef);
        }
        if (!changed) {
            return false;
        }

        // Injects an assign operator to evaluate the decoration expression.
        AssignOperator assignOperator = new AssignOperator(vars, exprs);
        assignOperator.getInputs().addAll(op.getInputs());
        op.getInputs().set(0, new MutableObject<>(assignOperator));
        return changed;
    }
}
