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
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public abstract class AbstractExtractExprRule implements IAlgebraicRewriteRule {

    protected static LogicalVariable extractExprIntoAssignOpRef(ILogicalExpression gExpr,
            Mutable<ILogicalOperator> opRef2, IOptimizationContext context) throws AlgebricksException {
        LogicalVariable v = context.newVar();
        AssignOperator a = new AssignOperator(v, new MutableObject<>(gExpr));
        a.setSourceLocation(gExpr.getSourceLocation());
        a.getInputs().add(new MutableObject<>(opRef2.getValue()));
        opRef2.setValue(a);
        if (gExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            context.addNotToBeInlinedVar(v);
        }
        context.computeAndSetTypeEnvironmentForOperator(a);
        return v;
    }

    protected static <T> boolean extractComplexExpressions(ILogicalOperator op, List<T> exprList,
            Function<T, Mutable<ILogicalExpression>> exprGetter, Predicate<ILogicalExpression> retainPredicate,
            IOptimizationContext context) throws AlgebricksException {
        if (!hasComplexExpressions(exprList, exprGetter)) {
            return false;
        }
        boolean rewritten = false;
        Mutable<ILogicalOperator> inputOpRef = op.getInputs().get(0);
        for (T item : exprList) {
            Mutable<ILogicalExpression> exprMutable = exprGetter.apply(item);
            ILogicalExpression expr = exprMutable.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE && !retainPredicate.test(expr)) {
                LogicalVariable v = extractExprIntoAssignOpRef(expr, inputOpRef, context);
                VariableReferenceExpression vRef = new VariableReferenceExpression(v);
                vRef.setSourceLocation(expr.getSourceLocation());
                exprMutable.setValue(vRef);
                rewritten = true;
            }
        }
        context.computeAndSetTypeEnvironmentForOperator(op);
        return rewritten;
    }

    private static <T> boolean hasComplexExpressions(List<T> exprList,
            Function<T, Mutable<ILogicalExpression>> exprGetter) {
        for (T item : exprList) {
            Mutable<ILogicalExpression> exprMutable = exprGetter.apply(item);
            if (exprMutable.getValue().getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return true;
            }
        }
        return false;
    }
}
