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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.base.IACollection;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule removes UNNEST of a constant singleton list and replaces it with ASSIGN as follows:
 * Before plan:
 * <ul>
 * <li>unnest $x <- scan-collection( [ constant ] ) (or {{ constant }})
 * </ul>
 * <p>
 * After plan:
 * <ul>
 * <li>assign $x <- constant
 * </ul>
 *
 * This rule must run after {@link ConstantFoldingRule}
 */
public final class CancelUnnestSingletonListRule implements IAlgebraicRewriteRule {
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnest = (UnnestOperator) op;
        if (unnest.getPositionalVariable() != null) {
            return false;
        }
        ILogicalExpression expr = unnest.getExpressionRef().getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        if (((AbstractFunctionCallExpression) expr).getFunctionIdentifier() != BuiltinFunctions.SCAN_COLLECTION) {
            return false;
        }
        AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) expr;
        ILogicalExpression argExpr = callExpr.getArguments().get(0).getValue();
        if (argExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        ConstantExpression cExpr = (ConstantExpression) argExpr;
        IAlgebricksConstantValue cValue = cExpr.getValue();
        if (!(cValue instanceof AsterixConstantValue)) {
            return false;
        }
        AsterixConstantValue aValue = (AsterixConstantValue) cValue;
        IAObject value = aValue.getObject();
        if (!value.getType().getTypeTag().isListType()) {
            return false;
        }
        IACollection list = (IACollection) value;
        if (list.size() != 1) {
            return false;
        }
        IACursor cur = list.getCursor();
        if (!cur.next()) {
            return false;
        }
        IAObject item = cur.get();

        List<LogicalVariable> assignVars = new ArrayList<>(1);
        assignVars.add(unnest.getVariable());
        List<Mutable<ILogicalExpression>> assignExprs = new ArrayList<>(1);
        ConstantExpression itemExpr = new ConstantExpression(new AsterixConstantValue(item));
        itemExpr.setSourceLocation(cExpr.getSourceLocation());
        assignExprs.add(new MutableObject<>(itemExpr));
        AssignOperator assignOp = new AssignOperator(assignVars, assignExprs);
        assignOp.setSourceLocation(op.getSourceLocation());
        assignOp.getInputs().addAll(op.getInputs());

        context.computeAndSetTypeEnvironmentForOperator(assignOp);
        opRef.setValue(assignOp);
        return true;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
}
