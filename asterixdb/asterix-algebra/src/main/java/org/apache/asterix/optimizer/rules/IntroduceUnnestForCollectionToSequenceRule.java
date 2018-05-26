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

import java.util.List;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.IAType;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule introduces a unnest operator for the collection-to-sequence function (if the input to the function is a collection).
 *
 * @author yingyib
 */
public class IntroduceUnnestForCollectionToSequenceRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assign = (AssignOperator) op;
        List<Mutable<ILogicalExpression>> exprs = assign.getExpressions();
        if (exprs.size() != 1) {
            return false;
        }
        ILogicalExpression expr = exprs.get(0).getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression func = (AbstractFunctionCallExpression) expr;
        if (func.getFunctionIdentifier() != BuiltinFunctions.COLLECTION_TO_SEQUENCE) {
            return false;
        }

        IVariableTypeEnvironment env = assign.computeInputTypeEnvironment(context);
        ILogicalExpression argExpr = func.getArguments().get(0).getValue();
        IAType outerExprType = (IAType) env.getType(expr);
        IAType innerExprType = (IAType) env.getType(argExpr);
        if (outerExprType.equals(innerExprType)) {
            /** nothing is changed with the collection-to-sequence function, remove the collection-sequence function call */
            assign.getExpressions().set(0, new MutableObject<ILogicalExpression>(argExpr));
            return true;
        }
        /** change the assign operator to an unnest operator */
        LogicalVariable var = assign.getVariables().get(0);
        UnnestingFunctionCallExpression scanCollExpr =
                new UnnestingFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.SCAN_COLLECTION),
                        new MutableObject<ILogicalExpression>(argExpr));
        scanCollExpr.setSourceLocation(func.getSourceLocation());
        @SuppressWarnings("unchecked")
        UnnestOperator unnest = new UnnestOperator(var, new MutableObject<ILogicalExpression>(scanCollExpr));
        unnest.setSourceLocation(assign.getSourceLocation());
        unnest.getInputs().addAll(assign.getInputs());
        opRef.setValue(unnest);
        context.computeAndSetTypeEnvironmentForOperator(unnest);
        return true;
    }
}
