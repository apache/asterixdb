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
package org.apache.asterix.optimizer.rules.pushdown.visitor;

import static org.apache.asterix.metadata.utils.PushdownUtil.SUPPORTED_FUNCTIONS;
import static org.apache.asterix.metadata.utils.PushdownUtil.YIELDABLE_FUNCTIONS;

import java.util.List;

import org.apache.asterix.metadata.utils.PushdownUtil;
import org.apache.asterix.optimizer.rules.pushdown.schema.ExpectedSchemaBuilder;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

public class ExpressionValueAccessPushdownVisitor {
    private final ExpectedSchemaBuilder builder;

    public ExpressionValueAccessPushdownVisitor(ExpectedSchemaBuilder builder) {
        this.builder = builder;
    }

    public boolean transform(ILogicalExpression expr, LogicalVariable producedVar, IVariableTypeEnvironment typeEnv)
            throws AlgebricksException {
        if (skipPushdown(expr)) {
            return false;
        }

        final AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

        if (isSuccessfullyPushedDown(funcExpr, producedVar, typeEnv)) {
            //We successfully pushed down the value access function
            return true;
        }

        //Check nested arguments if contains any pushable value access
        return pushValueAccessExpressionArg(funcExpr.getArguments(), producedVar, typeEnv);
    }

    /**
     * Check if we can push down an expression. Also, unregister a variable if we found that a common expression value is
     * required in its entirety.
     */
    private boolean skipPushdown(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            LogicalVariable variable = VariableUtilities.getVariable(expr);
            unregisterVariableIfNeeded(variable);
            return true;
        }
        return expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL || builder.isEmpty()
                || shouldYieldVariables(expr);
    }

    /**
     * If the expression is an 'allowed' function with only variable arguments, we should stop as we do not
     * want to unregister the variable used by the function.
     * <p>
     * Example:
     * SELECT p.personInfo.name
     * FROM Person p
     * WHERE p.personInfo IS NOT MISSING;
     * <p>
     * Plan:
     * ...
     * assign [$$17] <- [$$18.getField(\"name\")]
     * select (not(is-missing($$18)))
     * ...
     * assign [$$18] <- [$$p.getField(\"personInfo\")]
     * ...
     * data-scan []<-[$$p] <- test.ParquetDataset project ({personInfo:{name:VALUE}})
     * <p>
     * In this case, is-missing($$18) could unregister $$18 since it requires the entire value (personInfo) and we
     * won't be able to pushdown the access of (personInfo.name). This check would allow (personInfo.name) to be
     * pushed down to data scan.
     *
     * @param expression expression
     * @return if the function is a type-check function and has a variable argument.
     */
    private boolean shouldYieldVariables(ILogicalExpression expression) {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        return YIELDABLE_FUNCTIONS.contains(funcExpr.getFunctionIdentifier())
                && PushdownUtil.isAllVariableExpressions(funcExpr.getArguments());
    }

    private boolean pushValueAccessExpressionArg(List<Mutable<ILogicalExpression>> exprList,
            LogicalVariable producedVar, IVariableTypeEnvironment typeEnv) throws AlgebricksException {
        boolean changed = false;
        for (Mutable<ILogicalExpression> exprRef : exprList) {
            /*
             * We need to set the produced variable as null here as the produced variable will not correspond to the
             * nested expression.
             */
            changed |= transform(exprRef.getValue(), null, typeEnv);
        }
        return changed;
    }

    private boolean isSuccessfullyPushedDown(AbstractFunctionCallExpression funcExpr, LogicalVariable producedVar,
            IVariableTypeEnvironment typeEnv) throws AlgebricksException {
        return SUPPORTED_FUNCTIONS.contains(funcExpr.getFunctionIdentifier())
                && builder.setSchemaFromExpression(funcExpr, producedVar, typeEnv);
    }

    private void unregisterVariableIfNeeded(LogicalVariable variable) {
        if (builder.isVariableRegistered(variable)) {
            builder.unregisterVariable(variable);
        }
    }
}
