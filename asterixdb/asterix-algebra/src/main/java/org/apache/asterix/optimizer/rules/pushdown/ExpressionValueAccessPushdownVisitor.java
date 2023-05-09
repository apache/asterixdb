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
package org.apache.asterix.optimizer.rules.pushdown;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;

class ExpressionValueAccessPushdownVisitor implements ILogicalExpressionReferenceTransform {
    //Set of allowed functions that can request a type in its entirety
    static final Set<FunctionIdentifier> ALLOWED_FUNCTIONS = createAllowedFunctions();
    //Set of supported array functions
    static final Set<FunctionIdentifier> ARRAY_FUNCTIONS = createSupportedArrayFunctions();
    //Set of supported functions that we can push down
    static final Set<FunctionIdentifier> SUPPORTED_FUNCTIONS = createSupportedFunctions();

    private final ExpectedSchemaBuilder builder;
    private List<LogicalVariable> producedVariables;
    private IVariableTypeEnvironment typeEnv;
    private int producedVariableIndex;

    public ExpressionValueAccessPushdownVisitor(ExpectedSchemaBuilder builder) {
        this.builder = builder;
        end();
    }

    public void init(List<LogicalVariable> producedVariables, IVariableTypeEnvironment typeEnv) {
        this.producedVariables = producedVariables;
        this.typeEnv = typeEnv;
        producedVariableIndex = 0;
    }

    @Override
    public boolean transform(Mutable<ILogicalExpression> expression) throws AlgebricksException {
        if (producedVariableIndex == -1) {
            //This for ensuring that the produced variables (if any) should be set
            throw new IllegalStateException("init must be called first");
        }
        pushValueAccessExpression(expression, getNextProducedVariable(), typeEnv);
        return false;
    }

    public void end() {
        producedVariables = null;
        typeEnv = null;
        producedVariableIndex = -1;
    }

    private LogicalVariable getNextProducedVariable() {
        LogicalVariable variable = producedVariables != null ? producedVariables.get(producedVariableIndex) : null;
        producedVariableIndex++;
        return variable;
    }

    /**
     * Pushdown field access expressions and array access expressions down
     */
    private void pushValueAccessExpression(Mutable<ILogicalExpression> exprRef, LogicalVariable producedVar,
            IVariableTypeEnvironment typeEnv) throws AlgebricksException {
        final ILogicalExpression expr = exprRef.getValue();
        if (skipPushdown(expr)) {
            return;
        }

        final AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

        if (isSuccessfullyPushedDown(funcExpr, producedVar, typeEnv)) {
            //We successfully pushed down the value access function
            return;
        }

        //Check nested arguments if contains any pushable value access
        pushValueAccessExpressionArg(funcExpr.getArguments(), producedVar);
    }

    /**
     * Check if we can pushdown an expression. Also, unregister a variable if we found that a common expression value is
     * required in its entirety.
     */
    private boolean skipPushdown(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            LogicalVariable variable = VariableUtilities.getVariable(expr);
            unregisterVariableIfNeeded(variable);
            return true;
        }
        return expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL || !builder.containsRegisteredDatasets()
                || isTypeCheckOnVariable(expr);
    }

    /**
     * If the expression is a type-check function on a variable. We should stop as we do not want to unregister
     * the variable used by the type-check function.
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
    private boolean isTypeCheckOnVariable(ILogicalExpression expression) {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        return ALLOWED_FUNCTIONS.contains(funcExpr.getFunctionIdentifier())
                && funcExpr.getArguments().get(0).getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE;
    }

    private void pushValueAccessExpressionArg(List<Mutable<ILogicalExpression>> exprList, LogicalVariable producedVar)
            throws AlgebricksException {
        for (Mutable<ILogicalExpression> exprRef : exprList) {
            /*
             * We need to set the produced variable as null here as the produced variable will not correspond to the
             * nested expression.
             */
            pushValueAccessExpression(exprRef, producedVar, typeEnv);
        }
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

    private static Set<FunctionIdentifier> createSupportedArrayFunctions() {
        return Set.of(BuiltinFunctions.GET_ITEM, BuiltinFunctions.ARRAY_STAR, BuiltinFunctions.SCAN_COLLECTION);
    }

    private static Set<FunctionIdentifier> createSupportedFunctions() {
        Set<FunctionIdentifier> supportedFunctions = new HashSet<>();
        supportedFunctions.add(BuiltinFunctions.FIELD_ACCESS_BY_NAME);
        supportedFunctions.add(BuiltinFunctions.FIELD_ACCESS_BY_INDEX);
        supportedFunctions.addAll(ARRAY_FUNCTIONS);
        return supportedFunctions;
    }

    private static Set<FunctionIdentifier> createAllowedFunctions() {
        return Set.of(BuiltinFunctions.IS_ARRAY, BuiltinFunctions.IS_OBJECT, BuiltinFunctions.IS_ATOMIC,
                BuiltinFunctions.IS_NUMBER, BuiltinFunctions.IS_BOOLEAN, BuiltinFunctions.IS_STRING,
                AlgebricksBuiltinFunctions.IS_MISSING, AlgebricksBuiltinFunctions.IS_NULL, BuiltinFunctions.IS_UNKNOWN,
                BuiltinFunctions.LT, BuiltinFunctions.LE, BuiltinFunctions.EQ, BuiltinFunctions.GT, BuiltinFunctions.GE,
                BuiltinFunctions.SCALAR_SQL_COUNT);
    }
}
