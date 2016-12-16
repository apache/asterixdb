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
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.util.ConstantExpressionUtil;
import org.apache.asterix.runtime.evaluators.functions.FullTextContainsDescriptor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Checks whether the given parameters of the ftcontains() function are correct during the compilation.
 */
public class FullTextContainsParameterCheckRule implements IAlgebraicRewriteRule {

    // parameter name and its value
    HashMap<MutableObject<ILogicalExpression>, MutableObject<ILogicalExpression>> paramValueMap;

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        Mutable<ILogicalExpression> exprRef;
        switch (op.getOperatorTag()) {
            case SELECT:
                exprRef = ((SelectOperator) op).getCondition();
                break;
            case INNERJOIN:
            case LEFTOUTERJOIN:
                exprRef = ((AbstractBinaryJoinOperator) op).getCondition();
                break;
            default:
                return false;
        }

        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        if (checkParamter(op, exprRef, context)) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
            return true;
        }
        return false;
    }

    /**
     * Check the correctness of the parameters of the ftcontains(). Also rearrange options as arguments.
     * The expected form of ftcontains() is ftcontains(expression1, expression2, parameters as a record).
     */
    private boolean checkParamter(ILogicalOperator op, Mutable<ILogicalExpression> exprRef,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalExpression expr = exprRef.getValue();

        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier fi = funcExpr.getFunctionIdentifier();

        if (fi == BuiltinFunctions.FULLTEXT_CONTAINS) {
            // Don't need to check this operator again.
            context.addToDontApplySet(this, op);

            List<Mutable<ILogicalExpression>> oldExprs = funcExpr.getArguments();
            List<Mutable<ILogicalExpression>> newExprs = new ArrayList<>();

            // The number of parameters should be three: exp1, exp2, and the option
            if (oldExprs.size() != 3) {
                throw new AlgebricksException(
                        BuiltinFunctions.FULLTEXT_CONTAINS.getName() + " should have three parameters.");
            }

            // The last expression is a record that contains the parameters. That's why we deduct -1.
            for (int i = 0; i < oldExprs.size() - 1; i++) {
                newExprs.add(new MutableObject<ILogicalExpression>((ILogicalExpression) oldExprs.get(i).getValue()));
            }

            // Sanity check for the types of the first two parameters
            checkFirstAndSecondParamter(oldExprs);

            // Checks and transforms the actual full-text parameters.
            checkAndSetDefaultValueForThirdParameter(oldExprs.get(2), newExprs);

            // Resets the last argument.
            funcExpr.getArguments().clear();
            funcExpr.getArguments().addAll(newExprs);

            return true;
        }

        return false;
    }

    /**
     * Checks the correctness of the first and second argument. If the argument is a constant, we can check
     * it now. If the argument is not a constant, we will defer the checking until run-time.
     */
    void checkFirstAndSecondParamter(List<Mutable<ILogicalExpression>> exprs) throws AlgebricksException {
        // Check the first parameter - Expression1. If it's a constant, then we can check the type here.
        ILogicalExpression firstExpr = exprs.get(0).getValue();
        if (firstExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT
                && ConstantExpressionUtil.getConstantIaObjectType(firstExpr) != ATypeTag.STRING) {
            throw new AlgebricksException("The first expression of "
                    + BuiltinFunctions.FULLTEXT_CONTAINS.getName() + " should be a string.");
        }

        // Check the second parameter - Expression2. If it's a constant, then we can check the type here.
        ILogicalExpression secondExpr = exprs.get(1).getValue();
        if (secondExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ATypeTag exprTypeTag = ConstantExpressionUtil.getConstantIaObjectType(secondExpr);
            switch (exprTypeTag) {
                case STRING:
                case UNORDEREDLIST:
                case ORDEREDLIST:
                    break;
                default:
                    throw new AlgebricksException(
                            "The second expression of " + BuiltinFunctions.FULLTEXT_CONTAINS.getName()
                                    + "should be a string, an unordered list, or an ordered list.");
            }
        }
    }

    /**
     * Checks the option of the given ftcontains() function. Also, sets default value.
     *
     * @param expr
     * @throws AlgebricksException
     */
    void checkAndSetDefaultValueForThirdParameter(Mutable<ILogicalExpression> expr,
            List<Mutable<ILogicalExpression>> newArgs) throws AlgebricksException {
        // Get the last parameter - this should be a record-constructor.
        AbstractFunctionCallExpression openRecConsExpr = (AbstractFunctionCallExpression) expr.getValue();
        FunctionIdentifier openRecConsFi = openRecConsExpr.getFunctionIdentifier();
        if (openRecConsFi != BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR
                && openRecConsFi != BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR) {
            throw new AlgebricksException("ftcontains() option should be the form of a record { }.");
        }

        // We multiply 2 because the layout of the arguments are: [expr, val, expr1, val1, ...]
        if (openRecConsExpr.getArguments().size() > FullTextContainsDescriptor.getParamTypeMap().size() * 2) {
            throw new AlgebricksException("Too many options were specified.");
        }

        for (int i = 0; i < openRecConsExpr.getArguments().size(); i = i + 2) {
            ILogicalExpression optionExpr = openRecConsExpr.getArguments().get(i).getValue();
            ILogicalExpression optionExprVal = openRecConsExpr.getArguments().get(i + 1).getValue();

            if (optionExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                throw new AlgebricksException(
                        "Options must be in the form of constant strings. Check that the option at " + (i % 2 + 1)
                                + " is indeed a constant string");
            }

            String option = ConstantExpressionUtil.getStringArgument(openRecConsExpr, i).toLowerCase();
            if (!FullTextContainsDescriptor.getParamTypeMap().containsKey(option)) {
                throw new AlgebricksException(
                        "The given option " + option + " is not a valid argument to ftcontains()");
            }

            boolean typeError = false;
            String optionTypeStringVal = null;

            // If the option value is a constant, then we can check here.
            if (optionExprVal.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                switch (FullTextContainsDescriptor.getParamTypeMap().get(option)) {
                    case STRING:
                        optionTypeStringVal = ConstantExpressionUtil.getStringArgument(openRecConsExpr, i + 1)
                                .toLowerCase();
                        if (optionTypeStringVal == null) {
                            typeError = true;
                        }
                        break;
                    default:
                        // Currently, we only have a string parameter. So, the flow doesn't reach here.
                        typeError = true;
                        break;
                }
            }

            if (typeError) {
                throw new AlgebricksException(
                        "The given value for option " + option + " was not of the expected type");
            }

            // Check the validity of option value
            switch (option) {
                case FullTextContainsDescriptor.SEARCH_MODE_OPTION:
                    checkSearchModeOption(optionTypeStringVal);
                    break;
                default:
                    break;
            }

            // Add this option as arguments to the ftcontains().
            newArgs.add(new MutableObject<ILogicalExpression>(optionExpr));
            newArgs.add(new MutableObject<ILogicalExpression>(optionExprVal));
        }
    }

    void checkSearchModeOption(String optionVal) throws AlgebricksException {
        if (optionVal.equals(FullTextContainsDescriptor.CONJUNCTIVE_SEARCH_MODE_OPTION)
                || optionVal.equals(FullTextContainsDescriptor.DISJUNCTIVE_SEARCH_MODE_OPTION)) {
            return;
        } else {
            throw new AlgebricksException("The given value for the search mode (" + optionVal
                    + ") is not valid. Valid modes are " + FullTextContainsDescriptor.CONJUNCTIVE_SEARCH_MODE_OPTION
                    + " or " + FullTextContainsDescriptor.DISJUNCTIVE_SEARCH_MODE_OPTION + ".");
        }
    }
}
