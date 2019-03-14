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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.runtime.evaluators.functions.FullTextContainsDescriptor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Checks whether the given parameters of the ftcontains() function are correct during the compilation.
 */
public class FullTextContainsParameterCheckRule implements IAlgebraicRewriteRule {

    // Visitor for checking and transforming ftcontains() expression
    protected FullTextContainsExpressionVisitor ftcontainsExprVisitor = new FullTextContainsExpressionVisitor();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        if (checkParameter(opRef, context)) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
            return true;
        }
        return false;
    }

    /**
     * Check the correctness of the parameters of the ftcontains(). Also rearrange options as arguments.
     * The expected form of ftcontains() is ftcontains(expression1, expression2, parameters as a record).
     */
    private boolean checkParameter(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        boolean modified = op.acceptExpressionTransform(ftcontainsExprVisitor);
        if (modified) {
            context.addToDontApplySet(this, op);
        }
        return modified;
    }

    /**
     * This visitor class handles actual checking and transformation.
     */
    protected class FullTextContainsExpressionVisitor implements ILogicalExpressionReferenceTransform {

        // the last expression position before the option argument in the arguments array
        private static final int LAST_EXPRESSION_POS_BEFORE_OPTION = 1;
        // The number of anticipated arguments for a full-text query when a user doesn't provide any option.
        private static final int FULLTEXT_QUERY_WITHOUT_OPTION_NO_OF_ARGUMENTS = 2;
        // The number of anticipated arguments for a full-text query when a user provide option(s) as a record.
        private static final int FULLTEXT_QUERY_WITH_OPTION_NO_OF_ARGUMENTS = 3;

        public FullTextContainsExpressionVisitor() {
            // no parameter is needed.
        }

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            ILogicalExpression e = exprRef.getValue();
            switch (e.getExpressionTag()) {
                case FUNCTION_CALL:
                    return transformFunctionCallExpression((AbstractFunctionCallExpression) e);
                default:
                    return false;
            }
        }

        private boolean transformFunctionCallExpression(AbstractFunctionCallExpression fce) throws AlgebricksException {
            boolean modified = false;
            FunctionIdentifier fi = fce.getFunctionIdentifier();
            if (fi != BuiltinFunctions.FULLTEXT_CONTAINS && fi != BuiltinFunctions.FULLTEXT_CONTAINS_WO_OPTION) {
                for (Mutable<ILogicalExpression> arg : fce.getArguments()) {
                    if (transform(arg)) {
                        modified = true;
                    }
                }
            } else {
                modified = checkParameterForFuncExpr(fce, fi);
            }

            return modified;
        }

        private boolean checkParameterForFuncExpr(AbstractFunctionCallExpression funcExpr, FunctionIdentifier fi)
                throws AlgebricksException {
            // Collects the correct number of arguments - it can be 2 if a user doesn't provide any option.
            int numberOfCorrectArguments = 0;
            String functionName = "";
            if (fi == BuiltinFunctions.FULLTEXT_CONTAINS) {
                numberOfCorrectArguments = FULLTEXT_QUERY_WITH_OPTION_NO_OF_ARGUMENTS;
                functionName = BuiltinFunctions.FULLTEXT_CONTAINS.getName();
            } else if (fi == BuiltinFunctions.FULLTEXT_CONTAINS_WO_OPTION) {
                numberOfCorrectArguments = FULLTEXT_QUERY_WITHOUT_OPTION_NO_OF_ARGUMENTS;
                functionName = BuiltinFunctions.FULLTEXT_CONTAINS_WO_OPTION.getName();
            }

            // If numberOfCorrectArguments is greater than zero, then this is a full-text search query.
            if (numberOfCorrectArguments > 0) {

                List<Mutable<ILogicalExpression>> oldExprs = funcExpr.getArguments();
                List<Mutable<ILogicalExpression>> newExprs = new ArrayList<>();

                // The number of parameters should be three: exp1, exp2, and the option
                if (oldExprs.size() != numberOfCorrectArguments) {
                    throw CompilationException.create(ErrorCode.COMPILATION_INVALID_PARAMETER_NUMBER,
                            funcExpr.getSourceLocation(), fi.getName(), oldExprs.size());
                }

                // The last expression before the option needs to be copied first.
                for (int i = 0; i <= LAST_EXPRESSION_POS_BEFORE_OPTION; i++) {
                    newExprs.add(new MutableObject<ILogicalExpression>(oldExprs.get(i).getValue()));
                }

                // Sanity check for the types of the first two parameters
                checkFirstAndSecondParamter(oldExprs, functionName);

                // Checks and transforms the actual full-text parameters.
                if (numberOfCorrectArguments == FULLTEXT_QUERY_WITH_OPTION_NO_OF_ARGUMENTS) {
                    checkValueForThirdParameter(oldExprs.get(2), newExprs, functionName);
                } else {
                    // no option provided case: sets the default option here.
                    setDefaultValueForThirdParameter(newExprs);
                }

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
        private void checkFirstAndSecondParamter(List<Mutable<ILogicalExpression>> exprs, String functionName)
                throws AlgebricksException {
            // Check the first parameter - Expression1. If it's a constant, then we can check the type here.
            ILogicalExpression firstExpr = exprs.get(0).getValue();
            if (firstExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT
                    && ConstantExpressionUtil.getConstantIaObjectType(firstExpr) != ATypeTag.STRING) {
                throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED, firstExpr.getSourceLocation(),
                        functionName, ConstantExpressionUtil.getConstantIaObjectType(firstExpr));
            }

            // Check the second parameter - Expression2. If it's a constant, then we can check the type here.
            ILogicalExpression secondExpr = exprs.get(1).getValue();
            if (secondExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                ATypeTag exprTypeTag = ConstantExpressionUtil.getConstantIaObjectType(secondExpr);
                switch (exprTypeTag) {
                    case STRING:
                    case MULTISET:
                    case ARRAY:
                        break;
                    default:
                        throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED, secondExpr.getSourceLocation(),
                                functionName, exprTypeTag);
                }
            }
        }

        /**
         * Checks the option of the given ftcontains() function. Also, sets default value.
         *
         * @param expr
         * @throws AlgebricksException
         */
        private void checkValueForThirdParameter(Mutable<ILogicalExpression> expr,
                List<Mutable<ILogicalExpression>> newArgs, String functionName) throws AlgebricksException {
            // Get the last parameter - this should be a record-constructor.
            AbstractFunctionCallExpression openRecConsExpr = (AbstractFunctionCallExpression) expr.getValue();
            FunctionIdentifier openRecConsFi = openRecConsExpr.getFunctionIdentifier();
            if (openRecConsFi != BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR
                    && openRecConsFi != BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR) {
                throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED, openRecConsExpr.getSourceLocation(),
                        functionName, openRecConsFi);
            }

            // We multiply 2 because the layout of the arguments are: [expr, val, expr1, val1, ...]
            if (openRecConsExpr.getArguments().size() > FullTextContainsDescriptor.getParamTypeMap().size() * 2) {
                throw CompilationException.create(ErrorCode.TOO_MANY_OPTIONS_FOR_FUNCTION,
                        openRecConsExpr.getSourceLocation(), functionName);
            }

            for (int i = 0; i < openRecConsExpr.getArguments().size(); i = i + 2) {
                ILogicalExpression optionExpr = openRecConsExpr.getArguments().get(i).getValue();
                ILogicalExpression optionExprVal = openRecConsExpr.getArguments().get(i + 1).getValue();

                String option = ConstantExpressionUtil.getStringConstant(optionExpr);

                if (optionExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT || option == null) {
                    throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED, optionExpr.getSourceLocation(),
                            functionName, optionExpr.getExpressionTag());
                }

                option = option.toLowerCase();
                if (!FullTextContainsDescriptor.getParamTypeMap().containsKey(option)) {
                    throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED, optionExprVal.getSourceLocation(),
                            functionName, option);
                }

                String optionTypeStringVal = null;

                // If the option value is a constant, then we can check here.
                if (optionExprVal.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    switch (FullTextContainsDescriptor.getParamTypeMap().get(option)) {
                        case STRING:
                            optionTypeStringVal = ConstantExpressionUtil.getStringConstant(optionExprVal);
                            if (optionTypeStringVal == null) {
                                throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED,
                                        optionExprVal.getSourceLocation(), functionName, option);
                            }
                            optionTypeStringVal = optionTypeStringVal.toLowerCase();
                            break;
                        default:
                            // Currently, we only have a string parameter. So, the flow doesn't reach here.
                            throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED,
                                    optionExprVal.getSourceLocation(), functionName, option);
                    }

                    // Check the validity of option value
                    switch (option) {
                        case FullTextContainsDescriptor.SEARCH_MODE_OPTION:
                            checkSearchModeOption(optionTypeStringVal, functionName, optionExprVal.getSourceLocation());
                            break;
                        default:
                            throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED,
                                    optionExprVal.getSourceLocation(), functionName, option);
                    }
                }

                // Add this option as arguments to the ftcontains().
                newArgs.add(new MutableObject<ILogicalExpression>(optionExpr));
                newArgs.add(new MutableObject<ILogicalExpression>(optionExprVal));
            }
        }

        private void checkSearchModeOption(String optionVal, String functionName, SourceLocation sourceLoc)
                throws AlgebricksException {
            if (optionVal.equals(FullTextContainsDescriptor.CONJUNCTIVE_SEARCH_MODE_OPTION)
                    || optionVal.equals(FullTextContainsDescriptor.DISJUNCTIVE_SEARCH_MODE_OPTION)) {
                return;
            } else {
                throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED, sourceLoc, functionName, optionVal);
            }
        }

        /**
         * Sets the default option value(s) when a user doesn't provide any option.
         */
        void setDefaultValueForThirdParameter(List<Mutable<ILogicalExpression>> newArgs) throws AlgebricksException {
            // Sets the search mode option: the default option is conjunctive search.
            ILogicalExpression searchModeOptionExpr = new ConstantExpression(
                    new AsterixConstantValue(new AString(FullTextContainsDescriptor.SEARCH_MODE_OPTION)));
            ILogicalExpression searchModeValExpr = new ConstantExpression(
                    new AsterixConstantValue(new AString(FullTextContainsDescriptor.CONJUNCTIVE_SEARCH_MODE_OPTION)));

            // Add this option as arguments to the ftcontains().
            newArgs.add(new MutableObject<ILogicalExpression>(searchModeOptionExpr));
            newArgs.add(new MutableObject<ILogicalExpression>(searchModeValExpr));
        }

    }

}
