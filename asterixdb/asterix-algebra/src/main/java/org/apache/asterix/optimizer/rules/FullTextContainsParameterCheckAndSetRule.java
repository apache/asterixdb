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
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.utils.FullTextUtil;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.runtime.evaluators.functions.FullTextContainsFunctionDescriptor;
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

import com.google.common.base.Strings;

/**
 * Checks whether the given parameters of the ftcontains() function are correct during the compilation,
 * and fetch the full-text config from metadata which is necessary for the ftcontains() function
 */
public class FullTextContainsParameterCheckAndSetRule implements IAlgebraicRewriteRule {
    // Visitor for checking and transforming ftcontains() expression
    protected FullTextContainsExpressionVisitor ftcontainsExprVisitor = new FullTextContainsExpressionVisitor();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        if (checkAndSetParameter(opRef, context)) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
            return true;
        }
        return false;
    }

    /**
     * Check the correctness of the parameters of the ftcontains(). Also rearrange options as arguments.
     * The expected form of ftcontains() is ftcontains(expression1, expression2, parameters as a record).
     *
     * If ftcontains() has the full-text config argument, this method will also fetch it (FullTextConfigDescriptor) from metadata
     * and set it in the function expression so that the full-text config can be utilized later at run-time.
     */
    private boolean checkAndSetParameter(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        ftcontainsExprVisitor.setContext(context);
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

        private IOptimizationContext context;

        public FullTextContainsExpressionVisitor() {
        }

        public void setContext(IOptimizationContext context) {
            this.context = context;
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

                // We cannot make ftConfigName a class-level variable in the visitor class
                // because the visitor may be shared between multiple threads and such a variable may be corrupted
                String ftConfigName = null;
                // Checks and transforms the actual full-text parameters.
                if (numberOfCorrectArguments == FULLTEXT_QUERY_WITH_OPTION_NO_OF_ARGUMENTS) {
                    ftConfigName =
                            checkValueForThirdParameterAndGetFullTextConfig(oldExprs.get(2), newExprs, functionName);
                } else {
                    // no option provided case: sets the default option here.
                    setDefaultValueForThirdParameter(newExprs);
                }

                MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
                DataverseName dataverseName = metadataProvider.getDefaultDataverseName();
                funcExpr.setOpaqueParameters(new Object[] { FullTextUtil
                        .fetchFilterAndCreateConfigEvaluator(metadataProvider, dataverseName, ftConfigName) });
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
         *
         * @return the full-text config name if specified in the function option,
         * null if not specified which implies the default full-text config will be utilized later
         */
        private String checkValueForThirdParameterAndGetFullTextConfig(Mutable<ILogicalExpression> expr,
                List<Mutable<ILogicalExpression>> newArgs, String functionName) throws AlgebricksException {
            String ftConfigName = null;

            // Get the last parameter - this should be a record-constructor or a constant expression.
            if (expr.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                ConstantExpression constantExpression = (ConstantExpression) expr.getValue();
                ARecord record =
                        (ARecord) ConstantExpressionUtil.getConstantIaObject(constantExpression, ATypeTag.OBJECT);
                if (record == null) {
                    throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED,
                            constantExpression.getSourceLocation(), functionName,
                            constantExpression.getExpressionTag());
                }
                ARecordType recordType = record.getType();
                if (record.numberOfFields() > FullTextContainsFunctionDescriptor.getParamTypeMap().size()) {
                    throw CompilationException.create(ErrorCode.TOO_MANY_OPTIONS_FOR_FUNCTION,
                            constantExpression.getSourceLocation(), functionName);
                }
                for (int i = 0; i < record.numberOfFields(); i++) {
                    String option = recordType.getFieldNames()[i].toLowerCase();
                    ILogicalExpression optionExpr =
                            new ConstantExpression(new AsterixConstantValue(new AString(option)));
                    ILogicalExpression optionExprVal =
                            new ConstantExpression(new AsterixConstantValue(record.getValueByPos(i)));
                    ftConfigName = handleThirdParameterOptions(optionExpr, optionExprVal, newArgs, functionName);
                }
            } else {
                AbstractFunctionCallExpression openRecConsExpr = (AbstractFunctionCallExpression) expr.getValue();
                FunctionIdentifier openRecConsFi = openRecConsExpr.getFunctionIdentifier();
                if (openRecConsFi != BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR
                        && openRecConsFi != BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR) {
                    throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED, openRecConsExpr.getSourceLocation(),
                            functionName, openRecConsFi);
                }

                // We multiply 2 because the layout of the arguments are: [expr, val, expr1, val1, ...]
                if (openRecConsExpr.getArguments().size() > FullTextContainsFunctionDescriptor.getParamTypeMap().size()
                        * 2) {
                    throw CompilationException.create(ErrorCode.TOO_MANY_OPTIONS_FOR_FUNCTION,
                            openRecConsExpr.getSourceLocation(), functionName);
                }

                if (openRecConsExpr.getArguments().size() % 2 != 0) {
                    throw CompilationException.create(ErrorCode.COMPILATION_INVALID_PARAMETER_NUMBER,
                            openRecConsExpr.getSourceLocation(), functionName);
                }

                for (int i = 0; i < openRecConsExpr.getArguments().size(); i = i + 2) {
                    ILogicalExpression optionExpr = openRecConsExpr.getArguments().get(i).getValue();
                    ILogicalExpression optionExprVal = openRecConsExpr.getArguments().get(i + 1).getValue();
                    ftConfigName = handleThirdParameterOptions(optionExpr, optionExprVal, newArgs, functionName);
                }
            }
            return ftConfigName;
        }

        private String handleThirdParameterOptions(ILogicalExpression optionExpr, ILogicalExpression optionExprVal,
                List<Mutable<ILogicalExpression>> newArgs, String functionName) throws AlgebricksException {
            String ftConfigName = null;
            String option = ConstantExpressionUtil.getStringConstant(optionExpr);

            if (optionExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT || option == null) {
                throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED, optionExpr.getSourceLocation(),
                        functionName, optionExpr.getExpressionTag());
            }

            option = option.toLowerCase();
            if (!FullTextContainsFunctionDescriptor.getParamTypeMap().containsKey(option)) {
                throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED, optionExprVal.getSourceLocation(),
                        functionName, option);
            }

            String optionTypeStringVal = null;
            // If the option value is a constant, then we can check here.
            if (optionExprVal.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                switch (FullTextContainsFunctionDescriptor.getParamTypeMap().get(option)) {
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
                        throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED, optionExprVal.getSourceLocation(),
                                functionName, option);
                }

                // Check the validity of option value
                switch (option) {
                    case FullTextContainsFunctionDescriptor.SEARCH_MODE_OPTION:
                        checkSearchModeOption(optionTypeStringVal, functionName, optionExprVal.getSourceLocation());
                        break;
                    case FullTextContainsFunctionDescriptor.FULLTEXT_CONFIG_OPTION:
                        checkFullTextConfigOption(optionTypeStringVal, functionName, optionExprVal.getSourceLocation());
                        ftConfigName = optionTypeStringVal;
                        break;
                    default:
                        throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED, optionExprVal.getSourceLocation(),
                                functionName, option);
                }
            }

            // Add this option as arguments to the ftcontains().
            newArgs.add(new MutableObject<>(optionExpr));
            newArgs.add(new MutableObject<>(optionExprVal));
            return ftConfigName;
        }

        private void checkSearchModeOption(String optionVal, String functionName, SourceLocation sourceLoc)
                throws AlgebricksException {
            if (optionVal.equals(FullTextContainsFunctionDescriptor.SearchMode.ALL.getValue())
                    || optionVal.equals(FullTextContainsFunctionDescriptor.SearchMode.ANY.getValue())) {
                return;
            } else {
                throw CompilationException.create(ErrorCode.TYPE_UNSUPPORTED, sourceLoc, functionName, optionVal);
            }
        }

        private void checkFullTextConfigOption(String optionVal, String functionName, SourceLocation sourceLoc)
                throws AlgebricksException {
            // Currently, here we only check if the full-text config is null or empty string
            // We will check if the full-text config exists at run time
            if (Strings.isNullOrEmpty(optionVal)) {
                throw CompilationException.create(ErrorCode.COMPILATION_INVALID_EXPRESSION, sourceLoc, functionName,
                        FullTextContainsFunctionDescriptor.FULLTEXT_CONFIG_OPTION, "not-null", "null");
            } else {
                return;
            }
        }

        /**
         * Sets the default option value(s) when a user doesn't provide any option.
         */
        void setDefaultValueForThirdParameter(List<Mutable<ILogicalExpression>> newArgs) {
            // Sets the search mode option: the default option is conjunctive search.
            ILogicalExpression searchModeOptionExpr = new ConstantExpression(
                    new AsterixConstantValue(new AString(FullTextContainsFunctionDescriptor.SEARCH_MODE_OPTION)));
            ILogicalExpression searchModeValExpr = new ConstantExpression(new AsterixConstantValue(
                    new AString(FullTextContainsFunctionDescriptor.SearchMode.ALL.getValue())));
            // Add this option as arguments to the ftcontains().
            newArgs.add(new MutableObject<ILogicalExpression>(searchModeOptionExpr));
            newArgs.add(new MutableObject<ILogicalExpression>(searchModeValExpr));

            // We don't set the full-text config option here because the default value should be null
        }

    }

}
