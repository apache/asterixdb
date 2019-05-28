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

package org.apache.asterix.lang.sqlpp.rewrites.visitor;

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * A pre-processor that transforms window expressions as follows:
 * <ul>
 * <li>adds window frame variable</li>
 * <li>extracts list arguments of window functions into separate LET clauses</li>
 * </ul>
 *
 * Must be executed before {@link VariableCheckAndRewriteVisitor}
 */
public final class SqlppWindowRewriteVisitor extends AbstractSqlppExpressionExtractionVisitor {

    public SqlppWindowRewriteVisitor(LangRewritingContext context) {
        super(context);
    }

    @Override
    public Expression visit(WindowExpression winExpr, ILangExpression arg) throws CompilationException {
        super.visit(winExpr, arg);

        if (!winExpr.hasWindowVar()) {
            VariableExpr winVar = new VariableExpr(context.newVariable());
            winVar.setSourceLocation(winExpr.getSourceLocation());
            winExpr.setWindowVar(winVar);
        }

        FunctionSignature signature = winExpr.getFunctionSignature();
        FunctionIdentifier winfi = FunctionMapUtil.getInternalWindowFunction(signature);
        if (winfi != null) {
            rewriteSpecificWindowFunctions(winfi, winExpr);
            if (BuiltinFunctions.builtinFunctionHasProperty(winfi,
                    BuiltinFunctions.WindowFunctionProperty.HAS_LIST_ARG)) {
                List<Expression> newExprList = extractExpressions(winExpr.getExprList(), 1);
                if (newExprList == null) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, winExpr.getSourceLocation(), "");
                }
                winExpr.setExprList(newExprList);
            }
        } else if (FunctionMapUtil.isSql92AggregateFunction(signature)) {
            List<Expression> newExprList = extractExpressions(winExpr.getExprList(), winExpr.getExprList().size());
            if (newExprList == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, winExpr.getSourceLocation(), "");
            }
            winExpr.setExprList(newExprList);
        } else if (!FunctionMapUtil.isCoreAggregateFunction(signature)) {
            throw new CompilationException(ErrorCode.COMPILATION_EXPECTED_WINDOW_FUNCTION, winExpr.getSourceLocation(),
                    signature.getName());
        }

        return winExpr;
    }

    @Override
    protected boolean isExtractableExpression(Expression expr) {
        switch (expr.getKind()) {
            case LITERAL_EXPRESSION:
            case VARIABLE_EXPRESSION:
                return false;
            default:
                return true;
        }
    }

    @Override
    void handleUnsupportedClause(FromClause clause, List<Pair<Expression, VarIdentifier>> extractionList)
            throws CompilationException {
        throw new CompilationException(ErrorCode.COMPILATION_UNEXPECTED_WINDOW_EXPRESSION, clause.getSourceLocation());
    }

    /**
     * Apply rewritings for specific window functions:
     * <ul>
     * <li>
     * Add a copy of the first argument as the last argument for all functions
     * that have {@link BuiltinFunctions.WindowFunctionProperty#HAS_LIST_ARG} modifier.
     * The first argument will then be rewritten by
     * {@link SqlppWindowAggregationSugarVisitor#wrapAggregationArguments(WindowExpression, int)}.
     * The new last argument will be handled by expression to plan translator
     * </li>
     * </ul>
     */
    private void rewriteSpecificWindowFunctions(FunctionIdentifier winfi, WindowExpression winExpr)
            throws CompilationException {
        if (BuiltinFunctions.builtinFunctionHasProperty(winfi, BuiltinFunctions.WindowFunctionProperty.HAS_LIST_ARG)) {
            duplicateFirstArgument(winExpr);
        }
    }

    private void duplicateFirstArgument(WindowExpression winExpr) throws CompilationException {
        List<Expression> exprList = winExpr.getExprList();
        Expression arg = exprList.get(0);
        exprList.add((Expression) SqlppRewriteUtil.deepCopy(arg));
    }
}
