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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * An AST pre-processor to rewrite distinct aggregates into regular aggregates as follows: <br/>
 * {@code agg-distinct(expr) -> agg((FROM expr AS i SELECT DISTINCT VALUE i))} <br/>
 * where {@code agg-distinct} is a distinct aggregate function, {@code agg} - a regular aggregate function
 */
public class SqlppDistinctAggregationSugarVisitor extends AbstractSqlppSimpleExpressionVisitor {
    protected final LangRewritingContext context;

    public SqlppDistinctAggregationSugarVisitor(LangRewritingContext context) {
        this.context = context;
    }

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        FunctionIdentifier newAggFn = getAggregateFunctionForDistinct(callExpr.getFunctionSignature());
        if (newAggFn == null) {
            return super.visit(callExpr, arg);
        }
        List<Expression> newExprList = rewriteArgumentList(callExpr.getExprList(), arg);
        callExpr.setFunctionSignature(new FunctionSignature(newAggFn));
        callExpr.setExprList(newExprList);
        return callExpr;
    }

    @Override
    public Expression visit(WindowExpression winExpr, ILangExpression arg) throws CompilationException {
        FunctionIdentifier newAggFn = getAggregateFunctionForDistinct(winExpr.getFunctionSignature());
        if (newAggFn == null) {
            return super.visit(winExpr, arg);
        }
        List<Expression> newExprList = rewriteArgumentList(winExpr.getExprList(), arg);
        winExpr.setFunctionSignature(new FunctionSignature(newAggFn));
        winExpr.setExprList(newExprList);
        return winExpr;
    }

    private FunctionIdentifier getAggregateFunctionForDistinct(FunctionSignature signature) {
        IFunctionInfo finfo = FunctionUtil.getFunctionInfo(signature);
        FunctionIdentifier aggFn =
                finfo != null ? BuiltinFunctions.getAggregateFunction(finfo.getFunctionIdentifier()) : null;
        return aggFn != null ? BuiltinFunctions.getAggregateFunctionForDistinct(aggFn) : null;
    }

    private List<Expression> rewriteArgumentList(List<Expression> exprList, ILangExpression arg)
            throws CompilationException {
        List<Expression> result = new ArrayList<>(exprList.size());
        for (Expression expr : exprList) {
            Expression newExpr = rewriteArgument(expr);
            result.add(newExpr.accept(this, arg));
        }
        return result;
    }

    /**
     * rewrites {@code expr -> FROM expr AS i SELECT DISTINCT VALUE i}
     */
    private Expression rewriteArgument(Expression argExpr) throws CompilationException {
        SourceLocation sourceLoc = argExpr.getSourceLocation();
        // From clause
        VariableExpr fromBindingVar = new VariableExpr(context.newVariable());
        fromBindingVar.setSourceLocation(sourceLoc);
        FromTerm fromTerm = new FromTerm(argExpr, fromBindingVar, null, null);
        fromTerm.setSourceLocation(sourceLoc);
        FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
        fromClause.setSourceLocation(sourceLoc);

        // Select clause.
        SelectElement selectElement = new SelectElement(fromBindingVar);
        selectElement.setSourceLocation(sourceLoc);
        SelectClause selectClause = new SelectClause(selectElement, null, true);
        selectClause.setSourceLocation(sourceLoc);

        // Construct the select expression.
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, null, null, null, null, null);
        selectBlock.setSourceLocation(sourceLoc);
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        selectSetOperation.setSourceLocation(sourceLoc);
        SelectExpression selectExpr = new SelectExpression(null, selectSetOperation, null, null, true);
        selectExpr.setSourceLocation(sourceLoc);
        return selectExpr;
    }
}
