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
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.sqlpp.expression.CaseExpression;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;

public class SqlppBuiltinFunctionRewriteVisitor extends AbstractSqlppSimpleExpressionVisitor {

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        //TODO(buyingyi): rewrite SQL temporal functions
        callExpr.setFunctionSignature(FunctionMapUtil.normalizeBuiltinFunctionSignature(callExpr.getFunctionSignature(),
                true, callExpr.getSourceLocation()));
        return super.visit(callExpr, arg);
    }

    @Override
    public Expression visit(WindowExpression winExpr, ILangExpression arg) throws CompilationException {
        winExpr.setFunctionSignature(FunctionMapUtil.normalizeBuiltinFunctionSignature(winExpr.getFunctionSignature(),
                true, winExpr.getSourceLocation()));
        return super.visit(winExpr, arg);
    }

    @Override
    public Expression visit(CaseExpression caseExpr, ILangExpression arg) throws CompilationException {
        Expression expr = super.visit(caseExpr, arg); // Visits it as usual first.
        if (expr != caseExpr) {
            return expr.accept(this, arg);
        }
        CaseExpression newCaseExpr = normalizeCaseExpr(caseExpr);
        if (SqlppRewriteUtil.constainsSubquery(newCaseExpr)) {
            // If the CASE expression contains a subquery, we do not rewrite it to a switch-case function call.
            return newCaseExpr;
        }
        // If the CASE expression does not contain a subquery, we rewrite it to a switch-case function call.
        FunctionSignature functionSignature = new FunctionSignature(BuiltinFunctions.SWITCH_CASE);
        List<Expression> whenExprList = newCaseExpr.getWhenExprs();
        List<Expression> thenExprList = newCaseExpr.getThenExprs();
        List<Expression> newExprList = new ArrayList<>();
        newExprList.add(newCaseExpr.getConditionExpr());
        for (int index = 0; index < whenExprList.size(); ++index) {
            newExprList.add(whenExprList.get(index));
            newExprList.add(thenExprList.get(index));
        }
        newExprList.add(newCaseExpr.getElseExpr());
        CallExpr callExpr = new CallExpr(functionSignature, newExprList);
        callExpr.setSourceLocation(caseExpr.getSourceLocation());
        return callExpr;
    }

    // Normalizes WHEN expressions so that it can have correct NULL/MISSING semantics as well
    // as type promotion semantics.
    private CaseExpression normalizeCaseExpr(CaseExpression caseExpr) throws CompilationException {
        LiteralExpr trueLiteral = new LiteralExpr(TrueLiteral.INSTANCE);
        trueLiteral.setSourceLocation(caseExpr.getSourceLocation());
        Expression conditionExpr = caseExpr.getConditionExpr();
        if (trueLiteral.equals(conditionExpr)) {
            return caseExpr;
        }
        List<Expression> normalizedWhenExprs = new ArrayList<>();
        for (Expression expr : caseExpr.getWhenExprs()) {
            OperatorExpr operatorExpr = new OperatorExpr();
            operatorExpr.addOperand((Expression) SqlppRewriteUtil.deepCopy(expr));
            operatorExpr.addOperand(caseExpr.getConditionExpr());
            operatorExpr.addOperator(OperatorType.EQ);
            operatorExpr.setSourceLocation(expr.getSourceLocation());
            normalizedWhenExprs.add(operatorExpr);
        }
        CaseExpression newCaseExpr =
                new CaseExpression(trueLiteral, normalizedWhenExprs, caseExpr.getThenExprs(), caseExpr.getElseExpr());
        newCaseExpr.setSourceLocation(caseExpr.getSourceLocation());
        return newCaseExpr;
    }

}
