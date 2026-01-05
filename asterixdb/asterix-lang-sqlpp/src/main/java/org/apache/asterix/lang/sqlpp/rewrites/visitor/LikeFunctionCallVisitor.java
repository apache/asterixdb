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
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.util.ExpressionUtils;

public class LikeFunctionCallVisitor extends OperatorExpressionVisitor {

    public LikeFunctionCallVisitor(LangRewritingContext context) {
        super(context);
    }

    @Override
    public Expression visit(OperatorExpr operatorExpr, ILangExpression arg) throws CompilationException {
        List<Expression> newExprList = new ArrayList<>();
        for (Expression expr : operatorExpr.getExprList()) {
            newExprList.add(expr.accept(this, operatorExpr));
        }
        operatorExpr.setExprList(newExprList);
        OperatorType opType = operatorExpr.getOpList().get(0);
        if (opType == OperatorType.LIKE) {
            return processLikeOperator(operatorExpr);
        }
        return operatorExpr;
    }

    private Expression processLikeOperator(OperatorExpr operatorExpr) throws CompilationException {
        Expression target = operatorExpr.getExprList().get(0);
        Expression patternExpr = operatorExpr.getExprList().get(1);
        String patternStr = ExpressionUtils.getStringLiteral(patternExpr);
        if (patternStr != null) {
            StringBuilder likePatternStr = new StringBuilder();
            LikePattern likePattern = processPattern(patternStr, likePatternStr);
            if (likePattern == LikePattern.PREFIX) {
                return convertLikeToRange(operatorExpr, target, likePatternStr.toString());
            } else if (likePattern == LikePattern.EQUAL) {
                Expression processedExpr = new LiteralExpr(new StringLiteral(likePatternStr.toString()));
                return createOperatorExpression(OperatorType.EQ, target, processedExpr, operatorExpr.getHints(),
                        operatorExpr.getSourceLocation());
            }
        }
        return createLikeExpression(operatorExpr);
    }
}
