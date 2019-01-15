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

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.visitor.SqlppSubstituteExpressionVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;

/**
 * <ul>
 * <li> Generates group by key variables if they were not specified in the query </li>
 * <li> Replaces expressions that appear in having/select/order-by/limit clause and are identical to some
 *      group by key expression with the group by key variable </li>
 * </ul>
 */
public class SubstituteGroupbyExpressionWithVariableVisitor extends AbstractSqlppExpressionScopingVisitor {

    public SubstituteGroupbyExpressionWithVariableVisitor(LangRewritingContext context) {
        super(context);
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        if (selectBlock.hasGroupbyClause()) {
            Map<Expression, Expression> map = new HashMap<>();
            for (GbyVariableExpressionPair gbyKeyPair : selectBlock.getGroupbyClause().getGbyPairList()) {
                Expression gbyKeyExpr = gbyKeyPair.getExpr();
                if (gbyKeyExpr.getKind() != Kind.VARIABLE_EXPRESSION) {
                    map.put(gbyKeyExpr, gbyKeyPair.getVar());
                }
            }

            // Creates a substitution visitor.
            SubstituteGroupbyExpressionVisitor visitor = new SubstituteGroupbyExpressionVisitor(context, map);

            // Rewrites LET/HAVING/SELECT clauses.
            if (selectBlock.hasLetHavingClausesAfterGroupby()) {
                for (AbstractClause letHavingClause : selectBlock.getLetHavingListAfterGroupby()) {
                    letHavingClause.accept(visitor, arg);
                }
            }
            selectBlock.getSelectClause().accept(visitor, arg);
            SelectExpression selectExpression = (SelectExpression) arg;

            // For SET operation queries, the GROUP BY key variables will not substitute ORDER BY nor LIMIT expressions.
            if (!selectExpression.getSelectSetOperation().hasRightInputs()) {
                if (selectExpression.hasOrderby()) {
                    selectExpression.getOrderbyClause().accept(visitor, arg);
                }
                if (selectExpression.hasLimit()) {
                    selectExpression.getLimitClause().accept(visitor, arg);
                }
            }
        }
        return super.visit(selectBlock, arg);
    }

    private static class SubstituteGroupbyExpressionVisitor extends SqlppSubstituteExpressionVisitor {

        private SubstituteGroupbyExpressionVisitor(LangRewritingContext context, Map<Expression, Expression> exprMap) {
            super(context, exprMap);
        }

        @Override
        public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
            FunctionSignature signature = callExpr.getFunctionSignature();
            if (FunctionMapUtil.isSql92AggregateFunction(signature)) {
                return callExpr;
            } else {
                return super.visit(callExpr, arg);
            }
        }
    }
}
