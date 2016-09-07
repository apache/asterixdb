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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.SqlppSubstituteExpressionVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;

public class InlineColumnAliasVisitor extends AbstractSqlppExpressionScopingVisitor {

    public InlineColumnAliasVisitor(LangRewritingContext context) {
        super(context);
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws AsterixException {
        // Gets the map from select clause.
        Map<Expression, Expression> map = getMap(selectBlock.getSelectClause());

        // Removes all FROM/LET binding variables
        if (selectBlock.hasFromClause()) {
            map.keySet().removeAll(SqlppVariableUtil.getBindingVariables(selectBlock.getFromClause()));
        }
        if (selectBlock.hasLetClauses()) {
            map.keySet().removeAll(SqlppVariableUtil.getBindingVariables(selectBlock.getLetList()));
        }

        // Creates a substitution visitor.
        SqlppSubstituteExpressionVisitor visitor = new SqlppSubstituteExpressionVisitor(context, map);

        // Rewrites GROUP BY/LET/HAVING clauses.
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(visitor, arg);
        }
        if (selectBlock.hasLetClausesAfterGroupby()) {
            for (LetClause letClause : selectBlock.getLetListAfterGroupby()) {
                letClause.accept(visitor, arg);
            }
        }
        if (selectBlock.hasHavingClause()) {
            selectBlock.getHavingClause().accept(visitor, arg);
        }
        SelectExpression selectExpression = (SelectExpression) arg;

        // For SET operation queries, column aliases will not substitute ORDER BY nor LIMIT expressions.
        if (!selectExpression.getSelectSetOperation().hasRightInputs()) {
            if (selectExpression.hasOrderby()) {
                selectExpression.getOrderbyClause().accept(visitor, arg);
            }
            if (selectExpression.hasLimit()) {
                selectExpression.getLimitClause().accept(visitor, arg);
            }
        }
        return super.visit(selectBlock, arg);
    }

    private Map<Expression, Expression> getMap(SelectClause selectClause) throws AsterixException {
        if (selectClause.selectElement()) {
            return getMap(selectClause.getSelectElement());
        }
        if (selectClause.selectRegular()) {
            return getMap(selectClause.getSelectRegular());
        }
        return null;
    }

    private Map<Expression, Expression> getMap(SelectElement selectElement) {
        Expression expr = selectElement.getExpression();
        if (expr.getKind() == Kind.RECORD_CONSTRUCTOR_EXPRESSION) {
            // Rewrite top-level field names (aliases), in order to be consistent with SelectRegular.
            return mapRecordConstructor((RecordConstructor) expr);
        }
        return Collections.emptyMap();
    }

    private Map<Expression, Expression> getMap(SelectRegular selectRegular) {
        return mapProjections(selectRegular.getProjections());
    }

    private Map<Expression, Expression> mapRecordConstructor(RecordConstructor rc) {
        Map<Expression, Expression> exprMap = new HashMap<>();
        for (FieldBinding binding : rc.getFbList()) {
            Expression leftExpr = binding.getLeftExpr();
            // We only need to deal with the case that the left expression (for a field name) is
            // a string literal. Otherwise, it is different from a column alias in a projection
            // (e.g., foo.name AS name) in regular SQL SELECT.
            if (leftExpr.getKind() != Kind.LITERAL_EXPRESSION) {
                continue;
            }
            LiteralExpr literalExpr = (LiteralExpr) leftExpr;
            if (literalExpr.getValue().getLiteralType() == Literal.Type.STRING) {
                String fieldName = SqlppVariableUtil.toInternalVariableName(literalExpr.getValue().getStringValue());
                exprMap.put(new VariableExpr(new VarIdentifier(fieldName)), binding.getRightExpr());
            }
        }
        return exprMap;
    }

    private Map<Expression, Expression> mapProjections(List<Projection> projections) {
        Map<Expression, Expression> exprMap = new HashMap<>();
        for (Projection projection : projections) {
            exprMap.put(
                    new VariableExpr(new VarIdentifier(SqlppVariableUtil.toInternalVariableName(projection.getName()))),
                    projection.getExpression());
        }
        return exprMap;
    }
}
