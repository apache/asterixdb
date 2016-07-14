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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.expression.IndependentSubquery;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableSubstitutionUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;

public class InlineWithExpressionVisitor extends AbstractSqlppSimpleExpressionVisitor {

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws AsterixException {
        if (selectExpression.hasLetClauses()) {
            // Inlines the leading WITH list.
            Map<VariableExpr, Expression> varExprMap = new HashMap<>();
            List<LetClause> withs = selectExpression.getLetList();
            Iterator<LetClause> with = withs.iterator();
            while (with.hasNext()) {
                LetClause letClause = with.next();
                // Replaces the let binding Expr.
                Expression expr = letClause.getBindingExpr();
                letClause.setBindingExpr(
                        (Expression) SqlppVariableSubstitutionUtil.substituteVariableWithoutContext(expr, varExprMap));
                with.remove();
                Expression bindingExpr = letClause.getBindingExpr();
                // Wraps the binding expression with IndependentSubquery, so that free identifier references
                // in the binding expression will not be resolved use outer-scope variables.
                varExprMap.put(letClause.getVarExpr(), new IndependentSubquery(bindingExpr));
            }

            // Inlines WITH expressions into the select expression.
            SelectExpression newSelectExpression = (SelectExpression) SqlppVariableSubstitutionUtil
                    .substituteVariableWithoutContext(selectExpression, varExprMap);

            // Continues to visit the rewritten select expression.
            return super.visit(newSelectExpression, arg);
        } else {
            // Continues to visit inside the select expression.
            return super.visit(selectExpression, arg);
        }
    }
}
