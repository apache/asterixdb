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
package org.apache.asterix.lang.sqlpp.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.sqlpp.visitor.CheckSubqueryVisitor;
import org.apache.asterix.lang.sqlpp.visitor.DeepCopyVisitor;
import org.apache.asterix.lang.sqlpp.visitor.FreeVariableVisitor;
import org.apache.asterix.lang.sqlpp.visitor.SqlppSubstituteExpressionVisitor;

public class SqlppRewriteUtil {

    private SqlppRewriteUtil() {
    }

    public static Set<VariableExpr> getFreeVariable(Expression expr) throws CompilationException {
        Set<VariableExpr> vars = new HashSet<>();
        FreeVariableVisitor visitor = new FreeVariableVisitor();
        expr.accept(visitor, vars);
        return vars;
    }

    public static ILangExpression deepCopy(ILangExpression expr) throws CompilationException {
        if (expr == null) {
            return expr;
        }
        DeepCopyVisitor visitor = new DeepCopyVisitor();
        return expr.accept(visitor, null);
    }

    // Checks if an ILangExpression contains a subquery.
    public static boolean constainsSubquery(ILangExpression expr) throws CompilationException {
        if (expr == null) {
            return false;
        }
        CheckSubqueryVisitor visitor = new CheckSubqueryVisitor();
        return expr.accept(visitor, null);
    }

    /**
     * Substitutes expression with replacement expressions according to the exprMap.
     *
     * @param expression
     *            ,
     *            an input expression.
     * @param exprMap
     *            a map that maps expressions to their corresponding replacement expressions.
     * @return an expression, where sub-expressions of the input expression (including the input expression itself)
     *         are replaced with deep copies with their mapped replacements in the exprMap if there exists such a
     *         replacement expression.
     * @throws CompilationException
     */
    public static Expression substituteExpression(Expression expression, Map<Expression, Expression> exprMap,
            LangRewritingContext context) throws CompilationException {
        if (exprMap.isEmpty()) {
            return expression;
        }
        // Creates a wrapper query for the expression so that if the expression itself
        // is the key, it can also be replaced.
        Query wrapper = new Query(false);
        wrapper.setSourceLocation(expression.getSourceLocation());
        wrapper.setBody(expression);
        // Creates a substitution visitor.
        SqlppSubstituteExpressionVisitor visitor = new SqlppSubstituteExpressionVisitor(context, exprMap);
        wrapper.accept(visitor, wrapper);
        return wrapper.getBody();
    }
}
