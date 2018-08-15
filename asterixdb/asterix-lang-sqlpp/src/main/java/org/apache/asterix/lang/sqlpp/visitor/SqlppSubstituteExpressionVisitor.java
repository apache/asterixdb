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
package org.apache.asterix.lang.sqlpp.visitor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;

/**
 * This visitor is used to substitute expressions in a visiting language structure
 * with an given map.
 */
public class SqlppSubstituteExpressionVisitor extends AbstractSqlppExpressionScopingVisitor {

    private final Map<Expression, Expression> exprMap = new HashMap<>();

    public SqlppSubstituteExpressionVisitor(LangRewritingContext context, Map<Expression, Expression> exprMap) {
        super(context);
        this.exprMap.putAll(exprMap);
    }

    // Note: we intentionally override preVisit instead of postVisit because we wants larger expressions
    // get substituted first if some of their child expressions are present as keys in the exprMap.
    // An example is:
    // asterixdb/asterix-app/src/test/resources/runtimets/queries_sqlpp/group-by/gby-expr-3/gby-expr-3.3.query.sqlpp
    @Override
    protected Expression preVisit(Expression expr) throws CompilationException {
        return substitute(expr);
    }

    protected Expression substitute(Expression expr) throws CompilationException {
        Expression mappedExpr = getMappedExpr(expr);
        // Makes a deep copy before returning to avoid shared references.
        return mappedExpr == null ? expr : (Expression) SqlppRewriteUtil.deepCopy(mappedExpr);
    }

    protected Expression getMappedExpr(Expression expr) throws CompilationException {
        Expression mappedExpr = exprMap.get(expr);
        if (mappedExpr == null) {
            return null;
        }
        Collection<VariableExpr> freeVars = SqlppVariableUtil.getFreeVariables(expr);
        for (VariableExpr freeVar : freeVars) {
            Scope currentScope = scopeChecker.getCurrentScope();
            if (currentScope.findSymbol(freeVar.getVar().getValue()) != null) {
                // If the expression to be substituted uses variables defined in the outer-most expresion
                // that is being visited, we shouldn't perform the substitution.
                return null;
            }
        }
        return mappedExpr;
    }
}
