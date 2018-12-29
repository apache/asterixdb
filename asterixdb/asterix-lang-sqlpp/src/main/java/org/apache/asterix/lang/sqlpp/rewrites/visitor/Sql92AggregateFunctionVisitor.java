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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Rewrites SQL-92 aggregate function into a SQL++ core aggregate function. <br/>
 * For example
 * <code>SUM(e.salary + i.bonus)</code>
 * is turned into
 * <code>array_sum( (FROM g AS gi SELECT ELEMENT gi.e.salary + gi.i.bonus) )</code>
 * where <code>g</code> is a 'group as' variable
 */
class Sql92AggregateFunctionVisitor extends AbstractSqlppSimpleExpressionVisitor {

    private final LangRewritingContext context;

    private final Expression groupVar;

    private final Map<Expression, Identifier> fieldVars;

    private final Collection<VariableExpr> outerVars;

    Sql92AggregateFunctionVisitor(LangRewritingContext context, VariableExpr groupVar,
            Map<Expression, Identifier> fieldVars, Collection<VariableExpr> outerVars) {
        this.context = context;
        this.groupVar = groupVar;
        this.fieldVars = fieldVars;
        this.outerVars = outerVars;
    }

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        List<Expression> newExprList = new ArrayList<>();
        FunctionSignature signature = callExpr.getFunctionSignature();
        boolean aggregate = FunctionMapUtil.isSql92AggregateFunction(signature);
        boolean rewritten = false;
        for (Expression expr : callExpr.getExprList()) {
            Expression newExpr =
                    aggregate ? wrapAggregationArgument(expr, groupVar, fieldVars, outerVars, context) : expr;
            rewritten |= newExpr != expr;
            newExprList.add(newExpr.accept(this, arg));
        }
        if (rewritten) {
            // Rewrites the SQL-92 function name to core functions,
            // e.g., SUM --> array_sum
            callExpr.setFunctionSignature(FunctionMapUtil.sql92ToCoreAggregateFunction(signature));
        }
        callExpr.setExprList(newExprList);
        return callExpr;
    }

    static Expression wrapAggregationArgument(Expression expr, Expression groupVar,
            Map<Expression, Identifier> fieldVars, Collection<VariableExpr> outerVars, LangRewritingContext context)
            throws CompilationException {
        SourceLocation sourceLoc = expr.getSourceLocation();
        Set<VariableExpr> freeVars = SqlppRewriteUtil.getFreeVariable(expr);

        VariableExpr fromBindingVar = new VariableExpr(context.newVariable());
        fromBindingVar.setSourceLocation(sourceLoc);
        FromTerm fromTerm = new FromTerm(groupVar, fromBindingVar, null, null);
        fromTerm.setSourceLocation(sourceLoc);
        FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
        fromClause.setSourceLocation(sourceLoc);

        // Maps field variable expressions to field accesses.
        Map<Expression, Expression> varExprMap = new HashMap<>();
        for (VariableExpr usedVar : freeVars) {
            // Reference to a field in the group variable.
            if (fieldVars.containsKey(usedVar)) {
                // Rewrites to a reference to a field in the group variable.
                FieldAccessor fa =
                        new FieldAccessor(fromBindingVar, new VarIdentifier(fieldVars.get(usedVar).getValue()));
                fa.setSourceLocation(usedVar.getSourceLocation());
                varExprMap.put(usedVar, fa);
            } else if (outerVars.contains(usedVar)) {
                // Do nothing
            } else {
                // Rewrites to a reference to a single field in the group variable.
                Identifier ident = findField(fieldVars, usedVar, context);
                FieldAccessor faInner = new FieldAccessor(fromBindingVar, ident);
                faInner.setSourceLocation(usedVar.getSourceLocation());
                FieldAccessor faOuter =
                        new FieldAccessor(faInner, SqlppVariableUtil.toUserDefinedVariableName(usedVar.getVar()));
                faOuter.setSourceLocation(usedVar.getSourceLocation());
                varExprMap.put(usedVar, faOuter);
            }
        }

        // Select clause.
        SelectElement selectElement =
                new SelectElement(SqlppRewriteUtil.substituteExpression(expr, varExprMap, context));
        selectElement.setSourceLocation(sourceLoc);
        SelectClause selectClause = new SelectClause(selectElement, null, false);
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

    private static Identifier findField(Map<Expression, Identifier> fieldVars, VariableExpr usedVar,
            LangRewritingContext context) throws CompilationException {
        Identifier ident = null;
        for (Map.Entry<Expression, Identifier> me : fieldVars.entrySet()) {
            Expression fieldVarExpr = me.getKey();
            if (fieldVarExpr.getKind() == Expression.Kind.VARIABLE_EXPRESSION
                    && context.isExcludedForFieldAccessVar(((VariableExpr) fieldVarExpr).getVar())) {
                continue;
            }
            if (ident != null) {
                throw new CompilationException(ErrorCode.AMBIGUOUS_IDENTIFIER, usedVar.getSourceLocation(),
                        SqlppVariableUtil.toUserDefinedVariableName(usedVar.getVar().getValue()).getValue());
            }
            ident = me.getValue();
        }
        if (ident == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, usedVar.getSourceLocation());
        }
        return ident;
    }
}
