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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
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
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;

/**
 * An AST pre-processor to rewrite group-by sugar queries, which does the following transformations:
 * 1. Rewrite the argument expression of an aggregation function into a subquery if the argument
 * expression is not a subquery;
 * 2. Turn a SQL-92 aggregate function into a SQL++ core aggregate function when performing 1.
 */

// For example, this visitor turns the following query
//
// FROM Employee e
// JOIN Incentive i ON e.job_category = i.job_category
// JOIN SuperStars s ON e.id = s.id
// GROUP BY e.department_id AS deptId
// GROUP AS eis(e AS e, i AS i, s AS s)
// SELECT deptId as deptId, SUM(e.salary + i.bonus) AS star_cost;
//
// into the following core-version query:
//
// FROM Employee e
// JOIN Incentive i ON e.job_category = i.job_category
// JOIN SuperStars s ON e.id = s.id
// GROUP BY e.department_id AS deptId
// GROUP AS eis(e AS e, i AS i, s AS s)
// SELECT ELEMENT {
//          'deptId': deptId,
//          'star_cost': coll_sum( (FROM eis AS p SELECT ELEMENT p.e.salary + p.i.bonus) )
// };
//
// where SUM(e.salary + i.bonus) is turned into coll_sum( (FROM eis AS p SELECT ELEMENT p.e.salary + p.i.bonus) ).

public class SqlppGroupBySugarVisitor extends AbstractSqlppExpressionScopingVisitor {

    private final Expression groupVar;
    private final Collection<VariableExpr> fieldVars;

    public SqlppGroupBySugarVisitor(LangRewritingContext context, Expression groupVar,
            Collection<VariableExpr> fieldVars) {
        super(context);
        this.groupVar = groupVar;
        this.fieldVars = fieldVars;
    }

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<>();
        FunctionSignature signature = callExpr.getFunctionSignature();
        boolean aggregate = FunctionMapUtil.isSql92AggregateFunction(signature);
        boolean rewritten = false;
        for (Expression expr : callExpr.getExprList()) {
            Expression newExpr = aggregate ? wrapAggregationArgument(expr) : expr;
            rewritten |= newExpr != expr;
            newExprList.add(newExpr.accept(this, arg));
        }
        if (rewritten) {
            // Rewrites the SQL-92 function name to core functions,
            // e.g., SUM --> coll_sum
            callExpr.setFunctionSignature(FunctionMapUtil.sql92ToCoreAggregateFunction(signature));
        }
        callExpr.setExprList(newExprList);
        return callExpr;
    }

    private Expression wrapAggregationArgument(Expression argExpr) throws AsterixException {
        Expression expr = argExpr;
        Set<VariableExpr> freeVars = SqlppRewriteUtil.getFreeVariable(expr);

        VariableExpr fromBindingVar = new VariableExpr(context.newVariable());
        FromTerm fromTerm = new FromTerm(groupVar, fromBindingVar, null, null);
        FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));

        // Maps field variable expressions to field accesses.
        Map<Expression, Expression> varExprMap = new HashMap<>();
        for (VariableExpr usedVar : freeVars) {
            // Reference to a field in the group variable.
            if (fieldVars.contains(usedVar)) {
                // Rewrites to a reference to a field in the group variable.
                varExprMap.put(usedVar,
                                new FieldAccessor(fromBindingVar, SqlppVariableUtil.toUserDefinedVariableName(usedVar
                                        .getVar())));
            }
        }

        // Select clause.
        SelectElement selectElement = new SelectElement(
                SqlppRewriteUtil.substituteExpression(expr, varExprMap, context));
        SelectClause selectClause = new SelectClause(selectElement, null, false);

        // Construct the select expression.
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, null, null, null, null, null);
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        return new SelectExpression(null, selectSetOperation, null, null, true);
    }
}
