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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.HavingClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * A pre-processor that adds the group variable as well as its group field
 * list into the AST. It will also invoke SQL group-by aggregation sugar rewritings.
 */
// This visitor rewrites non-core SQL++ group-by queries into their SQL++ core version
// queries. For example, for the non-core query in
// asterix-app/src/test/resources/runtimets/queries_sqlpp/group-by/sugar-01/sugar-01.3.query.sqlpp,
//
// FROM Employee e
// JOIN Incentive i ON e.job_category = i.job_category
// JOIN SuperStars s ON e.id = s.id
// GROUP BY e.department_id AS deptId
// SELECT deptId as deptId, SUM(e.salary + i.bonus) AS star_cost;
//
// this visitor transforms it into the core version in
// asterix-app/src/test/resources/runtimets/queries_sqlpp/group-by/sugar-01/sugar-01.3.query.sqlpp,
//
// FROM Employee e
// JOIN Incentive i ON e.job_category = i.job_category
// JOIN SuperStars s ON e.id = s.id
// GROUP BY e.department_id AS deptId
// GROUP AS eis(e AS e, i AS i, s AS s)
// SELECT ELEMENT {
//  'deptId': deptId,
//  'star_cost': coll_sum( (FROM eis AS p SELECT ELEMENT p.e.salary + p.i.bonus) )
// };
/**
 * The transformation include three things:
 * 1. Add a group variable as well as its definition, e.g., GROUP AS eis(e AS e, i AS i, s AS s);
 * 2. Rewrite the argument expression of an aggregation function into a subquery if the argument
 * expression is not a subquery;
 * 3. Turn a SQL-92 aggregate function into a SQL++ core aggregate function when performing 2, e.g.,
 * SUM(e.salary + i.bonus) becomes
 * coll_sum( (FROM eis AS p SELECT ELEMENT p.e.salary + p.i.bonus) ).
 */

public class SqlppGroupByVisitor extends AbstractSqlppExpressionScopingVisitor {

    public SqlppGroupByVisitor(LangRewritingContext context) {
        super(context);
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws AsterixException {
        // Traverses the select block in the order of "from", "let"s, "where",
        // "group by", "let"s, "having" and "select".
        FromClause fromClause = selectBlock.getFromClause();
        if (selectBlock.hasFromClause()) {
            fromClause.accept(this, arg);
        }
        if (selectBlock.hasLetClauses()) {
            List<LetClause> letList = selectBlock.getLetList();
            for (LetClause letClause : letList) {
                letClause.accept(this, arg);
            }
        }
        if (selectBlock.hasWhereClause()) {
            selectBlock.getWhereClause().accept(this, arg);
        }
        if (selectBlock.hasGroupbyClause()) {
            GroupbyClause groupbyClause = selectBlock.getGroupbyClause();
            groupbyClause.accept(this, fromClause);
            Collection<VariableExpr> visibleVarsInCurrentScope = SqlppVariableUtil.getBindingVariables(groupbyClause);

            VariableExpr groupVar = groupbyClause.getGroupVar();
            Set<VariableExpr> groupFieldVars = getGroupFieldVariables(groupbyClause);

            Collection<VariableExpr> freeVariablesInGbyLets = new HashSet<>();
            if (selectBlock.hasLetClausesAfterGroupby()) {
                List<LetClause> letListAfterGby = selectBlock.getLetListAfterGroupby();
                for (LetClause letClauseAfterGby : letListAfterGby) {
                    letClauseAfterGby.accept(this, arg);
                    // Rewrites each let clause after the group-by.
                    SqlppRewriteUtil.rewriteExpressionUsingGroupVariable(groupVar, groupFieldVars, letClauseAfterGby,
                            context);
                    Collection<VariableExpr> freeVariablesInLet = SqlppVariableUtil.getFreeVariables(letClauseAfterGby
                            .getBindingExpr());
                    freeVariablesInLet.removeAll(visibleVarsInCurrentScope);
                    freeVariablesInGbyLets.addAll(freeVariablesInLet);
                    visibleVarsInCurrentScope.add(letClauseAfterGby.getVarExpr());
                }
            }

            Collection<VariableExpr> freeVariables = new HashSet<>();
            if (selectBlock.hasHavingClause()) {
                // Rewrites the having clause.
                HavingClause havingClause = selectBlock.getHavingClause();
                havingClause.accept(this, arg);
                SqlppRewriteUtil.rewriteExpressionUsingGroupVariable(groupVar, groupFieldVars, havingClause, context);
                freeVariables.addAll(SqlppVariableUtil.getFreeVariables(havingClause));
            }

            SelectExpression parentSelectExpression = (SelectExpression) arg;
            // We cannot rewrite ORDER BY and LIMIT if it's a SET operation query.
            if (!parentSelectExpression.getSelectSetOperation().hasRightInputs()) {
                if (parentSelectExpression.hasOrderby()) {
                    // Rewrites the ORDER BY clause.
                    OrderbyClause orderbyClause = parentSelectExpression.getOrderbyClause();
                    orderbyClause.accept(this, arg);
                    SqlppRewriteUtil.rewriteExpressionUsingGroupVariable(groupVar, groupFieldVars, orderbyClause,
                            context);
                    freeVariables.addAll(SqlppVariableUtil.getFreeVariables(orderbyClause));
                }
                if (parentSelectExpression.hasLimit()) {
                    // Rewrites the LIMIT clause.
                    LimitClause limitClause = parentSelectExpression.getLimitClause();
                    limitClause.accept(this, arg);
                    SqlppRewriteUtil
                            .rewriteExpressionUsingGroupVariable(groupVar, groupFieldVars, limitClause, context);
                    freeVariables.addAll(SqlppVariableUtil.getFreeVariables(limitClause));
                }
            }

            // Visits the select clause.
            SelectClause selectClause = selectBlock.getSelectClause();
            selectClause.accept(this, arg);
            // Rewrites the select clause.
            SqlppRewriteUtil.rewriteExpressionUsingGroupVariable(groupVar, groupFieldVars, selectClause, context);
            freeVariables.addAll(SqlppVariableUtil.getFreeVariables(selectClause));
            freeVariables.removeAll(visibleVarsInCurrentScope);

            // Gets the final free variables.
            freeVariables.addAll(freeVariablesInGbyLets);

            // Gets outer scope variables.
            Collection<VariableExpr> decorVars = SqlppVariableUtil.getLiveVariables(
                    scopeChecker.getCurrentScope(), true);
            decorVars.removeAll(visibleVarsInCurrentScope);

            // Need path resolution or not?
            boolean needResolution = !decorVars.containsAll(freeVariables);
            // If path resolution is needed, we need to include all outer scope variables in the decoration list.
            // Otherwise, we only need to retain used free variables.
            if (needResolution) {
                // Tracks used variables, including WITH variables.
                decorVars.retainAll(freeVariables);
                // Adds all non-WITH outer scope variables, for path resolution.
                Collection<VariableExpr> visibleOuterScopeNonWithVars = SqlppVariableUtil.getLiveVariables(
                        scopeChecker.getCurrentScope(), false);
                visibleOuterScopeNonWithVars.removeAll(visibleVarsInCurrentScope);
                decorVars.addAll(visibleOuterScopeNonWithVars);
            } else {
                // Only retains used free variables.
                decorVars.retainAll(freeVariables);
            }
            if (!decorVars.isEmpty()) {
                // Adds used WITH variables.
                Collection<VariableExpr> visibleOuterScopeNonWithVars = SqlppVariableUtil.getLiveVariables(
                        scopeChecker.getCurrentScope(), false);
                visibleOuterScopeNonWithVars.retainAll(freeVariables);
                decorVars.addAll(visibleOuterScopeNonWithVars);

                // Adds necessary decoration variables for the GROUP BY.
                // NOTE: we need to include WITH binding variables so as they can be evaluated before
                // the GROUP BY instead of being inlined as part of nested pipepline. The current optimzier
                // is not able to optimize the latter case. The following query is such an example:
                // asterixdb/asterix-app/src/test/resources/runtimets/queries_sqlpp/dapd/q2-11
                List<GbyVariableExpressionPair> decorList = new ArrayList<>();
                for (VariableExpr var : decorVars) {
                    decorList.add(new GbyVariableExpressionPair((VariableExpr) SqlppRewriteUtil.deepCopy(var),
                            (Expression) SqlppRewriteUtil.deepCopy(var)));
                }
                groupbyClause.getDecorPairList().addAll(decorList);
            }
        } else {
            selectBlock.getSelectClause().accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(GroupbyClause gc, ILangExpression arg) throws AsterixException {
        // Puts all FROM binding variables into withVarList.
        FromClause fromClause = (FromClause) arg;
        Collection<VariableExpr> fromBindingVars =
                fromClause == null ? new ArrayList<>() : SqlppVariableUtil.getBindingVariables(fromClause);
        Map<Expression, VariableExpr> withVarMap = new HashMap<>();
        for (VariableExpr fromBindingVar : fromBindingVars) {
            VariableExpr varExpr = new VariableExpr();
            varExpr.setIsNewVar(false);
            varExpr.setVar(fromBindingVar.getVar());
            VariableExpr newVarExpr = (VariableExpr) SqlppRewriteUtil.deepCopy(varExpr);
            withVarMap.put(varExpr, newVarExpr);
        }
        // Sets the field list for the group variable.
        List<Pair<Expression, Identifier>> groupFieldList = new ArrayList<>();
        if (!gc.hasGroupFieldList()) {
            for (VariableExpr varExpr : fromBindingVars) {
                Pair<Expression, Identifier> varIdPair = new Pair<>(new VariableExpr(varExpr.getVar()),
                        SqlppVariableUtil.toUserDefinedVariableName(varExpr.getVar()));
                groupFieldList.add(varIdPair);
            }
            gc.setGroupFieldList(groupFieldList);
        } else {
            for (Pair<Expression, Identifier> groupField : gc.getGroupFieldList()) {
                Expression newFieldExpr = groupField.first.accept(this, arg);
                groupFieldList.add(new Pair<>(newFieldExpr, groupField.second));
                // Adds a field binding variable into withVarList.
                VariableExpr bindingVar = new VariableExpr(
                        new VarIdentifier(SqlppVariableUtil.toInternalVariableName(groupField.second.getValue())));
                withVarMap.put(newFieldExpr, bindingVar);
            }
        }
        gc.setGroupFieldList(groupFieldList);

        // Sets the group variable.
        if (!gc.hasGroupVar()) {
            VariableExpr groupVar = new VariableExpr(context.newVariable());
            gc.setGroupVar(groupVar);
        }

        // Adds the group variable into the "with" (i.e., re-binding) variable list.
        VariableExpr gbyVarRef = new VariableExpr(gc.getGroupVar().getVar());
        gbyVarRef.setIsNewVar(false);
        withVarMap.put(gbyVarRef, (VariableExpr) SqlppRewriteUtil.deepCopy(gbyVarRef));
        gc.setWithVarMap(withVarMap);

        // Call super.visit(...) to scope variables.
        return super.visit(gc, arg);
    }

    private Set<VariableExpr> getGroupFieldVariables(GroupbyClause groupbyClause) {
        Set<VariableExpr> fieldVars = new HashSet<>();
        if (groupbyClause.hasGroupFieldList()) {
            for (Pair<Expression, Identifier> groupField : groupbyClause.getGroupFieldList()) {
                fieldVars.add(new VariableExpr(new VarIdentifier(SqlppVariableUtil
                        .toInternalVariableName(groupField.second.getValue()))));
            }
        }
        return fieldVars;
    }
}
