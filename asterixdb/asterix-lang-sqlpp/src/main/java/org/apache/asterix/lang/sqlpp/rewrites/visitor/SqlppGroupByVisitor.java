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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
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
    public Expression visit(SelectBlock selectBlock, Expression arg) throws AsterixException {
        // Traverses the select block in the order of "from", "let"s, "where",
        // "group by", "let"s, "having" and "select".
        if (selectBlock.hasFromClause()) {
            selectBlock.getFromClause().accept(this, arg);
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
            selectBlock.getGroupbyClause().accept(this, arg);
            Set<VariableExpr> withVarSet = new HashSet<>(selectBlock.getGroupbyClause().getWithVarList());
            withVarSet.remove(selectBlock.getGroupbyClause().getGroupVar());
            if (selectBlock.hasLetClausesAfterGroupby()) {
                List<LetClause> letListAfterGby = selectBlock.getLetListAfterGroupby();
                for (LetClause letClauseAfterGby : letListAfterGby) {
                    // Rewrites each let clause after the group-by.
                    SqlppRewriteUtil.rewriteExpressionUsingGroupVariable(selectBlock.getGroupbyClause().getGroupVar(),
                            withVarSet, letClauseAfterGby, context);
                    letClauseAfterGby.accept(this, arg);
                }
            }
            if (selectBlock.hasHavingClause()) {
                // Rewrites the having clause.
                SqlppRewriteUtil.rewriteExpressionUsingGroupVariable(selectBlock.getGroupbyClause().getGroupVar(),
                        withVarSet, selectBlock.getHavingClause(), context);
                selectBlock.getHavingClause().accept(this, arg);
            }
            // Rewrites the select clause.
            SqlppRewriteUtil.rewriteExpressionUsingGroupVariable(selectBlock.getGroupbyClause().getGroupVar(),
                    withVarSet, selectBlock.getSelectClause(), context);

            SelectExpression parentSelectExpression = (SelectExpression) arg;
            if (parentSelectExpression.hasOrderby()) {
                // Rewrites the order-by clause.
                SqlppRewriteUtil.rewriteExpressionUsingGroupVariable(selectBlock.getGroupbyClause().getGroupVar(),
                        withVarSet, parentSelectExpression.getOrderbyClause(), context);
            }
            if (parentSelectExpression.hasLimit()) {
                // Rewrites the limit clause.
                SqlppRewriteUtil.rewriteExpressionUsingGroupVariable(selectBlock.getGroupbyClause().getGroupVar(),
                        withVarSet, parentSelectExpression.getLimitClause(), context);
            }
        }
        selectBlock.getSelectClause().accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(GroupbyClause gc, Expression arg) throws AsterixException {
        Scope newScope = scopeChecker.extendCurrentScopeNoPush(true);
        // Puts all group-by variables into the symbol set of the new scope.
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            gbyVarExpr.setExpr(gbyVarExpr.getExpr().accept(this, arg));
            VariableExpr gbyVar = gbyVarExpr.getVar();
            if (gbyVar != null) {
                newScope.addNewVarSymbolToScope(gbyVarExpr.getVar().getVar());
            }
        }
        // Puts all live variables into withVarList.
        List<VariableExpr> withVarList = new ArrayList<VariableExpr>();
        Iterator<Identifier> varIterator = scopeChecker.getCurrentScope().liveSymbols();
        while (varIterator.hasNext()) {
            Identifier ident = varIterator.next();
            VariableExpr varExpr = new VariableExpr();
            if (ident instanceof VarIdentifier) {
                varExpr.setIsNewVar(false);
                varExpr.setVar((VarIdentifier) ident);
                withVarList.add(varExpr);
                newScope.addNewVarSymbolToScope((VarIdentifier) ident);
            }
        }

        // Sets the field list for the group variable.
        List<Pair<Expression, Identifier>> groupFieldList = new ArrayList<>();
        if (!gc.hasGroupFieldList()) {
            for (VariableExpr varExpr : withVarList) {
                Pair<Expression, Identifier> varIdPair = new Pair<>(new VariableExpr(varExpr.getVar()),
                        SqlppVariableUtil.toUserDefinedVariableName(varExpr.getVar()));
                groupFieldList.add(varIdPair);
            }
            gc.setGroupFieldList(groupFieldList);
        } else {
            // Check the scopes of group field variables.
            for (Pair<Expression, Identifier> groupField : gc.getGroupFieldList()) {
                Expression newVar = groupField.first.accept(this, arg);
                groupFieldList.add(new Pair<>(newVar, groupField.second));
            }
        }
        gc.setGroupFieldList(groupFieldList);

        // Sets the group variable.
        if (!gc.hasGroupVar()) {
            VariableExpr groupVar = new VariableExpr(context.newVariable());
            gc.setGroupVar(groupVar);
        }
        newScope.addNewVarSymbolToScope(gc.getGroupVar().getVar());

        // Adds the group variable into the "with" (i.e., re-binding) variable list.
        VariableExpr gbyVarRef = new VariableExpr(gc.getGroupVar().getVar());
        gbyVarRef.setIsNewVar(false);
        withVarList.add(gbyVarRef);
        gc.setWithVarList(withVarList);

        scopeChecker.replaceCurrentScope(newScope);
        return null;
    }
}
