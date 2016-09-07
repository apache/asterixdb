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
import java.util.HashSet;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Clause.ClauseType;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.IfExpr;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.HavingClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.NestClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.CaseExpression;
import org.apache.asterix.lang.sqlpp.expression.IndependentSubquery;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppQueryExpressionVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class FreeVariableVisitor extends AbstractSqlppQueryExpressionVisitor<Void, Collection<VariableExpr>> {

    @Override
    public Void visit(FromClause fromClause, Collection<VariableExpr> freeVars) throws AsterixException {
        Collection<VariableExpr> bindingVars = new HashSet<>();
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            Collection<VariableExpr> fromTermFreeVars = new HashSet<>();
            fromTerm.accept(this, fromTermFreeVars);

            // Since a right from term can refer to variables defined in a left from term,
            // we remove binding variables from the free variables.
            fromTermFreeVars.removeAll(bindingVars);

            // Adds binding variables.
            bindingVars.addAll(SqlppVariableUtil.getBindingVariables(fromTerm));

            // Adds into freeVars.
            freeVars.addAll(fromTermFreeVars);
        }
        return null;
    }

    @Override
    public Void visit(FromTerm fromTerm, Collection<VariableExpr> freeVars) throws AsterixException {
        // The encountered binding variables so far in the fromterm.
        Collection<VariableExpr> bindingVariables = new HashSet<>();

        // Visit the left expression of a from term.
        fromTerm.getLeftExpression().accept(this, freeVars);

        // Adds binding variables.
        bindingVariables.add(fromTerm.getLeftVariable());
        if (fromTerm.hasPositionalVariable()) {
            bindingVariables.add(fromTerm.getPositionalVariable());
        }

        // Visits join/unnest/nest clauses.
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            Collection<VariableExpr> correlateFreeVars = new HashSet<>();
            correlateClause.accept(this, correlateFreeVars);
            if (correlateClause.getClauseType() != ClauseType.JOIN_CLAUSE) {
                // Correlation is allowed if the clause is not a join clause,
                // therefore we remove left-side binding variables for these cases.
                correlateFreeVars.removeAll(bindingVariables);

                // Adds binding variables.
                bindingVariables.add(correlateClause.getRightVariable());
                if (correlateClause.hasPositionalVariable()) {
                    bindingVariables.add(correlateClause.getPositionalVariable());
                }
            }
            freeVars.addAll(correlateFreeVars);
        }
        return null;
    }

    @Override
    public Void visit(JoinClause joinClause, Collection<VariableExpr> freeVars) throws AsterixException {
        visitJoinAndNest(joinClause, joinClause.getConditionExpression(), freeVars);
        return null;
    }

    @Override
    public Void visit(NestClause nestClause, Collection<VariableExpr> freeVars) throws AsterixException {
        visitJoinAndNest(nestClause, nestClause.getConditionExpression(), freeVars);
        return null;
    }

    @Override
    public Void visit(UnnestClause unnestClause, Collection<VariableExpr> freeVars) throws AsterixException {
        unnestClause.getRightExpression().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(Projection projection, Collection<VariableExpr> freeVars) throws AsterixException {
        if (!projection.star()) {
            projection.getExpression().accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(SelectBlock selectBlock, Collection<VariableExpr> freeVars) throws AsterixException {
        Collection<VariableExpr> selectFreeVars = new HashSet<>();
        Collection<VariableExpr> fromFreeVars = new HashSet<>();
        Collection<VariableExpr> letsFreeVars = new HashSet<>();
        Collection<VariableExpr> whereFreeVars = new HashSet<>();
        Collection<VariableExpr> gbyFreeVars = new HashSet<>();
        Collection<VariableExpr> gbyLetsFreeVars = new HashSet<>();

        Collection<VariableExpr> fromBindingVars = SqlppVariableUtil.getBindingVariables(selectBlock.getFromClause());
        Collection<VariableExpr> letsBindingVars = SqlppVariableUtil.getBindingVariables(selectBlock.getLetList());
        Collection<VariableExpr> gbyBindingVars = SqlppVariableUtil.getBindingVariables(selectBlock.getGroupbyClause());
        Collection<VariableExpr> gbyLetsBindingVars =
                SqlppVariableUtil.getBindingVariables(selectBlock.getLetListAfterGroupby());

        selectBlock.getSelectClause().accept(this, selectFreeVars);
        // Removes group-by, from, let, and gby-let binding vars.
        removeAllBindingVarsInSelectBlock(selectFreeVars, fromBindingVars, letsBindingVars, gbyLetsBindingVars);

        if (selectBlock.hasFromClause()) {
            selectBlock.getFromClause().accept(this, fromFreeVars);
        }
        if (selectBlock.hasLetClauses()) {
            visitLetClauses(selectBlock.getLetList(), letsFreeVars);
            letsFreeVars.removeAll(fromBindingVars);
        }
        if (selectBlock.hasWhereClause()) {
            selectBlock.getWhereClause().accept(this, whereFreeVars);
            whereFreeVars.removeAll(fromBindingVars);
            whereFreeVars.removeAll(letsBindingVars);
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, gbyFreeVars);
            // Remove group-by and let binding vars.
            gbyFreeVars.removeAll(fromBindingVars);
            gbyFreeVars.removeAll(letsBindingVars);
            if (selectBlock.hasLetClausesAfterGroupby()) {
                visitLetClauses(selectBlock.getLetListAfterGroupby(), gbyLetsFreeVars);
                gbyLetsFreeVars.removeAll(fromBindingVars);
                gbyLetsFreeVars.removeAll(letsBindingVars);
                gbyLetsFreeVars.removeAll(gbyBindingVars);
            }
            if (selectBlock.hasHavingClause()) {
                selectBlock.getHavingClause().accept(this, selectFreeVars);
                removeAllBindingVarsInSelectBlock(selectFreeVars, fromBindingVars, letsBindingVars, gbyLetsBindingVars);
            }
        }

        // Removes all binding vars from <code>freeVars</code>, which contains the free
        // vars in the order-by and limit.
        removeAllBindingVarsInSelectBlock(freeVars, fromBindingVars, letsBindingVars, gbyLetsBindingVars);

        // Adds all free vars.
        freeVars.addAll(selectFreeVars);
        freeVars.addAll(fromFreeVars);
        freeVars.addAll(letsFreeVars);
        freeVars.addAll(whereFreeVars);
        freeVars.addAll(gbyFreeVars);
        freeVars.addAll(gbyLetsFreeVars);
        return null;
    }

    @Override
    public Void visit(SelectClause selectClause, Collection<VariableExpr> freeVars) throws AsterixException {
        if (selectClause.selectElement()) {
            selectClause.getSelectElement().accept(this, freeVars);
        }
        if (selectClause.selectRegular()) {
            selectClause.getSelectRegular().accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(SelectElement selectElement, Collection<VariableExpr> freeVars) throws AsterixException {
        selectElement.getExpression().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(SelectRegular selectRegular, Collection<VariableExpr> freeVars) throws AsterixException {
        for (Projection projection : selectRegular.getProjections()) {
            projection.accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(SelectSetOperation selectSetOperation, Collection<VariableExpr> freeVars)
            throws AsterixException {
        selectSetOperation.getLeftInput().accept(this, freeVars);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            right.getSetOperationRightInput().accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(HavingClause havingClause, Collection<VariableExpr> freeVars) throws AsterixException {
        havingClause.getFilterExpression().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(Query q, Collection<VariableExpr> freeVars) throws AsterixException {
        q.getBody().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(FunctionDecl fd, Collection<VariableExpr> freeVars) throws AsterixException {
        fd.getFuncBody().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(WhereClause whereClause, Collection<VariableExpr> freeVars) throws AsterixException {
        whereClause.getWhereExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(OrderbyClause oc, Collection<VariableExpr> freeVars) throws AsterixException {
        visit(oc.getOrderbyList(), freeVars);
        return null;
    }

    @Override
    public Void visit(GroupbyClause gc, Collection<VariableExpr> freeVars) throws AsterixException {
        // Puts all group-by variables into the symbol set of the new scope.
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            gbyVarExpr.getExpr().accept(this, freeVars);
        }
        for (GbyVariableExpressionPair decorVarExpr : gc.getDecorPairList()) {
            decorVarExpr.getExpr().accept(this, freeVars);
        }
        if (gc.hasGroupFieldList()) {
            for (Pair<Expression, Identifier> groupField : gc.getGroupFieldList()) {
                groupField.first.accept(this, freeVars);
            }
        }
        return null;
    }

    @Override
    public Void visit(LimitClause limitClause, Collection<VariableExpr> freeVars) throws AsterixException {
        limitClause.getLimitExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(LetClause letClause, Collection<VariableExpr> freeVars) throws AsterixException {
        letClause.getBindingExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(SelectExpression selectExpression, Collection<VariableExpr> freeVars) throws AsterixException {
        Collection<VariableExpr> letsFreeVars = new HashSet<>();
        Collection<VariableExpr> selectFreeVars = new HashSet<>();
        visitLetClauses(selectExpression.getLetList(), letsFreeVars);

        // visit order by
        if (selectExpression.hasOrderby()) {
            for (Expression orderExpr : selectExpression.getOrderbyClause().getOrderbyList()) {
                orderExpr.accept(this, selectFreeVars);
            }
        }

        // visit limit
        if (selectExpression.hasLimit()) {
            selectExpression.getLimitClause().accept(this, selectFreeVars);
        }

        // visit the main select
        selectExpression.getSelectSetOperation().accept(this, selectFreeVars);

        // Removed let binding variables.
        selectFreeVars.removeAll(SqlppVariableUtil.getBindingVariables(selectExpression.getLetList()));
        freeVars.addAll(letsFreeVars);
        freeVars.addAll(selectFreeVars);
        return null;
    }

    @Override
    public Void visit(LiteralExpr l, Collection<VariableExpr> freeVars) throws AsterixException {
        return null;
    }

    @Override
    public Void visit(ListConstructor lc, Collection<VariableExpr> freeVars) throws AsterixException {
        visit(lc.getExprList(), freeVars);
        return null;
    }

    @Override
    public Void visit(RecordConstructor rc, Collection<VariableExpr> freeVars) throws AsterixException {
        for (FieldBinding binding : rc.getFbList()) {
            binding.getLeftExpr().accept(this, freeVars);
            binding.getRightExpr().accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(OperatorExpr operatorExpr, Collection<VariableExpr> freeVars) throws AsterixException {
        visit(operatorExpr.getExprList(), freeVars);
        return null;
    }

    @Override
    public Void visit(IfExpr ifExpr, Collection<VariableExpr> freeVars) throws AsterixException {
        ifExpr.getCondExpr().accept(this, freeVars);
        ifExpr.getThenExpr().accept(this, freeVars);
        ifExpr.getElseExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(QuantifiedExpression qe, Collection<VariableExpr> freeVars) throws AsterixException {
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            pair.getExpr().accept(this, freeVars);
        }
        qe.getSatisfiesExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(CallExpr callExpr, Collection<VariableExpr> freeVars) throws AsterixException {
        for (Expression expr : callExpr.getExprList()) {
            expr.accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(VariableExpr varExpr, Collection<VariableExpr> freeVars) throws AsterixException {
        freeVars.add(varExpr);
        return null;
    }

    @Override
    public Void visit(UnaryExpr u, Collection<VariableExpr> freeVars) throws AsterixException {
        u.getExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(FieldAccessor fa, Collection<VariableExpr> freeVars) throws AsterixException {
        fa.getExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(IndexAccessor ia, Collection<VariableExpr> freeVars) throws AsterixException {
        ia.getExpr().accept(this, freeVars);
        if (ia.getIndexExpr() != null) {
            ia.getIndexExpr();
        }
        return null;
    }

    @Override
    public Void visit(IndependentSubquery independentSubquery, Collection<VariableExpr> freeVars)
            throws AsterixException {
        independentSubquery.getExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(CaseExpression caseExpr, Collection<VariableExpr> freeVars) throws AsterixException {
        caseExpr.getConditionExpr().accept(this, freeVars);
        visit(caseExpr.getWhenExprs(), freeVars);
        visit(caseExpr.getThenExprs(), freeVars);
        caseExpr.getElseExpr().accept(this, freeVars);
        return null;
    }

    private void visitLetClauses(List<LetClause> letClauses, Collection<VariableExpr> freeVars)
            throws AsterixException {
        if (letClauses == null || letClauses.isEmpty()) {
            return;
        }
        Collection<VariableExpr> bindingVars = new HashSet<>();
        for (LetClause letClause : letClauses) {
            Collection<VariableExpr> letFreeVars = new HashSet<>();
            letClause.accept(this, letFreeVars);

            // Removes previous binding variables.
            letFreeVars.removeAll(bindingVars);
            freeVars.addAll(letFreeVars);

            // Adds let binding variables into the binding variable collection.
            bindingVars.add(letClause.getVarExpr());
        }
    }

    private void visitJoinAndNest(AbstractBinaryCorrelateClause clause, Expression condition,
            Collection<VariableExpr> freeVars) throws AsterixException {
        clause.getRightExpression().accept(this, freeVars);
        Collection<VariableExpr> conditionFreeVars = new HashSet<>();
        condition.accept(this, freeVars);

        // The condition expression can free binding variables defined in the join clause.
        conditionFreeVars.remove(clause.getRightVariable());
        if (clause.hasPositionalVariable()) {
            conditionFreeVars.remove(clause.getPositionalVariable());
        }
        freeVars.addAll(conditionFreeVars);
    }

    private void visit(List<Expression> exprs, Collection<VariableExpr> arg) throws AsterixException {
        for (Expression expr : exprs) {
            expr.accept(this, arg);
        }
    }

    /**
     * Removes all binding variables defined in the select block for a free variable collection.
     *
     * @param freeVars,
     *            free variables.
     * @param fromBindingVars,
     *            binding variables defined in the from clause of a select block.
     * @param letsBindingVars,
     *            binding variables defined in the let clauses of the select block.
     * @param gbyLetsBindingVars,
     *            binding variables defined in the let clauses after a group-by in the select block.
     */
    private void removeAllBindingVarsInSelectBlock(Collection<VariableExpr> selectFreeVars,
            Collection<VariableExpr> fromBindingVars, Collection<VariableExpr> letsBindingVars,
            Collection<VariableExpr> gbyLetsBindingVars) {
        selectFreeVars.removeAll(fromBindingVars);
        selectFreeVars.removeAll(letsBindingVars);
        selectFreeVars.removeAll(gbyLetsBindingVars);
        selectFreeVars.removeAll(gbyLetsBindingVars);
    }

}
