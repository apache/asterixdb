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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause.ClauseType;
import org.apache.asterix.lang.common.base.Expression;
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
import org.apache.asterix.lang.common.expression.ListSliceExpression;
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
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppQueryExpressionVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class FreeVariableVisitor extends AbstractSqlppQueryExpressionVisitor<Void, Collection<VariableExpr>> {

    @Override
    public Void visit(FromClause fromClause, Collection<VariableExpr> freeVars) throws CompilationException {
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
    public Void visit(FromTerm fromTerm, Collection<VariableExpr> freeVars) throws CompilationException {
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
    public Void visit(JoinClause joinClause, Collection<VariableExpr> freeVars) throws CompilationException {
        visitJoinAndNest(joinClause, joinClause.getConditionExpression(), freeVars);
        return null;
    }

    @Override
    public Void visit(NestClause nestClause, Collection<VariableExpr> freeVars) throws CompilationException {
        visitJoinAndNest(nestClause, nestClause.getConditionExpression(), freeVars);
        return null;
    }

    @Override
    public Void visit(UnnestClause unnestClause, Collection<VariableExpr> freeVars) throws CompilationException {
        unnestClause.getRightExpression().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(Projection projection, Collection<VariableExpr> freeVars) throws CompilationException {
        if (!projection.star()) {
            projection.getExpression().accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(SelectBlock selectBlock, Collection<VariableExpr> freeVars) throws CompilationException {
        Collection<VariableExpr> selectFreeVars = new HashSet<>();
        Collection<VariableExpr> fromFreeVars = new HashSet<>();
        Collection<VariableExpr> letWheresFreeVars = new HashSet<>();
        Collection<VariableExpr> gbyFreeVars = new HashSet<>();
        Collection<VariableExpr> gbyLetHavingsFreeVars = new HashSet<>();

        Collection<VariableExpr> fromBindingVars = SqlppVariableUtil.getBindingVariables(selectBlock.getFromClause());
        Collection<VariableExpr> letsBindingVars =
                SqlppVariableUtil.getLetBindingVariables(selectBlock.getLetWhereList());
        Collection<VariableExpr> gbyBindingVars = SqlppVariableUtil.getBindingVariables(selectBlock.getGroupbyClause());
        Collection<VariableExpr> gbyLetsBindingVars =
                SqlppVariableUtil.getLetBindingVariables(selectBlock.getLetHavingListAfterGroupby());

        selectBlock.getSelectClause().accept(this, selectFreeVars);
        // Removes group-by, from, let, and gby-let binding vars.
        removeAllBindingVarsInSelectBlock(selectFreeVars, fromBindingVars, letsBindingVars, gbyBindingVars,
                gbyLetsBindingVars);

        if (selectBlock.hasFromClause()) {
            selectBlock.getFromClause().accept(this, fromFreeVars);
        }
        if (selectBlock.hasLetWhereClauses()) {
            visitLetWhereClauses(selectBlock.getLetWhereList(), letWheresFreeVars);
            letWheresFreeVars.removeAll(fromBindingVars);
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, gbyFreeVars);
            // Remove group-by and let binding vars.
            gbyFreeVars.removeAll(fromBindingVars);
            gbyFreeVars.removeAll(letsBindingVars);
            if (selectBlock.hasLetHavingClausesAfterGroupby()) {
                visitLetWhereClauses(selectBlock.getLetHavingListAfterGroupby(), gbyLetHavingsFreeVars);
                gbyLetHavingsFreeVars.removeAll(fromBindingVars);
                gbyLetHavingsFreeVars.removeAll(letsBindingVars);
                gbyLetHavingsFreeVars.removeAll(gbyBindingVars);
            }
        }

        // Removes all binding vars from <code>freeVars</code>, which contains the free
        // vars in the order-by and limit.
        removeAllBindingVarsInSelectBlock(freeVars, fromBindingVars, letsBindingVars, gbyBindingVars,
                gbyLetsBindingVars);

        // Adds all free vars.
        freeVars.addAll(selectFreeVars);
        freeVars.addAll(fromFreeVars);
        freeVars.addAll(letWheresFreeVars);
        freeVars.addAll(gbyFreeVars);
        freeVars.addAll(gbyLetHavingsFreeVars);
        return null;
    }

    @Override
    public Void visit(SelectClause selectClause, Collection<VariableExpr> freeVars) throws CompilationException {
        if (selectClause.selectElement()) {
            selectClause.getSelectElement().accept(this, freeVars);
        }
        if (selectClause.selectRegular()) {
            selectClause.getSelectRegular().accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(SelectElement selectElement, Collection<VariableExpr> freeVars) throws CompilationException {
        selectElement.getExpression().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(SelectRegular selectRegular, Collection<VariableExpr> freeVars) throws CompilationException {
        for (Projection projection : selectRegular.getProjections()) {
            projection.accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(SelectSetOperation selectSetOperation, Collection<VariableExpr> freeVars)
            throws CompilationException {
        selectSetOperation.getLeftInput().accept(this, freeVars);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            right.getSetOperationRightInput().accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(HavingClause havingClause, Collection<VariableExpr> freeVars) throws CompilationException {
        havingClause.getFilterExpression().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(Query q, Collection<VariableExpr> freeVars) throws CompilationException {
        q.getBody().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(FunctionDecl fd, Collection<VariableExpr> freeVars) throws CompilationException {
        fd.getFuncBody().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(WhereClause whereClause, Collection<VariableExpr> freeVars) throws CompilationException {
        whereClause.getWhereExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(OrderbyClause oc, Collection<VariableExpr> freeVars) throws CompilationException {
        visit(oc.getOrderbyList(), freeVars);
        return null;
    }

    @Override
    public Void visit(GroupbyClause gc, Collection<VariableExpr> freeVars) throws CompilationException {
        // Puts all group-by variables into the symbol set of the new scope.
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            gbyVarExpr.getExpr().accept(this, freeVars);
        }
        if (gc.hasDecorList()) {
            for (GbyVariableExpressionPair decorVarExpr : gc.getDecorPairList()) {
                decorVarExpr.getExpr().accept(this, freeVars);
            }
        }
        if (gc.hasGroupFieldList()) {
            for (Pair<Expression, Identifier> groupField : gc.getGroupFieldList()) {
                groupField.first.accept(this, freeVars);
            }
        }
        if (gc.hasWithMap()) {
            for (Expression expr : gc.getWithVarMap().keySet()) {
                expr.accept(this, freeVars);
            }
        }
        return null;
    }

    @Override
    public Void visit(LimitClause limitClause, Collection<VariableExpr> freeVars) throws CompilationException {
        limitClause.getLimitExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(LetClause letClause, Collection<VariableExpr> freeVars) throws CompilationException {
        letClause.getBindingExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(SelectExpression selectExpression, Collection<VariableExpr> freeVars)
            throws CompilationException {
        Collection<VariableExpr> letsFreeVars = new HashSet<>();
        Collection<VariableExpr> selectFreeVars = new HashSet<>();
        visitLetWhereClauses(selectExpression.getLetList(), letsFreeVars);

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
        selectFreeVars.removeAll(SqlppVariableUtil.getLetBindingVariables(selectExpression.getLetList()));
        freeVars.addAll(letsFreeVars);
        freeVars.addAll(selectFreeVars);
        return null;
    }

    @Override
    public Void visit(LiteralExpr l, Collection<VariableExpr> freeVars) throws CompilationException {
        return null;
    }

    @Override
    public Void visit(ListConstructor lc, Collection<VariableExpr> freeVars) throws CompilationException {
        visit(lc.getExprList(), freeVars);
        return null;
    }

    @Override
    public Void visit(RecordConstructor rc, Collection<VariableExpr> freeVars) throws CompilationException {
        for (FieldBinding binding : rc.getFbList()) {
            binding.getLeftExpr().accept(this, freeVars);
            binding.getRightExpr().accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(OperatorExpr operatorExpr, Collection<VariableExpr> freeVars) throws CompilationException {
        visit(operatorExpr.getExprList(), freeVars);
        return null;
    }

    @Override
    public Void visit(IfExpr ifExpr, Collection<VariableExpr> freeVars) throws CompilationException {
        ifExpr.getCondExpr().accept(this, freeVars);
        ifExpr.getThenExpr().accept(this, freeVars);
        ifExpr.getElseExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(QuantifiedExpression qe, Collection<VariableExpr> freeVars) throws CompilationException {
        Collection<VariableExpr> qeBindingVars = SqlppVariableUtil.getBindingVariables(qe);
        Collection<VariableExpr> qeFreeVars = new HashSet<>();
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            pair.getExpr().accept(this, qeFreeVars);
        }
        qe.getSatisfiesExpr().accept(this, qeFreeVars);
        qeFreeVars.removeAll(qeBindingVars);
        freeVars.addAll(qeFreeVars);
        return null;
    }

    @Override
    public Void visit(CallExpr callExpr, Collection<VariableExpr> freeVars) throws CompilationException {
        for (Expression expr : callExpr.getExprList()) {
            expr.accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(VariableExpr varExpr, Collection<VariableExpr> freeVars) throws CompilationException {
        freeVars.add(varExpr);
        return null;
    }

    @Override
    public Void visit(UnaryExpr u, Collection<VariableExpr> freeVars) throws CompilationException {
        u.getExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(FieldAccessor fa, Collection<VariableExpr> freeVars) throws CompilationException {
        fa.getExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(IndexAccessor ia, Collection<VariableExpr> freeVars) throws CompilationException {
        ia.getExpr().accept(this, freeVars);
        if (ia.getIndexExpr() != null) {
            ia.getIndexExpr().accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(ListSliceExpression expression, Collection<VariableExpr> freeVars) throws CompilationException {
        expression.getExpr().accept(this, freeVars);
        expression.getStartIndexExpression().accept(this, freeVars);

        // End index expression can be null (optional)
        if (expression.hasEndExpression()) {
            expression.getEndIndexExpression().accept(this, freeVars);
        }
        return null;
    }

    @Override
    public Void visit(CaseExpression caseExpr, Collection<VariableExpr> freeVars) throws CompilationException {
        caseExpr.getConditionExpr().accept(this, freeVars);
        visit(caseExpr.getWhenExprs(), freeVars);
        visit(caseExpr.getThenExprs(), freeVars);
        caseExpr.getElseExpr().accept(this, freeVars);
        return null;
    }

    @Override
    public Void visit(WindowExpression winExpr, Collection<VariableExpr> freeVars) throws CompilationException {
        if (winExpr.hasPartitionList()) {
            visit(winExpr.getPartitionList(), freeVars);
        }
        if (winExpr.hasOrderByList()) {
            visit(winExpr.getOrderbyList(), freeVars);
        }
        if (winExpr.hasFrameStartExpr()) {
            winExpr.getFrameStartExpr().accept(this, freeVars);
        }
        if (winExpr.hasFrameEndExpr()) {
            winExpr.getFrameEndExpr().accept(this, freeVars);
        }
        if (winExpr.hasWindowFieldList()) {
            for (Pair<Expression, Identifier> field : winExpr.getWindowFieldList()) {
                field.first.accept(this, freeVars);
            }
        }
        visit(winExpr.getExprList(), freeVars);
        if (winExpr.hasWindowVar()) {
            freeVars.remove(winExpr.getWindowVar());
        }
        return null;
    }

    private void visitLetWhereClauses(List<? extends AbstractClause> clauseList, Collection<VariableExpr> freeVars)
            throws CompilationException {
        if (clauseList == null || clauseList.isEmpty()) {
            return;
        }
        Collection<VariableExpr> bindingVars = new HashSet<>();
        for (AbstractClause clause : clauseList) {
            Collection<VariableExpr> clauseFreeVars = new HashSet<>();
            clause.accept(this, clauseFreeVars);

            // Removes previous binding variables.
            clauseFreeVars.removeAll(bindingVars);
            freeVars.addAll(clauseFreeVars);

            // Adds let binding variables into the binding variable collection.
            if (clause.getClauseType() == ClauseType.LET_CLAUSE) {
                bindingVars.add(((LetClause) clause).getVarExpr());
            }
        }
    }

    private void visitJoinAndNest(AbstractBinaryCorrelateClause clause, Expression condition,
            Collection<VariableExpr> freeVars) throws CompilationException {
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

    private void visit(List<Expression> exprs, Collection<VariableExpr> arg) throws CompilationException {
        for (Expression expr : exprs) {
            expr.accept(this, arg);
        }
    }

    /**
     * Removes all binding variables defined in the select block for a free variable collection.
     *  @param selectFreeVars,
     *            free variables.
     * @param fromBindingVars,
     *            binding variables defined in the from clause of a select block.
     * @param letsBindingVars,
     *            binding variables defined in the let clauses of the select block.
     * @param gbyBindingVars
     *            binding variables defined in the groupby clauses of the select block
     * @param gbyLetsBindingVars
     *            binding variables defined in the let clauses after groupby of the select block.
     */
    private void removeAllBindingVarsInSelectBlock(Collection<VariableExpr> selectFreeVars,
            Collection<VariableExpr> fromBindingVars, Collection<VariableExpr> letsBindingVars,
            Collection<VariableExpr> gbyBindingVars, Collection<VariableExpr> gbyLetsBindingVars) {
        selectFreeVars.removeAll(fromBindingVars);
        selectFreeVars.removeAll(letsBindingVars);
        selectFreeVars.removeAll(gbyBindingVars);
        selectFreeVars.removeAll(gbyLetsBindingVars);
    }
}
