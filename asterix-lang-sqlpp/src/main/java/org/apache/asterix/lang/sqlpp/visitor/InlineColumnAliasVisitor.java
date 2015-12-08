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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.base.Literal;
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
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.struct.VarIdentifier;
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
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableSubstitutionUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppQueryExpressionVisitor;

public class InlineColumnAliasVisitor extends AbstractSqlppQueryExpressionVisitor<Void, Boolean> {

    private final ScopeChecker scopeChecker = new ScopeChecker();

    @Override
    public Void visit(WhereClause whereClause, Boolean arg) throws AsterixException {
        whereClause.getWhereExpr().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(FromClause fromClause, Boolean arg) throws AsterixException {
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fromTerm.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(FromTerm fromTerm, Boolean arg) throws AsterixException {
        fromTerm.getLeftExpression().accept(this, arg);
        // A from binding variable will override the alias to substitute.
        scopeChecker.getCurrentScope().removeSymbolExpressionMapping(fromTerm.getLeftVariable());
        if (fromTerm.hasPositionalVariable()) {
            scopeChecker.getCurrentScope().removeSymbolExpressionMapping(fromTerm.getPositionalVariable());
        }

        for (AbstractBinaryCorrelateClause correlate : fromTerm.getCorrelateClauses()) {
            correlate.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(JoinClause joinClause, Boolean arg) throws AsterixException {
        joinClause.getRightExpression().accept(this, arg);
        removeSubsutitions(joinClause);
        joinClause.getConditionExpression().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(NestClause nestClause, Boolean arg) throws AsterixException {
        nestClause.getRightExpression().accept(this, arg);
        nestClause.getConditionExpression().accept(this, arg);
        removeSubsutitions(nestClause);
        return null;
    }

    @Override
    public Void visit(UnnestClause unnestClause, Boolean arg) throws AsterixException {
        unnestClause.getRightExpression().accept(this, arg);
        removeSubsutitions(unnestClause);
        return null;
    }

    @Override
    public Void visit(Projection projection, Boolean arg) throws AsterixException {
        projection.getExpression().accept(this, arg);
        scopeChecker.getCurrentScope().addSymbolExpressionMappingToScope(
                new VariableExpr(new VarIdentifier(projection.getName())), projection.getExpression());
        return null;
    }

    @Override
    public Void visit(SelectBlock selectBlock, Boolean arg) throws AsterixException {
        // Traverses the select block in the order of "select", "group-by",
        // "group-by" lets and "having".
        selectBlock.getSelectClause().accept(this, arg);

        if (selectBlock.hasFromClause()) {
            selectBlock.getFromClause().accept(this, arg);
        }
        if (selectBlock.hasLetClauses()) {
            for (LetClause letClause : selectBlock.getLetList()) {
                letClause.accept(this, arg);
            }
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, arg);
        }
        if (selectBlock.hasLetClausesAfterGroupby()) {
            for (LetClause letClauseAfterGby : selectBlock.getLetListAfterGroupby()) {
                letClauseAfterGby.accept(this, true);
            }
        }
        if (selectBlock.hasHavingClause()) {
            selectBlock.getHavingClause().accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(SelectClause selectClause, Boolean arg) throws AsterixException {
        if (selectClause.selectElement()) {
            selectClause.getSelectElement().accept(this, arg);
        }
        if (selectClause.selectRegular()) {
            selectClause.getSelectRegular().accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(SelectElement selectElement, Boolean arg) throws AsterixException {
        selectElement.getExpression().accept(this, true);
        return null;
    }

    @Override
    public Void visit(SelectRegular selectRegular, Boolean arg) throws AsterixException {
        for (Projection projection : selectRegular.getProjections()) {
            projection.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(SelectSetOperation selectSetOperation, Boolean arg) throws AsterixException {
        selectSetOperation.getLeftInput().accept(this, arg);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            right.getSetOperationRightInput().accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(SelectExpression selectExpression, Boolean arg) throws AsterixException {
        scopeChecker.createNewScope();

        // Visits let bindings.
        if (selectExpression.hasLetClauses()) {
            for (LetClause lc : selectExpression.getLetList()) {
                lc.accept(this, arg);
            }
        }

        // Visits selectSetOperation.
        selectExpression.getSelectSetOperation().accept(this, arg);

        // Visits order by.
        if (selectExpression.hasOrderby()) {
            selectExpression.getOrderbyClause().accept(this, arg);
        }

        // Visits limit.
        if (selectExpression.hasLimit()) {
            selectExpression.getLimitClause().accept(this, arg);
        }

        // Exits the scope that were entered within this select expression
        scopeChecker.removeCurrentScope();
        return null;
    }

    @Override
    public Void visit(LetClause letClause, Boolean rewrite) throws AsterixException {
        VariableSubstitutionEnvironment env = scopeChecker.getCurrentScope().getVarSubstitutionEnvironment();
        if (rewrite) {
            Expression newBindExpr = (Expression) SqlppVariableSubstitutionUtil
                    .substituteVariableWithoutContext(letClause.getBindingExpr(), env);
            letClause.setBindingExpr(newBindExpr);
        }
        letClause.getBindingExpr().accept(this, false);
        // A let binding variable will override the alias to substitute.
        scopeChecker.getCurrentScope().removeSymbolExpressionMapping(letClause.getVarExpr());
        return null;
    }

    @Override
    public Void visit(OrderbyClause oc, Boolean arg) throws AsterixException {
        VariableSubstitutionEnvironment env = scopeChecker.getCurrentScope().getVarSubstitutionEnvironment();
        List<Expression> orderExprs = new ArrayList<Expression>();
        for (Expression orderExpr : oc.getOrderbyList()) {
            orderExprs.add((Expression) SqlppVariableSubstitutionUtil.substituteVariableWithoutContext(orderExpr, env));
            orderExpr.accept(this, arg);
        }
        oc.setOrderbyList(orderExprs);
        return null;
    }

    @Override
    public Void visit(GroupbyClause gc, Boolean arg) throws AsterixException {
        VariableSubstitutionEnvironment env = scopeChecker.getCurrentScope().getVarSubstitutionEnvironment();
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            Expression newExpr = (Expression) SqlppVariableSubstitutionUtil.substituteVariableWithoutContext(gbyVarExpr.getExpr(),
                    env);
            newExpr.accept(this, arg);
            gbyVarExpr.setExpr(newExpr);
        }
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            // A group-by variable will override the alias to substitute.
            scopeChecker.getCurrentScope().removeSymbolExpressionMapping(gbyVarExpr.getVar());
        }
        return null;
    }

    @Override
    public Void visit(LimitClause limitClause, Boolean arg) throws AsterixException {
        limitClause.getLimitExpr().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(HavingClause havingClause, Boolean arg) throws AsterixException {
        VariableSubstitutionEnvironment env = scopeChecker.getCurrentScope().getVarSubstitutionEnvironment();
        Expression newFilterExpr = (Expression) SqlppVariableSubstitutionUtil
                .substituteVariableWithoutContext(havingClause.getFilterExpression(), env);
        newFilterExpr.accept(this, arg);
        havingClause.setFilterExpression(newFilterExpr);
        return null;
    }

    @Override
    public Void visit(Query q, Boolean arg) throws AsterixException {
        q.getBody().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(FunctionDecl fd, Boolean arg) throws AsterixException {
        scopeChecker.createNewScope();
        fd.getFuncBody().accept(this, arg);
        scopeChecker.removeCurrentScope();
        return null;
    }

    @Override
    public Void visit(LiteralExpr l, Boolean arg) throws AsterixException {
        return null;
    }

    @Override
    public Void visit(ListConstructor lc, Boolean arg) throws AsterixException {
        for (Expression expr : lc.getExprList()) {
            expr.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(RecordConstructor rc, Boolean rewrite) throws AsterixException {
        for (FieldBinding binding : rc.getFbList()) {
            Expression leftExpr = binding.getLeftExpr();
            leftExpr.accept(this, false);
            binding.getRightExpr().accept(this, false);
            if (rewrite && leftExpr.getKind() == Kind.LITERAL_EXPRESSION) {
                LiteralExpr literalExpr = (LiteralExpr) leftExpr;
                if (literalExpr.getValue().getLiteralType() == Literal.Type.STRING) {
                    String fieldName = literalExpr.getValue().getStringValue();
                    scopeChecker.getCurrentScope().addSymbolExpressionMappingToScope(
                            new VariableExpr(new VarIdentifier(fieldName)), binding.getRightExpr());
                }
            }
        }
        return null;
    }

    @Override
    public Void visit(OperatorExpr operatorExpr, Boolean arg) throws AsterixException {
        for (Expression expr : operatorExpr.getExprList()) {
            expr.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(IfExpr ifExpr, Boolean arg) throws AsterixException {
        ifExpr.getCondExpr().accept(this, arg);
        ifExpr.getThenExpr().accept(this, arg);
        ifExpr.getElseExpr().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(QuantifiedExpression qe, Boolean arg) throws AsterixException {
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            pair.getExpr().accept(this, arg);
        }
        qe.getSatisfiesExpr().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(CallExpr callExpr, Boolean arg) throws AsterixException {
        for (Expression expr : callExpr.getExprList()) {
            expr.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(VariableExpr varExpr, Boolean arg) throws AsterixException {
        return null;
    }

    @Override
    public Void visit(UnaryExpr u, Boolean arg) throws AsterixException {
        u.getExpr().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(FieldAccessor fa, Boolean arg) throws AsterixException {
        fa.getExpr().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(IndexAccessor ia, Boolean arg) throws AsterixException {
        ia.getExpr().accept(this, arg);
        Expression indexExpr = ia.getExpr();
        if (indexExpr != null) {
            indexExpr.accept(this, arg);
        }
        return null;
    }

    private void removeSubsutitions(AbstractBinaryCorrelateClause unnestClause) {
        scopeChecker.getCurrentScope().removeSymbolExpressionMapping(unnestClause.getRightVariable());
        if (unnestClause.hasPositionalVariable()) {
            scopeChecker.getCurrentScope().removeSymbolExpressionMapping(unnestClause.getPositionalVariable());
        }
    }
}
