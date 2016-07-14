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
package org.apache.asterix.lang.sqlpp.visitor.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
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
import org.apache.asterix.lang.sqlpp.expression.IndependentSubquery;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;

public class AbstractSqlppSimpleExpressionVisitor
        extends AbstractSqlppQueryExpressionVisitor<Expression, ILangExpression> {

    @Override
    public Expression visit(FromClause fromClause, ILangExpression arg) throws AsterixException {
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fromTerm.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(FromTerm fromTerm, ILangExpression arg) throws AsterixException {
        // Visit the left expression of a from term.
        fromTerm.setLeftExpression(fromTerm.getLeftExpression().accept(this, arg));

        // Visits join/unnest/nest clauses.
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            correlateClause.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(JoinClause joinClause, ILangExpression arg) throws AsterixException {
        joinClause.setRightExpression(joinClause.getRightExpression().accept(this, arg));
        joinClause.setConditionExpression(joinClause.getConditionExpression().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(NestClause nestClause, ILangExpression arg) throws AsterixException {
        nestClause.setRightExpression(nestClause.getRightExpression().accept(this, arg));
        nestClause.setConditionExpression(nestClause.getConditionExpression().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(UnnestClause unnestClause, ILangExpression arg) throws AsterixException {
        unnestClause.setRightExpression(unnestClause.getRightExpression().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(Projection projection, ILangExpression arg) throws AsterixException {
        if (!projection.star()) {
            projection.setExpression(projection.getExpression().accept(this, arg));
        }
        return null;
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws AsterixException {
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
        }
        if (selectBlock.hasLetClausesAfterGroupby()) {
            List<LetClause> letListAfterGby = selectBlock.getLetListAfterGroupby();
            for (LetClause letClauseAfterGby : letListAfterGby) {
                letClauseAfterGby.accept(this, arg);
            }
        }
        if (selectBlock.hasHavingClause()) {
            selectBlock.getHavingClause().accept(this, arg);
        }
        selectBlock.getSelectClause().accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(SelectClause selectClause, ILangExpression arg) throws AsterixException {
        if (selectClause.selectElement()) {
            selectClause.getSelectElement().accept(this, selectClause);
        }
        if (selectClause.selectRegular()) {
            selectClause.getSelectRegular().accept(this, selectClause);
        }
        return null;
    }

    @Override
    public Expression visit(SelectElement selectElement, ILangExpression arg) throws AsterixException {
        selectElement.setExpression(selectElement.getExpression().accept(this, selectElement));
        return null;
    }

    @Override
    public Expression visit(SelectRegular selectRegular, ILangExpression arg) throws AsterixException {
        for (Projection projection : selectRegular.getProjections()) {
            projection.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(SelectSetOperation selectSetOperation, ILangExpression arg) throws AsterixException {
        selectSetOperation.getLeftInput().accept(this, arg);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            right.getSetOperationRightInput().accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(HavingClause havingClause, ILangExpression arg) throws AsterixException {
        havingClause.setFilterExpression(havingClause.getFilterExpression().accept(this, havingClause));
        return null;
    }

    @Override
    public Expression visit(Query q, ILangExpression arg) throws AsterixException {
        q.setBody(q.getBody().accept(this, q));
        return null;
    }

    @Override
    public Expression visit(FunctionDecl fd, ILangExpression arg) throws AsterixException {
        fd.setFuncBody(fd.getFuncBody().accept(this, fd));
        return null;
    }

    @Override
    public Expression visit(WhereClause whereClause, ILangExpression arg) throws AsterixException {
        whereClause.setWhereExpr(whereClause.getWhereExpr().accept(this, whereClause));
        return null;
    }

    @Override
    public Expression visit(OrderbyClause oc, ILangExpression arg) throws AsterixException {
        List<Expression> newOrderbyList = new ArrayList<>();
        for (Expression orderExpr : oc.getOrderbyList()) {
            newOrderbyList.add(orderExpr.accept(this, oc));
        }
        oc.setOrderbyList(newOrderbyList);
        return null;
    }

    @Override
    public Expression visit(GroupbyClause gc, ILangExpression arg) throws AsterixException {
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            gbyVarExpr.setExpr(gbyVarExpr.getExpr().accept(this, gc));
        }
        return null;
    }

    @Override
    public Expression visit(LimitClause limitClause, ILangExpression arg) throws AsterixException {
        limitClause.setLimitExpr(limitClause.getLimitExpr().accept(this, limitClause));
        if (limitClause.hasOffset()) {
            limitClause.setOffset(limitClause.getOffset().accept(this, limitClause));
        }
        return null;
    }

    @Override
    public Expression visit(LetClause letClause, ILangExpression arg) throws AsterixException {
        letClause.setBindingExpr(letClause.getBindingExpr().accept(this, letClause));
        return null;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws AsterixException {
        // visit let list
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                letClause.accept(this, selectExpression);
            }
        }

        // visit the main select.
        selectExpression.getSelectSetOperation().accept(this, selectExpression);

        // visit order by
        if (selectExpression.hasOrderby()) {
            selectExpression.getOrderbyClause().accept(this, selectExpression);
        }

        // visit limit
        if (selectExpression.hasLimit()) {
            selectExpression.getLimitClause().accept(this, selectExpression);
        }
        return selectExpression;
    }

    @Override
    public Expression visit(LiteralExpr l, ILangExpression arg) throws AsterixException {
        return l;
    }

    @Override
    public Expression visit(ListConstructor lc, ILangExpression arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<>();
        for (Expression expr : lc.getExprList()) {
            newExprList.add(expr.accept(this, lc));
        }
        lc.setExprList(newExprList);
        return lc;
    }

    @Override
    public Expression visit(RecordConstructor rc, ILangExpression arg) throws AsterixException {
        for (FieldBinding binding : rc.getFbList()) {
            binding.setLeftExpr(binding.getLeftExpr().accept(this, rc));
            binding.setRightExpr(binding.getRightExpr().accept(this, rc));
        }
        return rc;
    }

    @Override
    public Expression visit(OperatorExpr operatorExpr, ILangExpression arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<>();
        for (Expression expr : operatorExpr.getExprList()) {
            newExprList.add(expr.accept(this, operatorExpr));
        }
        operatorExpr.setExprList(newExprList);
        return operatorExpr;
    }

    @Override
    public Expression visit(IfExpr ifExpr, ILangExpression arg) throws AsterixException {
        ifExpr.setCondExpr(ifExpr.getCondExpr().accept(this, ifExpr));
        ifExpr.setThenExpr(ifExpr.getThenExpr().accept(this, ifExpr));
        ifExpr.setElseExpr(ifExpr.getElseExpr().accept(this, ifExpr));
        return ifExpr;
    }

    @Override
    public Expression visit(QuantifiedExpression qe, ILangExpression arg) throws AsterixException {
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            pair.setExpr(pair.getExpr().accept(this, qe));
        }
        qe.setSatisfiesExpr(qe.getSatisfiesExpr().accept(this, qe));
        return qe;
    }

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<>();
        for (Expression expr : callExpr.getExprList()) {
            newExprList.add(expr.accept(this, callExpr));
        }
        callExpr.setExprList(newExprList);
        return callExpr;
    }

    @Override
    public Expression visit(VariableExpr varExpr, ILangExpression arg) throws AsterixException {
        return varExpr;
    }

    @Override
    public Expression visit(UnaryExpr u, ILangExpression arg) throws AsterixException {
        u.setExpr(u.getExpr().accept(this, u));
        return u;
    }

    @Override
    public Expression visit(FieldAccessor fa, ILangExpression arg) throws AsterixException {
        fa.setExpr(fa.getExpr().accept(this, fa));
        return fa;
    }

    @Override
    public Expression visit(IndexAccessor ia, ILangExpression arg) throws AsterixException {
        ia.setExpr(ia.getExpr().accept(this, ia));
        if (ia.getIndexExpr() != null) {
            ia.setIndexExpr(ia.getIndexExpr());
        }
        return ia;
    }

    @Override
    public Expression visit(IndependentSubquery independentSubquery, ILangExpression arg) throws AsterixException {
        independentSubquery.setExpr(independentSubquery.getExpr().accept(this, arg));
        return independentSubquery;
    }

}
