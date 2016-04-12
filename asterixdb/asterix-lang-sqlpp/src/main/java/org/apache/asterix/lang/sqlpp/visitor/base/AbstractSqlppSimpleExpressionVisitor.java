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
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;

public class AbstractSqlppSimpleExpressionVisitor extends AbstractSqlppQueryExpressionVisitor<Expression, Expression> {

    @Override
    public Expression visit(FromClause fromClause, Expression arg) throws AsterixException {
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fromTerm.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(FromTerm fromTerm, Expression arg) throws AsterixException {
        // Visit the left expression of a from term.
        fromTerm.setLeftExpression(fromTerm.getLeftExpression().accept(this, arg));

        // Visits join/unnest/nest clauses.
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            correlateClause.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(JoinClause joinClause, Expression arg) throws AsterixException {
        joinClause.setRightExpression(joinClause.getRightExpression().accept(this, arg));
        joinClause.setConditionExpression(joinClause.getConditionExpression().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(NestClause nestClause, Expression arg) throws AsterixException {
        nestClause.setRightExpression(nestClause.getRightExpression().accept(this, arg));
        nestClause.setConditionExpression(nestClause.getConditionExpression().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(UnnestClause unnestClause, Expression arg) throws AsterixException {
        unnestClause.setRightExpression(unnestClause.getRightExpression().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(Projection projection, Expression arg) throws AsterixException {
        projection.setExpression(projection.getExpression().accept(this, arg));
        return null;
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
    public Expression visit(SelectClause selectClause, Expression arg) throws AsterixException {
        if (selectClause.selectElement()) {
            selectClause.getSelectElement().accept(this, arg);
        }
        if (selectClause.selectRegular()) {
            selectClause.getSelectRegular().accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(SelectElement selectElement, Expression arg) throws AsterixException {
        selectElement.setExpression(selectElement.getExpression().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(SelectRegular selectRegular, Expression arg) throws AsterixException {
        for (Projection projection : selectRegular.getProjections()) {
            projection.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(SelectSetOperation selectSetOperation, Expression arg) throws AsterixException {
        selectSetOperation.getLeftInput().accept(this, arg);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            right.getSetOperationRightInput().accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(HavingClause havingClause, Expression arg) throws AsterixException {
        havingClause.setFilterExpression(havingClause.getFilterExpression().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(Query q, Expression arg) throws AsterixException {
        q.setBody(q.getBody().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(FunctionDecl fd, Expression arg) throws AsterixException {
        fd.setFuncBody(fd.getFuncBody().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(WhereClause whereClause, Expression arg) throws AsterixException {
        whereClause.setWhereExpr(whereClause.getWhereExpr().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(OrderbyClause oc, Expression arg) throws AsterixException {
        List<Expression> newOrderbyList = new ArrayList<Expression>();
        for (Expression orderExpr : oc.getOrderbyList()) {
            newOrderbyList.add(orderExpr.accept(this, arg));
        }
        oc.setOrderbyList(newOrderbyList);
        return null;
    }

    @Override
    public Expression visit(GroupbyClause gc, Expression arg) throws AsterixException {
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            gbyVarExpr.setExpr(gbyVarExpr.getExpr().accept(this, arg));
        }
        return null;
    }

    @Override
    public Expression visit(LimitClause limitClause, Expression arg) throws AsterixException {
        limitClause.setLimitExpr(limitClause.getLimitExpr().accept(this, arg));
        if (limitClause.hasOffset()) {
            limitClause.setOffset(limitClause.getOffset().accept(this, arg));
        }
        return null;
    }

    @Override
    public Expression visit(LetClause letClause, Expression arg) throws AsterixException {
        letClause.setBindingExpr(letClause.getBindingExpr().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, Expression arg) throws AsterixException {
        // visit let list
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                letClause.accept(this, arg);
            }
        }

        // visit the main select.
        selectExpression.getSelectSetOperation().accept(this, arg);

        // visit order by
        if (selectExpression.hasOrderby()) {
            for (Expression orderExpr : selectExpression.getOrderbyClause().getOrderbyList()) {
                orderExpr.accept(this, arg);
            }
        }

        // visit limit
        if (selectExpression.hasLimit()) {
            selectExpression.getLimitClause().accept(this, arg);
        }
        return selectExpression;
    }

    @Override
    public Expression visit(LiteralExpr l, Expression arg) throws AsterixException {
        return l;
    }

    @Override
    public Expression visit(ListConstructor lc, Expression arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<Expression>();
        for (Expression expr : lc.getExprList()) {
            newExprList.add(expr.accept(this, arg));
        }
        lc.setExprList(newExprList);
        return lc;
    }

    @Override
    public Expression visit(RecordConstructor rc, Expression arg) throws AsterixException {
        for (FieldBinding binding : rc.getFbList()) {
            binding.setLeftExpr(binding.getLeftExpr().accept(this, arg));
            binding.setRightExpr(binding.getRightExpr().accept(this, arg));
        }
        return rc;
    }

    @Override
    public Expression visit(OperatorExpr operatorExpr, Expression arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<Expression>();
        for (Expression expr : operatorExpr.getExprList()) {
            newExprList.add(expr.accept(this, arg));
        }
        operatorExpr.setExprList(newExprList);
        return operatorExpr;
    }

    @Override
    public Expression visit(IfExpr ifExpr, Expression arg) throws AsterixException {
        ifExpr.setCondExpr(ifExpr.getCondExpr().accept(this, arg));
        ifExpr.setThenExpr(ifExpr.getThenExpr().accept(this, arg));
        ifExpr.setElseExpr(ifExpr.getElseExpr().accept(this, arg));
        return ifExpr;
    }

    @Override
    public Expression visit(QuantifiedExpression qe, Expression arg) throws AsterixException {
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            pair.setExpr(pair.getExpr().accept(this, arg));
        }
        qe.setSatisfiesExpr(qe.getSatisfiesExpr().accept(this, arg));
        return qe;
    }

    @Override
    public Expression visit(CallExpr callExpr, Expression arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<Expression>();
        for (Expression expr : callExpr.getExprList()) {
            newExprList.add(expr.accept(this, arg));
        }
        callExpr.setExprList(newExprList);
        return callExpr;
    }

    @Override
    public Expression visit(VariableExpr varExpr, Expression arg) throws AsterixException {
        return varExpr;
    }

    @Override
    public Expression visit(UnaryExpr u, Expression arg) throws AsterixException {
        u.setExpr(u.getExpr().accept(this, arg));
        return u;
    }

    @Override
    public Expression visit(FieldAccessor fa, Expression arg) throws AsterixException {
        fa.setExpr(fa.getExpr().accept(this, arg));
        return fa;
    }

    @Override
    public Expression visit(IndexAccessor ia, Expression arg) throws AsterixException {
        ia.setExpr(ia.getExpr().accept(this, arg));
        if (ia.getIndexExpr() != null) {
            ia.setIndexExpr(ia.getIndexExpr());
        }
        return ia;
    }

}
