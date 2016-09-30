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

package org.apache.asterix.lang.aql.visitor.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.aql.clause.DistinctClause;
import org.apache.asterix.lang.aql.clause.ForClause;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.aql.expression.UnionExpr;
import org.apache.asterix.lang.common.base.Clause;
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

public class AbstractAqlSimpleExpressionVisitor extends AbstractAqlQueryExpressionVisitor<Expression, ILangExpression> {

    @Override
    public Expression visit(FLWOGRExpression flwogreExpr, ILangExpression arg) throws AsterixException {
        for (Clause clause : flwogreExpr.getClauseList()) {
            clause.accept(this, arg);
        }
        flwogreExpr.setReturnExpr(flwogreExpr.getReturnExpr().accept(this, arg));
        return flwogreExpr;
    }

    @Override
    public Expression visit(UnionExpr u, ILangExpression arg) throws AsterixException {
        u.setExprs(visit(u.getExprs(), arg));
        return u;
    }

    @Override
    public Expression visit(ForClause forClause, ILangExpression arg) throws AsterixException {
        forClause.setInExpr(forClause.getInExpr().accept(this, arg));
        return null;
    }

    @Override
    public Expression visit(DistinctClause distinctClause, ILangExpression arg) throws AsterixException {
        distinctClause.setDistinctByExpr(visit(distinctClause.getDistinctByExpr(), arg));
        return null;
    }

    @Override
    public Expression visit(Query q, ILangExpression arg) throws AsterixException {
        q.setBody(visit(q.getBody(), q));
        return null;
    }

    @Override
    public Expression visit(FunctionDecl fd, ILangExpression arg) throws AsterixException {
        fd.setFuncBody(visit(fd.getFuncBody(), fd));
        return null;
    }

    @Override
    public Expression visit(WhereClause whereClause, ILangExpression arg) throws AsterixException {
        whereClause.setWhereExpr(visit(whereClause.getWhereExpr(), whereClause));
        return null;
    }

    @Override
    public Expression visit(OrderbyClause oc, ILangExpression arg) throws AsterixException {
        oc.setOrderbyList(visit(oc.getOrderbyList(), arg));
        return null;
    }

    @Override
    public Expression visit(GroupbyClause gc, ILangExpression arg) throws AsterixException {
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            gbyVarExpr.setExpr(visit(gbyVarExpr.getExpr(), gc));
        }
        return null;
    }

    @Override
    public Expression visit(LimitClause limitClause, ILangExpression arg) throws AsterixException {
        limitClause.setLimitExpr(visit(limitClause.getLimitExpr(), limitClause));
        if (limitClause.hasOffset()) {
            limitClause.setOffset(visit(limitClause.getOffset(), limitClause));
        }
        return null;
    }

    @Override
    public Expression visit(LetClause letClause, ILangExpression arg) throws AsterixException {
        letClause.setBindingExpr(visit(letClause.getBindingExpr(), letClause));
        return null;
    }

    @Override
    public Expression visit(LiteralExpr l, ILangExpression arg) throws AsterixException {
        return l;
    }

    @Override
    public Expression visit(ListConstructor lc, ILangExpression arg) throws AsterixException {
        lc.setExprList(visit(lc.getExprList(), arg));
        return lc;
    }

    @Override
    public Expression visit(RecordConstructor rc, ILangExpression arg) throws AsterixException {
        for (FieldBinding binding : rc.getFbList()) {
            binding.setLeftExpr(visit(binding.getLeftExpr(), rc));
            binding.setRightExpr(visit(binding.getRightExpr(), rc));
        }
        return rc;
    }

    @Override
    public Expression visit(OperatorExpr operatorExpr, ILangExpression arg) throws AsterixException {
        operatorExpr.setExprList(visit(operatorExpr.getExprList(), arg));
        return operatorExpr;
    }

    @Override
    public Expression visit(IfExpr ifExpr, ILangExpression arg) throws AsterixException {
        ifExpr.setCondExpr(visit(ifExpr.getCondExpr(), ifExpr));
        ifExpr.setThenExpr(visit(ifExpr.getThenExpr(), ifExpr));
        ifExpr.setElseExpr(visit(ifExpr.getElseExpr(), ifExpr));
        return ifExpr;
    }

    @Override
    public Expression visit(QuantifiedExpression qe, ILangExpression arg) throws AsterixException {
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            pair.setExpr(visit(pair.getExpr(), qe));
        }
        qe.setSatisfiesExpr(visit(qe.getSatisfiesExpr(), qe));
        return qe;
    }

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws AsterixException {
        callExpr.setExprList(visit(callExpr.getExprList(), arg));
        return callExpr;
    }

    @Override
    public Expression visit(VariableExpr varExpr, ILangExpression arg) throws AsterixException {
        return varExpr;
    }

    @Override
    public Expression visit(UnaryExpr u, ILangExpression arg) throws AsterixException {
        u.setExpr(visit(u.getExpr(), u));
        return u;
    }

    @Override
    public Expression visit(FieldAccessor fa, ILangExpression arg) throws AsterixException {
        fa.setExpr(visit(fa.getExpr(), fa));
        return fa;
    }

    @Override
    public Expression visit(IndexAccessor ia, ILangExpression arg) throws AsterixException {
        ia.setExpr(visit(ia.getExpr(), ia));
        if (ia.getIndexExpr() != null) {
            ia.setIndexExpr(visit(ia.getIndexExpr(), arg));
        }
        return ia;
    }

    protected Expression visit(Expression expr, ILangExpression arg) throws AsterixException {
        return postVisit(preVisit(expr).accept(this, arg));
    }

    protected Expression preVisit(Expression expr) throws AsterixException {
        return expr;
    }

    protected Expression postVisit(Expression expr) throws AsterixException {
        return expr;
    }

    private List<Expression> visit(List<Expression> exprs, ILangExpression arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<>();
        for (Expression expr : exprs) {
            newExprList.add(visit(expr, arg));
        }
        return newExprList;
    }
}
