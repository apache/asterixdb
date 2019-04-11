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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractClause;
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
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.InsertStatement;
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
import org.apache.hyracks.algebricks.common.utils.Pair;

public class AbstractSqlppSimpleExpressionVisitor
        extends AbstractSqlppQueryExpressionVisitor<Expression, ILangExpression> {

    @Override
    public Expression visit(FromClause fromClause, ILangExpression arg) throws CompilationException {
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            visit(fromTerm, arg);
        }
        return null;
    }

    @Override
    public Expression visit(FromTerm fromTerm, ILangExpression arg) throws CompilationException {
        // Visit the left expression of a from term.
        fromTerm.setLeftExpression(visit(fromTerm.getLeftExpression(), arg));

        // Visits join/unnest/nest clauses.
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            correlateClause.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(JoinClause joinClause, ILangExpression arg) throws CompilationException {
        joinClause.setRightExpression(visit(joinClause.getRightExpression(), arg));
        joinClause.setConditionExpression(visit(joinClause.getConditionExpression(), arg));
        return null;
    }

    @Override
    public Expression visit(NestClause nestClause, ILangExpression arg) throws CompilationException {
        nestClause.setRightExpression(visit(nestClause.getRightExpression(), arg));
        nestClause.setConditionExpression(visit(nestClause.getConditionExpression(), arg));
        return null;
    }

    @Override
    public Expression visit(UnnestClause unnestClause, ILangExpression arg) throws CompilationException {
        unnestClause.setRightExpression(visit(unnestClause.getRightExpression(), arg));
        return null;
    }

    @Override
    public Expression visit(Projection projection, ILangExpression arg) throws CompilationException {
        if (!projection.star()) {
            projection.setExpression(visit(projection.getExpression(), arg));
        }
        return null;
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        // Traverses the select block in the order of "from", "let/where"s, "group by", "let/having"s and "select".
        if (selectBlock.hasFromClause()) {
            selectBlock.getFromClause().accept(this, arg);
        }
        if (selectBlock.hasLetWhereClauses()) {
            for (AbstractClause clause : selectBlock.getLetWhereList()) {
                clause.accept(this, arg);
            }
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, arg);
        }
        if (selectBlock.hasLetHavingClausesAfterGroupby()) {
            for (AbstractClause clause : selectBlock.getLetHavingListAfterGroupby()) {
                clause.accept(this, arg);
            }
        }
        selectBlock.getSelectClause().accept(this, arg);
        return null;
    }

    @Override
    public Expression visit(SelectClause selectClause, ILangExpression arg) throws CompilationException {
        if (selectClause.selectElement()) {
            selectClause.getSelectElement().accept(this, selectClause);
        }
        if (selectClause.selectRegular()) {
            selectClause.getSelectRegular().accept(this, selectClause);
        }
        return null;
    }

    @Override
    public Expression visit(SelectElement selectElement, ILangExpression arg) throws CompilationException {
        selectElement.setExpression(visit(selectElement.getExpression(), selectElement));
        return null;
    }

    @Override
    public Expression visit(SelectRegular selectRegular, ILangExpression arg) throws CompilationException {
        for (Projection projection : selectRegular.getProjections()) {
            projection.accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(SelectSetOperation selectSetOperation, ILangExpression arg) throws CompilationException {
        selectSetOperation.getLeftInput().accept(this, arg);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            right.getSetOperationRightInput().accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(HavingClause havingClause, ILangExpression arg) throws CompilationException {
        havingClause.setFilterExpression(visit(havingClause.getFilterExpression(), havingClause));
        return null;
    }

    @Override
    public Expression visit(Query q, ILangExpression arg) throws CompilationException {
        q.setBody(visit(q.getBody(), q));
        return null;
    }

    @Override
    public Expression visit(FunctionDecl fd, ILangExpression arg) throws CompilationException {
        fd.setFuncBody(visit(fd.getFuncBody(), fd));
        return null;
    }

    @Override
    public Expression visit(WhereClause whereClause, ILangExpression arg) throws CompilationException {
        whereClause.setWhereExpr(visit(whereClause.getWhereExpr(), whereClause));
        return null;
    }

    @Override
    public Expression visit(OrderbyClause oc, ILangExpression arg) throws CompilationException {
        oc.setOrderbyList(visit(oc.getOrderbyList(), oc));
        return null;
    }

    @Override
    public Expression visit(GroupbyClause gc, ILangExpression arg) throws CompilationException {
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            gbyVarExpr.setExpr(visit(gbyVarExpr.getExpr(), gc));
        }
        if (gc.hasDecorList()) {
            for (GbyVariableExpressionPair decVarExpr : gc.getDecorPairList()) {
                decVarExpr.setExpr(visit(decVarExpr.getExpr(), gc));
            }
        }
        return null;
    }

    @Override
    public Expression visit(LimitClause limitClause, ILangExpression arg) throws CompilationException {
        limitClause.setLimitExpr(visit(limitClause.getLimitExpr(), limitClause));
        if (limitClause.hasOffset()) {
            limitClause.setOffset(visit(limitClause.getOffset(), limitClause));
        }
        return null;
    }

    @Override
    public Expression visit(LetClause letClause, ILangExpression arg) throws CompilationException {
        letClause.setBindingExpr(visit(letClause.getBindingExpr(), letClause));
        return null;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
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
    public Expression visit(LiteralExpr l, ILangExpression arg) throws CompilationException {
        return l;
    }

    @Override
    public Expression visit(ListConstructor lc, ILangExpression arg) throws CompilationException {
        lc.setExprList(visit(lc.getExprList(), arg));
        return lc;
    }

    @Override
    public Expression visit(RecordConstructor rc, ILangExpression arg) throws CompilationException {
        for (FieldBinding binding : rc.getFbList()) {
            binding.setLeftExpr(visit(binding.getLeftExpr(), arg));
            binding.setRightExpr(visit(binding.getRightExpr(), arg));
        }
        return rc;
    }

    @Override
    public Expression visit(OperatorExpr operatorExpr, ILangExpression arg) throws CompilationException {
        operatorExpr.setExprList(visit(operatorExpr.getExprList(), arg));
        return operatorExpr;
    }

    @Override
    public Expression visit(IfExpr ifExpr, ILangExpression arg) throws CompilationException {
        ifExpr.setCondExpr(visit(ifExpr.getCondExpr(), arg));
        ifExpr.setThenExpr(visit(ifExpr.getThenExpr(), arg));
        ifExpr.setElseExpr(visit(ifExpr.getElseExpr(), arg));
        return ifExpr;
    }

    @Override
    public Expression visit(QuantifiedExpression qe, ILangExpression arg) throws CompilationException {
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            pair.setExpr(visit(pair.getExpr(), qe));
        }
        qe.setSatisfiesExpr(visit(qe.getSatisfiesExpr(), qe));
        return qe;
    }

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        callExpr.setExprList(visit(callExpr.getExprList(), arg));
        return callExpr;
    }

    @Override
    public Expression visit(VariableExpr varExpr, ILangExpression arg) throws CompilationException {
        return varExpr;
    }

    @Override
    public Expression visit(UnaryExpr u, ILangExpression arg) throws CompilationException {
        u.setExpr(visit(u.getExpr(), arg));
        return u;
    }

    @Override
    public Expression visit(WindowExpression winExpr, ILangExpression arg) throws CompilationException {
        visitWindowExpressionExcludingExprList(winExpr, arg);
        winExpr.setExprList(visit(winExpr.getExprList(), arg));
        return winExpr;
    }

    protected void visitWindowExpressionExcludingExprList(WindowExpression winExpr, ILangExpression arg)
            throws CompilationException {
        if (winExpr.hasPartitionList()) {
            winExpr.setPartitionList(visit(winExpr.getPartitionList(), arg));
        }
        if (winExpr.hasOrderByList()) {
            winExpr.setOrderbyList(visit(winExpr.getOrderbyList(), arg));
        }
        if (winExpr.hasFrameStartExpr()) {
            winExpr.setFrameStartExpr(visit(winExpr.getFrameStartExpr(), arg));
        }
        if (winExpr.hasFrameEndExpr()) {
            winExpr.setFrameEndExpr(visit(winExpr.getFrameEndExpr(), arg));
        }
        if (winExpr.hasWindowFieldList()) {
            for (Pair<Expression, Identifier> field : winExpr.getWindowFieldList()) {
                field.first = visit(field.first, arg);
            }
        }
    }

    @Override
    public Expression visit(FieldAccessor fa, ILangExpression arg) throws CompilationException {
        fa.setExpr(visit(fa.getExpr(), arg));
        return fa;
    }

    @Override
    public Expression visit(IndexAccessor ia, ILangExpression arg) throws CompilationException {
        ia.setExpr(visit(ia.getExpr(), arg));
        if (ia.getIndexExpr() != null) {
            ia.setIndexExpr(visit(ia.getIndexExpr(), arg));
        }
        return ia;
    }

    @Override
    public Expression visit(ListSliceExpression expression, ILangExpression arg) throws CompilationException {
        expression.setExpr(visit(expression.getExpr(), arg));
        expression.setStartIndexExpression(visit(expression.getStartIndexExpression(), arg));

        // End index expression can be null (optional)
        if (expression.hasEndExpression()) {
            expression.setEndIndexExpression(visit(expression.getEndIndexExpression(), arg));
        }
        return expression;
    }

    @Override
    public Expression visit(CaseExpression caseExpr, ILangExpression arg) throws CompilationException {
        caseExpr.setConditionExpr(visit(caseExpr.getConditionExpr(), arg));
        caseExpr.setWhenExprs(visit(caseExpr.getWhenExprs(), arg));
        caseExpr.setThenExprs(visit(caseExpr.getThenExprs(), arg));
        caseExpr.setElseExpr(visit(caseExpr.getElseExpr(), arg));
        return caseExpr;
    }

    @Override
    public Expression visit(InsertStatement insertStatement, ILangExpression arg) throws CompilationException {
        Expression returnExpr = insertStatement.getReturnExpression();
        if (returnExpr != null) {
            insertStatement.setReturnExpression(visit(returnExpr, arg));
        }
        Query bodyQuery = insertStatement.getQuery();
        bodyQuery.accept(this, arg);
        return null;
    }

    protected Expression visit(Expression expr, ILangExpression arg) throws CompilationException {
        return postVisit(preVisit(expr).accept(this, arg));
    }

    protected Expression preVisit(Expression expr) throws CompilationException {
        return expr;
    }

    protected Expression postVisit(Expression expr) throws CompilationException {
        return expr;
    }

    protected List<Expression> visit(List<Expression> exprs, ILangExpression arg) throws CompilationException {
        List<Expression> newExprList = new ArrayList<>(exprs.size());
        for (Expression expr : exprs) {
            newExprList.add(visit(expr, arg));
        }
        return newExprList;
    }
}
