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

import org.apache.asterix.common.exceptions.CompilationException;
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
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppQueryExpressionVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * This visitor recursively checks if there is a subquery in the argument language construct.
 */
public class CheckSubqueryVisitor extends AbstractSqlppQueryExpressionVisitor<Boolean, ILangExpression> {

    @Override
    public Boolean visit(FromClause fromClause, ILangExpression arg) throws CompilationException {
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            if (fromTerm.accept(this, arg)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean visit(FromTerm fromTerm, ILangExpression arg) throws CompilationException {
        if (visit(fromTerm.getLeftExpression(), arg)) {
            return true;
        }
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            if (correlateClause.accept(this, arg)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visit(JoinClause joinClause, ILangExpression arg) throws CompilationException {
        return visit(joinClause.getRightExpression(), arg) || visit(joinClause.getConditionExpression(), arg);
    }

    @Override
    public Boolean visit(NestClause nestClause, ILangExpression arg) throws CompilationException {
        return nestClause.accept(this, arg);
    }

    @Override
    public Boolean visit(Projection projection, ILangExpression arg) throws CompilationException {
        if (projection.star()) {
            return false;
        }
        return visit(projection.getExpression(), arg);
    }

    @Override
    public Boolean visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        return visit(selectBlock.getFromClause(), arg) || visit(selectBlock.getGroupbyClause(), arg)
                || visit(selectBlock.getSelectClause(), arg) || visitExprList(selectBlock.getLetWhereList(), arg)
                || visitExprList(selectBlock.getLetHavingListAfterGroupby(), arg);
    }

    @Override
    public Boolean visit(SelectClause selectClause, ILangExpression arg) throws CompilationException {
        return visit(selectClause.getSelectElement(), arg) || visit(selectClause.getSelectRegular(), arg);
    }

    @Override
    public Boolean visit(SelectElement selectElement, ILangExpression arg) throws CompilationException {
        return visit(selectElement.getExpression(), arg);
    }

    @Override
    public Boolean visit(SelectRegular selectRegular, ILangExpression arg) throws CompilationException {
        return visitExprList(selectRegular.getProjections(), arg);
    }

    @Override
    public Boolean visit(SelectSetOperation selectSetOperation, ILangExpression arg) throws CompilationException {
        if (selectSetOperation.getLeftInput().accept(this, arg)) {
            return true;
        }
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            if (right.getSetOperationRightInput().accept(this, arg)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visit(SelectExpression selectStatement, ILangExpression arg) throws CompilationException {
        if (selectStatement.isSubquery()) {
            return true;
        }
        return visitExprList(selectStatement.getLetList(), arg) || visit(selectStatement.getSelectSetOperation(), arg)
                || visit(selectStatement.getOrderbyClause(), arg) || visit(selectStatement.getLimitClause(), arg);

    }

    @Override
    public Boolean visit(UnnestClause unnestClause, ILangExpression arg) throws CompilationException {
        return visit(unnestClause.getRightExpression(), arg);
    }

    @Override
    public Boolean visit(HavingClause havingClause, ILangExpression arg) throws CompilationException {
        return visit(havingClause.getFilterExpression(), arg);
    }

    @Override
    public Boolean visit(CaseExpression caseExpression, ILangExpression arg) throws CompilationException {
        return visit(caseExpression.getConditionExpr(), arg) || visitExprList(caseExpression.getWhenExprs(), arg)
                || visitExprList(caseExpression.getThenExprs(), arg) || visit(caseExpression.getElseExpr(), arg);
    }

    @Override
    public Boolean visit(Query q, ILangExpression arg) throws CompilationException {
        return visit(q.getBody(), arg);
    }

    @Override
    public Boolean visit(FunctionDecl fd, ILangExpression arg) throws CompilationException {
        return fd.getFuncBody().accept(this, arg);
    }

    @Override
    public Boolean visit(LiteralExpr l, ILangExpression arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(VariableExpr v, ILangExpression arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(ListConstructor lc, ILangExpression arg) throws CompilationException {
        return visitExprList(lc.getExprList(), arg);
    }

    @Override
    public Boolean visit(RecordConstructor rc, ILangExpression arg) throws CompilationException {
        for (FieldBinding fb : rc.getFbList()) {
            if (visit(fb.getLeftExpr(), arg)) {
                return true;
            }
            if (visit(fb.getRightExpr(), arg)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visit(OperatorExpr operatorExpr, ILangExpression arg) throws CompilationException {
        return visitExprList(operatorExpr.getExprList(), arg);
    }

    @Override
    public Boolean visit(FieldAccessor fa, ILangExpression arg) throws CompilationException {
        return visit(fa.getExpr(), arg);
    }

    @Override
    public Boolean visit(IndexAccessor ia, ILangExpression arg) throws CompilationException {
        return visit(ia.getExpr(), arg) || visit(ia.getIndexExpr(), arg);
    }

    @Override
    public Boolean visit(ListSliceExpression expression, ILangExpression arg) throws CompilationException {
        return visit(expression.getExpr(), arg) || visit(expression.getStartIndexExpression(), arg)
                || visit(expression.getEndIndexExpression(), arg);
    }

    @Override
    public Boolean visit(IfExpr ifexpr, ILangExpression arg) throws CompilationException {
        return visit(ifexpr.getCondExpr(), arg) || visit(ifexpr.getThenExpr(), arg) || visit(ifexpr.getElseExpr(), arg);
    }

    @Override
    public Boolean visit(QuantifiedExpression qe, ILangExpression arg) throws CompilationException {
        for (QuantifiedPair qf : qe.getQuantifiedList()) {
            if (visit(qf.getExpr(), arg)) {
                return true;
            }
        }
        return visit(qe.getSatisfiesExpr(), arg);
    }

    @Override
    public Boolean visit(LetClause lc, ILangExpression arg) throws CompilationException {
        return visit(lc.getBindingExpr(), arg);
    }

    @Override
    public Boolean visit(WhereClause wc, ILangExpression arg) throws CompilationException {
        return visit(wc.getWhereExpr(), arg);
    }

    @Override
    public Boolean visit(OrderbyClause oc, ILangExpression arg) throws CompilationException {
        return visitExprList(oc.getOrderbyList(), arg);
    }

    @Override
    public Boolean visit(GroupbyClause gc, ILangExpression arg) throws CompilationException {
        for (GbyVariableExpressionPair key : gc.getGbyPairList()) {
            if (visit(key.getExpr(), arg)) {
                return true;
            }
        }
        if (gc.hasDecorList()) {
            for (GbyVariableExpressionPair key : gc.getDecorPairList()) {
                if (visit(key.getExpr(), arg)) {
                    return true;
                }
            }
        }
        if (gc.hasGroupFieldList() && visitFieldList(gc.getGroupFieldList(), arg)) {
            return true;
        }
        if (gc.hasWithMap() && visitExprList(gc.getWithVarMap().keySet(), arg)) {
            return true;
        }
        return false;
    }

    @Override
    public Boolean visit(LimitClause lc, ILangExpression arg) throws CompilationException {
        return visit(lc.getLimitExpr(), arg) || visit(lc.getOffset(), arg);
    }

    @Override
    public Boolean visit(UnaryExpr u, ILangExpression arg) throws CompilationException {
        return visit(u.getExpr(), arg);
    }

    @Override
    public Boolean visit(WindowExpression winExpr, ILangExpression arg) throws CompilationException {
        return (winExpr.hasPartitionList() && visitExprList(winExpr.getPartitionList(), arg))
                || (winExpr.hasOrderByList() && visitExprList(winExpr.getOrderbyList(), arg))
                || (winExpr.hasFrameStartExpr() && visit(winExpr.getFrameStartExpr(), arg))
                || (winExpr.hasFrameEndExpr() && visit(winExpr.getFrameEndExpr(), arg))
                || (winExpr.hasWindowFieldList() && visitFieldList(winExpr.getWindowFieldList(), arg))
                || visitExprList(winExpr.getExprList(), arg);
    }

    @Override
    public Boolean visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        return visitExprList(callExpr.getExprList(), arg);
    }

    private boolean visit(ILangExpression expr, ILangExpression arg) throws CompilationException {
        if (expr == null) {
            return false;
        }
        return expr.accept(this, arg);
    }

    private <T extends ILangExpression> boolean visitExprList(Collection<T> exprList, ILangExpression arg)
            throws CompilationException {
        for (T langExpr : exprList) {
            if (visit(langExpr, arg)) {
                return true;
            }
        }
        return false;
    }

    private <T extends ILangExpression> boolean visitFieldList(Collection<Pair<T, Identifier>> fieldList,
            ILangExpression arg) throws CompilationException {
        for (Pair<T, Identifier> p : fieldList) {
            if (visit(p.first, arg)) {
                return true;
            }
        }
        return false;
    }
}
