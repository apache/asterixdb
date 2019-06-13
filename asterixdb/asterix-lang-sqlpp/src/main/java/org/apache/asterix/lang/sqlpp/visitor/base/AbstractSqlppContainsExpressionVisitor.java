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

/**
 * Base class for visitors that search for expressions having certain properties and return a boolean value
 * indicating whether such expressions were found or not, or {@code null} if search could not be performed.
 */
public abstract class AbstractSqlppContainsExpressionVisitor<T>
        extends AbstractSqlppQueryExpressionVisitor<Boolean, T> {

    @Override
    public Boolean visit(FromClause fromClause, T arg) throws CompilationException {
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            if (fromTerm.accept(this, arg)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visit(FromTerm fromTerm, T arg) throws CompilationException {
        if (visit(fromTerm.getLeftExpression(), arg)) {
            return true;
        }
        if (fromTerm.hasCorrelateClauses()) {
            for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
                if (correlateClause.accept(this, arg)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Boolean visit(JoinClause joinClause, T arg) throws CompilationException {
        return visit(joinClause.getRightExpression(), arg) || visit(joinClause.getConditionExpression(), arg);
    }

    @Override
    public Boolean visit(NestClause nestClause, T arg) throws CompilationException {
        return nestClause.accept(this, arg);
    }

    @Override
    public Boolean visit(Projection projection, T arg) throws CompilationException {
        return visit(projection.getExpression(), arg);
    }

    @Override
    public Boolean visit(SelectBlock selectBlock, T arg) throws CompilationException {
        return (selectBlock.hasFromClause() && visit(selectBlock.getFromClause(), arg))
                || (selectBlock.hasLetWhereClauses() && visitExprList(selectBlock.getLetWhereList(), arg))
                || (selectBlock.hasGroupbyClause() && visit(selectBlock.getGroupbyClause(), arg))
                || (selectBlock.hasLetHavingClausesAfterGroupby()
                        && visitExprList(selectBlock.getLetHavingListAfterGroupby(), arg))
                || visit(selectBlock.getSelectClause(), arg);
    }

    @Override
    public Boolean visit(SelectClause selectClause, T arg) throws CompilationException {
        return (selectClause.selectElement() && visit(selectClause.getSelectElement(), arg))
                || (selectClause.selectRegular() && visit(selectClause.getSelectRegular(), arg));
    }

    @Override
    public Boolean visit(SelectElement selectElement, T arg) throws CompilationException {
        return visit(selectElement.getExpression(), arg);
    }

    @Override
    public Boolean visit(SelectRegular selectRegular, T arg) throws CompilationException {
        return visitExprList(selectRegular.getProjections(), arg);
    }

    @Override
    public Boolean visit(SelectSetOperation selectSetOperation, T arg) throws CompilationException {
        if (selectSetOperation.getLeftInput().accept(this, arg)) {
            return true;
        }
        if (selectSetOperation.hasRightInputs()) {
            for (SetOperationRight right : selectSetOperation.getRightInputs()) {
                if (right.getSetOperationRightInput().accept(this, arg)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Boolean visit(SelectExpression selectStatement, T arg) throws CompilationException {
        return (selectStatement.hasLetClauses() && visitExprList(selectStatement.getLetList(), arg))
                || visit(selectStatement.getSelectSetOperation(), arg)
                || (selectStatement.hasOrderby() && visit(selectStatement.getOrderbyClause(), arg))
                || (selectStatement.hasLimit() && visit(selectStatement.getLimitClause(), arg));
    }

    @Override
    public Boolean visit(UnnestClause unnestClause, T arg) throws CompilationException {
        return visit(unnestClause.getRightExpression(), arg);
    }

    @Override
    public Boolean visit(HavingClause havingClause, T arg) throws CompilationException {
        return visit(havingClause.getFilterExpression(), arg);
    }

    @Override
    public Boolean visit(CaseExpression caseExpression, T arg) throws CompilationException {
        return visit(caseExpression.getConditionExpr(), arg) || visitExprList(caseExpression.getWhenExprs(), arg)
                || visitExprList(caseExpression.getThenExprs(), arg) || visit(caseExpression.getElseExpr(), arg);
    }

    @Override
    public Boolean visit(LiteralExpr l, T arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(VariableExpr v, T arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(ListConstructor lc, T arg) throws CompilationException {
        return visitExprList(lc.getExprList(), arg);
    }

    @Override
    public Boolean visit(RecordConstructor rc, T arg) throws CompilationException {
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
    public Boolean visit(OperatorExpr operatorExpr, T arg) throws CompilationException {
        return visitExprList(operatorExpr.getExprList(), arg);
    }

    @Override
    public Boolean visit(FieldAccessor fa, T arg) throws CompilationException {
        return visit(fa.getExpr(), arg);
    }

    @Override
    public Boolean visit(IndexAccessor ia, T arg) throws CompilationException {
        return visit(ia.getExpr(), arg) || visit(ia.getIndexExpr(), arg);
    }

    @Override
    public Boolean visit(ListSliceExpression expression, T arg) throws CompilationException {
        return visit(expression.getExpr(), arg) || visit(expression.getStartIndexExpression(), arg)
                || visit(expression.getEndIndexExpression(), arg);
    }

    @Override
    public Boolean visit(IfExpr ifexpr, T arg) throws CompilationException {
        return visit(ifexpr.getCondExpr(), arg) || visit(ifexpr.getThenExpr(), arg) || visit(ifexpr.getElseExpr(), arg);
    }

    @Override
    public Boolean visit(QuantifiedExpression qe, T arg) throws CompilationException {
        for (QuantifiedPair qf : qe.getQuantifiedList()) {
            if (visit(qf.getExpr(), arg)) {
                return true;
            }
        }
        return visit(qe.getSatisfiesExpr(), arg);
    }

    @Override
    public Boolean visit(LetClause lc, T arg) throws CompilationException {
        return visit(lc.getBindingExpr(), arg);
    }

    @Override
    public Boolean visit(WhereClause wc, T arg) throws CompilationException {
        return visit(wc.getWhereExpr(), arg);
    }

    @Override
    public Boolean visit(OrderbyClause oc, T arg) throws CompilationException {
        return visitExprList(oc.getOrderbyList(), arg);
    }

    @Override
    public Boolean visit(GroupbyClause gc, T arg) throws CompilationException {
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
    public Boolean visit(LimitClause lc, T arg) throws CompilationException {
        return visit(lc.getLimitExpr(), arg) || visit(lc.getOffset(), arg);
    }

    @Override
    public Boolean visit(UnaryExpr u, T arg) throws CompilationException {
        return visit(u.getExpr(), arg);
    }

    @Override
    public Boolean visit(WindowExpression winExpr, T arg) throws CompilationException {
        return (winExpr.hasPartitionList() && visitExprList(winExpr.getPartitionList(), arg))
                || (winExpr.hasOrderByList() && visitExprList(winExpr.getOrderbyList(), arg))
                || (winExpr.hasFrameStartExpr() && visit(winExpr.getFrameStartExpr(), arg))
                || (winExpr.hasFrameEndExpr() && visit(winExpr.getFrameEndExpr(), arg))
                || (winExpr.hasWindowFieldList() && visitFieldList(winExpr.getWindowFieldList(), arg))
                || visitExprList(winExpr.getExprList(), arg);
    }

    @Override
    public Boolean visit(CallExpr callExpr, T arg) throws CompilationException {
        return visitExprList(callExpr.getExprList(), arg);
    }

    private boolean visit(ILangExpression expr, T arg) throws CompilationException {
        return expr != null && expr.accept(this, arg);
    }

    private <E extends ILangExpression> boolean visitExprList(Collection<E> exprList, T arg)
            throws CompilationException {
        for (E langExpr : exprList) {
            if (visit(langExpr, arg)) {
                return true;
            }
        }
        return false;
    }

    private <E extends ILangExpression> boolean visitFieldList(Collection<Pair<E, Identifier>> fieldList, T arg)
            throws CompilationException {
        for (Pair<E, Identifier> p : fieldList) {
            if (visit(p.first, arg)) {
                return true;
            }
        }
        return false;
    }
}
