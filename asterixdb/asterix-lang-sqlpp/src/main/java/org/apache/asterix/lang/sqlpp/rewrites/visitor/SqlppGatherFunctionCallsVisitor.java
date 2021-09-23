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

import java.util.Collection;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.GatherFunctionCallsVisitor;
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
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public final class SqlppGatherFunctionCallsVisitor extends GatherFunctionCallsVisitor
        implements ISqlppVisitor<Void, Void> {

    public SqlppGatherFunctionCallsVisitor(Collection<? super AbstractCallExpression> calls) {
        super(calls);
    }

    @Override
    public Void visit(FromClause fromClause, Void arg) throws CompilationException {
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fromTerm.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(FromTerm fromTerm, Void arg) throws CompilationException {
        fromTerm.getLeftExpression().accept(this, arg);
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            correlateClause.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(JoinClause joinClause, Void arg) throws CompilationException {
        joinClause.getRightExpression().accept(this, arg);
        joinClause.getConditionExpression().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(NestClause nestClause, Void arg) throws CompilationException {
        nestClause.getRightExpression().accept(this, arg);
        nestClause.getConditionExpression().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(Projection projection, Void arg) throws CompilationException {
        return projection.hasExpression() ? projection.getExpression().accept(this, arg) : null;
    }

    @Override
    public Void visit(SelectBlock selectBlock, Void arg) throws CompilationException {
        if (selectBlock.hasFromClause()) {
            selectBlock.getFromClause().accept(this, arg);
        }
        if (selectBlock.hasLetWhereClauses()) {
            for (AbstractClause letWhereClause : selectBlock.getLetWhereList()) {
                letWhereClause.accept(this, arg);
            }
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, arg);
        }
        if (selectBlock.hasLetHavingClausesAfterGroupby()) {
            for (AbstractClause letHavingClause : selectBlock.getLetHavingListAfterGroupby()) {
                letHavingClause.accept(this, arg);
            }
        }
        selectBlock.getSelectClause().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(SelectClause selectClause, Void arg) throws CompilationException {
        if (selectClause.selectElement()) {
            selectClause.getSelectElement().accept(this, arg);
        } else {
            selectClause.getSelectRegular().accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(SelectElement selectElement, Void arg) throws CompilationException {
        selectElement.getExpression().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(SelectRegular selectRegular, Void arg) throws CompilationException {
        for (Projection projection : selectRegular.getProjections()) {
            projection.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(SelectSetOperation selectSetOperation, Void arg) throws CompilationException {
        selectSetOperation.getLeftInput().accept(this, arg);
        for (SetOperationRight setOperationRight : selectSetOperation.getRightInputs()) {
            setOperationRight.getSetOperationRightInput().accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(SelectExpression selectStatement, Void arg) throws CompilationException {
        if (selectStatement.hasLetClauses()) {
            for (LetClause letClause : selectStatement.getLetList()) {
                letClause.accept(this, arg);
            }
        }
        selectStatement.getSelectSetOperation().accept(this, arg);
        if (selectStatement.hasOrderby()) {
            selectStatement.getOrderbyClause().accept(this, arg);
        }
        if (selectStatement.hasLimit()) {
            selectStatement.getLimitClause().accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(UnnestClause unnestClause, Void arg) throws CompilationException {
        unnestClause.getRightExpression().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(HavingClause havingClause, Void arg) throws CompilationException {
        havingClause.getFilterExpression().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(CaseExpression caseExpression, Void arg) throws CompilationException {
        caseExpression.getConditionExpr().accept(this, arg);
        for (Expression expr : caseExpression.getWhenExprs()) {
            expr.accept(this, arg);
        }
        for (Expression expr : caseExpression.getThenExprs()) {
            expr.accept(this, arg);
        }
        caseExpression.getElseExpr().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(WindowExpression winExpr, Void arg) throws CompilationException {
        calls.add(winExpr);
        if (winExpr.hasPartitionList()) {
            for (Expression expr : winExpr.getPartitionList()) {
                expr.accept(this, arg);
            }
        }
        if (winExpr.hasOrderByList()) {
            for (Expression expr : winExpr.getOrderbyList()) {
                expr.accept(this, arg);
            }
        }
        if (winExpr.hasFrameStartExpr()) {
            winExpr.getFrameStartExpr().accept(this, arg);
        }
        if (winExpr.hasFrameEndExpr()) {
            winExpr.getFrameEndExpr().accept(this, arg);
        }
        if (winExpr.hasWindowFieldList()) {
            for (Pair<Expression, Identifier> p : winExpr.getWindowFieldList()) {
                p.first.accept(this, arg);
            }
        }
        if (winExpr.hasAggregateFilterExpr()) {
            winExpr.getAggregateFilterExpr().accept(this, arg);
        }
        for (Expression expr : winExpr.getExprList()) {
            expr.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(ListSliceExpression expression, Void arg) throws CompilationException {
        expression.getExpr().accept(this, arg);
        expression.getStartIndexExpression().accept(this, arg);

        if (expression.hasEndExpression()) {
            expression.getEndIndexExpression().accept(this, arg);
        }
        return null;
    }
}
