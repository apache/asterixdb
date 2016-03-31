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
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppQueryExpressionVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class UsedVariableVisitor extends AbstractSqlppQueryExpressionVisitor<Void, Collection<VariableExpr>> {

    @Override
    public Void visit(FromClause fromClause, Collection<VariableExpr> usedVars) throws AsterixException {
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fromTerm.accept(this, usedVars);
        }
        return null;
    }

    @Override
    public Void visit(FromTerm fromTerm, Collection<VariableExpr> usedVars) throws AsterixException {
        // Visit the left expression of a from term.
        fromTerm.getLeftExpression().accept(this, usedVars);

        // Visits join/unnest/nest clauses.
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            correlateClause.accept(this, usedVars);
        }
        return null;
    }

    @Override
    public Void visit(JoinClause joinClause, Collection<VariableExpr> usedVars) throws AsterixException {
        // NOTE: the two join branches cannot be correlated, instead of checking
        // the correlation here,
        // we defer the check to the query optimizer.
        joinClause.getRightExpression().accept(this, usedVars);

        // The condition expression can refer to the just registered variables
        // for the right branch.
        joinClause.getConditionExpression().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(NestClause nestClause, Collection<VariableExpr> usedVars) throws AsterixException {
        // NOTE: the two branches of a NEST cannot be correlated, instead of
        // checking the correlation here, we defer the check to the query
        // optimizer.
        nestClause.getRightExpression().accept(this, usedVars);

        // The condition expression can refer to the just registered variables
        // for the right branch.
        nestClause.getConditionExpression().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(UnnestClause unnestClause, Collection<VariableExpr> usedVars) throws AsterixException {
        unnestClause.getRightExpression().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(Projection projection, Collection<VariableExpr> usedVars) throws AsterixException {
        projection.getExpression().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(SelectBlock selectBlock, Collection<VariableExpr> usedVars) throws AsterixException {
        // Traverses the select block in the order of "from", "let"s, "where",
        // "group by", "let"s, "having" and "select".
        if (selectBlock.hasFromClause()) {
            selectBlock.getFromClause().accept(this, usedVars);
        }
        if (selectBlock.hasLetClauses()) {
            List<LetClause> letList = selectBlock.getLetList();
            for (LetClause letClause : letList) {
                letClause.accept(this, usedVars);
            }
        }
        if (selectBlock.hasWhereClause()) {
            selectBlock.getWhereClause().accept(this, usedVars);
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, usedVars);
            if (selectBlock.hasLetClausesAfterGroupby()) {
                List<LetClause> letListAfterGby = selectBlock.getLetListAfterGroupby();
                for (LetClause letClauseAfterGby : letListAfterGby) {
                    letClauseAfterGby.accept(this, usedVars);
                }
            }
            if (selectBlock.hasHavingClause()) {
                selectBlock.getHavingClause().accept(this, usedVars);
            }
        }
        selectBlock.getSelectClause().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(SelectClause selectClause, Collection<VariableExpr> usedVars) throws AsterixException {
        if (selectClause.selectElement()) {
            selectClause.getSelectElement().accept(this, usedVars);
        }
        if (selectClause.selectRegular()) {
            selectClause.getSelectRegular().accept(this, usedVars);
        }
        return null;
    }

    @Override
    public Void visit(SelectElement selectElement, Collection<VariableExpr> usedVars) throws AsterixException {
        selectElement.getExpression().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(SelectRegular selectRegular, Collection<VariableExpr> usedVars) throws AsterixException {
        for (Projection projection : selectRegular.getProjections()) {
            projection.accept(this, usedVars);
        }
        return null;
    }

    @Override
    public Void visit(SelectSetOperation selectSetOperation, Collection<VariableExpr> usedVars)
            throws AsterixException {
        selectSetOperation.getLeftInput().accept(this, usedVars);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            right.getSetOperationRightInput().accept(this, usedVars);
        }
        return null;
    }

    @Override
    public Void visit(HavingClause havingClause, Collection<VariableExpr> usedVars) throws AsterixException {
        havingClause.getFilterExpression().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(Query q, Collection<VariableExpr> usedVars) throws AsterixException {
        q.getBody().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(FunctionDecl fd, Collection<VariableExpr> usedVars) throws AsterixException {
        fd.getFuncBody().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(WhereClause whereClause, Collection<VariableExpr> usedVars) throws AsterixException {
        whereClause.getWhereExpr().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(OrderbyClause oc, Collection<VariableExpr> usedVars) throws AsterixException {
        for (Expression orderExpr : oc.getOrderbyList()) {
            orderExpr.accept(this, usedVars);
        }
        return null;
    }

    @Override
    public Void visit(GroupbyClause gc, Collection<VariableExpr> usedVars) throws AsterixException {
        // Puts all group-by variables into the symbol set of the new scope.
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            gbyVarExpr.getExpr().accept(this, usedVars);
        }
        for (GbyVariableExpressionPair decorVarExpr : gc.getDecorPairList()) {
            decorVarExpr.getExpr().accept(this, usedVars);
        }
        if (gc.hasGroupFieldList()) {
            for (Pair<Expression, Identifier> groupField : gc.getGroupFieldList()) {
                groupField.first.accept(this, usedVars);
            }
        }
        return null;
    }

    @Override
    public Void visit(LimitClause limitClause, Collection<VariableExpr> usedVars) throws AsterixException {
        limitClause.getLimitExpr().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(LetClause letClause, Collection<VariableExpr> usedVars) throws AsterixException {
        letClause.getBindingExpr().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(SelectExpression selectExpression, Collection<VariableExpr> usedVars) throws AsterixException {
        // visit let list
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                letClause.accept(this, usedVars);
            }
        }

        // visit the main select.
        selectExpression.getSelectSetOperation().accept(this, usedVars);

        // visit order by
        if (selectExpression.hasOrderby()) {
            for (Expression orderExpr : selectExpression.getOrderbyClause().getOrderbyList()) {
                orderExpr.accept(this, usedVars);
            }
        }

        // visit limit
        if (selectExpression.hasLimit()) {
            selectExpression.getLimitClause().accept(this, usedVars);
        }
        return null;
    }

    @Override
    public Void visit(LiteralExpr l, Collection<VariableExpr> usedVars) throws AsterixException {
        return null;
    }

    @Override
    public Void visit(ListConstructor lc, Collection<VariableExpr> usedVars) throws AsterixException {
        for (Expression expr : lc.getExprList()) {
            expr.accept(this, usedVars);
        }
        return null;
    }

    @Override
    public Void visit(RecordConstructor rc, Collection<VariableExpr> usedVars) throws AsterixException {
        for (FieldBinding binding : rc.getFbList()) {
            binding.getLeftExpr().accept(this, usedVars);
            binding.getRightExpr().accept(this, usedVars);
        }
        return null;
    }

    @Override
    public Void visit(OperatorExpr operatorExpr, Collection<VariableExpr> usedVars) throws AsterixException {
        for (Expression expr : operatorExpr.getExprList()) {
            expr.accept(this, usedVars);
        }
        return null;
    }

    @Override
    public Void visit(IfExpr ifExpr, Collection<VariableExpr> usedVars) throws AsterixException {
        ifExpr.getCondExpr().accept(this, usedVars);
        ifExpr.getThenExpr().accept(this, usedVars);
        ifExpr.getElseExpr().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(QuantifiedExpression qe, Collection<VariableExpr> usedVars) throws AsterixException {
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            pair.getExpr().accept(this, usedVars);
        }
        qe.getSatisfiesExpr().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(CallExpr callExpr, Collection<VariableExpr> usedVars) throws AsterixException {
        for (Expression expr : callExpr.getExprList()) {
            expr.accept(this, usedVars);
        }
        return null;
    }

    @Override
    public Void visit(VariableExpr varExpr, Collection<VariableExpr> usedVars) throws AsterixException {
        usedVars.add(varExpr);
        return null;
    }

    @Override
    public Void visit(UnaryExpr u, Collection<VariableExpr> usedVars) throws AsterixException {
        u.getExpr().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(FieldAccessor fa, Collection<VariableExpr> usedVars) throws AsterixException {
        fa.getExpr().accept(this, usedVars);
        return null;
    }

    @Override
    public Void visit(IndexAccessor ia, Collection<VariableExpr> usedVars) throws AsterixException {
        ia.getExpr().accept(this, usedVars);
        if (ia.getIndexExpr() != null) {
            ia.getIndexExpr();
        }
        return null;
    }

}
