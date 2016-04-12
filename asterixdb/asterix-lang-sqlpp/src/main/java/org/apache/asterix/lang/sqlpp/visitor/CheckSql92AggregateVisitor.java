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

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
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
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppQueryExpressionVisitor;

/**
 * This visitor checks if a language construct contains SQL-92 aggregates.
 */
public class CheckSql92AggregateVisitor extends AbstractSqlppQueryExpressionVisitor<Boolean, ILangExpression> {

    @Override
    public Boolean visit(Query q, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(FunctionDecl fd, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(LiteralExpr l, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(VariableExpr v, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(ListConstructor lc, ILangExpression parentSelectBlock) throws AsterixException {
        return visitExprList(lc.getExprList(), parentSelectBlock);
    }

    @Override
    public Boolean visit(RecordConstructor rc, ILangExpression parentSelectBlock) throws AsterixException {
        for (FieldBinding fieldBinding : rc.getFbList()) {
            ILangExpression leftExpr = fieldBinding.getLeftExpr();
            ILangExpression rightExpr = fieldBinding.getRightExpr();
            if (leftExpr.accept(this, parentSelectBlock)) {
                return true;
            }
            if (rightExpr.accept(this, parentSelectBlock)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visit(OperatorExpr ifbo, ILangExpression parentSelectBlock) throws AsterixException {
        return visitExprList(ifbo.getExprList(), parentSelectBlock);
    }

    @Override
    public Boolean visit(FieldAccessor fa, ILangExpression parentSelectBlock) throws AsterixException {
        return fa.getExpr().accept(this, parentSelectBlock);
    }

    @Override
    public Boolean visit(IndexAccessor ia, ILangExpression parentSelectBlock) throws AsterixException {
        return ia.getExpr().accept(this, parentSelectBlock);
    }

    @Override
    public Boolean visit(IfExpr ifexpr, ILangExpression parentSelectBlock) throws AsterixException {
        if (ifexpr.getCondExpr().accept(this, parentSelectBlock)) {
            return true;
        } else {
            return ifexpr.getThenExpr().accept(this, parentSelectBlock)
                    || ifexpr.getElseExpr().accept(this, parentSelectBlock);
        }
    }

    @Override
    public Boolean visit(QuantifiedExpression qe, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(UnaryExpr u, ILangExpression parentSelectBlock) throws AsterixException {
        return u.getExpr().accept(this, parentSelectBlock);
    }

    @Override
    public Boolean visit(CallExpr pf, ILangExpression parentSelectBlock) throws AsterixException {
        FunctionSignature fs = pf.getFunctionSignature();
        if (FunctionMapUtil.isSql92AggregateFunction(fs)) {
            return true;
        }
        for (Expression parameter : pf.getExprList()) {
            if (parameter.accept(this, parentSelectBlock)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visit(LetClause lc, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(WhereClause wc, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(OrderbyClause oc, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(GroupbyClause gc, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(LimitClause lc, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(FromClause fromClause, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(FromTerm fromTerm, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(JoinClause joinClause, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(NestClause nestClause, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(Projection projection, ILangExpression parentSelectBlock) throws AsterixException {
        return projection.getExpression().accept(this, parentSelectBlock);
    }

    @Override
    public Boolean visit(SelectBlock selectBlock, ILangExpression parentSelectBlock) throws AsterixException {
        return selectBlock.getSelectClause().accept(this, selectBlock);
    }

    @Override
    public Boolean visit(SelectClause selectClause, ILangExpression parentSelectBlock) throws AsterixException {
        if (selectClause.selectElement()) {
            return selectClause.getSelectElement().accept(this, parentSelectBlock);
        } else {
            return selectClause.getSelectRegular().accept(this, parentSelectBlock);
        }
    }

    @Override
    public Boolean visit(SelectElement selectElement, ILangExpression parentSelectBlock) throws AsterixException {
        return selectElement.getExpression().accept(this, parentSelectBlock);
    }

    @Override
    public Boolean visit(SelectRegular selectRegular, ILangExpression parentSelectBlock) throws AsterixException {
        for (Projection projection : selectRegular.getProjections()) {
            if (projection.accept(this, parentSelectBlock)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visit(SelectSetOperation selectSetOperation, ILangExpression parentSelectBlock)
            throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(SelectExpression selectStatement, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(UnnestClause unnestClause, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(HavingClause havingClause, ILangExpression parentSelectBlock) throws AsterixException {
        return false;
    }

    private Boolean visitExprList(List<Expression> exprs, ILangExpression parentSelectBlock) throws AsterixException {
        for (Expression item : exprs) {
            if (item.accept(this, parentSelectBlock)) {
                return true;
            }
        }
        return false;
    }

}
