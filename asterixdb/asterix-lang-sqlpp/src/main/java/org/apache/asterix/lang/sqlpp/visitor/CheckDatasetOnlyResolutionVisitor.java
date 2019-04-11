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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
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
import org.apache.asterix.lang.common.struct.QuantifiedPair;
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
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppQueryExpressionVisitor;

/**
 * This class checks whether a reference to an undefined identifier (the second parameter of the visit method)
 * that is directly enclosed in the first parameter of the visit method should only be resolved to a dataset.
 */
public final class CheckDatasetOnlyResolutionVisitor
        extends AbstractSqlppQueryExpressionVisitor<Boolean, VariableExpr> {

    public static final CheckDatasetOnlyResolutionVisitor INSTANCE = new CheckDatasetOnlyResolutionVisitor();

    private CheckDatasetOnlyResolutionVisitor() {
    }

    @Override
    public Boolean visit(FromTerm fromTerm, VariableExpr arg) throws CompilationException {
        return contains(fromTerm.getLeftExpression(), arg);
    }

    @Override
    public Boolean visit(JoinClause joinClause, VariableExpr arg) throws CompilationException {
        return contains(joinClause.getRightExpression(), arg);
    }

    @Override
    public Boolean visit(NestClause nestClause, VariableExpr arg) throws CompilationException {
        return contains(nestClause.getRightExpression(), arg);
    }

    @Override
    public Boolean visit(UnnestClause unnestClause, VariableExpr arg) throws CompilationException {
        return contains(unnestClause.getRightExpression(), arg);
    }

    @Override
    public Boolean visit(QuantifiedExpression qe, VariableExpr arg) throws CompilationException {
        for (QuantifiedPair qp : qe.getQuantifiedList()) {
            // If the target reference of undefined variable is a binding expression in a quantified pair,
            // then we only resolve it to dataset.
            if (contains(qp.getExpr(), arg)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visit(Query q, VariableExpr arg) throws CompilationException {
        return contains(q, arg);
    }

    @Override
    public Boolean visit(InsertStatement insert, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(FunctionDecl fd, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(LiteralExpr l, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(VariableExpr v, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(ListConstructor lc, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(RecordConstructor rc, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(OperatorExpr ifbo, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(FieldAccessor fa, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(IndexAccessor ia, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(ListSliceExpression expression, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(IfExpr ifexpr, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(UnaryExpr u, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(CallExpr pf, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(LetClause lc, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(WhereClause wc, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(OrderbyClause oc, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(GroupbyClause gc, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(LimitClause lc, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(FromClause fromClause, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(Projection projection, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(SelectBlock selectBlock, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(SelectClause selectClause, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(SelectElement selectElement, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(SelectRegular selectRegular, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(SelectSetOperation selectSetOperation, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(SelectExpression selectStatement, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(HavingClause havingClause, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(CaseExpression caseExpr, VariableExpr arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(WindowExpression windowExpression, VariableExpr arg) throws CompilationException {
        return false;
    }

    private boolean contains(ILangExpression expr, VariableExpr var) throws CompilationException {
        return SqlppVariableUtil.getFreeVariables(expr).contains(var);
    }
}
