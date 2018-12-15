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
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.FunctionDecl;
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
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppQueryExpressionVisitor;

/**
 * This class checks whether a reference to an undefined identifier (the second parameter of the visit method)
 * that is directly enclosed in the first parameter of the visit method should only be resolved to a dataset.
 */
public class CheckDatasetOnlyResolutionVisitor extends AbstractSqlppQueryExpressionVisitor<Boolean, ILangExpression> {

    @Override
    public Boolean visit(Query q, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(FunctionDecl fd, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(LiteralExpr l, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(VariableExpr v, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(ListConstructor lc, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(RecordConstructor rc, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(OperatorExpr ifbo, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(FieldAccessor fa, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(IndexAccessor ia, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(ListSliceExpression expression, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(IfExpr ifexpr, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(QuantifiedExpression qe, ILangExpression expr) throws CompilationException {
        for (QuantifiedPair qp : qe.getQuantifiedList()) {
            // If the target reference of undefined variable is a binding expression in a quantified pair,
            // then we only resolve it to dataset.
            if (expr == qp.getExpr()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Boolean visit(UnaryExpr u, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(CallExpr pf, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(LetClause lc, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(WhereClause wc, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(OrderbyClause oc, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(GroupbyClause gc, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(LimitClause lc, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(FromClause fromClause, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(FromTerm fromTerm, ILangExpression expr) throws CompilationException {
        return expr == fromTerm.getLeftExpression();
    }

    @Override
    public Boolean visit(JoinClause joinClause, ILangExpression expr) throws CompilationException {
        return expr == joinClause.getRightExpression();
    }

    @Override
    public Boolean visit(NestClause nestClause, ILangExpression expr) throws CompilationException {
        return expr == nestClause.getRightExpression();
    }

    @Override
    public Boolean visit(Projection projection, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(SelectBlock selectBlock, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(SelectClause selectClause, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(SelectElement selectElement, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(SelectRegular selectRegular, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(SelectSetOperation selectSetOperation, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(SelectExpression selectStatement, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(UnnestClause unnestClause, ILangExpression expr) throws CompilationException {
        return expr == unnestClause.getRightExpression();
    }

    @Override
    public Boolean visit(HavingClause havingClause, ILangExpression expr) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(CaseExpression caseExpr, ILangExpression arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(WindowExpression windowExpression, ILangExpression arg) throws CompilationException {
        return false;
    }
}
