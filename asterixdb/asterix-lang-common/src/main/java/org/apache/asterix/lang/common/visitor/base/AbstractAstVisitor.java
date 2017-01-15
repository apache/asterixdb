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
package org.apache.asterix.lang.common.visitor.base;

import org.apache.asterix.common.exceptions.CompilationException;
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
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;

public abstract class AbstractAstVisitor<R, T> extends AbstractQueryExpressionVisitor<R, T> {

    @Override
    public R visit(Query q, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(FunctionDecl fd, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(LiteralExpr l, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(VariableExpr v, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(ListConstructor lc, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(RecordConstructor rc, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(OperatorExpr ifbo, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(FieldAccessor fa, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(IndexAccessor ia, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(IfExpr ifexpr, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(QuantifiedExpression qe, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(LetClause lc, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(WhereClause wc, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(OrderbyClause oc, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(GroupbyClause gc, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(LimitClause lc, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(UnaryExpr u, T arg) throws CompilationException {
        return null;
    }

    @Override
    public R visit(CallExpr pf, T arg) throws CompilationException {
        return null;
    }

}
