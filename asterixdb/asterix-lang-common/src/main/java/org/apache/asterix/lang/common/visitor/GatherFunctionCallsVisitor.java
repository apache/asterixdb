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

package org.apache.asterix.lang.common.visitor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
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
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.OrderedListTypeDefinition;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.TypeReferenceExpression;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.visitor.base.AbstractQueryExpressionVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class GatherFunctionCallsVisitor extends AbstractQueryExpressionVisitor<Void, Void> {

    protected final Set<CallExpr> calls = new HashSet<>();

    @Override
    public Void visit(CallExpr pf, Void arg) throws CompilationException {
        calls.add(pf);
        for (Expression e : pf.getExprList()) {
            e.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(FieldAccessor fa, Void arg) throws CompilationException {
        fa.getExpr().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(GroupbyClause gc, Void arg) throws CompilationException {
        for (GbyVariableExpressionPair p : gc.getGbyPairList()) {
            p.getExpr().accept(this, arg);
        }
        if (gc.hasDecorList()) {
            for (GbyVariableExpressionPair p : gc.getDecorPairList()) {
                p.getExpr().accept(this, arg);
            }
        }
        if (gc.hasGroupFieldList()) {
            for (Pair<Expression, Identifier> p : gc.getGroupFieldList()) {
                p.first.accept(this, arg);
            }
        }
        if (gc.hasWithMap()) {
            for (Map.Entry<Expression, VariableExpr> me : gc.getWithVarMap().entrySet()) {
                me.getKey().accept(this, arg);
            }
        }
        return null;
    }

    @Override
    public Void visit(IfExpr ifexpr, Void arg) throws CompilationException {
        ifexpr.getCondExpr().accept(this, arg);
        ifexpr.getThenExpr().accept(this, arg);
        ifexpr.getElseExpr().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(IndexAccessor ia, Void arg) throws CompilationException {
        ia.getExpr().accept(this, arg);

        if (!ia.isAny()) {
            ia.getIndexExpr().accept(this, arg);
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

    @Override
    public Void visit(LetClause lc, Void arg) throws CompilationException {
        lc.getBindingExpr().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(LimitClause lc, Void arg) throws CompilationException {
        lc.getLimitExpr().accept(this, arg);
        if (lc.getOffset() != null) {
            lc.getOffset().accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(ListConstructor lc, Void arg) throws CompilationException {
        for (Expression e : lc.getExprList()) {
            e.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(LiteralExpr l, Void arg) throws CompilationException {
        // do nothing
        return null;
    }

    @Override
    public Void visit(OperatorExpr op, Void arg) throws CompilationException {
        for (Expression e : op.getExprList()) {
            e.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(OrderbyClause oc, Void arg) throws CompilationException {
        for (Expression e : oc.getOrderbyList()) {
            e.accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(OrderedListTypeDefinition olte, Void arg) throws CompilationException {
        return null;
    }

    @Override
    public Void visit(QuantifiedExpression qe, Void arg) throws CompilationException {
        for (QuantifiedPair qp : qe.getQuantifiedList()) {
            qp.getExpr().accept(this, arg);
        }
        qe.getSatisfiesExpr().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(Query q, Void arg) throws CompilationException {
        q.getBody().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(RecordConstructor rc, Void arg) throws CompilationException {
        for (FieldBinding fb : rc.getFbList()) {
            fb.getLeftExpr().accept(this, arg);
            fb.getRightExpr().accept(this, arg);
        }
        return null;
    }

    @Override
    public Void visit(TypeReferenceExpression tre, Void arg) throws CompilationException {
        return null;
    }

    @Override
    public Void visit(UnaryExpr u, Void arg) throws CompilationException {
        u.getExpr().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(VariableExpr v, Void arg) throws CompilationException {
        // do nothing
        return null;
    }

    @Override
    public Void visit(WhereClause wc, Void arg) throws CompilationException {
        wc.getWhereExpr().accept(this, arg);
        return null;
    }

    @Override
    public Void visit(InsertStatement wc, Void arg) throws CompilationException {
        wc.getQuery().accept(this, arg);
        Expression returnExpression = wc.getReturnExpression();
        if (returnExpression != null) {
            returnExpression.accept(this, arg);
        }
        return null;
    }

    public Set<CallExpr> getCalls() {
        return calls;
    }

    @Override
    public Void visit(FunctionDecl fd, Void arg) throws CompilationException {
        return null;
    }

}
