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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.base.IRewriterFactory;
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
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.visitor.base.AbstractQueryExpressionVisitor;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.hyracks.algebricks.common.utils.Pair;

public abstract class AbstractInlineUdfsVisitor extends AbstractQueryExpressionVisitor<Boolean, List<FunctionDecl>> {

    protected final LangRewritingContext context;
    protected final CloneAndSubstituteVariablesVisitor cloneVisitor;
    private final IRewriterFactory rewriterFactory;
    private final List<FunctionDecl> declaredFunctions;
    private final AqlMetadataProvider metadataProvider;

    public AbstractInlineUdfsVisitor(LangRewritingContext context, IRewriterFactory rewriterFactory,
            List<FunctionDecl> declaredFunctions, AqlMetadataProvider metadataProvider,
            CloneAndSubstituteVariablesVisitor cloneVisitor) {
        this.context = context;
        this.cloneVisitor = cloneVisitor;
        this.rewriterFactory = rewriterFactory;
        this.declaredFunctions = declaredFunctions;
        this.metadataProvider = metadataProvider;
    }

    /**
     * @param letClauses
     *            , a list of let-binding clauses
     * @param returnExpr
     *            , a return expression
     * @return a query expression which is upto a specific langauge, e.g., FLWOGR in AQL and expression query in SQL++.
     */
    protected abstract Expression generateQueryExpression(List<LetClause> letClauses, Expression returnExpr)
            throws AsterixException;

    @Override
    public Boolean visit(Query q, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(q.getBody(), arg);
        q.setBody(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(FunctionDecl fd, List<FunctionDecl> arg) throws AsterixException {
        // Careful, we should only do this after analyzing the graph of function
        // calls.
        Pair<Boolean, Expression> p = inlineUdfsInExpr(fd.getFuncBody(), arg);
        fd.setFuncBody(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(ListConstructor lc, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, ArrayList<Expression>> p = inlineUdfsInExprList(lc.getExprList(), arg);
        lc.setExprList(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(RecordConstructor rc, List<FunctionDecl> arg) throws AsterixException {
        boolean changed = false;
        for (FieldBinding b : rc.getFbList()) {
            Pair<Boolean, Expression> leftExprInlined = inlineUdfsInExpr(b.getLeftExpr(), arg);
            b.setLeftExpr(leftExprInlined.second);
            changed = changed | leftExprInlined.first;
            Pair<Boolean, Expression> rightExprInlined = inlineUdfsInExpr(b.getRightExpr(), arg);
            b.setRightExpr(rightExprInlined.second);
            changed = changed | rightExprInlined.first;
        }
        return changed;
    }

    @Override
    public Boolean visit(CallExpr pf, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, ArrayList<Expression>> p = inlineUdfsInExprList(pf.getExprList(), arg);
        pf.setExprList(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(OperatorExpr ifbo, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, ArrayList<Expression>> p = inlineUdfsInExprList(ifbo.getExprList(), arg);
        ifbo.setExprList(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(FieldAccessor fa, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(fa.getExpr(), arg);
        fa.setExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(IndexAccessor fa, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(fa.getExpr(), arg);
        fa.setExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(IfExpr ifexpr, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p1 = inlineUdfsInExpr(ifexpr.getCondExpr(), arg);
        ifexpr.setCondExpr(p1.second);
        Pair<Boolean, Expression> p2 = inlineUdfsInExpr(ifexpr.getThenExpr(), arg);
        ifexpr.setThenExpr(p2.second);
        Pair<Boolean, Expression> p3 = inlineUdfsInExpr(ifexpr.getElseExpr(), arg);
        ifexpr.setElseExpr(p3.second);
        return p1.first || p2.first || p3.first;
    }

    @Override
    public Boolean visit(QuantifiedExpression qe, List<FunctionDecl> arg) throws AsterixException {
        boolean changed = false;
        for (QuantifiedPair t : qe.getQuantifiedList()) {
            Pair<Boolean, Expression> p = inlineUdfsInExpr(t.getExpr(), arg);
            t.setExpr(p.second);
            if (p.first) {
                changed = true;
            }
        }
        Pair<Boolean, Expression> p2 = inlineUdfsInExpr(qe.getSatisfiesExpr(), arg);
        qe.setSatisfiesExpr(p2.second);
        return changed || p2.first;
    }

    @Override
    public Boolean visit(LetClause lc, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(lc.getBindingExpr(), arg);
        lc.setBindingExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(WhereClause wc, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(wc.getWhereExpr(), arg);
        wc.setWhereExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(OrderbyClause oc, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, ArrayList<Expression>> p = inlineUdfsInExprList(oc.getOrderbyList(), arg);
        oc.setOrderbyList(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(GroupbyClause gc, List<FunctionDecl> arg) throws AsterixException {
        boolean changed = false;
        for (GbyVariableExpressionPair p : gc.getGbyPairList()) {
            Pair<Boolean, Expression> be = inlineUdfsInExpr(p.getExpr(), arg);
            p.setExpr(be.second);
            if (be.first) {
                changed = true;
            }
        }
        for (GbyVariableExpressionPair p : gc.getDecorPairList()) {
            Pair<Boolean, Expression> be = inlineUdfsInExpr(p.getExpr(), arg);
            p.setExpr(be.second);
            if (be.first) {
                changed = true;
            }
        }
        return changed;
    }

    @Override
    public Boolean visit(LimitClause lc, List<FunctionDecl> arg) throws AsterixException {
        Pair<Boolean, Expression> p1 = inlineUdfsInExpr(lc.getLimitExpr(), arg);
        lc.setLimitExpr(p1.second);
        boolean changed = p1.first;
        if (lc.getOffset() != null) {
            Pair<Boolean, Expression> p2 = inlineUdfsInExpr(lc.getOffset(), arg);
            lc.setOffset(p2.second);
            changed = changed || p2.first;
        }
        return changed;
    }

    @Override
    public Boolean visit(UnaryExpr u, List<FunctionDecl> arg) throws AsterixException {
        return u.getExpr().accept(this, arg);
    }

    @Override
    public Boolean visit(VariableExpr v, List<FunctionDecl> arg) throws AsterixException {
        return false;
    }

    @Override
    public Boolean visit(LiteralExpr l, List<FunctionDecl> arg) throws AsterixException {
        return false;
    }

    protected Pair<Boolean, Expression> inlineUdfsInExpr(Expression expr, List<FunctionDecl> arg)
            throws AsterixException {
        if (expr.getKind() != Kind.CALL_EXPRESSION) {
            boolean r = expr.accept(this, arg);
            return new Pair<Boolean, Expression>(r, expr);
        } else {
            CallExpr f = (CallExpr) expr;
            boolean r = expr.accept(this, arg);
            FunctionDecl implem = findFuncDeclaration(f.getFunctionSignature(), arg);
            if (implem == null) {
                return new Pair<Boolean, Expression>(r, expr);
            } else {
                // Rewrite the function body itself (without setting unbounded variables to dataset access).
                // TODO(buyingyi): throw an exception for recursive function definition or limit the stack depth.
                implem.setFuncBody(rewriteFunctionBody(implem.getFuncBody()));
                // it's one of the functions we want to inline
                List<LetClause> clauses = new ArrayList<LetClause>();
                Iterator<VarIdentifier> paramIter = implem.getParamList().iterator();
                VariableSubstitutionEnvironment subts = new VariableSubstitutionEnvironment();
                for (Expression e : f.getExprList()) {
                    VarIdentifier param = paramIter.next();
                    // Obs: we could do smth about passing also literals, or let
                    // variable inlining to take care of this.
                    if (e.getKind() == Kind.VARIABLE_EXPRESSION) {
                        subts.addSubstituion(new VariableExpr(param), e);
                    } else {
                        VarIdentifier newV = context.newVariable();
                        Pair<ILangExpression, VariableSubstitutionEnvironment> p1 = e.accept(cloneVisitor,
                                new VariableSubstitutionEnvironment());
                        LetClause c = new LetClause(new VariableExpr(newV), (Expression) p1.first);
                        clauses.add(c);
                        subts.addSubstituion(new VariableExpr(param), new VariableExpr(newV));
                    }
                }

                Pair<ILangExpression, VariableSubstitutionEnvironment> p2 = implem.getFuncBody().accept(cloneVisitor,
                        subts);
                Expression resExpr;
                if (clauses.isEmpty()) {
                    resExpr = (Expression) p2.first;
                } else {
                    resExpr = generateQueryExpression(clauses, (Expression) p2.first);
                }
                return new Pair<Boolean, Expression>(true, resExpr);
            }
        }
    }

    protected Pair<Boolean, ArrayList<Expression>> inlineUdfsInExprList(List<Expression> exprList,
            List<FunctionDecl> fds) throws AsterixException {
        ArrayList<Expression> newList = new ArrayList<Expression>();
        boolean changed = false;
        for (Expression e : exprList) {
            Pair<Boolean, Expression> p = inlineUdfsInExpr(e, fds);
            newList.add(p.second);
            if (p.first) {
                changed = true;
            }
        }
        return new Pair<Boolean, ArrayList<Expression>>(changed, newList);
    }

    protected Expression rewriteFunctionBody(Expression expr) throws AsterixException {
        Query wrappedQuery = new Query();
        wrappedQuery.setBody(expr);
        wrappedQuery.setTopLevel(false);
        IQueryRewriter queryRewriter = rewriterFactory.createQueryRewriter();
        queryRewriter.rewrite(declaredFunctions, wrappedQuery, metadataProvider, context);
        return wrappedQuery.getBody();
    }

    protected static FunctionDecl findFuncDeclaration(FunctionSignature fid, List<FunctionDecl> sequence) {
        for (FunctionDecl f : sequence) {
            if (f.getSignature().equals(fid)) {
                return f;
            }
        }
        return null;
    }
}
