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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
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
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.visitor.base.AbstractQueryExpressionVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.SourceLocation;

public abstract class AbstractInlineUdfsVisitor extends AbstractQueryExpressionVisitor<Boolean, List<FunctionDecl>> {

    protected final LangRewritingContext context;
    protected final CloneAndSubstituteVariablesVisitor cloneVisitor;
    private final IRewriterFactory rewriterFactory;
    private final List<FunctionDecl> declaredFunctions;
    private final MetadataProvider metadataProvider;

    public AbstractInlineUdfsVisitor(LangRewritingContext context, IRewriterFactory rewriterFactory,
            List<FunctionDecl> declaredFunctions, MetadataProvider metadataProvider,
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
            throws CompilationException;

    @Override
    public Boolean visit(Query q, List<FunctionDecl> arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(q.getBody(), arg);
        q.setBody(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(FunctionDecl fd, List<FunctionDecl> arg) throws CompilationException {
        // Careful, we should only do this after analyzing the graph of function
        // calls.
        Pair<Boolean, Expression> p = inlineUdfsInExpr(fd.getFuncBody(), arg);
        fd.setFuncBody(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(ListConstructor lc, List<FunctionDecl> arg) throws CompilationException {
        Pair<Boolean, List<Expression>> p = inlineUdfsInExprList(lc.getExprList(), arg);
        lc.setExprList(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(RecordConstructor rc, List<FunctionDecl> arg) throws CompilationException {
        boolean changed = false;
        for (FieldBinding b : rc.getFbList()) {
            Pair<Boolean, Expression> leftExprInlined = inlineUdfsInExpr(b.getLeftExpr(), arg);
            b.setLeftExpr(leftExprInlined.second);
            changed = changed || leftExprInlined.first;
            Pair<Boolean, Expression> rightExprInlined = inlineUdfsInExpr(b.getRightExpr(), arg);
            b.setRightExpr(rightExprInlined.second);
            changed = changed || rightExprInlined.first;
        }
        return changed;
    }

    @Override
    public Boolean visit(CallExpr pf, List<FunctionDecl> arg) throws CompilationException {
        Pair<Boolean, List<Expression>> p = inlineUdfsInExprList(pf.getExprList(), arg);
        pf.setExprList(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(OperatorExpr ifbo, List<FunctionDecl> arg) throws CompilationException {
        Pair<Boolean, List<Expression>> p = inlineUdfsInExprList(ifbo.getExprList(), arg);
        ifbo.setExprList(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(FieldAccessor fa, List<FunctionDecl> arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(fa.getExpr(), arg);
        fa.setExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(IndexAccessor fa, List<FunctionDecl> arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(fa.getExpr(), arg);
        fa.setExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(IfExpr ifexpr, List<FunctionDecl> arg) throws CompilationException {
        Pair<Boolean, Expression> p1 = inlineUdfsInExpr(ifexpr.getCondExpr(), arg);
        ifexpr.setCondExpr(p1.second);
        Pair<Boolean, Expression> p2 = inlineUdfsInExpr(ifexpr.getThenExpr(), arg);
        ifexpr.setThenExpr(p2.second);
        Pair<Boolean, Expression> p3 = inlineUdfsInExpr(ifexpr.getElseExpr(), arg);
        ifexpr.setElseExpr(p3.second);
        return p1.first || p2.first || p3.first;
    }

    @Override
    public Boolean visit(QuantifiedExpression qe, List<FunctionDecl> arg) throws CompilationException {
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
    public Boolean visit(LetClause lc, List<FunctionDecl> arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(lc.getBindingExpr(), arg);
        lc.setBindingExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(WhereClause wc, List<FunctionDecl> arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsInExpr(wc.getWhereExpr(), arg);
        wc.setWhereExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(OrderbyClause oc, List<FunctionDecl> arg) throws CompilationException {
        Pair<Boolean, List<Expression>> p = inlineUdfsInExprList(oc.getOrderbyList(), arg);
        oc.setOrderbyList(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(GroupbyClause gc, List<FunctionDecl> arg) throws CompilationException {
        Pair<Boolean, List<GbyVariableExpressionPair>> p1 = inlineUdfsInGbyPairList(gc.getGbyPairList(), arg);
        gc.setGbyPairList(p1.second);
        boolean changed = p1.first;
        if (gc.hasDecorList()) {
            Pair<Boolean, List<GbyVariableExpressionPair>> p2 = inlineUdfsInGbyPairList(gc.getDecorPairList(), arg);
            gc.setDecorPairList(p2.second);
            changed |= p2.first;
        }
        if (gc.hasGroupFieldList()) {
            Pair<Boolean, List<Pair<Expression, Identifier>>> p3 = inlineUdfsInFieldList(gc.getGroupFieldList(), arg);
            gc.setGroupFieldList(p3.second);
            changed |= p3.first;
        }
        if (gc.hasWithMap()) {
            Pair<Boolean, Map<Expression, VariableExpr>> p4 = inlineUdfsInVarMap(gc.getWithVarMap(), arg);
            gc.setWithVarMap(p4.second);
            changed |= p4.first;
        }
        return changed;
    }

    @Override
    public Boolean visit(LimitClause lc, List<FunctionDecl> arg) throws CompilationException {
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
    public Boolean visit(UnaryExpr u, List<FunctionDecl> arg) throws CompilationException {
        return u.getExpr().accept(this, arg);
    }

    @Override
    public Boolean visit(VariableExpr v, List<FunctionDecl> arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(LiteralExpr l, List<FunctionDecl> arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(InsertStatement insert, List<FunctionDecl> arg) throws CompilationException {
        boolean changed = false;
        Expression returnExpression = insert.getReturnExpression();
        if (returnExpression != null) {
            Pair<Boolean, Expression> rewrittenReturnExpr = inlineUdfsInExpr(returnExpression, arg);
            insert.setReturnExpression(rewrittenReturnExpr.second);
            changed |= rewrittenReturnExpr.first;
        }
        Pair<Boolean, Expression> rewrittenBodyExpression = inlineUdfsInExpr(insert.getBody(), arg);
        insert.setBody(rewrittenBodyExpression.second);
        return changed || rewrittenBodyExpression.first;
    }

    protected Pair<Boolean, Expression> inlineUdfsInExpr(Expression expr, List<FunctionDecl> arg)
            throws CompilationException {
        if (expr.getKind() != Kind.CALL_EXPRESSION) {
            boolean r = expr.accept(this, arg);
            return new Pair<>(r, expr);
        }
        CallExpr f = (CallExpr) expr;
        boolean r = expr.accept(this, arg);
        FunctionDecl implem = findFuncDeclaration(f.getFunctionSignature(), arg);
        if (implem == null) {
            return new Pair<>(r, expr);
        } else {
            // Rewrite the function body itself (without setting unbounded variables to dataset access).
            // TODO(buyingyi): throw an exception for recursive function definition or limit the stack depth.
            implem.setFuncBody(rewriteFunctionBody(implem));
            // it's one of the functions we want to inline
            List<LetClause> clauses = new ArrayList<>();
            Iterator<VarIdentifier> paramIter = implem.getParamList().iterator();
            VariableSubstitutionEnvironment subts = new VariableSubstitutionEnvironment();
            for (Expression e : f.getExprList()) {
                VarIdentifier param = paramIter.next();
                // Obs: we could do smth about passing also literals, or let
                // variable inlining to take care of this.
                if (e.getKind() == Kind.VARIABLE_EXPRESSION) {
                    subts.addSubstituion(new VariableExpr(param), e);
                } else {
                    SourceLocation sourceLoc = e.getSourceLocation();
                    VarIdentifier newV = context.newVariable();
                    Pair<ILangExpression, VariableSubstitutionEnvironment> p1 =
                            e.accept(cloneVisitor, new VariableSubstitutionEnvironment());
                    VariableExpr newVRef1 = new VariableExpr(newV);
                    newVRef1.setSourceLocation(sourceLoc);
                    LetClause c = new LetClause(newVRef1, (Expression) p1.first);
                    c.setSourceLocation(sourceLoc);
                    clauses.add(c);
                    VariableExpr newVRef2 = new VariableExpr(newV);
                    newVRef2.setSourceLocation(sourceLoc);
                    subts.addSubstituion(new VariableExpr(param), newVRef2);
                }
            }

            Pair<ILangExpression, VariableSubstitutionEnvironment> p2 =
                    implem.getFuncBody().accept(cloneVisitor, subts);
            Expression resExpr;
            if (clauses.isEmpty()) {
                resExpr = (Expression) p2.first;
            } else {
                resExpr = generateQueryExpression(clauses, (Expression) p2.first);
            }
            return new Pair<>(true, resExpr);
        }
    }

    protected Pair<Boolean, List<Expression>> inlineUdfsInExprList(List<Expression> exprList, List<FunctionDecl> fds)
            throws CompilationException {
        List<Expression> newList = new ArrayList<>(exprList.size());
        boolean changed = false;
        for (Expression e : exprList) {
            Pair<Boolean, Expression> be = inlineUdfsInExpr(e, fds);
            newList.add(be.second);
            changed |= be.first;
        }
        return new Pair<>(changed, newList);
    }

    private Pair<Boolean, List<GbyVariableExpressionPair>> inlineUdfsInGbyPairList(
            List<GbyVariableExpressionPair> gbyPairList, List<FunctionDecl> fds) throws CompilationException {
        List<GbyVariableExpressionPair> newList = new ArrayList<>(gbyPairList.size());
        boolean changed = false;
        for (GbyVariableExpressionPair p : gbyPairList) {
            Pair<Boolean, Expression> be = inlineUdfsInExpr(p.getExpr(), fds);
            newList.add(new GbyVariableExpressionPair(p.getVar(), be.second));
            changed |= be.first;
        }
        return new Pair<>(changed, newList);
    }

    protected Pair<Boolean, List<Pair<Expression, Identifier>>> inlineUdfsInFieldList(
            List<Pair<Expression, Identifier>> fieldList, List<FunctionDecl> fds) throws CompilationException {
        List<Pair<Expression, Identifier>> newList = new ArrayList<>(fieldList.size());
        boolean changed = false;
        for (Pair<Expression, Identifier> p : fieldList) {
            Pair<Boolean, Expression> be = inlineUdfsInExpr(p.first, fds);
            newList.add(new Pair<>(be.second, p.second));
            changed |= be.first;
        }
        return new Pair<>(changed, newList);
    }

    private Pair<Boolean, Map<Expression, VariableExpr>> inlineUdfsInVarMap(Map<Expression, VariableExpr> varMap,
            List<FunctionDecl> fds) throws CompilationException {
        Map<Expression, VariableExpr> newMap = new HashMap<>();
        boolean changed = false;
        for (Map.Entry<Expression, VariableExpr> me : varMap.entrySet()) {
            Pair<Boolean, Expression> be = inlineUdfsInExpr(me.getKey(), fds);
            newMap.put(be.second, me.getValue());
            changed |= be.first;
        }
        return new Pair<>(changed, newMap);
    }

    private Expression rewriteFunctionBody(FunctionDecl fnDecl) throws CompilationException {
        SourceLocation sourceLoc = fnDecl.getSourceLocation();
        Query wrappedQuery = new Query(false);
        wrappedQuery.setSourceLocation(sourceLoc);
        wrappedQuery.setBody(fnDecl.getFuncBody());
        wrappedQuery.setTopLevel(false);

        String fnNamespace = fnDecl.getSignature().getNamespace();
        Dataverse defaultDataverse = metadataProvider.getDefaultDataverse();

        Dataverse fnDataverse;
        if (fnNamespace == null || fnNamespace.equals(defaultDataverse.getDataverseName())) {
            fnDataverse = defaultDataverse;
        } else {
            try {
                fnDataverse = metadataProvider.findDataverse(fnNamespace);
            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, e, sourceLoc, fnNamespace);
            }
        }

        metadataProvider.setDefaultDataverse(fnDataverse);
        try {
            IQueryRewriter queryRewriter = rewriterFactory.createQueryRewriter();
            queryRewriter.rewrite(declaredFunctions, wrappedQuery, metadataProvider, context, true,
                    fnDecl.getParamList());
            return wrappedQuery.getBody();
        } finally {
            metadataProvider.setDefaultDataverse(defaultDataverse);
        }
    }

    private static FunctionDecl findFuncDeclaration(FunctionSignature fid, List<FunctionDecl> sequence) {
        for (FunctionDecl f : sequence) {
            if (f.getSignature().equals(fid)) {
                return f;
            }
        }
        return null;
    }
}
