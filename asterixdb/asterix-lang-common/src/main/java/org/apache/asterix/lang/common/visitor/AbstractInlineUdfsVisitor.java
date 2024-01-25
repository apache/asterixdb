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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.IVisitorExtension;
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
import org.apache.asterix.lang.common.statement.CopyToStatement;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.common.visitor.base.AbstractQueryExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public abstract class AbstractInlineUdfsVisitor extends AbstractQueryExpressionVisitor<Boolean, Void> {

    protected final LangRewritingContext context;

    protected final Map<FunctionSignature, FunctionDecl> usedUDFs;

    protected final Map<DatasetFullyQualifiedName, ViewDecl> usedViews;

    protected final CloneAndSubstituteVariablesVisitor cloneVisitor;

    public AbstractInlineUdfsVisitor(LangRewritingContext context, Map<FunctionSignature, FunctionDecl> usedUDFs,
            Map<DatasetFullyQualifiedName, ViewDecl> usedViews, CloneAndSubstituteVariablesVisitor cloneVisitor) {
        this.context = context;
        this.usedUDFs = usedUDFs;
        this.usedViews = usedViews;
        this.cloneVisitor = cloneVisitor;
    }

    /**
     * @param letClauses , a list of let-binding clauses
     * @param returnExpr , a return expression
     * @return a query expression which is upto a specific langauge, e.g., FLWOGR in AQL and expression query in SQL++.
     */
    protected abstract Expression generateQueryExpression(List<LetClause> letClauses, Expression returnExpr)
            throws CompilationException;

    @Override
    public Boolean visit(Query q, Void arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsAndViewsInExpr(q.getBody());
        q.setBody(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(ListConstructor lc, Void arg) throws CompilationException {
        Pair<Boolean, List<Expression>> p = inlineUdfsInExprList(lc.getExprList());
        lc.setExprList(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(RecordConstructor rc, Void arg) throws CompilationException {
        boolean changed = false;
        for (FieldBinding b : rc.getFbList()) {
            Pair<Boolean, Expression> leftExprInlined = inlineUdfsAndViewsInExpr(b.getLeftExpr());
            b.setLeftExpr(leftExprInlined.second);
            changed = changed || leftExprInlined.first;
            Pair<Boolean, Expression> rightExprInlined = inlineUdfsAndViewsInExpr(b.getRightExpr());
            b.setRightExpr(rightExprInlined.second);
            changed = changed || rightExprInlined.first;
        }
        return changed;
    }

    @Override
    public Boolean visit(CallExpr callExpr, Void arg) throws CompilationException {
        Pair<Boolean, List<Expression>> p = inlineUdfsInExprList(callExpr.getExprList());
        callExpr.setExprList(p.second);
        boolean changed = p.first;
        if (callExpr.hasAggregateFilterExpr()) {
            Pair<Boolean, Expression> be = inlineUdfsAndViewsInExpr(callExpr.getAggregateFilterExpr());
            callExpr.setAggregateFilterExpr(be.second);
            changed |= be.first;
        }
        return changed;
    }

    @Override
    public Boolean visit(OperatorExpr ifbo, Void arg) throws CompilationException {
        Pair<Boolean, List<Expression>> p = inlineUdfsInExprList(ifbo.getExprList());
        ifbo.setExprList(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(FieldAccessor fa, Void arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsAndViewsInExpr(fa.getExpr());
        fa.setExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(IndexAccessor fa, Void arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsAndViewsInExpr(fa.getExpr());
        fa.setExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(IfExpr ifexpr, Void arg) throws CompilationException {
        Pair<Boolean, Expression> p1 = inlineUdfsAndViewsInExpr(ifexpr.getCondExpr());
        ifexpr.setCondExpr(p1.second);
        Pair<Boolean, Expression> p2 = inlineUdfsAndViewsInExpr(ifexpr.getThenExpr());
        ifexpr.setThenExpr(p2.second);
        Pair<Boolean, Expression> p3 = inlineUdfsAndViewsInExpr(ifexpr.getElseExpr());
        ifexpr.setElseExpr(p3.second);
        return p1.first || p2.first || p3.first;
    }

    @Override
    public Boolean visit(QuantifiedExpression qe, Void arg) throws CompilationException {
        boolean changed = false;
        for (QuantifiedPair t : qe.getQuantifiedList()) {
            Pair<Boolean, Expression> p = inlineUdfsAndViewsInExpr(t.getExpr());
            t.setExpr(p.second);
            if (p.first) {
                changed = true;
            }
        }
        Pair<Boolean, Expression> p2 = inlineUdfsAndViewsInExpr(qe.getSatisfiesExpr());
        qe.setSatisfiesExpr(p2.second);
        return changed || p2.first;
    }

    @Override
    public Boolean visit(LetClause lc, Void arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsAndViewsInExpr(lc.getBindingExpr());
        lc.setBindingExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(WhereClause wc, Void arg) throws CompilationException {
        Pair<Boolean, Expression> p = inlineUdfsAndViewsInExpr(wc.getWhereExpr());
        wc.setWhereExpr(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(OrderbyClause oc, Void arg) throws CompilationException {
        Pair<Boolean, List<Expression>> p = inlineUdfsInExprList(oc.getOrderbyList());
        oc.setOrderbyList(p.second);
        return p.first;
    }

    @Override
    public Boolean visit(GroupbyClause gc, Void arg) throws CompilationException {
        boolean changed = false;
        List<List<GbyVariableExpressionPair>> gbyList = gc.getGbyPairList();
        List<List<GbyVariableExpressionPair>> newGbyList = new ArrayList<>(gbyList.size());
        for (List<GbyVariableExpressionPair> gbyPairList : gbyList) {
            Pair<Boolean, List<GbyVariableExpressionPair>> p1 = inlineUdfsInGbyPairList(gbyPairList);
            newGbyList.add(p1.second);
            changed |= p1.first;
        }
        gc.setGbyPairList(newGbyList);
        if (gc.hasDecorList()) {
            Pair<Boolean, List<GbyVariableExpressionPair>> p2 = inlineUdfsInGbyPairList(gc.getDecorPairList());
            gc.setDecorPairList(p2.second);
            changed |= p2.first;
        }
        if (gc.hasGroupFieldList()) {
            Pair<Boolean, List<Pair<Expression, Identifier>>> p3 = inlineUdfsInFieldList(gc.getGroupFieldList());
            gc.setGroupFieldList(p3.second);
            changed |= p3.first;
        }
        if (gc.hasWithMap()) {
            Pair<Boolean, Map<Expression, VariableExpr>> p4 = inlineUdfsInVarMap(gc.getWithVarMap());
            gc.setWithVarMap(p4.second);
            changed |= p4.first;
        }
        return changed;
    }

    @Override
    public Boolean visit(LimitClause lc, Void arg) throws CompilationException {
        boolean changed = false;
        if (lc.hasLimitExpr()) {
            Pair<Boolean, Expression> p1 = inlineUdfsAndViewsInExpr(lc.getLimitExpr());
            lc.setLimitExpr(p1.second);
            changed = p1.first;
        }
        if (lc.hasOffset()) {
            Pair<Boolean, Expression> p2 = inlineUdfsAndViewsInExpr(lc.getOffset());
            lc.setOffset(p2.second);
            changed |= p2.first;
        }
        return changed;
    }

    @Override
    public Boolean visit(UnaryExpr u, Void arg) throws CompilationException {
        return u.getExpr().accept(this, arg);
    }

    @Override
    public Boolean visit(VariableExpr v, Void arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(LiteralExpr l, Void arg) throws CompilationException {
        return false;
    }

    @Override
    public Boolean visit(IVisitorExtension ve, Void arg) throws CompilationException {
        return ve.inlineUDFsDispatch(this);
    }

    @Override
    public Boolean visit(InsertStatement insert, Void arg) throws CompilationException {
        boolean changed = false;
        Expression returnExpression = insert.getReturnExpression();
        if (returnExpression != null) {
            Pair<Boolean, Expression> rewrittenReturnExpr = inlineUdfsAndViewsInExpr(returnExpression);
            insert.setReturnExpression(rewrittenReturnExpr.second);
            changed |= rewrittenReturnExpr.first;
        }
        Pair<Boolean, Expression> rewrittenBodyExpression = inlineUdfsAndViewsInExpr(insert.getBody());
        insert.setBody(rewrittenBodyExpression.second);
        return changed || rewrittenBodyExpression.first;
    }

    @Override
    public Boolean visit(CopyToStatement stmtCopy, Void arg) throws CompilationException {
        boolean changed = false;

        Pair<Boolean, Expression> queryBody = inlineUdfsAndViewsInExpr(stmtCopy.getBody());
        changed |= queryBody.first;
        stmtCopy.setBody(queryBody.second);

        Pair<Boolean, List<Expression>> path = inlineUdfsInExprList(stmtCopy.getPathExpressions());
        changed |= path.first;
        stmtCopy.setPathExpressions(path.second);

        Pair<Boolean, List<Expression>> part = inlineUdfsInExprList(stmtCopy.getPartitionExpressions());
        changed |= part.first;
        stmtCopy.setPartitionExpressions(part.second);

        Pair<Boolean, List<Expression>> order = inlineUdfsInExprList(stmtCopy.getOrderByList());
        changed |= order.first;
        stmtCopy.setOrderByList(order.second);

        return changed;
    }

    protected Pair<Boolean, Expression> inlineUdfsAndViewsInExpr(Expression expr) throws CompilationException {
        if (expr.getKind() != Kind.CALL_EXPRESSION) {
            boolean r = expr.accept(this, null);
            return new Pair<>(r, expr);
        }
        CallExpr f = (CallExpr) expr;
        boolean r = expr.accept(this, null);

        List<LetClause> letClauses;
        VariableSubstitutionEnvironment bodyVarSubst;
        Expression normBodyExpr;

        FunctionSignature fs = f.getFunctionSignature();
        if (FunctionUtil.isBuiltinFunctionSignature(fs)) {
            if (!FunctionUtil.isBuiltinDatasetFunction(fs)) {
                return new Pair<>(r, expr);
            }
            Triple<DatasetFullyQualifiedName, Boolean, DatasetFullyQualifiedName> dsArgs =
                    FunctionUtil.parseDatasetFunctionArguments(f);
            if (!Boolean.TRUE.equals(dsArgs.second)) {
                // not a view
                return new Pair<>(r, expr);
            }
            DatasetFullyQualifiedName viewName = dsArgs.first;
            ViewDecl implem = usedViews.get(viewName);
            if (implem == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_VIEW, f.getSourceLocation(), viewName);
            }
            // it's one of the views we want to inline
            letClauses = Collections.emptyList();
            bodyVarSubst = new VariableSubstitutionEnvironment();
            normBodyExpr = implem.getNormalizedViewBody();
            if (normBodyExpr == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, f.getSourceLocation(),
                        viewName.toString());
            }
        } else {
            FunctionDecl implem = usedUDFs.get(fs);
            if (implem == null) {
                //it's an external UDF
                return new Pair<>(r, expr);
            }
            // it's one of the functions we want to inline
            boolean isVarargs = implem.getSignature().getArity() == FunctionIdentifier.VARARGS;
            Pair<List<LetClause>, VariableSubstitutionEnvironment> clausesAndSubst =
                    createFunctionParametersSubstitution(implem.getParamList(), isVarargs, f.getExprList(),
                            f.getSourceLocation());
            letClauses = clausesAndSubst.first;
            bodyVarSubst = clausesAndSubst.second;
            normBodyExpr = implem.getNormalizedFuncBody();
            if (normBodyExpr == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, f.getSourceLocation(),
                        fs.toString());
            }
        }

        if (f.hasAggregateFilterExpr()) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_USE_OF_FILTER_CLAUSE, f.getSourceLocation());
        }

        Pair<ILangExpression, VariableSubstitutionEnvironment> p2 = normBodyExpr.accept(cloneVisitor, bodyVarSubst);
        Expression resExpr = letClauses.isEmpty() ? (Expression) p2.first
                : generateQueryExpression(letClauses, (Expression) p2.first);
        return new Pair<>(true, resExpr);
    }

    private Pair<List<LetClause>, VariableSubstitutionEnvironment> createFunctionParametersSubstitution(
            List<VarIdentifier> paramList, boolean isVarargs, List<Expression> argList, SourceLocation sourceLoc)
            throws CompilationException {
        int argCount = argList.size();
        List<LetClause> clauses = new ArrayList<>(argCount + 1);
        List<Expression> argVars = new ArrayList<>(argCount);
        for (Expression e : argList) {
            // Obs: we could do smth about passing also literals, or let
            // variable inlining to take care of this.
            VarIdentifier argVar;
            if (e.getKind() == Kind.VARIABLE_EXPRESSION) {
                argVar = ((VariableExpr) e).getVar();
            } else {
                SourceLocation argSourceLoc = e.getSourceLocation();
                argVar = context.newVariable();
                Pair<ILangExpression, VariableSubstitutionEnvironment> p1 =
                        e.accept(cloneVisitor, new VariableSubstitutionEnvironment());
                VariableExpr newVRef1 = new VariableExpr(argVar);
                newVRef1.setSourceLocation(argSourceLoc);
                LetClause c = new LetClause(newVRef1, (Expression) p1.first);
                c.setSourceLocation(argSourceLoc);
                clauses.add(c);
            }

            VariableExpr argVarExpr = new VariableExpr(argVar);
            argVarExpr.setSourceLocation(e.getSourceLocation());
            argVars.add(argVarExpr);
        }

        VariableSubstitutionEnvironment subst = new VariableSubstitutionEnvironment();
        if (isVarargs) {
            if (paramList.size() != 1) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc, paramList.size());
            }
            VarIdentifier paramVarargs = paramList.get(0);
            CallExpr argsListExpr =
                    new CallExpr(new FunctionSignature(BuiltinFunctions.ORDERED_LIST_CONSTRUCTOR), argVars);
            argsListExpr.setSourceLocation(sourceLoc);

            VarIdentifier argsVar = context.newVariable();
            VariableExpr argsVarRef1 = new VariableExpr(argsVar);
            argsVarRef1.setSourceLocation(sourceLoc);
            LetClause c = new LetClause(argsVarRef1, argsListExpr);
            c.setSourceLocation(sourceLoc);
            clauses.add(c);

            VariableExpr argsVarRef2 = new VariableExpr(argsVar);
            argsVarRef2.setSourceLocation(sourceLoc);
            subst.addSubstituion(new VariableExpr(paramVarargs), argsVarRef2);
        } else {
            if (paramList.size() != argCount) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc, paramList.size());
            }
            for (int i = 0; i < argCount; i++) {
                subst.addSubstituion(new VariableExpr(paramList.get(i)), argVars.get(i));
            }
        }
        return new Pair<>(clauses, subst);
    }

    protected Pair<Boolean, List<Expression>> inlineUdfsInExprList(List<Expression> exprList)
            throws CompilationException {
        List<Expression> newList = new ArrayList<>(exprList.size());
        boolean changed = false;
        for (Expression e : exprList) {
            Pair<Boolean, Expression> be = inlineUdfsAndViewsInExpr(e);
            newList.add(be.second);
            changed |= be.first;
        }
        return new Pair<>(changed, newList);
    }

    private Pair<Boolean, List<GbyVariableExpressionPair>> inlineUdfsInGbyPairList(
            List<GbyVariableExpressionPair> gbyPairList) throws CompilationException {
        List<GbyVariableExpressionPair> newList = new ArrayList<>(gbyPairList.size());
        boolean changed = false;
        for (GbyVariableExpressionPair p : gbyPairList) {
            Pair<Boolean, Expression> be = inlineUdfsAndViewsInExpr(p.getExpr());
            newList.add(new GbyVariableExpressionPair(p.getVar(), be.second));
            changed |= be.first;
        }
        return new Pair<>(changed, newList);
    }

    protected Pair<Boolean, List<Pair<Expression, Identifier>>> inlineUdfsInFieldList(
            List<Pair<Expression, Identifier>> fieldList) throws CompilationException {
        List<Pair<Expression, Identifier>> newList = new ArrayList<>(fieldList.size());
        boolean changed = false;
        for (Pair<Expression, Identifier> p : fieldList) {
            Pair<Boolean, Expression> be = inlineUdfsAndViewsInExpr(p.first);
            newList.add(new Pair<>(be.second, p.second));
            changed |= be.first;
        }
        return new Pair<>(changed, newList);
    }

    private Pair<Boolean, Map<Expression, VariableExpr>> inlineUdfsInVarMap(Map<Expression, VariableExpr> varMap)
            throws CompilationException {
        Map<Expression, VariableExpr> newMap = new HashMap<>();
        boolean changed = false;
        for (Map.Entry<Expression, VariableExpr> me : varMap.entrySet()) {
            Pair<Boolean, Expression> be = inlineUdfsAndViewsInExpr(me.getKey());
            newMap.put(be.second, me.getValue());
            changed |= be.first;
        }
        return new Pair<>(changed, newMap);
    }
}
