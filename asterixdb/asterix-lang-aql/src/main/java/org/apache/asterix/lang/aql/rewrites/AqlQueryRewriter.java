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
package org.apache.asterix.lang.aql.rewrites;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.aql.clause.DistinctClause;
import org.apache.asterix.lang.aql.clause.ForClause;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.aql.expression.UnionExpr;
import org.apache.asterix.lang.aql.rewrites.visitor.AqlFunctionCallResolverVisitor;
import org.apache.asterix.lang.aql.visitor.AQLInlineUdfsVisitor;
import org.apache.asterix.lang.aql.visitor.base.IAQLVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.parser.FunctionParser;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.common.visitor.GatherFunctionCallsVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.utils.Pair;

class AqlQueryRewriter implements IQueryRewriter {

    private final IParserFactory parserFactory;
    private final FunctionParser functionParser;
    private IReturningStatement topStatement;
    private List<FunctionDecl> declaredFunctions;
    private LangRewritingContext context;
    private MetadataProvider metadataProvider;

    AqlQueryRewriter(IParserFactory parserFactory) {
        this.parserFactory = parserFactory;
        functionParser = new FunctionParser(parserFactory);
    }

    protected void setup(List<FunctionDecl> declaredFunctions, IReturningStatement topStatement,
            MetadataProvider metadataProvider, LangRewritingContext context) {
        this.topStatement = topStatement;
        this.context = context;
        this.declaredFunctions = declaredFunctions;
        this.metadataProvider = metadataProvider;
    }

    @Override
    public void rewrite(List<FunctionDecl> declaredFunctions, IReturningStatement topStatement,
            MetadataProvider metadataProvider, LangRewritingContext context, boolean inlineUdfs,
            Collection<VarIdentifier> externalVars) throws CompilationException {
        setup(declaredFunctions, topStatement, metadataProvider, context);
        if (topStatement.isTopLevel()) {
            wrapInLets();
        }
        resolveFunctionCalls();
        inlineDeclaredUdfs();
        topStatement.setVarCounter(context.getVarCounter().get());
    }

    protected void wrapInLets() {
        // If the top expression of the main statement is not a FLWOR, it wraps
        // it into a let clause.
        if (topStatement == null) {
            return;
        }
        Expression body = topStatement.getBody();
        if (body.getKind() != Kind.FLWOGR_EXPRESSION) {
            VarIdentifier var = context.newVariable();
            VariableExpr v = new VariableExpr(var);
            LetClause c1 = new LetClause(v, body);
            ArrayList<Clause> clauseList = new ArrayList<>(1);
            clauseList.add(c1);
            FLWOGRExpression newBody = new FLWOGRExpression(clauseList, new VariableExpr(var));
            topStatement.setBody(newBody);
        }
    }

    protected void resolveFunctionCalls() throws CompilationException {
        if (topStatement == null) {
            return;
        }
        AqlFunctionCallResolverVisitor visitor =
                new AqlFunctionCallResolverVisitor(metadataProvider, declaredFunctions);
        topStatement.accept(visitor, null);
    }

    protected void inlineDeclaredUdfs() throws CompilationException {
        if (topStatement == null) {
            return;
        }
        List<FunctionSignature> funIds = new ArrayList<FunctionSignature>();
        for (FunctionDecl fdecl : declaredFunctions) {
            funIds.add(fdecl.getSignature());
        }

        List<FunctionDecl> storedFunctionDecls = new ArrayList<>();
        for (Expression topLevelExpr : topStatement.getDirectlyEnclosedExpressions()) {
            storedFunctionDecls.addAll(FunctionUtil.retrieveUsedStoredFunctions(metadataProvider, topLevelExpr, funIds,
                    null, this::getFunctionCalls, functionParser, metadataProvider.getDefaultDataverseName()));
            declaredFunctions.addAll(storedFunctionDecls);
        }
        if (!declaredFunctions.isEmpty()) {
            AQLInlineUdfsVisitor visitor = new AQLInlineUdfsVisitor(context,
                    new AqlFunctionBodyRewriterFactory(parserFactory), declaredFunctions, metadataProvider);
            while (topStatement.accept(visitor, declaredFunctions)) {
                // loop until no more changes
            }
        }
        declaredFunctions.removeAll(storedFunctionDecls);
    }

    @Override
    public Set<CallExpr> getFunctionCalls(Expression expression) throws CompilationException {
        GatherFunctionCalls gfc = new GatherFunctionCalls();
        expression.accept(gfc, null);
        return gfc.getCalls();
    }

    @Override
    public Set<VariableExpr> getExternalVariables(Expression expr) {
        throw new UnsupportedOperationException("getExternalVariables not implemented for AQL");
    }

    private static class GatherFunctionCalls extends GatherFunctionCallsVisitor implements IAQLVisitor<Void, Void> {

        public GatherFunctionCalls() {
        }

        @Override
        public Void visit(DistinctClause dc, Void arg) throws CompilationException {
            for (Expression e : dc.getDistinctByExpr()) {
                e.accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(FLWOGRExpression flwor, Void arg) throws CompilationException {
            for (Clause c : flwor.getClauseList()) {
                c.accept(this, arg);
            }
            flwor.getReturnExpr().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(ForClause fc, Void arg) throws CompilationException {
            fc.getInExpr().accept(this, arg);
            if (fc.getPosVarExpr() != null) {
                fc.getPosVarExpr().accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(GroupbyClause gc, Void arg) throws CompilationException {
            for (List<GbyVariableExpressionPair> gbyPairList : gc.getGbyPairList()) {
                for (GbyVariableExpressionPair p : gbyPairList) {
                    p.getExpr().accept(this, arg);
                }
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
        public Void visit(LetClause lc, Void arg) throws CompilationException {
            lc.getBindingExpr().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(UnionExpr u, Void arg) throws CompilationException {
            for (Expression e : u.getExprs()) {
                e.accept(this, arg);
            }
            return null;
        }
    }
}
