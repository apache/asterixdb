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
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.aql.clause.DistinctClause;
import org.apache.asterix.lang.aql.clause.ForClause;
import org.apache.asterix.lang.aql.expression.FLWOGRExpression;
import org.apache.asterix.lang.aql.expression.UnionExpr;
import org.apache.asterix.lang.aql.parser.AQLParserFactory;
import org.apache.asterix.lang.aql.parser.FunctionParser;
import org.apache.asterix.lang.aql.rewrites.visitor.AqlBuiltinFunctionRewriteVisitor;
import org.apache.asterix.lang.common.util.CommonFunctionMapUtil;
import org.apache.asterix.lang.aql.visitor.AQLInlineUdfsVisitor;
import org.apache.asterix.lang.aql.visitor.base.IAQLVisitor;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.common.visitor.GatherFunctionCallsVisitor;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;

class AqlQueryRewriter implements IQueryRewriter {

    private final FunctionParser functionParser = new FunctionParser(new AQLParserFactory());
    private Query topExpr;
    private List<FunctionDecl> declaredFunctions;
    private LangRewritingContext context;
    private AqlMetadataProvider metadataProvider;

    private void setup(List<FunctionDecl> declaredFunctions, Query topExpr, AqlMetadataProvider metadataProvider,
            LangRewritingContext context) {
        this.topExpr = topExpr;
        this.context = context;
        this.declaredFunctions = declaredFunctions;
        this.metadataProvider = metadataProvider;
    }

    @Override
    public void rewrite(List<FunctionDecl> declaredFunctions, Query topExpr, AqlMetadataProvider metadataProvider,
            LangRewritingContext context) throws AsterixException {
        setup(declaredFunctions, topExpr, metadataProvider, context);
        if (topExpr.isTopLevel()) {
            wrapInLets();
        }
        inlineDeclaredUdfs();
        rewriteFunctionName();
        topExpr.setVarCounter(context.getVarCounter());
    }

    private void wrapInLets() {
        // If the top expression of the main statement is not a FLWOR, it wraps
        // it into a let clause.
        if (topExpr == null) {
            return;
        }
        Expression body = topExpr.getBody();
        if (body.getKind() != Kind.FLWOGR_EXPRESSION) {
            VarIdentifier var = context.newVariable();
            VariableExpr v = new VariableExpr(var);
            LetClause c1 = new LetClause(v, body);
            ArrayList<Clause> clauseList = new ArrayList<Clause>(1);
            clauseList.add(c1);
            FLWOGRExpression newBody = new FLWOGRExpression(clauseList, new VariableExpr(var));
            topExpr.setBody(newBody);
        }
    }

    private void rewriteFunctionName() throws AsterixException {
        if (topExpr == null) {
            return;
        }
        AqlBuiltinFunctionRewriteVisitor visitor = new AqlBuiltinFunctionRewriteVisitor();
        topExpr.accept(visitor, null);
    }

    private void inlineDeclaredUdfs() throws AsterixException {
        if (topExpr == null) {
            return;
        }
        List<FunctionSignature> funIds = new ArrayList<FunctionSignature>();
        for (FunctionDecl fdecl : declaredFunctions) {
            funIds.add(fdecl.getSignature());
        }

        List<FunctionDecl> storedFunctionDecls = FunctionUtil.retrieveUsedStoredFunctions(metadataProvider,
                topExpr.getBody(), funIds, null,
                expr -> getFunctionCalls(expr), func -> functionParser.getFunctionDecl(func),
                signature -> CommonFunctionMapUtil.normalizeBuiltinFunctionSignature(signature));
        declaredFunctions.addAll(storedFunctionDecls);
        if (!declaredFunctions.isEmpty()) {
            AQLInlineUdfsVisitor visitor =
                    new AQLInlineUdfsVisitor(context, new AQLRewriterFactory(), declaredFunctions, metadataProvider);
            while (topExpr.accept(visitor, declaredFunctions)) {
                // loop until no more changes
            }
        }
        declaredFunctions.removeAll(storedFunctionDecls);
    }

    private Set<FunctionSignature> getFunctionCalls(Expression expression) throws AsterixException {
        GatherFunctionCalls gfc = new GatherFunctionCalls();
        expression.accept(gfc, null);
        return gfc.getCalls();
    }

    private static class GatherFunctionCalls extends GatherFunctionCallsVisitor implements IAQLVisitor<Void, Void> {

        public GatherFunctionCalls() {
        }

        @Override
        public Void visit(DistinctClause dc, Void arg) throws AsterixException {
            for (Expression e : dc.getDistinctByExpr()) {
                e.accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(FLWOGRExpression flwor, Void arg) throws AsterixException {
            for (Clause c : flwor.getClauseList()) {
                c.accept(this, arg);
            }
            flwor.getReturnExpr().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(ForClause fc, Void arg) throws AsterixException {
            fc.getInExpr().accept(this, arg);
            if (fc.getPosVarExpr() != null) {
                fc.getPosVarExpr().accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(GroupbyClause gc, Void arg) throws AsterixException {
            for (GbyVariableExpressionPair p : gc.getGbyPairList()) {
                p.getExpr().accept(this, arg);
            }
            for (GbyVariableExpressionPair p : gc.getDecorPairList()) {
                p.getExpr().accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(LetClause lc, Void arg) throws AsterixException {
            lc.getBindingExpr().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(UnionExpr u, Void arg) throws AsterixException {
            for (Expression e : u.getExprs()) {
                e.accept(this, arg);
            }
            return null;
        }
    }
}
