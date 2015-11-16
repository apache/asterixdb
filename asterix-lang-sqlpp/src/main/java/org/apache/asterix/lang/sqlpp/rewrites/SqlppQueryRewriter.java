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
package org.apache.asterix.lang.sqlpp.rewrites;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.common.visitor.GatherFunctionCallsVisitor;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
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
import org.apache.asterix.lang.sqlpp.parser.FunctionParser;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.visitor.InlineColumnAliasVisitor;
import org.apache.asterix.lang.sqlpp.visitor.SqlppInlineUdfsVisitor;
import org.apache.asterix.lang.sqlpp.visitor.VariableCheckAndRewriteVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;

class SqlppQueryRewriter implements IQueryRewriter {
    private final FunctionParser functionRepository = new FunctionParser(new SqlppParserFactory());
    private Query topExpr;
    private List<FunctionDecl> declaredFunctions;
    private LangRewritingContext context;
    private MetadataTransactionContext mdTxnCtx;
    private AqlMetadataProvider metadataProvider;

    protected void setup(List<FunctionDecl> declaredFunctions, Query topExpr, AqlMetadataProvider metadataProvider,
            LangRewritingContext context) {
        this.topExpr = topExpr;
        this.context = context;
        this.declaredFunctions = declaredFunctions;
        this.mdTxnCtx = metadataProvider.getMetadataTxnContext();
        this.metadataProvider = metadataProvider;
    }

    @Override
    public void rewrite(List<FunctionDecl> declaredFunctions, Query topExpr, AqlMetadataProvider metadataProvider,
            LangRewritingContext context) throws AsterixException {
        // Sets up parameters.
        setup(declaredFunctions, topExpr, metadataProvider, context);

        // Inlines column aliases.
        inlineColumnAlias();

        // Generate ids for variables (considering scopes) and replace global variable access with the dataset function.
        variableCheckAndRewrite(true);

        // Inlines functions.
        inlineDeclaredUdfs();

        // Replace global variable access with the dataset function for inlined expressions.
        variableCheckAndRewrite(true);

        // Sets the var counter of the query.
        topExpr.setVarCounter(context.getVarCounter());
    }

    protected void inlineColumnAlias() throws AsterixException {
        if (topExpr == null) {
            return;
        }
        // Inline column aliases.
        InlineColumnAliasVisitor inlineColumnAliasVisitor = new InlineColumnAliasVisitor();
        inlineColumnAliasVisitor.visit(topExpr, false);
    }

    protected void variableCheckAndRewrite(boolean overwrite) throws AsterixException {
        if (topExpr == null) {
            return;
        }
        VariableCheckAndRewriteVisitor variableCheckAndRewriteVisitor = new VariableCheckAndRewriteVisitor(context,
                overwrite);
        variableCheckAndRewriteVisitor.visit(topExpr, null);
    }

    protected void inlineDeclaredUdfs() throws AsterixException {
        if (topExpr == null) {
            return;
        }
        List<FunctionSignature> funIds = new ArrayList<FunctionSignature>();
        for (FunctionDecl fdecl : declaredFunctions) {
            funIds.add(fdecl.getSignature());
        }

        List<FunctionDecl> otherFDecls = new ArrayList<FunctionDecl>();
        buildOtherUdfs(topExpr.getBody(), otherFDecls, funIds);
        declaredFunctions.addAll(otherFDecls);
        if (!declaredFunctions.isEmpty()) {
            SqlppInlineUdfsVisitor visitor = new SqlppInlineUdfsVisitor(context,
                    new SqlppFunctionBodyRewriterFactory() /* the rewriter for function bodies expressions*/,
                    declaredFunctions, metadataProvider);
            while (topExpr.accept(visitor, declaredFunctions)) {
                // loop until no more changes
            }
        }
        declaredFunctions.removeAll(otherFDecls);
    }

    protected void buildOtherUdfs(Expression expression, List<FunctionDecl> functionDecls,
            List<FunctionSignature> declaredFunctions) throws AsterixException {
        if (expression == null) {
            return;
        }
        String value = metadataProvider.getConfig().get(FunctionUtil.IMPORT_PRIVATE_FUNCTIONS);
        boolean includePrivateFunctions = (value != null) ? Boolean.valueOf(value.toLowerCase()) : false;
        Set<FunctionSignature> functionCalls = getFunctionCalls(expression);
        for (FunctionSignature signature : functionCalls) {

            if (declaredFunctions != null && declaredFunctions.contains(signature)) {
                continue;
            }

            Function function = lookupUserDefinedFunctionDecl(signature);
            if (function == null) {
                if (AsterixBuiltinFunctions.isBuiltinCompilerFunction(signature, includePrivateFunctions)) {
                    continue;
                }
                StringBuilder messageBuilder = new StringBuilder();
                if (functionDecls.size() > 0) {
                    messageBuilder.append(" function " + functionDecls.get(functionDecls.size() - 1).getSignature()
                            + " depends upon function " + signature + " which is undefined");
                } else {
                    messageBuilder.append(" function " + signature + " is undefined ");
                }
                throw new AsterixException(messageBuilder.toString());
            }

            if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_AQL)) {
                FunctionDecl functionDecl = functionRepository.getFunctionDecl(function);
                if (functionDecl != null) {
                    if (functionDecls.contains(functionDecl)) {
                        throw new AsterixException("ERROR:Recursive invocation "
                                + functionDecls.get(functionDecls.size() - 1).getSignature() + " <==> "
                                + functionDecl.getSignature());
                    }
                    functionDecls.add(functionDecl);
                    buildOtherUdfs(functionDecl.getFuncBody(), functionDecls, declaredFunctions);
                }
            }
        }

    }

    private Function lookupUserDefinedFunctionDecl(FunctionSignature signature) throws AsterixException {
        if (signature.getNamespace() == null) {
            return null;
        }
        return MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
    }

    private Set<FunctionSignature> getFunctionCalls(Expression expression) throws AsterixException {
        GatherFunctionCalls gfc = new GatherFunctionCalls();
        expression.accept(gfc, null);
        return gfc.getCalls();
    }

    private static class GatherFunctionCalls extends GatherFunctionCallsVisitor implements ISqlppVisitor<Void, Void> {

        public GatherFunctionCalls() {
        }

        @Override
        public Void visit(FromClause fromClause, Void arg) throws AsterixException {
            for (FromTerm fromTerm : fromClause.getFromTerms()) {
                fromTerm.accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(FromTerm fromTerm, Void arg) throws AsterixException {
            fromTerm.getLeftExpression().accept(this, arg);
            for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
                correlateClause.accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(JoinClause joinClause, Void arg) throws AsterixException {
            joinClause.getRightExpression().accept(this, arg);
            joinClause.getConditionExpression().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(NestClause nestClause, Void arg) throws AsterixException {
            nestClause.getRightExpression().accept(this, arg);
            nestClause.getConditionExpression().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(Projection projection, Void arg) throws AsterixException {
            projection.getExpression().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(SelectBlock selectBlock, Void arg) throws AsterixException {
            if (selectBlock.hasFromClause()) {
                selectBlock.getFromClause().accept(this, arg);
            }
            if (selectBlock.hasLetClauses()) {
                for (LetClause letClause : selectBlock.getLetList()) {
                    letClause.accept(this, arg);
                }
            }
            if (selectBlock.hasWhereClause()) {
                selectBlock.getWhereClause().accept(this, arg);
            }
            if (selectBlock.hasGroupbyClause()) {
                selectBlock.getGroupbyClause().accept(this, arg);
            }
            if (selectBlock.hasLetClausesAfterGroupby()) {
                for (LetClause letClause : selectBlock.getLetListAfterGroupby()) {
                    letClause.accept(this, arg);
                }
            }
            if (selectBlock.hasHavingClause()) {
                selectBlock.getHavingClause().accept(this, arg);
            }
            selectBlock.getSelectClause().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(SelectClause selectClause, Void arg) throws AsterixException {
            if (selectClause.selectElement()) {
                selectClause.getSelectElement().accept(this, arg);
            } else {
                selectClause.getSelectRegular().accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(SelectElement selectElement, Void arg) throws AsterixException {
            selectElement.getExpression().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(SelectRegular selectRegular, Void arg) throws AsterixException {
            for (Projection projection : selectRegular.getProjections()) {
                projection.accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(SelectSetOperation selectSetOperation, Void arg) throws AsterixException {
            selectSetOperation.getLeftInput().accept(this, arg);
            for (SetOperationRight setOperationRight : selectSetOperation.getRightInputs()) {
                setOperationRight.getSetOperationRightInput().accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(SelectExpression selectStatement, Void arg) throws AsterixException {
            selectStatement.getSelectSetOperation().accept(this, arg);
            if (selectStatement.hasOrderby()) {
                selectStatement.getOrderbyClause().accept(this, arg);
            }
            if (selectStatement.hasLimit()) {
                selectStatement.getLimitClause().accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(UnnestClause unnestClause, Void arg) throws AsterixException {
            unnestClause.getRightExpression().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(HavingClause havingClause, Void arg) throws AsterixException {
            havingClause.getFilterExpression().accept(this, arg);
            return null;
        }

    }
}
