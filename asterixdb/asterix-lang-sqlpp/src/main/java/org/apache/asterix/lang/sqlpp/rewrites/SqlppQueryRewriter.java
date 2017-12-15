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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
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
import org.apache.asterix.lang.sqlpp.expression.CaseExpression;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.parser.FunctionParser;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.GenerateColumnNameVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.InlineColumnAliasVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.InlineWithExpressionVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.OperatorExpressionVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SetOperationVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppBuiltinFunctionRewriteVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppDistinctAggregationSugarVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppGroupByAggregationSugarVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppGroupByVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppInlineUdfsVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppListInputFunctionRewriteVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SubstituteGroupbyExpressionWithVariableVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.VariableCheckAndRewriteVisitor;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;

class SqlppQueryRewriter implements IQueryRewriter {
    private static final String INLINE_WITH = "inline_with";
    private static final String NOT_INLINE_WITH = "false";
    private final FunctionParser functionRepository = new FunctionParser(new SqlppParserFactory());
    private IReturningStatement topExpr;
    private List<FunctionDecl> declaredFunctions;
    private LangRewritingContext context;
    private MetadataProvider metadataProvider;

    protected void setup(List<FunctionDecl> declaredFunctions, IReturningStatement topExpr,
            MetadataProvider metadataProvider, LangRewritingContext context) {
        this.topExpr = topExpr;
        this.context = context;
        this.declaredFunctions = declaredFunctions;
        this.metadataProvider = metadataProvider;
    }

    @Override
    public void rewrite(List<FunctionDecl> declaredFunctions, IReturningStatement topStatement,
            MetadataProvider metadataProvider, LangRewritingContext context) throws CompilationException {
        if (topStatement == null) {
            return;
        }

        // Sets up parameters.
        setup(declaredFunctions, topStatement, metadataProvider, context);

        // Inlines column aliases.
        inlineColumnAlias();

        // Generates column names.
        generateColumnNames();

        // Substitutes group-by key expressions.
        substituteGroupbyKeyExpression();

        // Group-by core rewrites
        rewriteGroupBys();

        // Rewrites set operations.
        rewriteSetOperations();

        // Generate ids for variables (considering scopes) and replace global variable access with the dataset function.
        variableCheckAndRewrite();

        // Rewrites SQL-92 aggregate functions
        rewriteGroupByAggregationSugar();

        // Rewrites like/not-like expressions.
        rewriteOperatorExpression();

        // Inlines WITH expressions after variableCheckAndRewrite(...) so that the variable scoping for WITH
        // expression is correct.
        inlineWithExpressions();

        // Rewrites several variable-arg functions into their corresponding internal list-input functions.
        rewriteListInputFunctions();

        // Inlines functions.
        inlineDeclaredUdfs();

        // Rewrites function names.
        // This should be done after inlineDeclaredUdfs() because user-defined function
        // names could be case sensitive.
        rewriteFunctionNames();

        // Rewrites distinct aggregates into regular aggregates
        rewriteDistinctAggregations();

        // Sets the var counter of the query.
        topStatement.setVarCounter(context.getVarCounter());
    }

    protected void rewriteGroupByAggregationSugar() throws CompilationException {
        SqlppGroupByAggregationSugarVisitor visitor = new SqlppGroupByAggregationSugarVisitor(context);
        topExpr.accept(visitor, null);
    }

    protected void rewriteDistinctAggregations() throws CompilationException {
        SqlppDistinctAggregationSugarVisitor distinctAggregationVisitor =
                new SqlppDistinctAggregationSugarVisitor(context);
        topExpr.accept(distinctAggregationVisitor, null);
    }

    protected void rewriteListInputFunctions() throws CompilationException {
        SqlppListInputFunctionRewriteVisitor listInputFunctionVisitor = new SqlppListInputFunctionRewriteVisitor();
        topExpr.accept(listInputFunctionVisitor, null);
    }

    protected void rewriteFunctionNames() throws CompilationException {
        SqlppBuiltinFunctionRewriteVisitor functionNameMapVisitor = new SqlppBuiltinFunctionRewriteVisitor();
        topExpr.accept(functionNameMapVisitor, null);
    }

    protected void inlineWithExpressions() throws CompilationException {
        String inlineWith = metadataProvider.getConfig().get(INLINE_WITH);
        if (inlineWith != null && inlineWith.equalsIgnoreCase(NOT_INLINE_WITH)) {
            return;
        }
        // Inlines with expressions.
        InlineWithExpressionVisitor inlineWithExpressionVisitor = new InlineWithExpressionVisitor(context);
        topExpr.accept(inlineWithExpressionVisitor, null);
    }

    protected void generateColumnNames() throws CompilationException {
        // Generate column names if they are missing in the user query.
        GenerateColumnNameVisitor generateColumnNameVisitor = new GenerateColumnNameVisitor(context);
        topExpr.accept(generateColumnNameVisitor, null);
    }

    protected void substituteGroupbyKeyExpression() throws CompilationException {
        // Substitute group-by key expressions that appear in the select clause.
        SubstituteGroupbyExpressionWithVariableVisitor substituteGbyExprVisitor =
                new SubstituteGroupbyExpressionWithVariableVisitor(context);
        topExpr.accept(substituteGbyExprVisitor, null);
    }

    protected void rewriteSetOperations() throws CompilationException {
        // Rewrites set operation queries that contain order-by and limit clauses.
        SetOperationVisitor setOperationVisitor = new SetOperationVisitor(context);
        topExpr.accept(setOperationVisitor, null);
    }

    protected void rewriteOperatorExpression() throws CompilationException {
        // Rewrites like/not-like/in/not-in operators into function call expressions.
        OperatorExpressionVisitor operatorExpressionVisitor = new OperatorExpressionVisitor(context);
        topExpr.accept(operatorExpressionVisitor, null);
    }

    protected void inlineColumnAlias() throws CompilationException {
        // Inline column aliases.
        InlineColumnAliasVisitor inlineColumnAliasVisitor = new InlineColumnAliasVisitor(context);
        topExpr.accept(inlineColumnAliasVisitor, null);
    }

    protected void variableCheckAndRewrite() throws CompilationException {
        VariableCheckAndRewriteVisitor variableCheckAndRewriteVisitor =
                new VariableCheckAndRewriteVisitor(context, metadataProvider, topExpr.getExternalVars());
        topExpr.accept(variableCheckAndRewriteVisitor, null);
    }

    protected void rewriteGroupBys() throws CompilationException {
        SqlppGroupByVisitor groupByVisitor = new SqlppGroupByVisitor(context);
        topExpr.accept(groupByVisitor, null);
    }

    protected void inlineDeclaredUdfs() throws CompilationException {
        List<FunctionSignature> funIds = new ArrayList<FunctionSignature>();
        for (FunctionDecl fdecl : declaredFunctions) {
            funIds.add(fdecl.getSignature());
        }

        List<FunctionDecl> usedStoredFunctionDecls = new ArrayList<>();
        for (Expression topLevelExpr : topExpr.getDirectlyEnclosedExpressions()) {
            usedStoredFunctionDecls.addAll(FunctionUtil.retrieveUsedStoredFunctions(metadataProvider, topLevelExpr,
                    funIds, null, expr -> getFunctionCalls(expr), func -> functionRepository.getFunctionDecl(func),
                    signature -> FunctionMapUtil.normalizeBuiltinFunctionSignature(signature, false)));
        }
        declaredFunctions.addAll(usedStoredFunctionDecls);
        if (!declaredFunctions.isEmpty()) {
            SqlppInlineUdfsVisitor visitor = new SqlppInlineUdfsVisitor(context,
                    new SqlppFunctionBodyRewriterFactory() /* the rewriter for function bodies expressions*/,
                    declaredFunctions, metadataProvider);
            while (topExpr.accept(visitor, declaredFunctions)) {
                // loop until no more changes
            }
        }
        declaredFunctions.removeAll(usedStoredFunctionDecls);
    }

    private Set<FunctionSignature> getFunctionCalls(Expression expression) throws CompilationException {
        GatherFunctionCalls gfc = new GatherFunctionCalls();
        expression.accept(gfc, null);
        return gfc.getCalls();
    }

    private static class GatherFunctionCalls extends GatherFunctionCallsVisitor implements ISqlppVisitor<Void, Void> {

        public GatherFunctionCalls() {
        }

        @Override
        public Void visit(FromClause fromClause, Void arg) throws CompilationException {
            for (FromTerm fromTerm : fromClause.getFromTerms()) {
                fromTerm.accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(FromTerm fromTerm, Void arg) throws CompilationException {
            fromTerm.getLeftExpression().accept(this, arg);
            for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
                correlateClause.accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(JoinClause joinClause, Void arg) throws CompilationException {
            joinClause.getRightExpression().accept(this, arg);
            joinClause.getConditionExpression().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(NestClause nestClause, Void arg) throws CompilationException {
            nestClause.getRightExpression().accept(this, arg);
            nestClause.getConditionExpression().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(Projection projection, Void arg) throws CompilationException {
            if (!projection.star()) {
                projection.getExpression().accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(SelectBlock selectBlock, Void arg) throws CompilationException {
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
        public Void visit(SelectClause selectClause, Void arg) throws CompilationException {
            if (selectClause.selectElement()) {
                selectClause.getSelectElement().accept(this, arg);
            } else {
                selectClause.getSelectRegular().accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(SelectElement selectElement, Void arg) throws CompilationException {
            selectElement.getExpression().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(SelectRegular selectRegular, Void arg) throws CompilationException {
            for (Projection projection : selectRegular.getProjections()) {
                projection.accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(SelectSetOperation selectSetOperation, Void arg) throws CompilationException {
            selectSetOperation.getLeftInput().accept(this, arg);
            for (SetOperationRight setOperationRight : selectSetOperation.getRightInputs()) {
                setOperationRight.getSetOperationRightInput().accept(this, arg);
            }
            return null;
        }

        @Override
        public Void visit(SelectExpression selectStatement, Void arg) throws CompilationException {
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
        public Void visit(UnnestClause unnestClause, Void arg) throws CompilationException {
            unnestClause.getRightExpression().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(HavingClause havingClause, Void arg) throws CompilationException {
            havingClause.getFilterExpression().accept(this, arg);
            return null;
        }

        @Override
        public Void visit(CaseExpression caseExpression, Void arg) throws CompilationException {
            caseExpression.getConditionExpr().accept(this, arg);
            for (Expression expr : caseExpression.getWhenExprs()) {
                expr.accept(this, arg);
            }
            for (Expression expr : caseExpression.getThenExprs()) {
                expr.accept(this, arg);
            }
            caseExpression.getElseExpr().accept(this, arg);
            return null;
        }

    }
}
