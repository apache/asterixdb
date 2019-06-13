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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.common.visitor.GatherFunctionCallsVisitor;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
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
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.parser.FunctionParser;
import org.apache.asterix.lang.sqlpp.parser.SqlppParserFactory;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.GenerateColumnNameVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.InlineColumnAliasVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.InlineWithExpressionVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.OperatorExpressionVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SetOperationVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppBuiltinFunctionRewriteVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppGroupByAggregationSugarVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppGroupByVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppInlineUdfsVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppListInputFunctionRewriteVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppWindowAggregationSugarVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppWindowRewriteVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SubstituteGroupbyExpressionWithVariableVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.VariableCheckAndRewriteVisitor;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppAstPrintUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SqlppQueryRewriter implements IQueryRewriter {

    private static final Logger LOGGER = LogManager.getLogger(SqlppQueryRewriter.class);

    public static final String INLINE_WITH_OPTION = "inline_with";
    private static final boolean INLINE_WITH_OPTION_DEFAULT = true;
    private final FunctionParser functionRepository = new FunctionParser(new SqlppParserFactory());
    private IReturningStatement topExpr;
    private List<FunctionDecl> declaredFunctions;
    private LangRewritingContext context;
    private MetadataProvider metadataProvider;
    private Collection<VarIdentifier> externalVars;
    private boolean isLogEnabled;

    protected void setup(List<FunctionDecl> declaredFunctions, IReturningStatement topExpr,
            MetadataProvider metadataProvider, LangRewritingContext context, Collection<VarIdentifier> externalVars)
            throws CompilationException {
        this.topExpr = topExpr;
        this.context = context;
        this.declaredFunctions = declaredFunctions;
        this.metadataProvider = metadataProvider;
        this.externalVars = externalVars;
        this.isLogEnabled = LOGGER.isTraceEnabled();
        logExpression("Starting AST rewrites on", "");
    }

    @Override
    public void rewrite(List<FunctionDecl> declaredFunctions, IReturningStatement topStatement,
            MetadataProvider metadataProvider, LangRewritingContext context, boolean inlineUdfs,
            Collection<VarIdentifier> externalVars) throws CompilationException {
        if (topStatement == null) {
            return;
        }

        // Sets up parameters.
        setup(declaredFunctions, topStatement, metadataProvider, context, externalVars);

        // Generates column names.
        generateColumnNames();

        // Substitutes group-by key expressions.
        substituteGroupbyKeyExpression();

        // Group-by core rewrites
        rewriteGroupBys();

        // Rewrites set operations.
        rewriteSetOperations();

        // Inlines column aliases.
        inlineColumnAlias();

        // Window expression core rewrites.
        rewriteWindowExpressions();

        // Generate ids for variables (considering scopes) and replace global variable access with the dataset function.
        variableCheckAndRewrite();

        // Rewrites SQL-92 aggregate functions
        rewriteGroupByAggregationSugar();

        // Rewrite window expression aggregations.
        rewriteWindowAggregationSugar();

        // Rewrites like/not-like expressions.
        rewriteOperatorExpression();

        // Rewrites several variable-arg functions into their corresponding internal list-input functions.
        rewriteListInputFunctions();

        // Inlines functions.
        inlineDeclaredUdfs(inlineUdfs);

        // Rewrites function names.
        // This should be done after inlineDeclaredUdfs() because user-defined function
        // names could be case sensitive.
        rewriteFunctionNames();

        // Inlines WITH expressions after variableCheckAndRewrite(...) so that the variable scoping for WITH
        // expression is correct.
        inlineWithExpressions();

        // Sets the var counter of the query.
        topStatement.setVarCounter(context.getVarCounter().get());
    }

    protected void rewriteGroupByAggregationSugar() throws CompilationException {
        SqlppGroupByAggregationSugarVisitor visitor = new SqlppGroupByAggregationSugarVisitor(context);
        rewriteTopExpr(visitor, null);
    }

    protected void rewriteListInputFunctions() throws CompilationException {
        SqlppListInputFunctionRewriteVisitor listInputFunctionVisitor = new SqlppListInputFunctionRewriteVisitor();
        rewriteTopExpr(listInputFunctionVisitor, null);
    }

    protected void rewriteFunctionNames() throws CompilationException {
        SqlppBuiltinFunctionRewriteVisitor functionNameMapVisitor = new SqlppBuiltinFunctionRewriteVisitor();
        rewriteTopExpr(functionNameMapVisitor, null);
    }

    protected void inlineWithExpressions() throws CompilationException {
        if (!metadataProvider.getBooleanProperty(INLINE_WITH_OPTION, INLINE_WITH_OPTION_DEFAULT)) {
            return;
        }
        // Inlines with expressions.
        InlineWithExpressionVisitor inlineWithExpressionVisitor = new InlineWithExpressionVisitor(context);
        rewriteTopExpr(inlineWithExpressionVisitor, null);
    }

    protected void generateColumnNames() throws CompilationException {
        // Generate column names if they are missing in the user query.
        GenerateColumnNameVisitor generateColumnNameVisitor = new GenerateColumnNameVisitor(context);
        rewriteTopExpr(generateColumnNameVisitor, null);
    }

    protected void substituteGroupbyKeyExpression() throws CompilationException {
        // Substitute group-by key expressions that appear in the select clause.
        SubstituteGroupbyExpressionWithVariableVisitor substituteGbyExprVisitor =
                new SubstituteGroupbyExpressionWithVariableVisitor(context);
        rewriteTopExpr(substituteGbyExprVisitor, null);
    }

    protected void rewriteSetOperations() throws CompilationException {
        // Rewrites set operation queries that contain order-by and limit clauses.
        SetOperationVisitor setOperationVisitor = new SetOperationVisitor(context);
        rewriteTopExpr(setOperationVisitor, null);
    }

    protected void rewriteOperatorExpression() throws CompilationException {
        // Rewrites like/not-like/in/not-in operators into function call expressions.
        OperatorExpressionVisitor operatorExpressionVisitor = new OperatorExpressionVisitor(context);
        rewriteTopExpr(operatorExpressionVisitor, null);
    }

    protected void inlineColumnAlias() throws CompilationException {
        // Inline column aliases.
        InlineColumnAliasVisitor inlineColumnAliasVisitor = new InlineColumnAliasVisitor(context);
        rewriteTopExpr(inlineColumnAliasVisitor, null);
    }

    protected void variableCheckAndRewrite() throws CompilationException {
        VariableCheckAndRewriteVisitor variableCheckAndRewriteVisitor =
                new VariableCheckAndRewriteVisitor(context, metadataProvider, externalVars);
        rewriteTopExpr(variableCheckAndRewriteVisitor, null);
    }

    protected void rewriteGroupBys() throws CompilationException {
        SqlppGroupByVisitor groupByVisitor = new SqlppGroupByVisitor(context);
        rewriteTopExpr(groupByVisitor, null);
    }

    protected void rewriteWindowExpressions() throws CompilationException {
        // Create window variables and extract aggregation inputs into LET clauses
        SqlppWindowRewriteVisitor windowVisitor = new SqlppWindowRewriteVisitor(context);
        rewriteTopExpr(windowVisitor, null);
    }

    protected void rewriteWindowAggregationSugar() throws CompilationException {
        SqlppWindowAggregationSugarVisitor windowVisitor = new SqlppWindowAggregationSugarVisitor(context);
        rewriteTopExpr(windowVisitor, null);
    }

    protected void inlineDeclaredUdfs(boolean inlineUdfs) throws CompilationException {
        List<FunctionSignature> funIds = new ArrayList<FunctionSignature>();
        for (FunctionDecl fdecl : declaredFunctions) {
            funIds.add(fdecl.getSignature());
        }

        List<FunctionDecl> usedStoredFunctionDecls = new ArrayList<>();
        for (Expression topLevelExpr : topExpr.getDirectlyEnclosedExpressions()) {
            usedStoredFunctionDecls.addAll(FunctionUtil.retrieveUsedStoredFunctions(metadataProvider, topLevelExpr,
                    funIds, null, expr -> getFunctionCalls(expr), func -> functionRepository.getFunctionDecl(func),
                    (signature, sourceLoc) -> FunctionMapUtil.normalizeBuiltinFunctionSignature(signature, false,
                            sourceLoc)));
        }
        declaredFunctions.addAll(usedStoredFunctionDecls);
        if (inlineUdfs && !declaredFunctions.isEmpty()) {
            SqlppInlineUdfsVisitor visitor = new SqlppInlineUdfsVisitor(context,
                    new SqlppFunctionBodyRewriterFactory() /* the rewriter for function bodies expressions*/,
                    declaredFunctions, metadataProvider);
            while (rewriteTopExpr(visitor, declaredFunctions)) {
                // loop until no more changes
            }
        }
        declaredFunctions.removeAll(usedStoredFunctionDecls);
    }

    private <R, T> R rewriteTopExpr(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        R result = topExpr.accept(visitor, arg);
        logExpression(">>>> AST After", visitor.getClass().getSimpleName());
        return result;
    }

    private void logExpression(String p0, String p1) throws CompilationException {
        if (isLogEnabled) {
            LOGGER.trace("{} {}\n{}", p0, p1, LogRedactionUtil.userData(SqlppAstPrintUtil.toString(topExpr)));
        }
    }

    @Override
    public Set<CallExpr> getFunctionCalls(Expression expression) throws CompilationException {
        GatherFunctionCalls gfc = new GatherFunctionCalls();
        expression.accept(gfc, null);
        return gfc.getCalls();
    }

    @Override
    public Set<VariableExpr> getExternalVariables(Expression expr) throws CompilationException {
        Set<VariableExpr> freeVars = SqlppVariableUtil.getFreeVariables(expr);

        Set<VariableExpr> extVars = new HashSet<>();
        for (VariableExpr ve : freeVars) {
            if (SqlppVariableUtil.isExternalVariableReference(ve)) {
                extVars.add(ve);
            }
        }
        return extVars;
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
            if (selectBlock.hasLetWhereClauses()) {
                for (AbstractClause letWhereClause : selectBlock.getLetWhereList()) {
                    letWhereClause.accept(this, arg);
                }
            }
            if (selectBlock.hasGroupbyClause()) {
                selectBlock.getGroupbyClause().accept(this, arg);
            }
            if (selectBlock.hasLetHavingClausesAfterGroupby()) {
                for (AbstractClause letHavingClause : selectBlock.getLetHavingListAfterGroupby()) {
                    letHavingClause.accept(this, arg);
                }
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

        @Override
        public Void visit(WindowExpression winExpr, Void arg) throws CompilationException {
            if (winExpr.hasPartitionList()) {
                for (Expression expr : winExpr.getPartitionList()) {
                    expr.accept(this, arg);
                }
            }
            if (winExpr.hasOrderByList()) {
                for (Expression expr : winExpr.getOrderbyList()) {
                    expr.accept(this, arg);
                }
            }
            if (winExpr.hasFrameStartExpr()) {
                winExpr.getFrameStartExpr().accept(this, arg);
            }
            if (winExpr.hasFrameEndExpr()) {
                winExpr.getFrameEndExpr().accept(this, arg);
            }
            if (winExpr.hasWindowFieldList()) {
                for (Pair<Expression, Identifier> p : winExpr.getWindowFieldList()) {
                    p.first.accept(this, arg);
                }
            }
            for (Expression expr : winExpr.getExprList()) {
                expr.accept(this, arg);
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
    }
}
