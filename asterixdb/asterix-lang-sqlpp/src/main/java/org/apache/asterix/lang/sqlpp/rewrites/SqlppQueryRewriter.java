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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.GenerateColumnNameVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.InlineColumnAliasVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.InlineWithExpressionVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.OperatorExpressionVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SetOperationVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppCaseAggregateExtractionVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppCaseExpressionVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppFunctionCallResolverVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppGatherFunctionCallsVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppGroupByAggregationSugarVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppGroupByVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppGroupingSetsVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppInlineUdfsVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppListInputFunctionRewriteVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppRightJoinRewriteVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppSpecialFunctionNameRewriteVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppWindowAggregationSugarVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlppWindowRewriteVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SubstituteGroupbyExpressionWithVariableVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.VariableCheckAndRewriteVisitor;
import org.apache.asterix.lang.sqlpp.util.SqlppAstPrintUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Function;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SqlppQueryRewriter implements IQueryRewriter {

    private static final Logger LOGGER = LogManager.getLogger(SqlppQueryRewriter.class);

    public static final String INLINE_WITH_OPTION = "inline_with";
    private static final boolean INLINE_WITH_OPTION_DEFAULT = true;
    private final IParserFactory parserFactory;
    private SqlppFunctionBodyRewriter functionBodyRewriter;
    private IReturningStatement topStatement;
    private LangRewritingContext context;
    private MetadataProvider metadataProvider;
    private Collection<VarIdentifier> externalVars;
    private boolean allowNonStoredUdfCalls;
    private boolean inlineUdfs;
    private boolean isLogEnabled;

    public SqlppQueryRewriter(IParserFactory parserFactory) {
        this.parserFactory = parserFactory;
    }

    protected void setup(LangRewritingContext context, IReturningStatement topStatement,
            Collection<VarIdentifier> externalVars, boolean allowNonStoredUdfCalls, boolean inlineUdfs)
            throws CompilationException {
        this.context = context;
        this.metadataProvider = context.getMetadataProvider();
        this.topStatement = topStatement;
        this.externalVars = externalVars != null ? externalVars : Collections.emptyList();
        this.allowNonStoredUdfCalls = allowNonStoredUdfCalls;
        this.inlineUdfs = inlineUdfs;
        this.isLogEnabled = LOGGER.isTraceEnabled();
        logExpression("Starting AST rewrites on", "");
    }

    @Override
    public void rewrite(LangRewritingContext context, IReturningStatement topStatement, boolean allowNonStoredUdfCalls,
            boolean inlineUdfs, Collection<VarIdentifier> externalVars) throws CompilationException {

        // Sets up parameters.
        setup(context, topStatement, externalVars, allowNonStoredUdfCalls, inlineUdfs);

        // Resolves function calls
        resolveFunctionCalls();

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

        // Rewrites Group-By clauses with multiple grouping sets into UNION ALL
        // Must run after rewriteSetOperations() and before variableCheckAndRewrite()
        rewriteGroupingSets();

        // Generate ids for variables (considering scopes) and replace global variable access with the dataset function.
        variableCheckAndRewrite();

        //  Extracts SQL-92 aggregate functions from CASE/IF expressions into LET clauses
        extractAggregatesFromCaseExpressions();

        // Rewrites SQL-92 aggregate functions
        rewriteGroupByAggregationSugar();

        // Rewrites window expression aggregations.
        rewriteWindowAggregationSugar();

        // Rewrites like/not-like expressions.
        rewriteOperatorExpression();

        // Normalizes CASE expressions and rewrites simple ones into switch-case()
        rewriteCaseExpressions();

        // Rewrites several variable-arg functions into their corresponding internal list-input functions.
        rewriteListInputFunctions();

        // Rewrites RIGHT OUTER JOINs into LEFT OUTER JOINs if possible
        rewriteRightJoins();

        // Inlines functions.
        loadAndInlineDeclaredUdfs();

        // Rewrites SQL++ core aggregate function names into internal names
        rewriteSpecialFunctionNames();

        // Inlines WITH expressions after variableCheckAndRewrite(...) so that the variable scoping for WITH
        // expression is correct.
        //
        // Must run after rewriteSpecialFunctionNames() because it needs to have FunctionInfo
        // for all functions to avoid inlining non-deterministic expressions.
        // (CallExprs with special function names do not have FunctionInfo)
        //
        // Must run after inlineDeclaredUdfs() because we only maintain deterministic modifiers for built-in
        // and external UDFs, therefore need to inline SQL++ UDFs to check the deterministic property.
        inlineWithExpressions();

        // Sets the var counter of the query.
        topStatement.setVarCounter(context.getVarCounter().get());
    }

    protected void rewriteGroupByAggregationSugar() throws CompilationException {
        SqlppGroupByAggregationSugarVisitor visitor = new SqlppGroupByAggregationSugarVisitor(context, externalVars);
        rewriteTopExpr(visitor, null);
    }

    protected void rewriteListInputFunctions() throws CompilationException {
        SqlppListInputFunctionRewriteVisitor listInputFunctionVisitor = new SqlppListInputFunctionRewriteVisitor();
        rewriteTopExpr(listInputFunctionVisitor, null);
    }

    protected void resolveFunctionCalls() throws CompilationException {
        SqlppFunctionCallResolverVisitor visitor =
                new SqlppFunctionCallResolverVisitor(context, allowNonStoredUdfCalls);
        rewriteTopExpr(visitor, null);
    }

    protected void rewriteSpecialFunctionNames() throws CompilationException {
        SqlppSpecialFunctionNameRewriteVisitor visitor = new SqlppSpecialFunctionNameRewriteVisitor();
        rewriteTopExpr(visitor, null);
    }

    protected void inlineWithExpressions() throws CompilationException {
        if (!metadataProvider.getBooleanProperty(INLINE_WITH_OPTION, INLINE_WITH_OPTION_DEFAULT)) {
            return;
        }
        // Inlines with expressions.
        InlineWithExpressionVisitor inlineWithExpressionVisitor =
                new InlineWithExpressionVisitor(context, metadataProvider);
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

    protected void rewriteGroupingSets() throws CompilationException {
        SqlppGroupingSetsVisitor groupingSetsVisitor = new SqlppGroupingSetsVisitor(context);
        rewriteTopExpr(groupingSetsVisitor, null);
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

    protected void extractAggregatesFromCaseExpressions() throws CompilationException {
        SqlppCaseAggregateExtractionVisitor visitor = new SqlppCaseAggregateExtractionVisitor(context);
        rewriteTopExpr(visitor, null);
    }

    protected void rewriteCaseExpressions() throws CompilationException {
        // Normalizes CASE expressions and rewrites simple ones into switch-case()
        SqlppCaseExpressionVisitor visitor = new SqlppCaseExpressionVisitor();
        rewriteTopExpr(visitor, null);
    }

    protected void rewriteRightJoins() throws CompilationException {
        // Rewrites RIGHT OUTER JOINs into LEFT OUTER JOINs if possible
        SqlppRightJoinRewriteVisitor visitor = new SqlppRightJoinRewriteVisitor(context, externalVars);
        rewriteTopExpr(visitor, null);
    }

    protected void loadAndInlineDeclaredUdfs() throws CompilationException {
        Map<FunctionSignature, FunctionDecl> udfs = fetchUserDefinedSqlppFunctions(topStatement);
        FunctionUtil.checkFunctionRecursion(udfs, SqlppGatherFunctionCallsVisitor::new,
                topStatement.getSourceLocation());
        if (!udfs.isEmpty() && inlineUdfs) {
            SqlppInlineUdfsVisitor visitor = new SqlppInlineUdfsVisitor(context, udfs);
            while (rewriteTopExpr(visitor, null)) {
                // loop until no more changes
            }
        }
    }

    private <R, T> R rewriteTopExpr(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        R result = topStatement.accept(visitor, arg);
        logExpression(">>>> AST After", visitor.getClass().getSimpleName());
        return result;
    }

    private void logExpression(String p0, String p1) throws CompilationException {
        if (isLogEnabled) {
            LOGGER.trace("{} {}\n{}", p0, p1, LogRedactionUtil.userData(SqlppAstPrintUtil.toString(topStatement)));
        }
    }

    @Override
    public void getFunctionCalls(Expression expression, Collection<? super AbstractCallExpression> outCalls)
            throws CompilationException {
        SqlppGatherFunctionCallsVisitor gfc = new SqlppGatherFunctionCallsVisitor(outCalls);
        expression.accept(gfc, null);
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

    private Map<FunctionSignature, FunctionDecl> fetchUserDefinedSqlppFunctions(IReturningStatement topExpr)
            throws CompilationException {
        Map<FunctionSignature, FunctionDecl> udfs = new LinkedHashMap<>();

        Deque<AbstractCallExpression> workQueue = new ArrayDeque<>();
        SqlppGatherFunctionCallsVisitor gfc = new SqlppGatherFunctionCallsVisitor(workQueue);
        for (Expression expr : topExpr.getDirectlyEnclosedExpressions()) {
            expr.accept(gfc, null);
        }
        AbstractCallExpression fnCall;
        while ((fnCall = workQueue.poll()) != null) {
            switch (fnCall.getKind()) {
                case CALL_EXPRESSION:
                    FunctionSignature fs = fnCall.getFunctionSignature();
                    DataverseName fsDataverse = fs.getDataverseName();
                    if (fsDataverse == null) {
                        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, fnCall.getSourceLocation(),
                                fs);
                    }
                    if (FunctionUtil.isBuiltinFunctionSignature(fs) || udfs.containsKey(fs)) {
                        continue;
                    }
                    FunctionDecl fd = context.getDeclaredFunctions().get(fs);
                    if (fd == null) {
                        Function function;
                        try {
                            function = metadataProvider.lookupUserDefinedFunction(fs);
                        } catch (AlgebricksException e) {
                            throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, fnCall.getSourceLocation(),
                                    fs.toString());
                        }
                        if (function == null) {
                            throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, fnCall.getSourceLocation(),
                                    fs.toString());
                        }
                        if (function.isExternal()) {
                            continue;
                        }
                        fd = FunctionUtil.parseStoredFunction(function, parserFactory, context.getWarningCollector(),
                                fnCall.getSourceLocation());
                    }
                    prepareFunction(fd);
                    udfs.put(fs, fd);
                    fd.getNormalizedFuncBody().accept(gfc, null);
                    break;
                case WINDOW_EXPRESSION:
                    // there cannot be used-defined window functions
                    break;
                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, fnCall.getSourceLocation(),
                            fnCall.getFunctionSignature().toString(false));
            }
        }
        return udfs;
    }

    private void prepareFunction(FunctionDecl fd) throws CompilationException {
        Expression fnNormBody = fd.getNormalizedFuncBody();
        if (fnNormBody == null) {
            fnNormBody = rewriteFunctionBody(fd);
            fd.setNormalizedFuncBody(fnNormBody);
        }
    }

    private Expression rewriteFunctionBody(FunctionDecl fnDecl) throws CompilationException {
        DataverseName fnDataverseName = fnDecl.getSignature().getDataverseName();
        Dataverse defaultDataverse = metadataProvider.getDefaultDataverse();
        Dataverse fnDataverse;
        if (fnDataverseName == null || fnDataverseName.equals(defaultDataverse.getDataverseName())) {
            fnDataverse = defaultDataverse;
        } else {
            try {
                fnDataverse = metadataProvider.findDataverse(fnDataverseName);
            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, e, fnDecl.getSourceLocation(),
                        fnDataverseName);
            }
        }

        metadataProvider.setDefaultDataverse(fnDataverse);
        try {
            Query wrappedQuery = new Query(false);
            wrappedQuery.setSourceLocation(fnDecl.getSourceLocation());
            wrappedQuery.setBody(fnDecl.getFuncBody());
            wrappedQuery.setTopLevel(false);
            boolean allowNonStoredUdfCalls = !fnDecl.isStored();
            getFunctionBodyRewriter().rewrite(context, wrappedQuery, allowNonStoredUdfCalls, false,
                    fnDecl.getParamList());
            return wrappedQuery.getBody();
        } catch (CompilationException e) {
            throw new CompilationException(ErrorCode.COMPILATION_BAD_FUNCTION_DEFINITION, e, fnDecl.getSignature(),
                    e.getMessage());
        } finally {
            metadataProvider.setDefaultDataverse(defaultDataverse);
        }
    }

    protected SqlppFunctionBodyRewriter getFunctionBodyRewriter() {
        if (functionBodyRewriter == null) {
            functionBodyRewriter = new SqlppFunctionBodyRewriter(parserFactory);
        }
        return functionBodyRewriter;
    }
}
