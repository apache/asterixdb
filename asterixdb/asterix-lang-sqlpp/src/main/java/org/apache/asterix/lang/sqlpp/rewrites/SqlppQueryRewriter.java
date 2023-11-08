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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.api.INamespaceResolver;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IQueryRewriter;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.expression.AbstractCallExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.MissingLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.common.util.ViewUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.GenerateColumnNameVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.InlineColumnAliasVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.InlineWithExpressionVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.OperatorExpressionVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SelectExcludeRewriteSugarVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SetOperationVisitor;
import org.apache.asterix.lang.sqlpp.rewrites.visitor.SqlCompatRewriteVisitor;
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
import org.apache.asterix.metadata.bootstrap.MetadataBuiltinEntities;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.ViewDetails;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.TypeUtil;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SqlppQueryRewriter implements IQueryRewriter {

    private static final Logger LOGGER = LogManager.getLogger(SqlppQueryRewriter.class);

    public static final String INLINE_WITH_OPTION = "inline_with";
    private static final boolean INLINE_WITH_OPTION_DEFAULT = true;

    public static final String SQL_COMPAT_OPTION = "sql_compat";
    private static final boolean SQL_COMPAT_OPTION_DEFAULT = false;

    private final IParserFactory parserFactory;
    private SqlppFunctionBodyRewriter functionAndViewBodyRewriter;
    private IReturningStatement topStatement;
    private LangRewritingContext context;
    private MetadataProvider metadataProvider;
    private Collection<VarIdentifier> externalVars;
    private boolean allowNonStoredUdfCalls;
    private boolean inlineUdfsAndViews;
    private boolean isLogEnabled;

    public SqlppQueryRewriter(IParserFactory parserFactory) {
        this.parserFactory = parserFactory;
    }

    protected void setup(LangRewritingContext context, IReturningStatement topStatement,
            Collection<VarIdentifier> externalVars, boolean allowNonStoredUdfCalls, boolean inlineUdfsAndViews)
            throws CompilationException {
        this.context = context;
        this.metadataProvider = context.getMetadataProvider();
        this.topStatement = topStatement;
        this.externalVars = externalVars != null ? externalVars : Collections.emptyList();
        this.allowNonStoredUdfCalls = allowNonStoredUdfCalls;
        this.inlineUdfsAndViews = inlineUdfsAndViews;
        this.isLogEnabled = LOGGER.isTraceEnabled();
        logExpression("Starting AST rewrites on", "");
    }

    @Override
    public void rewrite(LangRewritingContext context, IReturningStatement topStatement, boolean allowNonStoredUdfCalls,
            boolean inlineUdfsAndViews, Collection<VarIdentifier> externalVars) throws CompilationException {

        // Sets up parameters.
        setup(context, topStatement, externalVars, allowNonStoredUdfCalls, inlineUdfsAndViews);

        // Resolves function calls
        resolveFunctionCalls();

        // Generates column names.
        generateColumnNames();

        // SQL-compat mode rewrites
        // Must run after generateColumnNames() because it might need to generate new column names
        // for the new projections that it introduces
        rewriteSqlCompat();

        // Substitutes group-by key expressions.
        substituteGroupbyKeyExpression();

        // Group-by core rewrites
        rewriteGroupBys();

        // Rewrites set operations.
        rewriteSetOperations();

        // Inlines column aliases.
        inlineColumnAlias();

        // Rewrite SELECT EXCLUDE to use OBJECT_REMOVE_FIELDS.
        rewriteSelectExcludeSugar();

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

        // Inlines functions and views
        loadAndInlineUdfsAndViews();

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

    protected void rewriteSqlCompat() throws CompilationException {
        boolean sqlCompatMode = metadataProvider.getBooleanProperty(SQL_COMPAT_OPTION, SQL_COMPAT_OPTION_DEFAULT);
        if (!sqlCompatMode) {
            return;
        }
        SqlCompatRewriteVisitor visitor = new SqlCompatRewriteVisitor(context);
        rewriteTopExpr(visitor, null);
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

    protected void loadAndInlineUdfsAndViews() throws CompilationException {
        Pair<Map<FunctionSignature, FunctionDecl>, Map<DatasetFullyQualifiedName, ViewDecl>> udfAndViewDecls =
                loadUdfsAndViews(topStatement);
        Map<FunctionSignature, FunctionDecl> udfs = udfAndViewDecls.first;
        Map<DatasetFullyQualifiedName, ViewDecl> views = udfAndViewDecls.second;
        if (udfs.isEmpty() && views.isEmpty()) {
            // nothing to do
            return;
        }
        if (ExpressionUtils.hasFunctionOrViewRecursion(udfs, views, SqlppGatherFunctionCallsVisitor::new)) {
            throw new CompilationException(ErrorCode.ILLEGAL_FUNCTION_OR_VIEW_RECURSION,
                    topStatement.getSourceLocation());
        }
        if (inlineUdfsAndViews) {
            SqlppInlineUdfsVisitor visitor = new SqlppInlineUdfsVisitor(context, udfs, views);
            while (rewriteTopExpr(visitor, null)) {
                // loop until no more changes
            }
        }
    }

    protected void rewriteSelectExcludeSugar() throws CompilationException {
        SelectExcludeRewriteSugarVisitor selectExcludeRewriteSugarVisitor =
                new SelectExcludeRewriteSugarVisitor(context);
        rewriteTopExpr(selectExcludeRewriteSugarVisitor, null);
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

    @Override
    public VarIdentifier toExternalVariableName(String statementParameterName) {
        return SqlppVariableUtil.toExternalVariableIdentifier(statementParameterName);
    }

    @Override
    public String toFunctionParameterName(VarIdentifier paramVar) {
        return SqlppVariableUtil.toUserDefinedName(paramVar.getValue());
    }

    private Pair<Map<FunctionSignature, FunctionDecl>, Map<DatasetFullyQualifiedName, ViewDecl>> loadUdfsAndViews(
            IReturningStatement topExpr) throws CompilationException {
        Map<FunctionSignature, FunctionDecl> udfs = new LinkedHashMap<>();
        Map<DatasetFullyQualifiedName, ViewDecl> views = new LinkedHashMap<>();
        Deque<AbstractCallExpression> workQueue = new ArrayDeque<>();
        SqlppGatherFunctionCallsVisitor callVisitor = new SqlppGatherFunctionCallsVisitor(workQueue);
        for (Expression expr : topExpr.getDirectlyEnclosedExpressions()) {
            expr.accept(callVisitor, null);
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
                    if (FunctionUtil.isBuiltinFunctionSignature(fs)) {
                        if (FunctionUtil.isBuiltinDatasetFunction(fs)) {
                            Triple<DatasetFullyQualifiedName, Boolean, DatasetFullyQualifiedName> dsArgs =
                                    FunctionUtil.parseDatasetFunctionArguments(fnCall);
                            if (Boolean.TRUE.equals(dsArgs.second)) {
                                DatasetFullyQualifiedName viewName = dsArgs.first;
                                if (!views.containsKey(viewName)) {
                                    ViewDecl viewDecl = fetchViewDecl(viewName, fnCall.getSourceLocation());
                                    views.put(viewName, viewDecl);
                                    viewDecl.getNormalizedViewBody().accept(callVisitor, null);
                                }
                            }
                        }
                    } else {
                        if (!udfs.containsKey(fs)) {
                            FunctionDecl fd = fetchFunctionDecl(fs, fnCall.getSourceLocation());
                            if (fd != null) {
                                udfs.put(fs, fd);
                                fd.getNormalizedFuncBody().accept(callVisitor, null);
                            }
                        }
                    }
                    break;
                case WINDOW_EXPRESSION:
                    // there cannot be used-defined window functions
                    break;
                default:
                    throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, fnCall.getSourceLocation(),
                            fnCall.getFunctionSignature().toString(false));
            }
        }
        return new Pair<>(udfs, views);
    }

    private FunctionDecl fetchFunctionDecl(FunctionSignature fs, SourceLocation sourceLoc) throws CompilationException {
        FunctionDecl fd = context.getDeclaredFunctions().get(fs);
        if (fd == null) {
            Function function;
            try {
                function = metadataProvider.lookupUserDefinedFunction(fs);
            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, sourceLoc, fs.toString());
            }
            if (function == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_FUNCTION, sourceLoc, fs.toString());
            }
            if (function.isExternal()) {
                return null;
            }
            fd = FunctionUtil.parseStoredFunction(function, parserFactory, context.getWarningCollector(), sourceLoc);
        }
        Expression normBody = fd.getNormalizedFuncBody();
        if (normBody == null) {
            normBody = rewriteFunctionBody(fd);
            fd.setNormalizedFuncBody(normBody);
        }
        return fd;
    }

    private ViewDecl fetchViewDecl(DatasetFullyQualifiedName viewName, SourceLocation sourceLoc)
            throws CompilationException {
        IAType viewItemType = null;
        Boolean defaultNull = false;
        Triple<String, String, String> temporalDataFormat = null;
        ViewDecl viewDecl = context.getDeclaredViews().get(viewName);
        if (viewDecl == null) {
            Dataset dataset;
            try {
                dataset = metadataProvider.findDataset(viewName.getDatabaseName(), viewName.getDataverseName(),
                        viewName.getDatasetName(), true);
            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.UNKNOWN_VIEW, e, sourceLoc, viewName);
            }
            if (dataset == null || DatasetUtil.isNotView(dataset)) {
                throw new CompilationException(ErrorCode.UNKNOWN_VIEW, sourceLoc, viewName);
            }
            ViewDetails viewDetails = (ViewDetails) dataset.getDatasetDetails();
            viewDecl = ViewUtil.parseStoredView(viewName, viewDetails, parserFactory, context.getWarningCollector(),
                    sourceLoc);
            DataverseName itemTypeDataverseName = dataset.getItemTypeDataverseName();
            String itemTypeDatabase = dataset.getItemTypeDatabaseName();
            String itemTypeName = dataset.getItemTypeName();
            boolean isAnyType =
                    MetadataBuiltinEntities.ANY_OBJECT_DATATYPE.getDataverseName().equals(itemTypeDataverseName)
                            && MetadataBuiltinEntities.ANY_OBJECT_DATATYPE.getDatatypeName().equals(itemTypeName);
            if (!isAnyType) {
                try {
                    viewItemType = metadataProvider.findType(itemTypeDatabase, itemTypeDataverseName, itemTypeName);
                } catch (AlgebricksException e) {
                    throw new CompilationException(ErrorCode.UNKNOWN_TYPE,
                            TypeUtil.getFullyQualifiedDisplayName(itemTypeDataverseName, itemTypeName));
                }
                defaultNull = viewDetails.getDefaultNull();
                temporalDataFormat = new Triple<>(viewDetails.getDatetimeFormat(), viewDetails.getDateFormat(),
                        viewDetails.getTimeFormat());
            }
        }
        Expression normBody = viewDecl.getNormalizedViewBody();
        if (normBody == null) {
            normBody = rewriteViewBody(viewDecl, viewItemType, defaultNull, temporalDataFormat);
            viewDecl.setNormalizedViewBody(normBody);
        }
        return viewDecl;
    }

    private Expression rewriteFunctionBody(FunctionDecl fnDecl) throws CompilationException {
        FunctionSignature fs = fnDecl.getSignature();
        return rewriteFunctionOrViewBody(fs.getDatabaseName(), fs.getDataverseName(), fs, fnDecl.getFuncBody(),
                fnDecl.getParamList(), !fnDecl.isStored(), fnDecl.getSourceLocation());
    }

    private Expression rewriteViewBody(ViewDecl viewDecl, IAType viewItemType, Boolean defaultNull,
            Triple<String, String, String> temporalDataFormat) throws CompilationException {
        DatasetFullyQualifiedName viewName = viewDecl.getViewName();
        SourceLocation sourceLoc = viewDecl.getSourceLocation();
        Expression rewrittenBodyExpr =
                rewriteFunctionOrViewBody(viewName.getDatabaseName(), viewName.getDataverseName(), viewName,
                        viewDecl.getViewBody(), Collections.emptyList(), false, sourceLoc);
        if (viewItemType != null) {
            if (!Boolean.TRUE.equals(defaultNull)) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc,
                        "Default Null is required");
            }
            rewrittenBodyExpr = SqlppFunctionBodyRewriter.castViewBodyAsType(context, rewrittenBodyExpr, viewItemType,
                    temporalDataFormat, viewName, sourceLoc);
        }
        return rewrittenBodyExpr;
    }

    private Expression rewriteFunctionOrViewBody(String entityDatabaseName, DataverseName entityDataverseName,
            Object entityDisplayName, Expression bodyExpr, List<VarIdentifier> externalVars,
            boolean allowNonStoredUdfCalls, SourceLocation sourceLoc) throws CompilationException {
        Namespace defaultNamespace = metadataProvider.getDefaultNamespace();
        Namespace targetNamespace = null;
        if (entityDataverseName == null || (entityDatabaseName.equals(defaultNamespace.getDatabaseName())
                && entityDataverseName.equals(defaultNamespace.getDataverseName()))) {
            targetNamespace = defaultNamespace;
        } else {
            try {
                Dataverse dv = metadataProvider.findDataverse(entityDatabaseName, entityDataverseName);
                if (dv != null) {
                    targetNamespace = new Namespace(dv.getDatabaseName(), dv.getDataverseName());
                }
            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATAVERSE, e, sourceLoc, MetadataUtil
                        .dataverseName(entityDatabaseName, entityDataverseName, metadataProvider.isUsingDatabase()));
            }
        }

        metadataProvider.setDefaultNamespace(targetNamespace);
        try {
            Query wrappedQuery = ExpressionUtils.createWrappedQuery(bodyExpr, sourceLoc);
            getFunctionAndViewBodyRewriter().rewrite(context, wrappedQuery, allowNonStoredUdfCalls, false,
                    externalVars);
            return wrappedQuery.getBody();
        } catch (CompilationException e) {
            throw new CompilationException(ErrorCode.COMPILATION_BAD_FUNCTION_DEFINITION, e,
                    entityDisplayName.toString(), e.getMessage());
        } finally {
            metadataProvider.setDefaultNamespace(defaultNamespace);
        }
    }

    protected SqlppFunctionBodyRewriter getFunctionAndViewBodyRewriter() {
        if (functionAndViewBodyRewriter == null) {
            functionAndViewBodyRewriter = new SqlppFunctionBodyRewriter(parserFactory);
        }
        return functionAndViewBodyRewriter;
    }

    @Override
    public Query createFunctionAccessorQuery(FunctionDecl functionDecl) {
        // dataverse_name.function_name(MISSING, ... MISSING)
        FunctionSignature functionSignature = functionDecl.getSignature();
        int arity = functionSignature.getArity();
        List<Expression> args = arity == FunctionIdentifier.VARARGS ? Collections.emptyList()
                : Collections.nCopies(arity, new LiteralExpr(MissingLiteral.INSTANCE));
        CallExpr fcall = new CallExpr(functionSignature, args);
        fcall.setSourceLocation(functionDecl.getSourceLocation());
        return ExpressionUtils.createWrappedQuery(fcall, functionDecl.getSourceLocation());
    }

    @Override
    public Query createViewAccessorQuery(ViewDecl viewDecl, INamespaceResolver namespaceResolver) {
        boolean usingDatabase = namespaceResolver.isUsingDatabase();
        // dataverse_name.view_name
        String databaseName = viewDecl.getViewName().getDatabaseName();
        DataverseName dataverseName = viewDecl.getViewName().getDataverseName();
        String viewName = viewDecl.getViewName().getDatasetName();
        Expression vAccessExpr = createDatasetAccessExpression(databaseName, dataverseName, viewName,
                viewDecl.getSourceLocation(), usingDatabase);
        return ExpressionUtils.createWrappedQuery(vAccessExpr, viewDecl.getSourceLocation());
    }

    private static Expression createDatasetAccessExpression(String databaseName, DataverseName dataverseName,
            String datasetName, SourceLocation sourceLoc, boolean usingDatabase) {
        AbstractExpression resultExpr;
        List<String> dataverseNameParts = dataverseName.getParts();
        int startIdx;
        if (usingDatabase) {
            resultExpr = new VariableExpr(new VarIdentifier(SqlppVariableUtil.toInternalVariableName(databaseName)));
            startIdx = 0;
        } else {
            resultExpr = new VariableExpr(
                    new VarIdentifier(SqlppVariableUtil.toInternalVariableName(dataverseNameParts.get(0))));
            startIdx = 1;
        }
        resultExpr.setSourceLocation(sourceLoc);
        for (int i = startIdx, n = dataverseNameParts.size(); i < n; i++) {
            String part = dataverseNameParts.get(i);
            resultExpr = new FieldAccessor(resultExpr, new Identifier(part));
            resultExpr.setSourceLocation(sourceLoc);
        }
        resultExpr = new FieldAccessor(resultExpr, new Identifier(datasetName));
        resultExpr.setSourceLocation(sourceLoc);
        return resultExpr;
    }
}
