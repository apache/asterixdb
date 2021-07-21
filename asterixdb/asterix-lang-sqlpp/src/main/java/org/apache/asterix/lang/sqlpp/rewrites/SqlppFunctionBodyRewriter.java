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
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.ViewUtil;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * This rewriter is used to rewrite body expression of user defined functions and views
 */
class SqlppFunctionBodyRewriter extends SqlppQueryRewriter {

    public SqlppFunctionBodyRewriter(IParserFactory parserFactory) {
        super(parserFactory);
    }

    @Override
    public void rewrite(LangRewritingContext context, IReturningStatement topStatement, boolean allowNonStoredUdfCalls,
            boolean inlineUdfsAndViews, Collection<VarIdentifier> externalVars) throws CompilationException {
        if (inlineUdfsAndViews) {
            // When rewriting function or view body we do not inline UDFs or views into it.
            // The main query rewriter will inline everything later, when it processes the query
            throw new CompilationException(ErrorCode.ILLEGAL_STATE, topStatement.getSourceLocation(), "");
        }

        // Sets up parameters.
        setup(context, topStatement, externalVars, allowNonStoredUdfCalls, inlineUdfsAndViews);

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

        // Window expression core rewrites.
        rewriteWindowExpressions();

        // Generate ids for variables (considering scopes) and replace global variable access with the dataset function.
        variableCheckAndRewrite();

        //  Extracts SQL-92 aggregate functions from CASE/IF expressions into LET clauses
        extractAggregatesFromCaseExpressions();

        // Rewrites SQL-92 global aggregations.
        rewriteGroupByAggregationSugar();

        // Rewrite window expression aggregations.
        rewriteWindowAggregationSugar();

        // Rewrites like/not-like expressions.
        rewriteOperatorExpression();

        // Normalizes CASE expressions and rewrites simple ones into switch-case()
        rewriteCaseExpressions();

        // Rewrites several variable-arg functions into their corresponding internal list-input functions.
        rewriteListInputFunctions();

        // Rewrites RIGHT OUTER JOINs into LEFT OUTER JOINs if possible
        rewriteRightJoins();
    }

    static Expression castViewBodyAsType(LangRewritingContext context, Expression bodyExpr, IAType itemType,
            Triple<String, String, String> temporalDataFormat, DatasetFullyQualifiedName viewName,
            SourceLocation sourceLoc) throws CompilationException {
        if (itemType.getTypeTag() != ATypeTag.OBJECT) {
            throw new CompilationException(ErrorCode.COMPILATION_TYPE_UNSUPPORTED, sourceLoc, viewName,
                    itemType.getTypeName());
        }
        ARecordType recordType = (ARecordType) itemType;
        if (recordType.isOpen()) {
            throw new CompilationException(ErrorCode.COMPILATION_TYPE_UNSUPPORTED, sourceLoc, viewName,
                    itemType.getTypeName());
        }
        String[] fieldNames = recordType.getFieldNames();
        IAType[] fieldTypes = recordType.getFieldTypes();
        int n = fieldNames.length;
        if (n == 0) {
            throw new CompilationException(ErrorCode.COMPILATION_TYPE_UNSUPPORTED, sourceLoc, viewName,
                    itemType.getTypeName());
        }
        List<Projection> projections = new ArrayList<>(n);
        VarIdentifier fromVar = context.newVariable();
        for (int i = 0; i < n; i++) {
            String fieldName = fieldNames[i];
            IAType targetType = TypeComputeUtils.getActualType(fieldTypes[i]);
            Expression expr = ViewUtil.createFieldAccessExpression(fromVar, fieldName, sourceLoc);
            expr = ViewUtil.createMissingToNullExpression(expr, sourceLoc); // Default Null handling
            Expression projectExpr =
                    ViewUtil.createTypeConvertExpression(expr, targetType, temporalDataFormat, viewName, sourceLoc);
            projections.add(new Projection(projectExpr, fieldName, false, false));
        }

        VariableExpr fromVarRef = new VariableExpr(fromVar);
        fromVarRef.setSourceLocation(sourceLoc);
        FromClause fromClause =
                new FromClause(Collections.singletonList(new FromTerm(bodyExpr, fromVarRef, null, null)));
        fromClause.setSourceLocation(sourceLoc);
        SelectClause selectClause = new SelectClause(null, new SelectRegular(projections), false);
        selectClause.setSourceLocation(sourceLoc);
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, null, null, null);
        selectBlock.setSourceLocation(sourceLoc);
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        selectSetOperation.setSourceLocation(sourceLoc);
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, true);
        selectExpression.setSourceLocation(sourceLoc);
        return selectExpression;
    }
}
