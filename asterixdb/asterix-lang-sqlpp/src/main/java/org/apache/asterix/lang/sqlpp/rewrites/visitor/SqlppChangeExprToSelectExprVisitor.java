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
package org.apache.asterix.lang.sqlpp.rewrites.visitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.FalseLiteral;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.UpdateStatement;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.UnaryExprType;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.CaseExpression;
import org.apache.asterix.lang.sqlpp.expression.ChangeExpression;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Rewrites UPDATE, DELETE FROM and INSERT INTO expressions within the context
 * of a ChangeExpression to their SET fieldAccessor = SelectExpression counterpart.
 * <p>As an example:
 * <p>1. UPDATE UserTypes AS u
 * <pre>
 *    (INSERT INTO u.addresses AT 2([
 *    {"city": "Irvine, United States", "street" : "Second st","zipcode" : 98128}])) where u.userId= 11;
 * it gets rewritten to:
 *    SET u.addresses = CASE check-list(u.addresses) WHEN true THEN array-concat(
 *    (SELECT element $addresses FROM u.addresses AS $addresses WHERE posVar < 2),
 *    ListConstructor( {"city": "Irvine, United States", "street" : "Second st","zipcode" : 98128})
 *    (SELECT element $addresses FROM u.addresses AS $addresses WHERE posVar >= 2)) ELSE u.addresses END
 * </pre>
 * <p>Note: AT position is 0-based. Negative positions count from the end: AT -1 is last, AT -2 is second-to-last.
 *       Out-of-bounds positions return the original array unchanged.
 * <p>2. UPDATE UserTypes AS u
 * <pre>
 *    (DELETE FROM u.addresses AT number WHERE number IN [0,1])
 *    WHERE u.userId =11;
 * it gets rewritten to:
 *    SET u.addresses = CASE check-list(u.addresses) WHEN true THEN (SELECT element $addresses FROM u.addresses AS $addresses WHERE $number
 *    NOT IN [0,1]) ELSE u.addresses END
 * </pre>
 * <p>3. UPDATE UserTypes As u
 * <pre>
 *    (UPDATE u.roles AS r
 *    SET r.roleName = "Sr. developer"
 *    WHERE r.roleId="role2")
 *    WHERE u.userId = 1;
 * it gets rewritten to:
 *    SET u.roles = (
 *    SELECT CASE WHEN r.roleId="role2" THEN r.roleName = "Sr. developer" ELSE $r END
 *    FROM u.roles AS $r
 * )
 * </pre>
 */

public final class SqlppChangeExprToSelectExprVisitor extends VariableCheckAndRewriteVisitor {

    private static int generatedVarCounter = 0;
    private VariableExpr currentContextVariable = null;
    Map<String, String> generatedToOriginalVarMap = new HashMap<>();
    private final MetadataProvider metadataProvider;
    private String datasetName;
    private Namespace namespace;
    private final SubstituteVisitor substitueVisitor = new SubstituteVisitor();

    private class SubstituteVisitor extends AbstractSqlppSimpleExpressionVisitor {
        private String originalVarName = null;
        private VariableExpr newVar = null;

        @Override
        public Expression visit(VariableExpr varExpr, ILangExpression arg) throws CompilationException {
            return substituteVariableInExpression(varExpr, originalVarName, newVar, arg);
        }

        @Override
        public Expression visit(FieldAccessor fa, ILangExpression arg) throws CompilationException {
            return substituteVariableInExpression(fa, originalVarName, newVar, arg);
        }
    }

    public SqlppChangeExprToSelectExprVisitor(LangRewritingContext context, MetadataProvider metadataProvider,
            Collection<VarIdentifier> externalVars) {
        super(context, metadataProvider, externalVars);
        this.generatedVarCounter = 0;
        this.metadataProvider = metadataProvider;
    }

    @Override
    public Expression visit(UpdateStatement updateStatement, ILangExpression arg) throws CompilationException {
        this.datasetName = updateStatement.getDatasetName();
        this.namespace = updateStatement.getNamespace();
        return super.visit(updateStatement, arg);
    }

    @Override
    public Expression visit(ChangeExpression changeExpr, ILangExpression arg) throws CompilationException {
        Expression rewrittenFirstExpr = changeExpr.getPriorExpr().accept(this, changeExpr);
        VariableExpr newGeneratedVar = generateVarOverContext(rewrittenFirstExpr.getSourceLocation());
        VariableExpr parentVar = currentContextVariable;
        currentContextVariable = newGeneratedVar;

        if (changeExpr.hasPathValueExprs()) {
            changeExpr.setValueExprs(visit(changeExpr.getValueExprs(), arg));
            changeExpr.setPathExprs(visit(changeExpr.getPathExprs(), arg));
        } else {
            if (changeExpr.getType() == ChangeExpression.UpdateType.UPDATE) {
                changeExpr = update(changeExpr);
            } else if (changeExpr.getType() == ChangeExpression.UpdateType.DELETE) {
                changeExpr = delete(changeExpr);
            } else if (changeExpr.getType() == ChangeExpression.UpdateType.INSERT) {
                changeExpr = insert(changeExpr);

            }
        }
        List<Expression> pathExprList = changeExpr.getPathExprs();
        List<String> fieldPath = new ArrayList<>();
        for (Expression pathExpr : pathExprList) {
            try {
                if (isPrimaryKeyField(pathExpr, fieldPath)) {
                    throw new CompilationException(ErrorCode.UPDATE_PRIMARY_KEY, pathExpr.toString());
                }
            } catch (AlgebricksException e) {
                throw new RuntimeException(e);
            }
        }
        changeExpr.populateExprTree();
        changeExpr.createTransformationRecords();
        Expression dataRemovalRecord = changeExpr.getDataRemovalRecord();
        if (dataRemovalRecord != null && dataRemovalRecord.getKind() != Expression.Kind.RECORD_CONSTRUCTOR_EXPRESSION) {
            throw new CompilationException(ErrorCode.UPDATE_SET_EXPR_MISSING_ON_ROOT_PATH,
                    changeExpr.getSourceLocation());
        }

        Expression dataTransformRecord = changeExpr.getDataTransformRecord();
        FromTerm fromTerm = new FromTerm(rewrittenFirstExpr, newGeneratedVar, null, null);
        fromTerm.setSourceLocation(rewrittenFirstExpr.getSourceLocation());
        FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
        fromClause.setSourceLocation(rewrittenFirstExpr.getSourceLocation());

        Expression projectExpr = newGeneratedVar;

        if (dataRemovalRecord != null) {
            changeExpr.setPriorExpr(projectExpr);
            projectExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.RECORD_REMOVE_RECURSIVE),
                    new ArrayList<>(Arrays.asList(dataRemovalRecord, projectExpr)));
            ((CallExpr) projectExpr).setSourceLocation(rewrittenFirstExpr.getSourceLocation());
        }

        if (dataTransformRecord != null) {
            String originalVarName = generatedToOriginalVarMap.get(currentContextVariable.getVar().getValue());
            dataTransformRecord = substituteVariableInExpression(dataTransformRecord, originalVarName,
                    currentContextVariable, changeExpr);
            changeExpr.setPriorExpr(projectExpr);
            if (changeExpr.assignsWholeRecord()) {
                projectExpr = dataTransformRecord;
            } else if (dataTransformRecord.getKind() == Expression.Kind.RECORD_CONSTRUCTOR_EXPRESSION) {
                projectExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.RECORD_TRANSFORM),
                        new ArrayList<>(Arrays.asList(dataTransformRecord, projectExpr)));
                ((CallExpr) projectExpr).setSourceLocation(rewrittenFirstExpr.getSourceLocation());
            } else {
                projectExpr = dataTransformRecord;
            }
        }

        SelectElement selectElement = new SelectElement(projectExpr);
        selectElement.setSourceLocation(projectExpr.getSourceLocation());
        SelectClause selectClause = new SelectClause(selectElement, null, false);
        selectClause.setSourceLocation(projectExpr.getSourceLocation());

        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, null, null, null);
        selectBlock.setSourceLocation(projectExpr.getSourceLocation());
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        selectSetOperation.setSourceLocation(projectExpr.getSourceLocation());
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, true);
        selectExpression.setSourceLocation(projectExpr.getSourceLocation());
        Expression result = visit(selectExpression, arg);
        currentContextVariable = parentVar;
        return result;
    }

    public ChangeExpression update(ChangeExpression changeExpr) throws CompilationException {
        changeExpr.setChangeTargetExpr(visit(changeExpr.getChangeTargetExpr(), changeExpr));
        String originalVarName = generatedToOriginalVarMap.get(currentContextVariable.getVar().getValue());
        Expression pathExpr = substituteVariableInExpression(changeExpr.getChangeTargetExpr(), originalVarName,
                currentContextVariable, changeExpr);
        VariableExpr aliasVar = changeExpr.getAliasVar();
        FromTerm fromTerm = new FromTerm(pathExpr, aliasVar, changeExpr.getPosVar(), null);
        fromTerm.setSourceLocation(aliasVar.getSourceLocation());
        FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
        fromClause.setSourceLocation(aliasVar.getSourceLocation());
        Expression cond =
                changeExpr.getCondition() != null ? changeExpr.getCondition() : new LiteralExpr(TrueLiteral.INSTANCE);
        rewriteNegativeAtIndexLiterals(cond, pathExpr, changeExpr.getPosVar());
        LiteralExpr trueLiteral = new LiteralExpr(TrueLiteral.INSTANCE);
        IndexAccessor inverseListifyExpr = new IndexAccessor(changeExpr.getChangeSeq(), IndexAccessor.IndexKind.ELEMENT,
                new LiteralExpr(new IntegerLiteral(0)));
        CaseExpression caseExpr = new CaseExpression(cond, Collections.singletonList(trueLiteral),
                Collections.singletonList(inverseListifyExpr), aliasVar);
        SelectElement selectElement = new SelectElement(caseExpr);
        selectElement.setSourceLocation(changeExpr.getSourceLocation());
        SelectClause selectClause = new SelectClause(selectElement, null, false);
        selectClause.setSourceLocation(changeExpr.getSourceLocation());
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, null, null, null);
        selectBlock.setSourceLocation(aliasVar.getSourceLocation());
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        selectSetOperation.setSourceLocation(aliasVar.getSourceLocation());
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, true);
        selectExpression.setSourceLocation(aliasVar.getSourceLocation());
        changeExpr.setPathExprs(Collections.singletonList(changeExpr.getChangeTargetExpr()));
        changeExpr.setValueExprs(Collections.singletonList(guardListNestedRewrite(pathExpr, selectExpression)));
        return changeExpr;
    }

    /**
     * When the nested change target is not a list, {@code check-list} warns and the field stays unchanged (ELSE branch).
     */
    private static CaseExpression guardListNestedRewrite(Expression pathExpr, Expression nestedRewrite) {
        CallExpr listPred =
                new CallExpr(new FunctionSignature(BuiltinFunctions.CHECK_LIST), Collections.singletonList(pathExpr));
        listPred.setSourceLocation(pathExpr.getSourceLocation());
        CaseExpression guarded =
                new CaseExpression(listPred, Collections.singletonList(new LiteralExpr(TrueLiteral.INSTANCE)),
                        Collections.singletonList(nestedRewrite), pathExpr);
        guarded.setSourceLocation(pathExpr.getSourceLocation());
        return guarded;
    }

    public ChangeExpression delete(ChangeExpression changeExpr) throws CompilationException {
        changeExpr.setChangeTargetExpr(visit(changeExpr.getChangeTargetExpr(), changeExpr));
        String originalVarName = generatedToOriginalVarMap.get(currentContextVariable.getVar().getValue());
        Expression pathExpr = substituteVariableInExpression(changeExpr.getChangeTargetExpr(), originalVarName,
                currentContextVariable, changeExpr);
        VariableExpr aliasVar = changeExpr.getAliasVar();
        FromTerm fromTerm = new FromTerm(pathExpr, aliasVar, changeExpr.getPosVar(), null);
        fromTerm.setSourceLocation(aliasVar.getSourceLocation());
        FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
        fromClause.setSourceLocation(aliasVar.getSourceLocation());
        // Select clause
        SelectElement selectElement = new SelectElement(aliasVar);
        selectElement.setSourceLocation(changeExpr.getSourceLocation());
        SelectClause selectClause = new SelectClause(selectElement, null, false);
        selectClause.setSourceLocation(changeExpr.getSourceLocation());
        // Where clause.
        WhereClause whereClause;
        Expression condition =
                changeExpr.getCondition() != null ? changeExpr.getCondition() : new LiteralExpr(TrueLiteral.INSTANCE);
        rewriteNegativeAtIndexLiterals(condition, pathExpr, changeExpr.getPosVar());
        FunctionSignature funcSignature = new FunctionSignature(BuiltinFunctions.NOT);
        CallExpr callExpr = new CallExpr(funcSignature, Collections.singletonList(condition));
        callExpr.setSourceLocation(condition.getSourceLocation());
        whereClause = new WhereClause(callExpr);
        whereClause.setSourceLocation(callExpr.getSourceLocation());
        // Construct the select expression.
        SelectBlock selectBlock =
                new SelectBlock(selectClause, fromClause, Collections.singletonList(whereClause), null, null);
        selectBlock.setSourceLocation(aliasVar.getSourceLocation());
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        selectSetOperation.setSourceLocation(aliasVar.getSourceLocation());
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, true);
        selectExpression.setSourceLocation(aliasVar.getSourceLocation());
        changeExpr.setValueExprs(Collections.singletonList(guardListNestedRewrite(pathExpr, selectExpression)));
        changeExpr.setPathExprs(Collections.singletonList(changeExpr.getChangeTargetExpr()));
        return changeExpr;
    }

    public ChangeExpression insert(ChangeExpression changeExpr) throws CompilationException {
        changeExpr.setChangeTargetExpr(visit(changeExpr.getChangeTargetExpr(), changeExpr));
        String originalVarName = generatedToOriginalVarMap.get(currentContextVariable.getVar().getValue());
        Expression pathExpr = substituteVariableInExpression(changeExpr.getChangeTargetExpr(), originalVarName,
                currentContextVariable, changeExpr);
        SelectExpression leadingSliceExpression;
        Expression sourceListExpression;
        SelectExpression trailingSliceExpression;
        // Resolve the insert position once: non-negative is kept as-is; negative becomes len(path) + pos + 1
        // so AT -1 inserts after the last element. The resolved position is then wrapped in
        // check-insert-position(pos, len) which is a runtime no-op on the value but emits a warning
        // when pos is outside [0, len]; the source slice's bounds CASE empties it so the array stays
        // unchanged on out-of-bounds positions.
        Expression resolvedPosExpr = null;
        CallExpr arrayLengthCallExpr = null;
        if (changeExpr.getPosExpr() != null) {
            CallExpr ensureBigIntCallExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.CHECK_INTEGER),
                    Collections.singletonList(changeExpr.getPosExpr()));
            ensureBigIntCallExpr.setSourceLocation(changeExpr.getPosExpr().getSourceLocation());
            arrayLengthCallExpr =
                    new CallExpr(new FunctionSignature(BuiltinFunctions.LEN), Collections.singletonList(pathExpr));
            arrayLengthCallExpr.setSourceLocation(pathExpr.getSourceLocation());
            CallExpr nonNegativeCondExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.GE),
                    Arrays.asList(ensureBigIntCallExpr, new LiteralExpr(new IntegerLiteral(0))));
            nonNegativeCondExpr.setSourceLocation(ensureBigIntCallExpr.getSourceLocation());
            CallExpr lenPlusPosExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.NUMERIC_ADD),
                    Arrays.asList(arrayLengthCallExpr, ensureBigIntCallExpr));
            lenPlusPosExpr.setSourceLocation(ensureBigIntCallExpr.getSourceLocation());
            CallExpr fromEndExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.NUMERIC_ADD),
                    Arrays.asList(lenPlusPosExpr, new LiteralExpr(new IntegerLiteral(1))));
            fromEndExpr.setSourceLocation(ensureBigIntCallExpr.getSourceLocation());
            CaseExpression rawResolvedPosExpr = new CaseExpression(nonNegativeCondExpr,
                    Collections.singletonList(new LiteralExpr(TrueLiteral.INSTANCE)),
                    Collections.singletonList(ensureBigIntCallExpr), fromEndExpr);
            rawResolvedPosExpr.setSourceLocation(ensureBigIntCallExpr.getSourceLocation());
            CallExpr checkPosExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.CHECK_INSERT_POSITION),
                    Arrays.asList(rawResolvedPosExpr, arrayLengthCallExpr));
            checkPosExpr.setSourceLocation(ensureBigIntCallExpr.getSourceLocation());
            resolvedPosExpr = checkPosExpr;
        }
        // ******** Prepare the leading slice of array *******
        {
            // From clause.
            VariableExpr posVar = new VariableExpr(new VarIdentifier(UUID.randomUUID().toString()));
            VariableExpr aliasVar = changeExpr.getAliasVar();
            FromTerm fromTerm = new FromTerm(pathExpr, aliasVar, posVar, null);
            fromTerm.setSourceLocation(aliasVar.getSourceLocation());
            FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
            fromClause.setSourceLocation(aliasVar.getSourceLocation());
            // Where clause
            Expression whereExpr = new LiteralExpr(TrueLiteral.INSTANCE);
            if (changeExpr.getPosExpr() != null) {
                // Compare against resolvedPosExpr (which handles negative positions and emits the
                // out-of-bounds warning via the wrapping check-insert-position call).
                List<Expression> lessThanOperArgs = new ArrayList<>(2);
                lessThanOperArgs.add(posVar);
                lessThanOperArgs.add(resolvedPosExpr);
                whereExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.LT), lessThanOperArgs);
                ((CallExpr) whereExpr).setSourceLocation(resolvedPosExpr.getSourceLocation());
            }
            WhereClause whereClause = new WhereClause(whereExpr);
            whereClause.setSourceLocation(whereExpr.getSourceLocation());
            // Select clause
            SelectElement selectElement = new SelectElement(aliasVar);
            selectElement.setSourceLocation(changeExpr.getSourceLocation());
            SelectClause selectClause = new SelectClause(selectElement, null, false);
            selectClause.setSourceLocation(changeExpr.getSourceLocation());
            // Construct the select expression.
            SelectBlock selectBlock =
                    new SelectBlock(selectClause, fromClause, Collections.singletonList(whereClause), null, null);
            selectBlock.setSourceLocation(aliasVar.getSourceLocation());
            SelectSetOperation selectSetOperation =
                    new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
            selectSetOperation.setSourceLocation(aliasVar.getSourceLocation());
            leadingSliceExpression = new SelectExpression(null, selectSetOperation, null, null, true);
            leadingSliceExpression.setSourceLocation(aliasVar.getSourceLocation());
        }
        // ********* Prepare the source slice of array ***********
        {
            sourceListExpression = changeExpr.getSourceExpr();
            if (resolvedPosExpr != null) {
                // When the resolved position falls outside [0, len], replace the source slice with [] so
                // array_concat(leading, [], trailing) reproduces the original array unchanged.
                CallExpr geZeroExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.GE),
                        Arrays.asList(resolvedPosExpr, new LiteralExpr(new IntegerLiteral(0))));
                geZeroExpr.setSourceLocation(resolvedPosExpr.getSourceLocation());
                CallExpr leLenExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.LE),
                        Arrays.asList(resolvedPosExpr, arrayLengthCallExpr));
                leLenExpr.setSourceLocation(resolvedPosExpr.getSourceLocation());
                CallExpr boundsCheckExpr =
                        new CallExpr(new FunctionSignature(BuiltinFunctions.AND), Arrays.asList(geZeroExpr, leLenExpr));
                boundsCheckExpr.setSourceLocation(resolvedPosExpr.getSourceLocation());
                CallExpr emptyArrayExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.ORDERED_LIST_CONSTRUCTOR),
                        Collections.emptyList());
                emptyArrayExpr.setSourceLocation(resolvedPosExpr.getSourceLocation());
                sourceListExpression = new CaseExpression(boundsCheckExpr,
                        Collections.singletonList(new LiteralExpr(TrueLiteral.INSTANCE)),
                        Collections.singletonList(sourceListExpression), emptyArrayExpr);
            }
        }
        // ********* Prepare the trailing slice of array ***********
        {
            // From clause.
            VariableExpr posVar = new VariableExpr(new VarIdentifier(UUID.randomUUID().toString()));
            VariableExpr aliasVar = changeExpr.getAliasVar();
            FromTerm fromTerm = new FromTerm(pathExpr, aliasVar, posVar, null);
            fromTerm.setSourceLocation(aliasVar.getSourceLocation());
            FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
            fromClause.setSourceLocation(aliasVar.getSourceLocation());
            // Where clause
            Expression whereExpr = new LiteralExpr(FalseLiteral.INSTANCE);
            if (changeExpr.getPosExpr() != null) {
                List<Expression> GEOperArgs = new ArrayList<>(2);
                GEOperArgs.add(posVar);
                GEOperArgs.add(resolvedPosExpr);
                whereExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.GE), GEOperArgs);
                ((CallExpr) whereExpr).setSourceLocation(resolvedPosExpr.getSourceLocation());
            }
            WhereClause whereClause = new WhereClause(whereExpr);
            whereClause.setSourceLocation(whereExpr.getSourceLocation());
            // Select clause
            SelectElement selectElement = new SelectElement(aliasVar);
            selectElement.setSourceLocation(changeExpr.getSourceLocation());
            SelectClause selectClause = new SelectClause(selectElement, null, false);
            selectClause.setSourceLocation(changeExpr.getSourceLocation());
            // Construct the select expression.
            SelectBlock selectBlock =
                    new SelectBlock(selectClause, fromClause, Collections.singletonList(whereClause), null, null);
            selectBlock.setSourceLocation(aliasVar.getSourceLocation());
            SelectSetOperation selectSetOperation =
                    new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
            selectSetOperation.setSourceLocation(aliasVar.getSourceLocation());
            trailingSliceExpression = new SelectExpression(null, selectSetOperation, null, null, true);
            trailingSliceExpression.setSourceLocation(aliasVar.getSourceLocation());
        }
        // ******** Array concat ***********
        List<Expression> concatArgs = new ArrayList<>(3);
        concatArgs.add(leadingSliceExpression);
        concatArgs.add(sourceListExpression);
        concatArgs.add(trailingSliceExpression);
        CallExpr arrayConcatExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.ARRAY_CONCAT), concatArgs);
        // **** Rewrite Path expression of the SetExpression ****
        changeExpr.setPathExprs(Collections.singletonList(pathExpr));
        changeExpr.setValueExprs(Collections.singletonList(guardListNestedRewrite(pathExpr, arrayConcatExpr)));
        return changeExpr;
    }

    /**
     * Turns “index from end” literals into absolute indices: {@code pos = -N} and negative entries in
     * {@code pos IN [...]} / {@code NOT IN} become {@code len(path)+(-N)}. Descends the {@code WHERE} tree
     * ({@code AND}, {@code NOT}, etc.) because the parser does not always leave those comparisons at the root.
     */
    private static void rewriteNegativeAtIndexLiterals(Expression expr, Expression pathExpr, VariableExpr posVar) {
        if (posVar == null || expr == null) {
            return;
        }
        if (expr.getKind() == Expression.Kind.CALL_EXPRESSION) {
            CallExpr call = (CallExpr) expr;
            if ((call.getFunctionSignature()).createFunctionIdentifier() == BuiltinFunctions.NOT
                    && call.getExprList().size() == 1) {
                rewriteNegativeAtIndexLiterals(call.getExprList().get(0), pathExpr, posVar);
            }
            return;
        }
        if (expr.getKind() != Expression.Kind.OP_EXPRESSION) {
            return;
        }
        OperatorExpr opExpr = (OperatorExpr) expr;
        List<OperatorType> opList = opExpr.getOpList();
        List<Expression> exprList = opExpr.getExprList();
        if (opList.size() == 1) {
            String posName = posVar.getVar().getValue();
            OperatorType op = opList.get(0);
            if (op == OperatorType.EQ && exprList.size() >= 2) {
                for (int i = 0; i < 2; i++) {
                    Expression negSide = exprList.get(i);
                    Expression other = exprList.get(1 - i);
                    if (variableHasName(other, posName) && negSide.getKind() == Expression.Kind.UNARY_EXPRESSION
                            && ((UnaryExpr) negSide).getExprType() == UnaryExprType.NEGATIVE) {
                        exprList.set(i, buildLengthPlusNegativeIndex(pathExpr, negSide));
                        break;
                    }
                }
            } else if ((op == OperatorType.IN || op == OperatorType.NOT_IN) && exprList.size() >= 2
                    && variableHasName(exprList.get(0), posName)
                    && exprList.get(1).getKind() == Expression.Kind.LIST_CONSTRUCTOR_EXPRESSION) {
                List<Expression> elems = ((ListConstructor) exprList.get(1)).getExprList();
                for (int i = 0; i < elems.size(); i++) {
                    Expression e = elems.get(i);
                    if (e.getKind() == Expression.Kind.UNARY_EXPRESSION
                            && ((UnaryExpr) e).getExprType() == UnaryExprType.NEGATIVE) {
                        elems.set(i, buildLengthPlusNegativeIndex(pathExpr, e));
                    }
                }
            }
        }
        for (Expression operand : exprList) {
            rewriteNegativeAtIndexLiterals(operand, pathExpr, posVar);
        }
    }

    private static boolean variableHasName(Expression e, String name) {
        return e.getKind() == Expression.Kind.VARIABLE_EXPRESSION
                && name.equals(((VariableExpr) e).getVar().getValue());
    }

    private static Expression buildLengthPlusNegativeIndex(Expression pathExpr, Expression unaryNeg) {
        CallExpr len = new CallExpr(new FunctionSignature(BuiltinFunctions.LEN), Collections.singletonList(pathExpr));
        CallExpr add = new CallExpr(new FunctionSignature(BuiltinFunctions.NUMERIC_ADD), Arrays.asList(len, unaryNeg));
        add.setSourceLocation(unaryNeg.getSourceLocation());
        return add;
    }

    private VariableExpr generateVarOverContext(SourceLocation sourceLoc) throws CompilationException {
        Map<VariableExpr, Set<? extends Scope.SymbolAnnotation>> localVars =
                scopeChecker.getCurrentScope().getLiveVariables(scopeChecker.getPrecedingScope());
        Set<VariableExpr> contextVars = Scope.findVariablesAnnotatedBy(localVars.keySet(),
                SqlppVariableAnnotation.CONTEXT_VARIABLE, localVars, sourceLoc);
        VariableExpr contextVar = pickContextVar(contextVars, null);
        String newGeneratedVar = contextVar.getVar().getValue().concat(Integer.toString(++generatedVarCounter));
        VariableExpr generatedVar = new VariableExpr(new VarIdentifier(newGeneratedVar));
        generatedToOriginalVarMap.put(newGeneratedVar, contextVar.getVar().getValue());
        return generatedVar;
    }

    private Expression substituteVariableInExpression(Expression expr, String originalVarName, VariableExpr newVar,
            ILangExpression arg) throws CompilationException {
        if (expr == null) {
            return null;
        }
        if (expr.getKind() == Expression.Kind.VARIABLE_EXPRESSION) {
            VariableExpr varExpr = (VariableExpr) expr;
            if (varExpr.getVar().getValue().equals(originalVarName)) {
                VariableExpr substitutedVar = new VariableExpr(newVar.getVar());
                substitutedVar.setSourceLocation(varExpr.getSourceLocation());
                return substitutedVar;
            }
            return expr;
        }
        if (expr.getKind() == Expression.Kind.FIELD_ACCESSOR_EXPRESSION) {
            FieldAccessor fieldAccessor = (FieldAccessor) expr;
            Expression leadingExpr = fieldAccessor.getExpr();
            Expression substitutedLeadingExpr =
                    substituteVariableInExpression(leadingExpr, originalVarName, newVar, arg);
            if (substitutedLeadingExpr != leadingExpr) {
                FieldAccessor newFieldAccessor = new FieldAccessor(substitutedLeadingExpr, fieldAccessor.getIdent());
                newFieldAccessor.setSourceLocation(fieldAccessor.getSourceLocation());
                return newFieldAccessor;
            }
            return expr;
        }
        substitueVisitor.originalVarName = originalVarName;
        substitueVisitor.newVar = newVar;
        return expr.accept(substitueVisitor, arg);
    }

    public boolean isPrimaryKeyField(Expression pathExp, List<String> fieldPath) throws AlgebricksException {
        if (pathExp.getKind() != Expression.Kind.FIELD_ACCESSOR_EXPRESSION) {
            return false;
        }
        fieldPath.clear();
        // Extract the complete field path (handles nested fields like fieldX.pk)
        FieldAccessor current = (FieldAccessor) pathExp;
        while (true) {
            fieldPath.addFirst(current.getIdent().getValue());
            Expression expr = current.getExpr();
            if (expr.getKind() == Expression.Kind.FIELD_ACCESSOR_EXPRESSION) {
                current = (FieldAccessor) expr;
            } else if (expr.getKind() == Expression.Kind.VARIABLE_EXPRESSION) {
                // Found the base variable, now check primary keys
                DataverseName dataverseName = namespace.getDataverseName();
                String databaseName = namespace.getDatabaseName();
                Dataset dataset = metadataProvider.findDataset(databaseName, dataverseName, datasetName, false);
                return dataset != null && dataset.getPrimaryKeys().contains(fieldPath);
            } else {
                break;
            }
        }
        return false;
    }
}