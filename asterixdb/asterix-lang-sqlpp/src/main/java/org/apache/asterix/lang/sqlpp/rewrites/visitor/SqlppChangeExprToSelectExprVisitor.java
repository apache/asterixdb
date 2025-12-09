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
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.FalseLiteral;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.UpdateStatement;
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
 *    SET u.addresses = array-concat(
 *    (SELECT element $addresses FROM check-list(u.addresses) AS $addresses WHERE posVar < 2),
 *    ListConstructor( {"city": "Irvine, United States", "street" : "Second st","zipcode" : 98128})
 *    (SELECT element $addresses FROM check-list(u.addresses) AS $addresses WHERE posVar >= 2))
 * </pre>
 * <p>2. UPDATE UserTypes AS u
 * <pre>
 *    (DELETE FROM u.addresses AT number WHERE number IN [0,1])
 *    WHERE u.userId =11;
 * it gets rewritten to:
 *    SET u.addresses =(SELECT element $addresses FROM check-list(u.addresses) AS $addresses WHERE $number
 *    NOT IN [0,1])
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
 *    FROM check-list(u.roles) AS $r
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
                    throw new CompilationException("Cannot set primary key field: " + pathExpr);
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
            projectExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.RECORD_TRANSFORM),
                    new ArrayList<>(Arrays.asList(dataTransformRecord, projectExpr)));
            ((CallExpr) projectExpr).setSourceLocation(rewrittenFirstExpr.getSourceLocation());
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
        CallExpr sourceCollectionCallExpr =
                new CallExpr(new FunctionSignature(BuiltinFunctions.CHECK_LIST), Collections.singletonList(pathExpr));
        sourceCollectionCallExpr.setSourceLocation(pathExpr.getSourceLocation());
        VariableExpr aliasVar = changeExpr.getAliasVar();
        FromTerm fromTerm = new FromTerm(sourceCollectionCallExpr, aliasVar, changeExpr.getPosVar(), null);
        fromTerm.setSourceLocation(aliasVar.getSourceLocation());
        FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
        fromClause.setSourceLocation(aliasVar.getSourceLocation());
        Expression cond =
                changeExpr.getCondition() != null ? changeExpr.getCondition() : new LiteralExpr(TrueLiteral.INSTANCE);
        LiteralExpr trueLiteral = new LiteralExpr(TrueLiteral.INSTANCE);
        IndexAccessor inverseListifyExpr = new IndexAccessor(changeExpr.getChangeSeq(), IndexAccessor.IndexKind.ELEMENT,
                new LiteralExpr(new IntegerLiteral(0)));
        //CASE
        //  WHEN true THEN updated_value-- from inverseListifyExpr
        //  ELSE original_value-- from aliasVar
        //END
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
        changeExpr.setValueExprs(Collections.singletonList(selectExpression));
        return changeExpr;
    }

    public ChangeExpression delete(ChangeExpression changeExpr) throws CompilationException {
        changeExpr.setChangeTargetExpr(visit(changeExpr.getChangeTargetExpr(), changeExpr));
        String originalVarName = generatedToOriginalVarMap.get(currentContextVariable.getVar().getValue());
        Expression pathExpr = substituteVariableInExpression(changeExpr.getChangeTargetExpr(), originalVarName,
                currentContextVariable, changeExpr);
        CallExpr sourceCollectionCallExpr =
                new CallExpr(new FunctionSignature(BuiltinFunctions.CHECK_LIST), Collections.singletonList(pathExpr));
        sourceCollectionCallExpr.setSourceLocation(pathExpr.getSourceLocation());
        VariableExpr aliasVar = changeExpr.getAliasVar();
        FromTerm fromTerm = new FromTerm(sourceCollectionCallExpr, aliasVar, changeExpr.getPosVar(), null);
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
        changeExpr.setValueExprs(Collections.singletonList(selectExpression));
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
        // ******** Prepare the leading slice of array *******
        {
            CallExpr sourceCollectionCallExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.CHECK_LIST),
                    Collections.singletonList(pathExpr));
            sourceCollectionCallExpr.setSourceLocation(pathExpr.getSourceLocation());
            // From clause.
            VariableExpr posVar = new VariableExpr(new VarIdentifier(UUID.randomUUID().toString()));
            VariableExpr aliasVar = changeExpr.getAliasVar();
            FromTerm fromTerm = new FromTerm(sourceCollectionCallExpr, aliasVar, posVar, null);
            fromTerm.setSourceLocation(aliasVar.getSourceLocation());
            FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
            fromClause.setSourceLocation(aliasVar.getSourceLocation());
            // Where clause
            Expression whereExpr = new LiteralExpr(TrueLiteral.INSTANCE);
            if (changeExpr.getPosExpr() != null) {
                CallExpr ensureBigIntCallExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.CHECK_INTEGER),
                        Collections.singletonList(changeExpr.getPosExpr()));
                sourceCollectionCallExpr.setSourceLocation(changeExpr.getPosExpr().getSourceLocation());
                List<Expression> lessThanOperArgs = new ArrayList<>(2);
                lessThanOperArgs.add(posVar);
                lessThanOperArgs.add(ensureBigIntCallExpr);
                whereExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.LT), lessThanOperArgs);
                ((CallExpr) whereExpr).setSourceLocation(ensureBigIntCallExpr.getSourceLocation());
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
            sourceListExpression = new CallExpr(new FunctionSignature(BuiltinFunctions.CHECK_LIST),
                    Collections.singletonList(changeExpr.getSourceExpr()));
        }
        // ********* Prepare the trailing slice of array ***********
        {
            CallExpr sourceCollectionCallExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.CHECK_LIST),
                    Collections.singletonList(pathExpr));
            sourceCollectionCallExpr.setSourceLocation(pathExpr.getSourceLocation());
            // From clause.
            VariableExpr posVar = new VariableExpr(new VarIdentifier(UUID.randomUUID().toString()));
            VariableExpr aliasVar = changeExpr.getAliasVar();
            FromTerm fromTerm = new FromTerm(sourceCollectionCallExpr, aliasVar, posVar, null);
            fromTerm.setSourceLocation(aliasVar.getSourceLocation());
            FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
            fromClause.setSourceLocation(aliasVar.getSourceLocation());
            // Where clause
            Expression whereExpr = new LiteralExpr(FalseLiteral.INSTANCE);
            if (changeExpr.getPosExpr() != null) {
                CallExpr ensureBigIntCallExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.CHECK_INTEGER),
                        Collections.singletonList(changeExpr.getPosExpr()));
                sourceCollectionCallExpr.setSourceLocation(changeExpr.getPosExpr().getSourceLocation());
                List<Expression> GEOperArgs = new ArrayList<>(2);
                GEOperArgs.add(posVar);
                GEOperArgs.add(ensureBigIntCallExpr);
                whereExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.GE), GEOperArgs);
                ((CallExpr) whereExpr).setSourceLocation(ensureBigIntCallExpr.getSourceLocation());
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
        changeExpr.setValueExprs(Collections.singletonList(arrayConcatExpr));
        return changeExpr;
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