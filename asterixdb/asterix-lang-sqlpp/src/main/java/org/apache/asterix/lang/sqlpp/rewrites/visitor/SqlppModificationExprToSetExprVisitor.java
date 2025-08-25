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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.FalseLiteral;
import org.apache.asterix.lang.common.literal.IntegerLiteral;
import org.apache.asterix.lang.common.literal.TrueLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
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
import org.apache.asterix.lang.sqlpp.expression.SetExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * Rewrites UPDATE, DELETE FROM and INSERT INTO expressions within the context
 * of a ChangeExpression to their SET fieldAccessor = SelectExpression counterpart.
 * As an example:
 *
 * 1. UPDATE UserTypes AS u
 *    (INSERT INTO u.addresses AT 2([
 *    {"city": "Irvine, United States", "street" : "Second st","zipcode" : 98128}])) where u.userId= 11;
 * it gets rewritten to:
 *    SET u.addresses = array-concat(
 *    (SELECT element $addresses FROM check-list(u.addresses) AS $addresses WHERE posVar < 2),
 *    ListConstructor( {"city": "Irvine, United States", "street" : "Second st","zipcode" : 98128})
 *    (SELECT element $addresses FROM check-list(u.addresses) AS $addresses WHERE posVar >= 2))
 *
 * 2. UPDATE UserTypes AS u
 *    (DELETE FROM u.addresses AT number WHERE number IN [0,1])
 *    WHERE u.userId =11;
 * it gets rewritten to:
 *    SET u.addresses =(SELECT element $addresses FROM check-list(u.addresses) AS $addresses WHERE $number
 *    NOT IN [0,1])
 *
 * 3. UPDATE UserTypes As u
 *    (UPDATE u.roles AS r
 *    SET r.roleName = "Sr. developer"
 *    WHERE r.roleId="role2")
 *    WHERE u.userId = 1;
 * it gets rewritten to:
 *    SET u.roles = (
 *    SELECT CASE WHEN r.roleId="role2" THEN r.roleName = "Sr. developer" ELSE $r END
 *    FROM check-list(u.roles) AS $r
 * )
 *
 *
 */
public final class SqlppModificationExprToSetExprVisitor extends VariableCheckAndRewriteVisitor {

    public SqlppModificationExprToSetExprVisitor(LangRewritingContext context, MetadataProvider metadataProvider,
            Collection<VarIdentifier> externalVars) {
        super(context, metadataProvider, externalVars);

    }

    @Override
    public Expression visit(VariableExpr varExpr, ILangExpression arg) throws CompilationException {
        if (!(arg instanceof Expression)) {
            return super.visit(varExpr, arg);
        }
        Expression parentExpr = (Expression) arg;
        Expression.Kind kind = parentExpr.getKind();
        if (kind != Expression.Kind.UPDATE_CHANGE_EXPRESSION) {
            return super.visit(varExpr, arg);
        }
        boolean resolvesToContextVar = resolveAsContextVar(varExpr);
        // SetExpression path / UpdateExpr / DeleteExpr are attempting to access a live variable
        // other than the context. This is not allowed.
        if (resolveAsVariableReference(varExpr) && !resolvesToContextVar) {
            throw new CompilationException(ErrorCode.NON_CONTEXT_VARIABLE_ACCESS, varExpr.getSourceLocation(),
                    SqlppVariableUtil.toUserDefinedVariableName(varExpr.getVar().getValue()).getValue());
        }
        if (resolvesToContextVar) {
            VariableExpr contextVar = getContextVar(varExpr);
            if (!contextVar.getVar().equals(varExpr.getVar())) {
                throw new CompilationException(ErrorCode.NON_CONTEXT_VARIABLE_ACCESS, varExpr.getSourceLocation(),
                        SqlppVariableUtil.toUserDefinedVariableName(varExpr.getVar().getValue()).getValue());
            }
            return super.visit(varExpr, arg);
        }
        // Resolves to none of the variables in the scope. Treat it is nested field access.
        return super.visit(resolveAsFieldAccessOverContextVar(varExpr), arg);
    }

    @Override
    public Expression visit(FieldAccessor fa, ILangExpression arg) throws CompilationException {
        if (!(arg instanceof Expression)) {
            return super.visit(fa, arg);
        }
        Expression parentExpr = (Expression) arg;
        Expression.Kind kind = parentExpr.getKind();
        if (kind != Expression.Kind.UPDATE_CHANGE_EXPRESSION) {
            return super.visit(fa, arg);
        }
        Expression leadingExpr = fa.getExpr();
        if (leadingExpr.getKind() == Expression.Kind.VARIABLE_EXPRESSION) {
            // resolving a.b
            VariableExpr leadingVarExpr = (VariableExpr) leadingExpr;
            boolean resolvesToContextVar = resolveAsContextVar(leadingVarExpr);
            // SetExpression path / UpdateExpr / DeleteExpr are attempting to access a live variable
            // other than the context. This is not allowed.
            if (resolveAsVariableReference(leadingVarExpr) && !resolvesToContextVar) {
                throw new CompilationException(ErrorCode.NON_CONTEXT_VARIABLE_ACCESS,
                        leadingVarExpr.getSourceLocation(),
                        SqlppVariableUtil.toUserDefinedVariableName(leadingVarExpr.getVar().getValue()).getValue());
            }
            if (resolvesToContextVar) {
                return super.visit(fa, arg);
            }
            // Resolves to none of the variables in the scope. Treat it is nested field access.
            fa.setExpr(resolveAsFieldAccessOverContextVar(leadingVarExpr));
            return super.visit(fa, arg);
        }
        fa.setExpr(leadingExpr.accept(this, arg));
        return super.visit(fa, arg);
    }

    @Override
    public Expression visit(ChangeExpression changeExpr, ILangExpression arg) throws CompilationException {
        // Once rewritten, these expressions are not UpdateExpr, DeleteExpr or InsertExpr anymore.
        visit(changeExpr.getPriorExpr(), arg);
        if (changeExpr.hasSetExpr()) {
            changeExpr.setSetExpr(visit(changeExpr.getSetExpr(), arg));
        } else {
            if (changeExpr.getType() == ChangeExpression.UpdateType.UPDATE) {
                changeExpr.setSetExpr(update(changeExpr, arg));
            } else if (changeExpr.getType() == ChangeExpression.UpdateType.DELETE) {
                changeExpr.setSetExpr(delete(changeExpr, arg));
            } else if (changeExpr.getType() == ChangeExpression.UpdateType.INSERT) {
                changeExpr.setSetExpr(insert(changeExpr, arg));
            }

        }
        changeExpr.populateExprTree();
        changeExpr.createTransformationRecords();
        Expression dataRemovalRecord = changeExpr.getDataRemovalRecord();
        if (dataRemovalRecord != null && dataRemovalRecord.getKind() != Expression.Kind.RECORD_CONSTRUCTOR_EXPRESSION) {
            throw new CompilationException(ErrorCode.UPDATE_SET_EXPR_MISSING_ON_ROOT_PATH,
                    changeExpr.getSourceLocation());
        }
        return changeExpr;
    }

    public Expression update(ChangeExpression changeExpr, ILangExpression arg) throws CompilationException {
        changeExpr.setPathExpr(visit(changeExpr.getPathExpr(), changeExpr));
        CallExpr sourceCollectionCallExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.CHECK_LIST),
                Collections.singletonList(changeExpr.getPathExpr()));
        sourceCollectionCallExpr.setSourceLocation(changeExpr.getPathExpr().getSourceLocation());
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
        SelectExpression rewrittenSelectExpr = (SelectExpression) visit(selectExpression, arg);
        SetExpression setExpr = new SetExpression(Collections.singletonList(changeExpr.getPathExpr()),
                Collections.singletonList(rewrittenSelectExpr));
        setExpr.setSourceLocation(changeExpr.getSourceLocation());
        return setExpr;
    }

    public Expression delete(ChangeExpression changeExpr, ILangExpression arg) throws CompilationException {
        changeExpr.setPathExpr(visit(changeExpr.getPathExpr(), changeExpr));
        // From clause.
        CallExpr sourceCollectionCallExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.CHECK_LIST),
                Collections.singletonList(changeExpr.getPathExpr()));
        sourceCollectionCallExpr.setSourceLocation(changeExpr.getPathExpr().getSourceLocation());
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
        WhereClause whereClause = null;
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
        // **** Rewrite Path expression of the SetExpression ****
        // Note: Unlike UpdateExpression no need to visit the selectExpression as this does not
        // contain any nested changes
        SetExpression setExpr = new SetExpression(Collections.singletonList(changeExpr.getPathExpr()),
                Collections.singletonList(selectExpression));
        setExpr.setSourceLocation(changeExpr.getSourceLocation());
        return setExpr;
    }

    public Expression insert(ChangeExpression changeExpr, ILangExpression arg) throws CompilationException {
        changeExpr.setPathExpr(visit(changeExpr.getPathExpr(), changeExpr));
        SelectExpression leadingSliceExpression = null;
        Expression sourceListExpression = null;
        SelectExpression trailingSliceExpression = null;
        // ******** Prepare the leading slice of array *******
        {
            CallExpr sourceCollectionCallExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.CHECK_LIST),
                    Collections.singletonList(changeExpr.getPathExpr()));
            sourceCollectionCallExpr.setSourceLocation(changeExpr.getPathExpr().getSourceLocation());
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
                    Collections.singletonList(changeExpr.getPathExpr()));
            sourceCollectionCallExpr.setSourceLocation(changeExpr.getPathExpr().getSourceLocation());
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
        SetExpression setExpr = new SetExpression(Collections.singletonList(changeExpr.getPathExpr()),
                Collections.singletonList(arrayConcatExpr));
        setExpr.setSourceLocation(changeExpr.getSourceLocation());
        return setExpr;
    }
}
