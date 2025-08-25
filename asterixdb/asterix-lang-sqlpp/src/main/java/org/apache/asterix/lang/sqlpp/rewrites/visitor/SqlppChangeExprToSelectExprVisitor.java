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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.ChangeExpression;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * ChangeSeqExpression has two encapsulating expressions:
 * 1. priorExpr
 * 2. currentExpr
 * where semantically priorExpr feeds into currentExpr (priorExpr ---> currentExpr).
 * This relation gets rewritten as:
 * SELECT ELEMENT data-transform(currentExpr, <GENERATED_VAR>) FROM priorExpr AS <GENERATED_VAR>
 * Note: Very important to run this visitor before variableCheckAndRewrite so that the
 * <GENERATED_VAR> can be used as the new ContextVariable for further operations in the chain of changes.
 */

public final class SqlppChangeExprToSelectExprVisitor extends AbstractSqlppExpressionScopingVisitor {

    public SqlppChangeExprToSelectExprVisitor(LangRewritingContext context, Collection<VarIdentifier> externalVars) {
        super(context, externalVars);
    }

    @Override
    public Expression visit(ChangeExpression changeSeqExpr, ILangExpression arg) throws CompilationException {
        // recurse and rewrite the prior expression
        Expression rewrittenFirstExpr = changeSeqExpr.getPriorExpr().accept(this, changeSeqExpr);

        // rewrite this change expression
        Expression dataRemovalRecord = changeSeqExpr.getDataRemovalRecord();
        Expression dataTransformRecord = changeSeqExpr.getDataTransformRecord();

        //for UPDATE users SET name = "John", age = age + 1 WHERE id = 1
        // the we need the from to be: FROM (SELECT * FROM users WHERE id = 1) AS $generated_var
        VariableExpr newGeneratedVar = generateVarOverContext(rewrittenFirstExpr.getSourceLocation());
        // TODO (abhij) : Is there a need to generate positional variables?
        FromTerm fromTerm = new FromTerm(rewrittenFirstExpr, newGeneratedVar, null, null);
        fromTerm.setSourceLocation(rewrittenFirstExpr.getSourceLocation());
        FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));
        fromClause.setSourceLocation(rewrittenFirstExpr.getSourceLocation());

        Expression projectExpr = newGeneratedVar;

        if (dataRemovalRecord != null) {
            changeSeqExpr.setPriorExpr(projectExpr);
            projectExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.RECORD_REMOVE_RECURSIVE),
                    new ArrayList<>(Arrays.asList(dataRemovalRecord, projectExpr)));
            ((CallExpr) projectExpr).setSourceLocation(rewrittenFirstExpr.getSourceLocation());
        }

        if (dataTransformRecord != null) {
            changeSeqExpr.setPriorExpr(projectExpr);
            projectExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.RECORD_TRANSFORM),
                    new ArrayList<>(Arrays.asList(dataTransformRecord, projectExpr)));
            ((CallExpr) projectExpr).setSourceLocation(rewrittenFirstExpr.getSourceLocation());
        }
        //SELECT ELEMENT record-transform(
        //    {"name": "John", "age": age + 1},
        //    $generated_var
        //)
        //FROM (SELECT * FROM users WHERE id = 1) AS $generated_var
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
        return visit(selectExpression, arg);
    }
}
