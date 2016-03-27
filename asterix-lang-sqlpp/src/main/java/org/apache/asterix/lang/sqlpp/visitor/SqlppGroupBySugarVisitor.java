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
package org.apache.asterix.lang.sqlpp.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableSubstitutionUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

/**
 * An AST pre-processor to rewrite group-by sugar queries.
 */
public class SqlppGroupBySugarVisitor extends VariableCheckAndRewriteVisitor {

    private final Expression groupVar;
    private final Collection<VariableExpr> targetVars;

    public SqlppGroupBySugarVisitor(LangRewritingContext context, AqlMetadataProvider metadataProvider,
            Expression groupVar, Collection<VariableExpr> targetVars) {
        super(context, false, metadataProvider);
        this.groupVar = groupVar;
        this.targetVars = targetVars;
    }

    @Override
    public Expression visit(CallExpr callExpr, Expression arg) throws AsterixException {
        List<Expression> newExprList = new ArrayList<Expression>();
        boolean aggregate = isAggregateFunction(callExpr.getFunctionSignature());
        for (Expression expr : callExpr.getExprList()) {
            Expression newExpr = aggregate ? wrapAggregationArgument(expr) : expr;
            newExprList.add(newExpr.accept(this, arg));
        }
        callExpr.setExprList(newExprList);
        return callExpr;
    }

    private boolean isAggregateFunction(FunctionSignature signature) throws AsterixException {
        IFunctionInfo finfo = FunctionUtil.getFunctionInfo(
                new FunctionIdentifier(FunctionConstants.ASTERIX_NS, signature.getName(), signature.getArity()));
        if (finfo == null) {
            return false;
        }
        return AsterixBuiltinFunctions.getAggregateFunction(finfo.getFunctionIdentifier()) != null;
    }

    private Expression wrapAggregationArgument(Expression expr) throws AsterixException {
        if (expr.getKind() == Kind.SELECT_EXPRESSION) {
            return expr;
        }
        Set<VariableExpr> definedVars = scopeChecker.getCurrentScope().getLiveVariables();
        Set<VariableExpr> vars = new HashSet<>(targetVars);
        vars.remove(definedVars); // Exclude re-defined local variables.
        Set<VariableExpr> usedVars = SqlppRewriteUtil.getUsedVariable(expr);
        if (!vars.containsAll(usedVars)) {
            return expr;
        }
        VariableExpr var = new VariableExpr(context.newVariable());
        FromTerm fromTerm = new FromTerm(groupVar, var, null, null);
        FromClause fromClause = new FromClause(Collections.singletonList(fromTerm));

        // Select clause.
        SelectElement selectElement = new SelectElement(expr);
        SelectClause selectClause = new SelectClause(selectElement, null, false);

        // Construct the select expression.
        SelectBlock selectBlock = new SelectBlock(selectClause, fromClause, null, null, null, null, null);
        SelectSetOperation selectSetOperation = new SelectSetOperation(new SetOperationInput(selectBlock, null), null);
        SelectExpression selectExpression = new SelectExpression(null, selectSetOperation, null, null, false);
        selectExpression.setSubquery(true);

        // replace variable expressions with field access
        Map<VariableExpr, Expression> varExprMap = new HashMap<>();
        for (VariableExpr usedVar : usedVars) {
            varExprMap.put(usedVar,
                    new FieldAccessor(var, SqlppVariableUtil.toUserDefinedVariableName(usedVar.getVar())));
        }
        selectElement.setExpression(
                (Expression) SqlppVariableSubstitutionUtil.substituteVariableWithoutContext(expr, varExprMap));
        return selectExpression;
    }
}
