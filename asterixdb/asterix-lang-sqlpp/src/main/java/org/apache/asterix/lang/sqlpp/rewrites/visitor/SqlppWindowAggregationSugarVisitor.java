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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * A pre-processor that adds a window field list into the AST:
 *
 * <pre>
 * FROM ... AS e, ... AS i
 * SELECT fn() OVER (...)
 * ->
 * FROM ... AS e, ... AS i
 * SELECT fn() AS w(e AS e, i AS i) OVER (...)
 * </pre>
 *
 * Also rewrites SQL-92 aggregate functions inside window expressions into SQL++ core aggregate functions
 * using the same approach as {@link SqlppGroupByAggregationSugarVisitor}
 * <br/>
 * Must be executed after {@link VariableCheckAndRewriteVisitor}
 */
public class SqlppWindowAggregationSugarVisitor extends AbstractSqlppExpressionScopingVisitor {

    private final Deque<SelectBlock> stack = new ArrayDeque<>();

    public SqlppWindowAggregationSugarVisitor(LangRewritingContext context) {
        super(context);
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        stack.push(selectBlock);
        Expression expr = super.visit(selectBlock, arg);
        stack.pop();
        return expr;
    }

    @Override
    public Expression visit(WindowExpression winExpr, ILangExpression arg) throws CompilationException {
        if (!winExpr.hasWindowFieldList()) {
            SelectBlock selectBlock = stack.peek();
            List<Pair<Expression, Identifier>> winFieldList = createWindowFieldList(selectBlock);
            winExpr.setWindowFieldList(winFieldList);
        }

        FunctionSignature signature = winExpr.getFunctionSignature();
        FunctionIdentifier winfi = FunctionMapUtil.getInternalWindowFunction(signature);
        if (winfi != null) {
            winExpr.setFunctionSignature(new FunctionSignature(winfi));
            if (BuiltinFunctions.builtinFunctionHasProperty(winfi,
                    BuiltinFunctions.WindowFunctionProperty.HAS_LIST_ARG)) {
                wrapAggregationArguments(winExpr, 1);
            }
        } else if (FunctionMapUtil.isSql92AggregateFunction(signature)) {
            winExpr.setFunctionSignature(FunctionMapUtil.sql92ToCoreAggregateFunction(signature));
            wrapAggregationArguments(winExpr, winExpr.getExprList().size());
        }

        return super.visit(winExpr, arg);
    }

    void wrapAggregationArguments(WindowExpression winExpr, int limit) throws CompilationException {
        VariableExpr winVar = winExpr.getWindowVar();

        Map<VariableExpr, Set<? extends Scope.SymbolAnnotation>> liveAnnotatedVars =
                scopeChecker.getCurrentScope().getLiveVariables();
        Set<VariableExpr> liveVars = liveAnnotatedVars.keySet();
        Set<VariableExpr> liveContextVars = Scope.findVariablesAnnotatedBy(liveVars,
                SqlppVariableAnnotation.CONTEXT_VARIABLE, liveAnnotatedVars, winExpr.getSourceLocation());

        List<Pair<Expression, Identifier>> winFieldList = winExpr.getWindowFieldList();
        Map<VariableExpr, Identifier> winVarFieldMap =
                SqlppGroupByAggregationSugarVisitor.createGroupVarFieldMap(winFieldList);

        List<Expression> exprList = winExpr.getExprList();
        int n = exprList.size();
        List<Expression> newExprList = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Expression expr = exprList.get(i);
            Expression newExpr = i < limit ? Sql92AggregateFunctionVisitor.wrapAggregationArgument(expr, winVar,
                    winVarFieldMap, liveContextVars, null, liveVars, context) : expr;
            newExprList.add(newExpr);
        }
        winExpr.setExprList(newExprList);
    }

    private List<Pair<Expression, Identifier>> createWindowFieldList(SelectBlock selectBlock) {
        List<Pair<Expression, Identifier>> fieldList = new ArrayList<>();
        if (selectBlock != null) {
            addToFieldList(fieldList, SqlppVariableUtil.getBindingVariables(selectBlock.getFromClause()));
            addToFieldList(fieldList, SqlppVariableUtil.getLetBindingVariables(selectBlock.getLetWhereList()));
            addToFieldList(fieldList, SqlppVariableUtil.getBindingVariables(selectBlock.getGroupbyClause()));
            addToFieldList(fieldList,
                    SqlppVariableUtil.getLetBindingVariables(selectBlock.getLetHavingListAfterGroupby()));
        }
        return fieldList;
    }

    private void addToFieldList(List<Pair<Expression, Identifier>> outFieldList, List<VariableExpr> varList) {
        for (VariableExpr varExpr : varList) {
            if (scopeChecker.lookupSymbol(varExpr.getVar().getValue()) != null) {
                SqlppVariableUtil.addToFieldVariableList(varExpr, outFieldList);
            }
        }
    }
}
