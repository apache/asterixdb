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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.SqlppSubstituteExpressionVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Syntactic sugar rewriting: inlines column aliases definitions in SELECT clause into ORDER BY and LIMIT clauses.
 * <br/>
 * Notes
 * <ul>
 * <li> column aliases are not considered new variables, but they can be referenced from ORDER BY and LIMIT clauses
 *      because of this rewriting (like in SQL) </li>
 * <li> if a column alias expression is not a variable or a literal then we introduce a new let clause and replace
 *      that column expression with the let variable reference. The optimizer will then decide whether to inline that
 *      expression or not </li>
 * </ul>
 */
public class InlineColumnAliasVisitor extends AbstractSqlppExpressionScopingVisitor {

    public InlineColumnAliasVisitor(LangRewritingContext context) {
        super(context);
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        // Gets the map from select clause.
        Map<Expression, ColumnAliasBinding> map = getMap(selectBlock.getSelectClause());

        // Removes all FROM/LET binding variables
        if (selectBlock.hasGroupbyClause()) {
            map.keySet().removeAll(SqlppVariableUtil.getBindingVariables(selectBlock.getGroupbyClause()));
            if (selectBlock.hasLetHavingClausesAfterGroupby()) {
                map.keySet().removeAll(
                        SqlppVariableUtil.getLetBindingVariables(selectBlock.getLetHavingListAfterGroupby()));
            }
        } else {
            if (selectBlock.hasFromClause()) {
                map.keySet().removeAll(SqlppVariableUtil.getBindingVariables(selectBlock.getFromClause()));
            }
            if (selectBlock.hasLetWhereClauses()) {
                map.keySet().removeAll(SqlppVariableUtil.getLetBindingVariables(selectBlock.getLetWhereList()));
            }
        }

        SelectExpression selectExpression = (SelectExpression) arg;
        // For SET operation queries, column aliases will not substitute ORDER BY nor LIMIT expressions.
        if (!selectExpression.getSelectSetOperation().hasRightInputs()) {
            // Creates a substitution visitor.
            SubstituteColumnAliasVisitor visitor = new SubstituteColumnAliasVisitor(context, toExpressionMap(map));
            if (selectExpression.hasOrderby()) {
                selectExpression.getOrderbyClause().accept(visitor, arg);
            }
            if (selectExpression.hasLimit()) {
                selectExpression.getLimitClause().accept(visitor, arg);
            }
            if (!visitor.letVarMap.isEmpty()) {
                introduceLetClauses(visitor.letVarMap, map, selectBlock);
            }
        }
        return super.visit(selectBlock, arg);
    }

    private Map<Expression, ColumnAliasBinding> getMap(SelectClause selectClause) {
        if (selectClause.selectElement()) {
            return getMap(selectClause.getSelectElement());
        }
        if (selectClause.selectRegular()) {
            return getMap(selectClause.getSelectRegular());
        }
        return Collections.emptyMap();
    }

    private Map<Expression, ColumnAliasBinding> getMap(SelectElement selectElement) {
        Expression expr = selectElement.getExpression();
        if (expr.getKind() == Kind.RECORD_CONSTRUCTOR_EXPRESSION) {
            // Rewrite top-level field names (aliases), in order to be consistent with SelectRegular.
            return mapRecordConstructor((RecordConstructor) expr);
        }
        return Collections.emptyMap();
    }

    private Map<Expression, ColumnAliasBinding> getMap(SelectRegular selectRegular) {
        return mapProjections(selectRegular.getProjections());
    }

    private Map<Expression, ColumnAliasBinding> mapRecordConstructor(RecordConstructor rc) {
        Map<Expression, ColumnAliasBinding> exprMap = new HashMap<>();
        for (FieldBinding binding : rc.getFbList()) {
            Expression leftExpr = binding.getLeftExpr();
            // We only need to deal with the case that the left expression (for a field name) is
            // a string literal. Otherwise, it is different from a column alias in a projection
            // (e.g., foo.name AS name) in regular SQL SELECT.
            if (leftExpr.getKind() != Kind.LITERAL_EXPRESSION) {
                continue;
            }
            LiteralExpr literalExpr = (LiteralExpr) leftExpr;
            if (literalExpr.getValue().getLiteralType() == Literal.Type.STRING) {
                String fieldName = SqlppVariableUtil.toInternalVariableName(literalExpr.getValue().getStringValue());
                exprMap.put(new VariableExpr(new VarIdentifier(fieldName)), ColumnAliasBinding.of(binding));
            }
        }
        return exprMap;
    }

    private Map<Expression, ColumnAliasBinding> mapProjections(List<Projection> projections) {
        Map<Expression, ColumnAliasBinding> exprMap = new HashMap<>();
        for (Projection projection : projections) {
            if (!projection.star() && !projection.varStar()) {
                String varName = SqlppVariableUtil.toInternalVariableName(projection.getName());
                exprMap.put(new VariableExpr(new VarIdentifier(varName)), ColumnAliasBinding.of(projection));
            }
        }
        return exprMap;
    }

    private void introduceLetClauses(Map<Expression, VarIdentifier> letVarMap,
            Map<Expression, ColumnAliasBinding> aliasBindingMap, SelectBlock selectBlock) throws CompilationException {

        List<AbstractClause> targetLetClauses = selectBlock.hasGroupbyClause()
                ? selectBlock.getLetHavingListAfterGroupby() : selectBlock.getLetWhereList();

        for (Map.Entry<Expression, VarIdentifier> me : letVarMap.entrySet()) {
            Expression columnAliasVarExpr = me.getKey();
            SourceLocation sourceLoc = columnAliasVarExpr.getSourceLocation();
            ColumnAliasBinding columnAliasBinding = aliasBindingMap.get(columnAliasVarExpr);
            if (columnAliasBinding == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc);
            }
            VarIdentifier letVarId = me.getValue();

            // add a let clause defining the new variable
            VariableExpr letVarDefExpr = new VariableExpr(letVarId);
            letVarDefExpr.setSourceLocation(sourceLoc);
            LetClause newLetClause = new LetClause(letVarDefExpr, columnAliasBinding.getExpression());
            newLetClause.setSourceLocation(sourceLoc);
            targetLetClauses.add(newLetClause);

            // replace original column alias expression with variable reference
            VariableExpr letVarRefExpr = new VariableExpr(letVarId);
            letVarRefExpr.setSourceLocation(sourceLoc);
            columnAliasBinding.setExpression(letVarRefExpr);
        }
    }

    private static Map<Expression, Expression> toExpressionMap(Map<Expression, ColumnAliasBinding> bindingMap) {
        Map<Expression, Expression> exprMap = new HashMap<>();
        for (Map.Entry<Expression, ColumnAliasBinding> me : bindingMap.entrySet()) {
            exprMap.put(me.getKey(), me.getValue().getExpression());
        }
        return exprMap;
    }

    private abstract static class ColumnAliasBinding {

        abstract Expression getExpression();

        abstract void setExpression(Expression expr);

        static ColumnAliasBinding of(FieldBinding fieldBinding) {
            return new ColumnAliasBinding() {
                @Override
                Expression getExpression() {
                    return fieldBinding.getRightExpr();
                }

                @Override
                void setExpression(Expression expr) {
                    fieldBinding.setRightExpr(expr);
                }
            };
        }

        static ColumnAliasBinding of(Projection projection) {
            return new ColumnAliasBinding() {
                @Override
                Expression getExpression() {
                    return projection.getExpression();
                }

                @Override
                void setExpression(Expression expr) {
                    projection.setExpression(expr);
                }
            };
        }
    }

    /**
     * Dataset access functions have not yet been introduced at this point, so we need to perform substitution
     * on postVisit() to avoid infinite recursion in case of SELECT (SELECT ... FROM dataset_name) AS dataset_name.
     */
    private static class SubstituteColumnAliasVisitor extends SqlppSubstituteExpressionVisitor {

        private final Map<Expression, VarIdentifier> letVarMap = new LinkedHashMap<>();

        private SubstituteColumnAliasVisitor(LangRewritingContext context, Map<Expression, Expression> exprMap) {
            super(context, exprMap);
        }

        @Override
        protected Expression preVisit(Expression expr) {
            return expr;
        }

        @Override
        protected Expression postVisit(Expression expr) throws CompilationException {
            return substitute(expr);
        }

        @Override
        protected Expression getMappedExpr(Expression expr) throws CompilationException {
            Expression mappedExpr = super.getMappedExpr(expr);
            if (mappedExpr == null) {
                return null;
            }
            switch (mappedExpr.getKind()) {
                case LITERAL_EXPRESSION:
                case VARIABLE_EXPRESSION:
                    return mappedExpr;
                default:
                    // all other kinds of expressions must be moved out of column alias definitions into separate
                    // let clauses, so we need to return a variable reference expression here and
                    // create a new let variable if we're replacing given expression for the first time
                    VarIdentifier var = letVarMap.get(expr);
                    if (var == null) {
                        var = context.newVariable();
                        letVarMap.put(expr, var);
                    }
                    VariableExpr varExpr = new VariableExpr(var);
                    varExpr.setSourceLocation(expr.getSourceLocation());
                    return varExpr;
            }
        }
    }
}
