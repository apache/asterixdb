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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.IfExpr;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.HavingClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.NestClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableSubstitutionUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppQueryExpressionVisitor;

public class InlineColumnAliasVisitor extends AbstractSqlppQueryExpressionVisitor<Void, Boolean> {

    private final ScopeChecker scopeChecker = new ScopeChecker();
    private final LangRewritingContext context;

    public InlineColumnAliasVisitor(LangRewritingContext context) {
        this.context = context;
    }

    @Override
    public Void visit(WhereClause whereClause, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        whereClause.getWhereExpr().accept(this, overwriteWithGbyKeyVarRefs);
        return null;
    }

    @Override
    public Void visit(FromClause fromClause, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fromTerm.accept(this, overwriteWithGbyKeyVarRefs);
        }
        return null;
    }

    @Override
    public Void visit(FromTerm fromTerm, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        fromTerm.getLeftExpression().accept(this, overwriteWithGbyKeyVarRefs);
        // A from binding variable will override the alias to substitute.
        scopeChecker.getCurrentScope().removeSymbolExpressionMapping(fromTerm.getLeftVariable());
        if (fromTerm.hasPositionalVariable()) {
            scopeChecker.getCurrentScope().removeSymbolExpressionMapping(fromTerm.getPositionalVariable());
        }

        for (AbstractBinaryCorrelateClause correlate : fromTerm.getCorrelateClauses()) {
            correlate.accept(this, overwriteWithGbyKeyVarRefs);
        }
        return null;
    }

    @Override
    public Void visit(JoinClause joinClause, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        joinClause.getRightExpression().accept(this, overwriteWithGbyKeyVarRefs);
        removeSubsutitions(joinClause);
        joinClause.getConditionExpression().accept(this, overwriteWithGbyKeyVarRefs);
        return null;
    }

    @Override
    public Void visit(NestClause nestClause, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        nestClause.getRightExpression().accept(this, overwriteWithGbyKeyVarRefs);
        nestClause.getConditionExpression().accept(this, overwriteWithGbyKeyVarRefs);
        removeSubsutitions(nestClause);
        return null;
    }

    @Override
    public Void visit(UnnestClause unnestClause, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        unnestClause.getRightExpression().accept(this, overwriteWithGbyKeyVarRefs);
        removeSubsutitions(unnestClause);
        return null;
    }

    @Override
    public Void visit(Projection projection, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        projection.getExpression().accept(this, overwriteWithGbyKeyVarRefs);
        VariableExpr columnAlias = new VariableExpr(
                SqlppVariableUtil.toInternalVariableIdentifier(projection.getName()));
        VariableSubstitutionEnvironment env = scopeChecker.getCurrentScope().getVarSubstitutionEnvironment();
        Expression gbyKey = (Expression) SqlppRewriteUtil.deepCopy(env.findSubstituion(columnAlias));
        if (overwriteWithGbyKeyVarRefs) {
            if (gbyKey != null) {
                projection.setExpression(gbyKey);
            }
        } else {
            scopeChecker.getCurrentScope().addSymbolExpressionMappingToScope(columnAlias, projection.getExpression());
        }
        return null;
    }

    @Override
    public Void visit(SelectBlock selectBlock, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        // Traverses the select block in the order of "select", "group-by",
        // "group-by" lets and "having".
        // The first pass over the select clause will not overwrite projection expressions.
        selectBlock.getSelectClause().accept(this, false);

        if (selectBlock.hasFromClause()) {
            selectBlock.getFromClause().accept(this, overwriteWithGbyKeyVarRefs);
        }
        if (selectBlock.hasLetClauses()) {
            for (LetClause letClause : selectBlock.getLetList()) {
                letClause.accept(this, overwriteWithGbyKeyVarRefs);
            }
        }
        if (selectBlock.hasGroupbyClause()) {
            selectBlock.getGroupbyClause().accept(this, overwriteWithGbyKeyVarRefs);
        }
        if (selectBlock.hasLetClausesAfterGroupby()) {
            for (LetClause letClauseAfterGby : selectBlock.getLetListAfterGroupby()) {
                letClauseAfterGby.accept(this, true);
            }
        }
        if (selectBlock.hasHavingClause()) {
            selectBlock.getHavingClause().accept(this, overwriteWithGbyKeyVarRefs);
        }

        // Visit select clause again to overwrite projection expressions to group-by
        // key variable references if any group-by key is the original projection
        // column alias.
        selectBlock.getSelectClause().accept(this, true);
        return null;
    }

    @Override
    public Void visit(SelectClause selectClause, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        if (selectClause.selectElement()) {
            selectClause.getSelectElement().accept(this, overwriteWithGbyKeyVarRefs);
        }
        if (selectClause.selectRegular()) {
            selectClause.getSelectRegular().accept(this, overwriteWithGbyKeyVarRefs);
        }
        return null;
    }

    @Override
    public Void visit(SelectElement selectElement, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        Expression expr = selectElement.getExpression();
        expr.accept(this, overwriteWithGbyKeyVarRefs);
        if (expr.getKind() == Kind.RECORD_CONSTRUCTOR_EXPRESSION) {
            // Rewrite top-level field names (aliases), in order to be consistent with SelectRegular.
            mapForRecordConstructor(overwriteWithGbyKeyVarRefs, (RecordConstructor) expr);
        }
        return null;
    }

    /**
     * Map aliases for a record constructor in SELECT ELEMENT.
     *
     * @param overwriteWithGbyKeyVarRefs,
     *            whether we rewrite the record constructor with mapped group-by key variables.
     * @param rc,
     *            the RecordConstructor expression.
     * @throws AsterixException
     */
    private void mapForRecordConstructor(Boolean overwriteWithGbyKeyVarRefs, RecordConstructor rc)
            throws AsterixException {
        for (FieldBinding binding : rc.getFbList()) {
            Expression leftExpr = binding.getLeftExpr();
            // We only need to deal with the case that the left expression (for a field name) is
            // a string literal. Otherwise, it is different from a column alias in a projection
            // (e.g., foo.name AS name) in regular SQL SELECT.
            if (leftExpr.getKind() == Kind.LITERAL_EXPRESSION) {
                LiteralExpr literalExpr = (LiteralExpr) leftExpr;
                if (literalExpr.getValue().getLiteralType() == Literal.Type.STRING) {
                    String fieldName = literalExpr.getValue().getStringValue();
                    VariableExpr columnAlias = new VariableExpr(
                            SqlppVariableUtil.toInternalVariableIdentifier(fieldName));
                    VariableSubstitutionEnvironment env = scopeChecker.getCurrentScope()
                            .getVarSubstitutionEnvironment();
                    if (overwriteWithGbyKeyVarRefs) {
                        // Rewrites the field value expression by the mapped grouping key
                        // (for the column alias) if there exists such a mapping.
                        Expression gbyKey = (Expression) SqlppRewriteUtil.deepCopy(env.findSubstituion(columnAlias));
                        if (gbyKey != null) {
                            binding.setRightExpr(gbyKey);
                        }
                    } else {
                        // If this is the first pass, map a field name (i.e., column alias) to the field expression.
                        scopeChecker.getCurrentScope().addSymbolExpressionMappingToScope(columnAlias,
                                binding.getRightExpr());
                    }
                }
            }
        }
    }

    @Override
    public Void visit(SelectRegular selectRegular, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        for (Projection projection : selectRegular.getProjections()) {
            projection.accept(this, overwriteWithGbyKeyVarRefs);
        }
        return null;
    }

    @Override
    public Void visit(SelectSetOperation selectSetOperation, Boolean overwriteWithGbyKeyVarRefs)
            throws AsterixException {
        selectSetOperation.getLeftInput().accept(this, overwriteWithGbyKeyVarRefs);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            right.getSetOperationRightInput().accept(this, overwriteWithGbyKeyVarRefs);
        }
        return null;
    }

    @Override
    public Void visit(SelectExpression selectExpression, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        scopeChecker.createNewScope();

        // Visits let bindings.
        if (selectExpression.hasLetClauses()) {
            for (LetClause lc : selectExpression.getLetList()) {
                lc.accept(this, overwriteWithGbyKeyVarRefs);
            }
        }

        // Visits selectSetOperation.
        selectExpression.getSelectSetOperation().accept(this, overwriteWithGbyKeyVarRefs);

        // Visits order by.
        if (selectExpression.hasOrderby()) {
            selectExpression.getOrderbyClause().accept(this, overwriteWithGbyKeyVarRefs);
        }

        // Visits limit.
        if (selectExpression.hasLimit()) {
            selectExpression.getLimitClause().accept(this, overwriteWithGbyKeyVarRefs);
        }

        // Exits the scope that were entered within this select expression
        scopeChecker.removeCurrentScope();
        return null;
    }

    @Override
    public Void visit(LetClause letClause, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        VariableSubstitutionEnvironment env = scopeChecker.getCurrentScope().getVarSubstitutionEnvironment();
        if (overwriteWithGbyKeyVarRefs) {
            Expression newBindExpr = (Expression) SqlppVariableSubstitutionUtil
                    .substituteVariableWithoutContext(letClause.getBindingExpr(), env);
            letClause.setBindingExpr(newBindExpr);
        }
        letClause.getBindingExpr().accept(this, false);
        // A let binding variable will override the alias to substitute.
        scopeChecker.getCurrentScope().removeSymbolExpressionMapping(letClause.getVarExpr());
        return null;
    }

    @Override
    public Void visit(OrderbyClause oc, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        VariableSubstitutionEnvironment env = scopeChecker.getCurrentScope().getVarSubstitutionEnvironment();
        List<Expression> orderExprs = new ArrayList<Expression>();
        for (Expression orderExpr : oc.getOrderbyList()) {
            orderExprs.add((Expression) SqlppVariableSubstitutionUtil.substituteVariableWithoutContext(orderExpr, env));
            orderExpr.accept(this, overwriteWithGbyKeyVarRefs);
        }
        oc.setOrderbyList(orderExprs);
        return null;
    }

    @Override
    public Void visit(GroupbyClause gc, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        VariableSubstitutionEnvironment env = scopeChecker.getCurrentScope().getVarSubstitutionEnvironment();
        Map<VariableExpr, VariableExpr> oldGbyExprsToNewGbyVarMap = new HashMap<>();
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            Expression oldGbyExpr = gbyVarExpr.getExpr();
            Expression newExpr = (Expression) SqlppVariableSubstitutionUtil.substituteVariableWithoutContext(oldGbyExpr,
                    env);
            newExpr.accept(this, overwriteWithGbyKeyVarRefs);
            gbyVarExpr.setExpr(newExpr);
            if (gbyVarExpr.getVar() == null) {
                gbyVarExpr.setVar(new VariableExpr(context.newVariable()));
            }
            if (oldGbyExpr.getKind() == Kind.VARIABLE_EXPRESSION) {
                VariableExpr oldGbyVarExpr = (VariableExpr) oldGbyExpr;
                if (env.findSubstituion(oldGbyVarExpr) != null) {
                    // Re-mapping that needs to be added.
                    oldGbyExprsToNewGbyVarMap.put(oldGbyVarExpr, gbyVarExpr.getVar());
                }
            }
        }
        for (Entry<VariableExpr, VariableExpr> entry : oldGbyExprsToNewGbyVarMap.entrySet()) {
            // The group-by key variable will override the alias to substitute.
            scopeChecker.getCurrentScope().removeSymbolExpressionMapping(entry.getKey());
            scopeChecker.getCurrentScope().addSymbolExpressionMappingToScope(entry.getKey(), entry.getValue());
        }
        return null;
    }

    @Override
    public Void visit(LimitClause limitClause, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        limitClause.getLimitExpr().accept(this, overwriteWithGbyKeyVarRefs);
        return null;
    }

    @Override
    public Void visit(HavingClause havingClause, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        VariableSubstitutionEnvironment env = scopeChecker.getCurrentScope().getVarSubstitutionEnvironment();
        Expression newFilterExpr = (Expression) SqlppVariableSubstitutionUtil
                .substituteVariableWithoutContext(havingClause.getFilterExpression(), env);
        newFilterExpr.accept(this, overwriteWithGbyKeyVarRefs);
        havingClause.setFilterExpression(newFilterExpr);
        return null;
    }

    @Override
    public Void visit(Query q, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        q.getBody().accept(this, overwriteWithGbyKeyVarRefs);
        return null;
    }

    @Override
    public Void visit(FunctionDecl fd, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        scopeChecker.createNewScope();
        fd.getFuncBody().accept(this, overwriteWithGbyKeyVarRefs);
        scopeChecker.removeCurrentScope();
        return null;
    }

    @Override
    public Void visit(LiteralExpr l, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        return null;
    }

    @Override
    public Void visit(ListConstructor lc, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        for (Expression expr : lc.getExprList()) {
            expr.accept(this, overwriteWithGbyKeyVarRefs);
        }
        return null;
    }

    @Override
    public Void visit(RecordConstructor rc, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        for (FieldBinding binding : rc.getFbList()) {
            binding.getLeftExpr().accept(this, false);
            binding.getRightExpr().accept(this, false);
        }
        return null;
    }

    @Override
    public Void visit(OperatorExpr operatorExpr, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        for (Expression expr : operatorExpr.getExprList()) {
            expr.accept(this, overwriteWithGbyKeyVarRefs);
        }
        return null;
    }

    @Override
    public Void visit(IfExpr ifExpr, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        ifExpr.getCondExpr().accept(this, overwriteWithGbyKeyVarRefs);
        ifExpr.getThenExpr().accept(this, overwriteWithGbyKeyVarRefs);
        ifExpr.getElseExpr().accept(this, overwriteWithGbyKeyVarRefs);
        return null;
    }

    @Override
    public Void visit(QuantifiedExpression qe, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            pair.getExpr().accept(this, overwriteWithGbyKeyVarRefs);
        }
        qe.getSatisfiesExpr().accept(this, overwriteWithGbyKeyVarRefs);
        return null;
    }

    @Override
    public Void visit(CallExpr callExpr, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        for (Expression expr : callExpr.getExprList()) {
            expr.accept(this, overwriteWithGbyKeyVarRefs);
        }
        return null;
    }

    @Override
    public Void visit(VariableExpr varExpr, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        return null;
    }

    @Override
    public Void visit(UnaryExpr u, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        u.getExpr().accept(this, overwriteWithGbyKeyVarRefs);
        return null;
    }

    @Override
    public Void visit(FieldAccessor fa, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        fa.getExpr().accept(this, overwriteWithGbyKeyVarRefs);
        return null;
    }

    @Override
    public Void visit(IndexAccessor ia, Boolean overwriteWithGbyKeyVarRefs) throws AsterixException {
        ia.getExpr().accept(this, overwriteWithGbyKeyVarRefs);
        Expression indexExpr = ia.getExpr();
        if (indexExpr != null) {
            indexExpr.accept(this, overwriteWithGbyKeyVarRefs);
        }
        return null;
    }

    private void removeSubsutitions(AbstractBinaryCorrelateClause unnestClause) {
        scopeChecker.getCurrentScope().removeSymbolExpressionMapping(unnestClause.getRightVariable());
        if (unnestClause.hasPositionalVariable()) {
            scopeChecker.getCurrentScope().removeSymbolExpressionMapping(unnestClause.getPositionalVariable());
        }
    }
}
