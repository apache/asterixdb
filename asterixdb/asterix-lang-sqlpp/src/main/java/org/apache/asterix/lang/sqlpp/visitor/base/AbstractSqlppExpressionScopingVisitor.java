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
package org.apache.asterix.lang.sqlpp.visitor.base;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.statement.InsertStatement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.NestClause;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class AbstractSqlppExpressionScopingVisitor extends AbstractSqlppSimpleExpressionVisitor {

    protected final ScopeChecker scopeChecker = new ScopeChecker();
    protected final LangRewritingContext context;

    /**
     * @param context, manages ids of variables and guarantees uniqueness of variables.
     */
    public AbstractSqlppExpressionScopingVisitor(LangRewritingContext context) {
        this(context, null);
    }

    /**
     * @param context,
     *            manages ids of variables and guarantees uniqueness of variables.
     * @param externalVars
     *            pre-defined (external) variables that must be added to the initial scope
     */
    public AbstractSqlppExpressionScopingVisitor(LangRewritingContext context, Collection<VarIdentifier> externalVars) {
        this.context = context;
        this.scopeChecker.setVarCounter(context.getVarCounter());
        if (externalVars != null) {
            for (VarIdentifier paramVar : externalVars) {
                scopeChecker.getCurrentScope().addSymbolToScope(paramVar);
            }
        }
    }

    @Override
    public Expression visit(FromClause fromClause, ILangExpression arg) throws CompilationException {
        Scope scopeForFromClause = scopeChecker.extendCurrentScope();
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fromTerm.accept(this, fromClause);

            // Merges the variables defined in the current from term into the scope of the current from clause.
            Scope scopeForFromTerm = scopeChecker.removeCurrentScope();
            mergeScopes(scopeForFromClause, scopeForFromTerm, fromTerm.getSourceLocation());
        }
        return null;
    }

    @Override
    public Expression visit(FromTerm fromTerm, ILangExpression arg) throws CompilationException {
        // Visit the left expression of a from term.
        fromTerm.setLeftExpression(visit(fromTerm.getLeftExpression(), fromTerm));

        scopeChecker.createNewScope();

        // Registers the data item variable.
        VariableExpr leftVar = fromTerm.getLeftVariable();
        addNewVarSymbolToScope(scopeChecker.getCurrentScope(), leftVar.getVar(), leftVar.getSourceLocation(),
                SqlppVariableAnnotation.CONTEXT_VARIABLE);

        // Registers the positional variable
        if (fromTerm.hasPositionalVariable()) {
            VariableExpr posVar = fromTerm.getPositionalVariable();
            addNewVarSymbolToScope(scopeChecker.getCurrentScope(), posVar.getVar(), posVar.getSourceLocation());
        }
        // Visits join/unnest/nest clauses.
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            correlateClause.accept(this, fromTerm);
        }
        return null;
    }

    @Override
    public Expression visit(JoinClause joinClause, ILangExpression arg) throws CompilationException {
        Scope leftScope = scopeChecker.removeCurrentScope();
        scopeChecker.createNewScope();
        // NOTE: the two join branches cannot be correlated, instead of checking
        // the correlation here,
        // we defer the check to the query optimizer.
        joinClause.setRightExpression(visit(joinClause.getRightExpression(), joinClause));

        // Registers the data item variable.
        VariableExpr rightVar = joinClause.getRightVariable();
        addNewVarSymbolToScope(scopeChecker.getCurrentScope(), rightVar.getVar(), rightVar.getSourceLocation(),
                SqlppVariableAnnotation.CONTEXT_VARIABLE);

        if (joinClause.hasPositionalVariable()) {
            // Registers the positional variable.
            VariableExpr posVar = joinClause.getPositionalVariable();
            addNewVarSymbolToScope(scopeChecker.getCurrentScope(), posVar.getVar(), posVar.getSourceLocation());
        }

        Scope rightScope = scopeChecker.removeCurrentScope();
        mergeScopes(leftScope, rightScope, joinClause.getRightExpression().getSourceLocation());
        scopeChecker.pushExistingScope(leftScope);
        // The condition expression can refer to the just registered variables
        // for the right branch.
        joinClause.setConditionExpression(visit(joinClause.getConditionExpression(), joinClause));
        return null;
    }

    @Override
    public Expression visit(NestClause nestClause, ILangExpression arg) throws CompilationException {
        // NOTE: the two branches of a NEST cannot be correlated, instead of
        // checking the correlation here, we defer the check to the query
        // optimizer.
        nestClause.setRightExpression(visit(nestClause.getRightExpression(), nestClause));

        // Registers the data item variable.
        VariableExpr rightVar = nestClause.getRightVariable();
        addNewVarSymbolToScope(scopeChecker.getCurrentScope(), rightVar.getVar(), rightVar.getSourceLocation(),
                SqlppVariableAnnotation.CONTEXT_VARIABLE);

        if (nestClause.hasPositionalVariable()) {
            // Registers the positional variable.
            VariableExpr posVar = nestClause.getPositionalVariable();
            addNewVarSymbolToScope(scopeChecker.getCurrentScope(), posVar.getVar(), posVar.getSourceLocation());
        }

        // The condition expression can refer to the just registered variables
        // for the right branch.
        nestClause.setConditionExpression(visit(nestClause.getConditionExpression(), nestClause));
        return null;
    }

    @Override
    public Expression visit(UnnestClause unnestClause, ILangExpression arg) throws CompilationException {
        unnestClause.setRightExpression(visit(unnestClause.getRightExpression(), unnestClause));

        // register the data item variable
        VariableExpr rightVar = unnestClause.getRightVariable();
        addNewVarSymbolToScope(scopeChecker.getCurrentScope(), rightVar.getVar(), rightVar.getSourceLocation(),
                SqlppVariableAnnotation.CONTEXT_VARIABLE);

        if (unnestClause.hasPositionalVariable()) {
            // register the positional variable
            VariableExpr posVar = unnestClause.getPositionalVariable();
            addNewVarSymbolToScope(scopeChecker.getCurrentScope(), posVar.getVar(), posVar.getSourceLocation());
        }
        return null;
    }

    @Override
    public Expression visit(SelectSetOperation selectSetOperation, ILangExpression arg) throws CompilationException {
        Scope scopeBeforeCurrentBranch = scopeChecker.getCurrentScope();
        scopeChecker.createNewScope();
        selectSetOperation.getLeftInput().accept(this, arg);
        if (selectSetOperation.hasRightInputs()) {
            for (SetOperationRight right : selectSetOperation.getRightInputs()) {
                // Exit scopes that were entered within a previous select expression
                while (scopeChecker.getCurrentScope() != scopeBeforeCurrentBranch) {
                    scopeChecker.removeCurrentScope();
                }
                scopeChecker.createNewScope();
                right.getSetOperationRightInput().accept(this, arg);
            }
            // Exit scopes that were entered within the last branch of the set operation.
            while (scopeChecker.getCurrentScope() != scopeBeforeCurrentBranch) {
                scopeChecker.removeCurrentScope();
            }
        }
        return null;
    }

    @Override
    public Expression visit(Query q, ILangExpression arg) throws CompilationException {
        q.setBody(visit(q.getBody(), q));
        q.setVarCounter(scopeChecker.getVarCounter());
        return null;
    }

    @Override
    public Expression visit(FunctionDecl fd, ILangExpression arg) throws CompilationException {
        scopeChecker.createNewScope();
        fd.setFuncBody(visit(fd.getFuncBody(), fd));
        scopeChecker.removeCurrentScope();
        return null;
    }

    @Override
    public Expression visit(GroupbyClause gc, ILangExpression arg) throws CompilationException {
        // After a GROUP BY, variables defined before the current SELECT BLOCK (e.g., in a WITH clause
        // or an outer scope query) should still be visible.
        Scope newScope = new Scope(scopeChecker, scopeChecker.getPrecedingScope());
        // Puts all group-by variables into the symbol set of the new scope.
        for (GbyVariableExpressionPair gbyKeyVarExpr : gc.getGbyPairList()) {
            gbyKeyVarExpr.setExpr(visit(gbyKeyVarExpr.getExpr(), gc));
            VariableExpr gbyKeyVar = gbyKeyVarExpr.getVar();
            if (gbyKeyVar != null) {
                addNewVarSymbolToScope(newScope, gbyKeyVar.getVar(), gbyKeyVar.getSourceLocation());
            }
        }
        if (gc.hasGroupFieldList()) {
            for (Pair<Expression, Identifier> gbyField : gc.getGroupFieldList()) {
                gbyField.first = visit(gbyField.first, arg);
            }
        }
        if (gc.hasDecorList()) {
            for (GbyVariableExpressionPair decorVarExpr : gc.getDecorPairList()) {
                decorVarExpr.setExpr(visit(decorVarExpr.getExpr(), gc));
                VariableExpr decorVar = decorVarExpr.getVar();
                if (decorVar != null) {
                    addNewVarSymbolToScope(newScope, decorVar.getVar(), decorVar.getSourceLocation());
                }
            }
        }
        if (gc.hasGroupVar()) {
            VariableExpr groupVar = gc.getGroupVar();
            addNewVarSymbolToScope(newScope, groupVar.getVar(), groupVar.getSourceLocation());
        }
        if (gc.hasWithMap()) {
            Map<Expression, VariableExpr> newWithMap = new HashMap<>();
            for (Entry<Expression, VariableExpr> entry : gc.getWithVarMap().entrySet()) {
                Expression expr = visit(entry.getKey(), arg);
                Expression newKey = expr;
                VariableExpr value = entry.getValue();
                addNewVarSymbolToScope(newScope, value.getVar(), value.getSourceLocation());
                newWithMap.put(newKey, value);
            }
            gc.setWithVarMap(newWithMap);
        }
        // Replaces the current scope with the new scope.
        scopeChecker.replaceCurrentScope(newScope);
        return null;
    }

    @Override
    public Expression visit(LimitClause limitClause, ILangExpression arg) throws CompilationException {
        scopeChecker.pushForbiddenScope(scopeChecker.getCurrentScope());
        limitClause.setLimitExpr(visit(limitClause.getLimitExpr(), limitClause));
        if (limitClause.hasOffset()) {
            limitClause.setOffset(visit(limitClause.getOffset(), limitClause));
        }
        scopeChecker.popForbiddenScope();
        return null;
    }

    @Override
    public Expression visit(LetClause letClause, ILangExpression arg) throws CompilationException {
        scopeChecker.extendCurrentScope();
        letClause.setBindingExpr(visit(letClause.getBindingExpr(), letClause));
        VariableExpr varExpr = letClause.getVarExpr();
        addNewVarSymbolToScope(scopeChecker.getCurrentScope(), varExpr.getVar(), varExpr.getSourceLocation());
        return null;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
        Scope scopeBeforeSelectExpression = scopeChecker.getCurrentScope();
        scopeChecker.createNewScope();

        // visit let list
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                letClause.accept(this, selectExpression);
            }
            scopeChecker.createNewScope();
        }

        // visit the main select.
        selectExpression.getSelectSetOperation().accept(this, selectExpression);

        // visit order by
        if (selectExpression.hasOrderby()) {
            selectExpression.getOrderbyClause().accept(this, selectExpression);
        }

        // visit limit
        if (selectExpression.hasLimit()) {
            selectExpression.getLimitClause().accept(this, selectExpression);
        }

        // Exit scopes that were entered within this select expression
        while (scopeChecker.getCurrentScope() != scopeBeforeSelectExpression) {
            scopeChecker.removeCurrentScope();
        }
        return selectExpression;
    }

    @Override
    public Expression visit(QuantifiedExpression qe, ILangExpression arg) throws CompilationException {
        scopeChecker.createNewScope();
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            pair.setExpr(visit(pair.getExpr(), qe));
            VariableExpr varExpr = pair.getVarExpr();
            addNewVarSymbolToScope(scopeChecker.getCurrentScope(), varExpr.getVar(), varExpr.getSourceLocation(),
                    SqlppVariableAnnotation.CONTEXT_VARIABLE);
        }
        qe.setSatisfiesExpr(visit(qe.getSatisfiesExpr(), qe));
        scopeChecker.removeCurrentScope();
        return qe;
    }

    @Override
    public Expression visit(VariableExpr varExpr, ILangExpression arg) throws CompilationException {
        String varName = varExpr.getVar().getValue();
        if (scopeChecker.isInForbiddenScopes(varName)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, varExpr.getSourceLocation(),
                    "Inside limit clauses, it is disallowed to reference a variable having the same name as any variable bound in the same scope as the limit clause.");
        }
        Identifier ident = scopeChecker.lookupSymbol(varName);
        if (ident != null) {
            // Exists such an identifier, then this is a variable reference instead of a variable
            // definition.
            varExpr.setIsNewVar(false);
            varExpr.setVar((VarIdentifier) ident);
        }
        return varExpr;
    }

    @Override
    public Expression visit(InsertStatement insertStatement, ILangExpression arg) throws CompilationException {
        scopeChecker.createNewScope();

        // Visits the body query.
        insertStatement.getQuery().accept(this, insertStatement);

        // Registers the (inserted) data item variable.
        VariableExpr bindingVar = insertStatement.getVar();
        if (bindingVar != null) {
            addNewVarSymbolToScope(scopeChecker.getCurrentScope(), bindingVar.getVar(), bindingVar.getSourceLocation(),
                    SqlppVariableAnnotation.CONTEXT_VARIABLE);
        }

        // Visits the expression for the returning expression.
        Expression returningExpr = insertStatement.getReturnExpression();
        if (returningExpr != null) {
            insertStatement.setReturnExpression(visit(returningExpr, insertStatement));
        }
        return null;
    }

    @Override
    public Expression visit(WindowExpression winExpr, ILangExpression arg) throws CompilationException {
        visitWindowExpressionExcludingExprList(winExpr, arg);
        if (winExpr.hasWindowVar()) {
            Scope preScope = scopeChecker.getCurrentScope();
            Scope newScope = scopeChecker.extendCurrentScope();
            VariableExpr windowVar = winExpr.getWindowVar();
            addNewVarSymbolToScope(newScope, windowVar.getVar(), windowVar.getSourceLocation());
            winExpr.setExprList(visit(winExpr.getExprList(), arg));
            scopeChecker.replaceCurrentScope(preScope);
        } else {
            winExpr.setExprList(visit(winExpr.getExprList(), arg));
        }
        return winExpr;
    }

    // Adds a new encountered alias identifier into a scope
    private void addNewVarSymbolToScope(Scope scope, VarIdentifier var, SourceLocation sourceLoc,
            SqlppVariableAnnotation... varAnnotations) throws CompilationException {
        if (scope.findLocalSymbol(var.getValue()) != null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Duplicate alias definitions: " + SqlppVariableUtil.toUserDefinedName(var.getValue()));
        }
        Set<SqlppVariableAnnotation> annotations;
        if (varAnnotations == null || varAnnotations.length == 0) {
            annotations = Collections.emptySet();
        } else {
            annotations = EnumSet.noneOf(SqlppVariableAnnotation.class);
            Collections.addAll(annotations, varAnnotations);
        }
        scope.addNewVarSymbolToScope(var, annotations);
    }

    // Merges <code>scopeToBeMerged</code> into <code>hostScope</code>.
    private void mergeScopes(Scope hostScope, Scope scopeToBeMerged, SourceLocation sourceLoc)
            throws CompilationException {
        Set<String> symbolsToBeMerged = scopeToBeMerged.getLocalSymbols();
        for (String symbolToBeMerged : symbolsToBeMerged) {
            if (hostScope.findLocalSymbol(symbolToBeMerged) != null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Duplicate alias definitions: " + SqlppVariableUtil.toUserDefinedName(symbolToBeMerged));
            }
        }
        hostScope.merge(scopeToBeMerged);
    }

    public enum SqlppVariableAnnotation implements Scope.SymbolAnnotation {
        /**
         * Context variables are those that participate in the second stage of the name resolution process.
         * A single name identifier is first attempted to be resolved as a variable reference. If that fails
         * because there's no variable with such name then (second stage) it's resolved as a field access on a context
         * variable (if there's only one context variable defined in the local scope).
         *
         * See {@link org.apache.asterix.lang.sqlpp.rewrites.visitor.VariableCheckAndRewriteVisitor}
         */
        CONTEXT_VARIABLE
    }
}
