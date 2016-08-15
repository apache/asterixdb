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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.asterix.common.config.MetadataConstants;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.FunctionDecl;
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
import org.apache.asterix.lang.sqlpp.expression.IndependentSubquery;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class AbstractSqlppExpressionScopingVisitor extends AbstractSqlppSimpleExpressionVisitor {

    protected final FunctionSignature resolveFunction =
            new FunctionSignature(MetadataConstants.METADATA_DATAVERSE_NAME, "resolve", FunctionIdentifier.VARARGS);
    protected final ScopeChecker scopeChecker = new ScopeChecker();
    protected final LangRewritingContext context;

    /**
     * @param context,
     *            manages ids of variables and guarantees uniqueness of variables.
     */
    public AbstractSqlppExpressionScopingVisitor(LangRewritingContext context) {
        this.context = context;
        this.scopeChecker.setVarCounter(new Counter(context.getVarCounter()));
    }

    @Override
    public Expression visit(FromClause fromClause, ILangExpression arg) throws AsterixException {
        scopeChecker.extendCurrentScope();
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fromTerm.accept(this, fromClause);
        }
        return null;
    }

    @Override
    public Expression visit(FromTerm fromTerm, ILangExpression arg) throws AsterixException {
        scopeChecker.createNewScope();
        // Visit the left expression of a from term.
        fromTerm.setLeftExpression(visit(fromTerm.getLeftExpression(), fromTerm));

        // Registers the data item variable.
        VariableExpr leftVar = fromTerm.getLeftVariable();
        scopeChecker.getCurrentScope().addNewVarSymbolToScope(leftVar.getVar());

        // Registers the positional variable
        if (fromTerm.hasPositionalVariable()) {
            VariableExpr posVar = fromTerm.getPositionalVariable();
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(posVar.getVar());
        }
        // Visits join/unnest/nest clauses.
        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            correlateClause.accept(this, fromTerm);
        }
        return null;
    }

    @Override
    public Expression visit(JoinClause joinClause, ILangExpression arg) throws AsterixException {
        Scope backupScope = scopeChecker.removeCurrentScope();
        Scope parentScope = scopeChecker.getCurrentScope();
        scopeChecker.createNewScope();
        // NOTE: the two join branches cannot be correlated, instead of checking
        // the correlation here,
        // we defer the check to the query optimizer.
        joinClause.setRightExpression(visit(joinClause.getRightExpression(), joinClause));

        // Registers the data item variable.
        VariableExpr rightVar = joinClause.getRightVariable();
        scopeChecker.getCurrentScope().addNewVarSymbolToScope(rightVar.getVar());

        if (joinClause.hasPositionalVariable()) {
            // Registers the positional variable.
            VariableExpr posVar = joinClause.getPositionalVariable();
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(posVar.getVar());
        }

        Scope rightScope = scopeChecker.removeCurrentScope();
        Scope mergedScope = new Scope(scopeChecker, parentScope);
        mergedScope.merge(backupScope);
        mergedScope.merge(rightScope);
        scopeChecker.pushExistingScope(mergedScope);
        // The condition expression can refer to the just registered variables
        // for the right branch.
        joinClause.setConditionExpression(visit(joinClause.getConditionExpression(), joinClause));
        return null;
    }

    @Override
    public Expression visit(NestClause nestClause, ILangExpression arg) throws AsterixException {
        // NOTE: the two branches of a NEST cannot be correlated, instead of
        // checking the correlation here, we defer the check to the query
        // optimizer.
        nestClause.setRightExpression(visit(nestClause.getRightExpression(), nestClause));

        // Registers the data item variable.
        VariableExpr rightVar = nestClause.getRightVariable();
        scopeChecker.getCurrentScope().addNewVarSymbolToScope(rightVar.getVar());

        if (nestClause.hasPositionalVariable()) {
            // Registers the positional variable.
            VariableExpr posVar = nestClause.getPositionalVariable();
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(posVar.getVar());
        }

        // The condition expression can refer to the just registered variables
        // for the right branch.
        nestClause.setConditionExpression(visit(nestClause.getConditionExpression(), nestClause));
        return null;
    }

    @Override
    public Expression visit(UnnestClause unnestClause, ILangExpression arg) throws AsterixException {
        unnestClause.setRightExpression(visit(unnestClause.getRightExpression(), unnestClause));

        // register the data item variable
        VariableExpr rightVar = unnestClause.getRightVariable();
        scopeChecker.getCurrentScope().addNewVarSymbolToScope(rightVar.getVar());

        if (unnestClause.hasPositionalVariable()) {
            // register the positional variable
            VariableExpr posVar = unnestClause.getPositionalVariable();
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(posVar.getVar());
        }
        return null;
    }

    @Override
    public Expression visit(SelectSetOperation selectSetOperation, ILangExpression arg) throws AsterixException {
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
    public Expression visit(Query q, ILangExpression arg) throws AsterixException {
        q.setBody(visit(q.getBody(), q));
        q.setVarCounter(scopeChecker.getVarCounter());
        context.setVarCounter(scopeChecker.getVarCounter());
        return null;
    }

    @Override
    public Expression visit(FunctionDecl fd, ILangExpression arg) throws AsterixException {
        scopeChecker.createNewScope();
        fd.setFuncBody(visit(fd.getFuncBody(), fd));
        scopeChecker.removeCurrentScope();
        return null;
    }

    @Override
    public Expression visit(GroupbyClause gc, ILangExpression arg) throws AsterixException {
        // After a GROUP BY, variables defined before the current SELECT BLOCK (e.g., in a WITH clause
        // or an outer scope query) should still be visible.
        Scope newScope = new Scope(scopeChecker, scopeChecker.getCurrentScope().getParentScope());
        // Puts all group-by variables into the symbol set of the new scope.
        for (GbyVariableExpressionPair gbyKeyVarExpr : gc.getGbyPairList()) {
            gbyKeyVarExpr.setExpr(visit(gbyKeyVarExpr.getExpr(), gc));
            VariableExpr gbyKeyVar = gbyKeyVarExpr.getVar();
            if (gbyKeyVar != null) {
                newScope.addNewVarSymbolToScope(gbyKeyVar.getVar());
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
                    newScope.addNewVarSymbolToScope(decorVar.getVar());
                }
            }
        }
        if (gc.hasGroupVar()) {
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(gc.getGroupVar().getVar());
        }
        if (gc.hasWithMap()) {
            Map<Expression, VariableExpr> newWithMap = new HashMap<>();
            for (Entry<Expression, VariableExpr> entry : gc.getWithVarMap().entrySet()) {
                Expression expr = visit(entry.getKey(), arg);
                Expression newKey = expr;
                VariableExpr value = entry.getValue();
                newScope.addNewVarSymbolToScope(value.getVar());
                newWithMap.put(newKey, value);
            }
            gc.setWithVarMap(newWithMap);
        }
        // Replaces the current scope with the new scope.
        scopeChecker.replaceCurrentScope(newScope);
        return null;
    }

    @Override
    public Expression visit(LimitClause limitClause, ILangExpression arg) throws AsterixException {
        scopeChecker.pushForbiddenScope(scopeChecker.getCurrentScope());
        limitClause.setLimitExpr(visit(limitClause.getLimitExpr(), limitClause));
        if(limitClause.hasOffset()) {
            limitClause.setOffset(visit(limitClause.getOffset(), limitClause));
        }
        scopeChecker.popForbiddenScope();
        return null;
    }

    @Override
    public Expression visit(LetClause letClause, ILangExpression arg) throws AsterixException {
        scopeChecker.extendCurrentScope();
        letClause.setBindingExpr(visit(letClause.getBindingExpr(), letClause));
        scopeChecker.getCurrentScope().addNewVarSymbolToScope(letClause.getVarExpr().getVar());
        return null;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws AsterixException {
        Scope scopeBeforeSelectExpression = scopeChecker.getCurrentScope();
        scopeChecker.createNewScope();

        // visit let list
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                // Variables defined in WITH clauses are considered as named access instead of real variables.
                letClause.getVarExpr().getVar().setNamedValueAccess(true);
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
    public Expression visit(IndependentSubquery independentSubquery, ILangExpression arg) throws AsterixException {
        // Masks parent scopes so as that the subquery is independent of the environment.
        // In this way, free variables defined in the subquery will not be resolved using
        // variables defined in the parent scope.
        Scope scope = new Scope(scopeChecker, scopeChecker.getCurrentScope(), true);
        scopeChecker.pushExistingScope(scope);
        independentSubquery.setExpr(visit(independentSubquery.getExpr(), independentSubquery));
        scopeChecker.removeCurrentScope();
        return independentSubquery;
    }

    @Override
    public Expression visit(QuantifiedExpression qe, ILangExpression arg) throws AsterixException {
        scopeChecker.createNewScope();
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            pair.setExpr(visit(pair.getExpr(), qe));
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(pair.getVarExpr().getVar());
        }
        qe.setSatisfiesExpr(visit(qe.getSatisfiesExpr(), qe));
        scopeChecker.removeCurrentScope();
        return qe;
    }

    @Override
    public Expression visit(VariableExpr varExpr, ILangExpression arg) throws AsterixException {
        String varName = varExpr.getVar().getValue();
        if (scopeChecker.isInForbiddenScopes(varName)) {
            throw new AsterixException(
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

    // Rewrites for an undefined variable reference, which potentially could be a syntatic sugar.
    protected Expression wrapWithResolveFunction(VariableExpr expr, Set<VariableExpr> liveVars)
            throws AsterixException {
        List<Expression> argList = new ArrayList<>();
        //Ignore the parser-generated prefix "$" for a dataset.
        String varName = SqlppVariableUtil.toUserDefinedVariableName(expr.getVar().getValue()).getValue();
        argList.add(new LiteralExpr(new StringLiteral(varName)));
        argList.addAll(liveVars);
        return new CallExpr(resolveFunction, argList);
    }
}
