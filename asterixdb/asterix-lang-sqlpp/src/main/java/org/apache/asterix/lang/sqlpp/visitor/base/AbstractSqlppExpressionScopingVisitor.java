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
import java.util.List;
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
        fromTerm.setLeftExpression(fromTerm.getLeftExpression().accept(this, fromTerm));

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
        joinClause.setRightExpression(joinClause.getRightExpression().accept(this, joinClause));

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
        joinClause.setConditionExpression(joinClause.getConditionExpression().accept(this, joinClause));
        return null;
    }

    @Override
    public Expression visit(NestClause nestClause, ILangExpression arg) throws AsterixException {
        // NOTE: the two branches of a NEST cannot be correlated, instead of
        // checking the correlation here, we defer the check to the query
        // optimizer.
        nestClause.setRightExpression(nestClause.getRightExpression().accept(this, nestClause));

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
        nestClause.setConditionExpression(nestClause.getConditionExpression().accept(this, nestClause));
        return null;
    }

    @Override
    public Expression visit(UnnestClause unnestClause, ILangExpression arg) throws AsterixException {
        unnestClause.setRightExpression(unnestClause.getRightExpression().accept(this, unnestClause));

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
        selectSetOperation.getLeftInput().accept(this, arg);
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            scopeChecker.createNewScope();
            right.getSetOperationRightInput().accept(this, arg);
        }
        return null;
    }

    @Override
    public Expression visit(Query q, ILangExpression arg) throws AsterixException {
        q.setBody(q.getBody().accept(this, q));
        q.setVarCounter(scopeChecker.getVarCounter());
        context.setVarCounter(scopeChecker.getVarCounter());
        return null;
    }

    @Override
    public Expression visit(FunctionDecl fd, ILangExpression arg) throws AsterixException {
        scopeChecker.createNewScope();
        fd.setFuncBody(fd.getFuncBody().accept(this, fd));
        scopeChecker.removeCurrentScope();
        return null;
    }

    @Override
    public Expression visit(GroupbyClause gc, ILangExpression arg) throws AsterixException {
        Scope newScope = scopeChecker.extendCurrentScopeNoPush(true);
        // Puts all group-by variables into the symbol set of the new scope.
        for (GbyVariableExpressionPair gbyVarExpr : gc.getGbyPairList()) {
            gbyVarExpr.setExpr(gbyVarExpr.getExpr().accept(this, gc));
            VariableExpr gbyVar = gbyVarExpr.getVar();
            if (gbyVar != null) {
                newScope.addNewVarSymbolToScope(gbyVarExpr.getVar().getVar());
            }
        }
        for (VariableExpr withVar : gc.getWithVarList()) {
            newScope.addNewVarSymbolToScope(withVar.getVar());
        }
        scopeChecker.replaceCurrentScope(newScope);
        return null;
    }

    @Override
    public Expression visit(LimitClause limitClause, ILangExpression arg) throws AsterixException {
        scopeChecker.pushForbiddenScope(scopeChecker.getCurrentScope());
        limitClause.setLimitExpr(limitClause.getLimitExpr().accept(this, limitClause));
        scopeChecker.popForbiddenScope();
        return null;
    }

    @Override
    public Expression visit(LetClause letClause, ILangExpression arg) throws AsterixException {
        scopeChecker.extendCurrentScope();
        letClause.setBindingExpr(letClause.getBindingExpr().accept(this, letClause));
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
                letClause.accept(this, selectExpression);
            }
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
        independentSubquery.setExpr(independentSubquery.getExpr().accept(this, arg));
        scopeChecker.removeCurrentScope();
        return independentSubquery;
    }

    @Override
    public Expression visit(QuantifiedExpression qe, ILangExpression arg) throws AsterixException {
        scopeChecker.createNewScope();
        for (QuantifiedPair pair : qe.getQuantifiedList()) {
            scopeChecker.getCurrentScope().addNewVarSymbolToScope(pair.getVarExpr().getVar());
            pair.setExpr(pair.getExpr().accept(this, qe));
        }
        qe.setSatisfiesExpr(qe.getSatisfiesExpr().accept(this, qe));
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
