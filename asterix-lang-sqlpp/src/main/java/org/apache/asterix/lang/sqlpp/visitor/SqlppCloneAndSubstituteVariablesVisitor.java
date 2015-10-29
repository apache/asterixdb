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
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Clause.ClauseType;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.visitor.CloneAndSubstituteVariablesVisitor;
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
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class SqlppCloneAndSubstituteVariablesVisitor extends CloneAndSubstituteVariablesVisitor implements
        ISqlppVisitor<Pair<ILangExpression, VariableSubstitutionEnvironment>, VariableSubstitutionEnvironment> {

    private LangRewritingContext context;

    public SqlppCloneAndSubstituteVariablesVisitor(LangRewritingContext context) {
        super(context);
        this.context = context;
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(FromClause fromClause,
            VariableSubstitutionEnvironment env) throws AsterixException {
        VariableSubstitutionEnvironment currentEnv = new VariableSubstitutionEnvironment(env);
        List<FromTerm> newFromTerms = new ArrayList<FromTerm>();
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            Pair<ILangExpression, VariableSubstitutionEnvironment> p = fromTerm.accept(this, currentEnv);
            newFromTerms.add((FromTerm) p.first);
            // A right from term could be correlated from a left from term,
            // therefore we propagate the substitution environment.
            currentEnv = p.second;
        }
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(new FromClause(newFromTerms), currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(FromTerm fromTerm,
            VariableSubstitutionEnvironment env) throws AsterixException {
        VariableExpr leftVar = fromTerm.getLeftVariable();
        VariableExpr newLeftVar = generateNewVariable(context, leftVar);
        VariableExpr newLeftPosVar = fromTerm.hasPositionalVariable()
                ? generateNewVariable(context, fromTerm.getPositionalVariable()) : null;
        Expression newLeftExpr = (Expression) fromTerm.getLeftExpression().accept(this, env).first;
        List<AbstractBinaryCorrelateClause> newCorrelateClauses = new ArrayList<AbstractBinaryCorrelateClause>();

        VariableSubstitutionEnvironment currentEnv = new VariableSubstitutionEnvironment(env);
        currentEnv.removeSubstitution(newLeftVar);
        if (newLeftPosVar != null) {
            currentEnv.removeSubstitution(newLeftPosVar);
        }

        for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
            if (correlateClause.getClauseType() == ClauseType.UNNEST_CLAUSE) {
                // The right-hand-side of unnest could be correlated with the left side,
                // therefore we propagate the substitution environment of the left-side.
                Pair<ILangExpression, VariableSubstitutionEnvironment> p = correlateClause.accept(this, currentEnv);
                currentEnv = p.second;
                newCorrelateClauses.add((AbstractBinaryCorrelateClause) p.first);
            } else {
                // The right-hand-side of join and nest could not be correlated with the left side,
                // therefore we propagate the original substitution environment.
                newCorrelateClauses.add((AbstractBinaryCorrelateClause) correlateClause.accept(this, env).first);
            }
        }
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(
                new FromTerm(newLeftExpr, newLeftVar, newLeftPosVar, newCorrelateClauses), currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(JoinClause joinClause,
            VariableSubstitutionEnvironment env) throws AsterixException {
        VariableExpr rightVar = joinClause.getRightVariable();
        VariableExpr newRightVar = generateNewVariable(context, rightVar);
        VariableExpr newRightPosVar = joinClause.hasPositionalVariable()
                ? generateNewVariable(context, joinClause.getPositionalVariable()) : null;

        // Visits the right expression.
        Expression newRightExpr = (Expression) joinClause.getRightExpression().accept(this, env).first;

        // Visits the condition.
        VariableSubstitutionEnvironment currentEnv = new VariableSubstitutionEnvironment(env);
        currentEnv.removeSubstitution(newRightVar);
        if (newRightPosVar != null) {
            currentEnv.removeSubstitution(newRightPosVar);
        }
        // The condition can refer to the newRightVar and newRightPosVar.
        Expression conditionExpr = (Expression) joinClause.getConditionExpression().accept(this, currentEnv).first;

        JoinClause newJoinClause = new JoinClause(joinClause.getJoinType(), newRightExpr, newRightVar, newRightPosVar,
                conditionExpr);
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(newJoinClause, currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(NestClause nestClause,
            VariableSubstitutionEnvironment env) throws AsterixException {
        VariableExpr rightVar = nestClause.getRightVariable();
        VariableExpr newRightVar = generateNewVariable(context, rightVar);
        VariableExpr newRightPosVar = nestClause.hasPositionalVariable()
                ? generateNewVariable(context, nestClause.getPositionalVariable()) : null;

        // Visits the right expression.
        Expression rightExpr = (Expression) nestClause.getRightExpression().accept(this, env).first;

        // Visits the condition.
        VariableSubstitutionEnvironment currentEnv = new VariableSubstitutionEnvironment(env);
        currentEnv.removeSubstitution(newRightVar);
        if (newRightPosVar != null) {
            currentEnv.removeSubstitution(newRightPosVar);
        }
        // The condition can refer to the newRightVar and newRightPosVar.
        Expression conditionExpr = (Expression) nestClause.getConditionExpression().accept(this, currentEnv).first;

        NestClause newJoinClause = new NestClause(nestClause.getJoinType(), rightExpr, newRightVar, newRightPosVar,
                conditionExpr);
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(newJoinClause, currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(UnnestClause unnestClause,
            VariableSubstitutionEnvironment env) throws AsterixException {
        VariableExpr rightVar = unnestClause.getRightVariable();
        VariableExpr newRightVar = generateNewVariable(context, rightVar);
        VariableExpr newRightPosVar = unnestClause.hasPositionalVariable()
                ? generateNewVariable(context, unnestClause.getPositionalVariable()) : null;

        // Visits the right expression.
        Expression rightExpr = (Expression) unnestClause.getRightExpression().accept(this, env).first;

        // Visits the condition.
        VariableSubstitutionEnvironment currentEnv = new VariableSubstitutionEnvironment(env);
        currentEnv.removeSubstitution(newRightVar);
        if (newRightPosVar != null) {
            currentEnv.removeSubstitution(newRightPosVar);
        }
        // The condition can refer to the newRightVar and newRightPosVar.
        UnnestClause newJoinClause = new UnnestClause(unnestClause.getJoinType(), rightExpr, newRightVar,
                newRightPosVar);
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(newJoinClause, currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(Projection projection,
            VariableSubstitutionEnvironment env) throws AsterixException {
        Projection newProjection = (Projection) projection.getExpression().accept(this, env).first;
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(newProjection, env);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(SelectBlock selectBlock,
            VariableSubstitutionEnvironment env) throws AsterixException {
        Pair<ILangExpression, VariableSubstitutionEnvironment> newFrom = null;
        Pair<ILangExpression, VariableSubstitutionEnvironment> newLet = null;
        Pair<ILangExpression, VariableSubstitutionEnvironment> newWhere = null;
        Pair<ILangExpression, VariableSubstitutionEnvironment> newGroupby = null;
        Pair<ILangExpression, VariableSubstitutionEnvironment> newHaving = null;
        Pair<ILangExpression, VariableSubstitutionEnvironment> newSelect = null;
        List<LetClause> newLetClauses = new ArrayList<LetClause>();
        List<LetClause> newLetClausesAfterGby = new ArrayList<LetClause>();
        VariableSubstitutionEnvironment currentEnv = new VariableSubstitutionEnvironment(env);

        if (selectBlock.hasFromClause()) {
            newFrom = selectBlock.getFromClause().accept(this, currentEnv);
            currentEnv = newFrom.second;
        }

        if (selectBlock.hasLetClauses()) {
            for (LetClause letClause : selectBlock.getLetList()) {
                newLet = letClause.accept(this, env);
                currentEnv = newLet.second;
                newLetClauses.add(letClause);
            }
        }

        if (selectBlock.hasWhereClause()) {
            newWhere = selectBlock.getWhereClause().accept(this, currentEnv);
            currentEnv = newWhere.second;
        }

        if (selectBlock.hasGroupbyClause()) {
            newGroupby = selectBlock.getGroupbyClause().accept(this, currentEnv);
            currentEnv = newGroupby.second;
            if (selectBlock.hasLetClausesAfterGroupby()) {
                for (LetClause letClauseAfterGby : selectBlock.getLetListAfterGroupby()) {
                    newLet = letClauseAfterGby.accept(this, env);
                    currentEnv = newLet.second;
                    newLetClausesAfterGby.add(letClauseAfterGby);
                }
            }
        }

        if (selectBlock.hasHavingClause()) {
            newHaving = selectBlock.getHavingClause().accept(this, currentEnv);
            currentEnv = newHaving.second;
        }

        newSelect = selectBlock.getSelectClause().accept(this, currentEnv);
        currentEnv = newSelect.second;
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(
                new SelectBlock((SelectClause) newSelect.first, newFrom == null ? null : (FromClause) newFrom.first,
                        newLetClauses, newWhere == null ? null : (WhereClause) newWhere.first,
                        newGroupby == null ? null : (GroupbyClause) newGroupby.first, newLetClausesAfterGby,
                        newHaving == null ? null : (HavingClause) newHaving.first),
                currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(SelectClause selectClause,
            VariableSubstitutionEnvironment env) throws AsterixException {
        boolean distinct = selectClause.distinct();
        if (selectClause.selectElement()) {
            Pair<ILangExpression, VariableSubstitutionEnvironment> newSelectElement = selectClause.getSelectElement()
                    .accept(this, env);
            return new Pair<ILangExpression, VariableSubstitutionEnvironment>(
                    new SelectClause((SelectElement) newSelectElement.first, null, distinct), newSelectElement.second);
        } else {
            Pair<ILangExpression, VariableSubstitutionEnvironment> newSelectRegular = selectClause.getSelectRegular()
                    .accept(this, env);
            return new Pair<ILangExpression, VariableSubstitutionEnvironment>(
                    new SelectClause(null, (SelectRegular) newSelectRegular.first, distinct), newSelectRegular.second);
        }
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(SelectElement selectElement,
            VariableSubstitutionEnvironment env) throws AsterixException {
        Pair<ILangExpression, VariableSubstitutionEnvironment> newExpr = selectElement.getExpression().accept(this,
                env);
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(new SelectElement((Expression) newExpr.first),
                newExpr.second);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(SelectRegular selectRegular,
            VariableSubstitutionEnvironment env) throws AsterixException {
        List<Projection> newProjections = new ArrayList<Projection>();
        for (Projection projection : selectRegular.getProjections()) {
            newProjections.add((Projection) projection.accept(this, env).first);
        }
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(new SelectRegular(newProjections), env);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(SelectSetOperation selectSetOperation,
            VariableSubstitutionEnvironment env) throws AsterixException {
        SetOperationInput leftInput = selectSetOperation.getLeftInput();
        SetOperationInput newLeftInput = null;

        // Sets the left input.
        if (leftInput.selectBlock()) {
            Pair<ILangExpression, VariableSubstitutionEnvironment> p = leftInput.getSelectBlock().accept(this, env);
            newLeftInput = new SetOperationInput((SelectBlock) p.first, null);
        } else {
            Pair<ILangExpression, VariableSubstitutionEnvironment> p = leftInput.getSubquery().accept(this, env);
            newLeftInput = new SetOperationInput(null, (SelectExpression) p.first);
        }

        // Sets the right input
        List<SetOperationRight> newRightInputs = new ArrayList<SetOperationRight>();
        for (SetOperationRight right : selectSetOperation.getRightInputs()) {
            SetOperationInput newRightInput = null;
            SetOperationInput rightInput = right.getSetOperationRightInput();
            if (rightInput.selectBlock()) {
                Pair<ILangExpression, VariableSubstitutionEnvironment> p = rightInput.getSelectBlock().accept(this,
                        env);
                newRightInput = new SetOperationInput((SelectBlock) p.first, null);
            } else {
                Pair<ILangExpression, VariableSubstitutionEnvironment> p = rightInput.getSubquery().accept(this, env);
                newRightInput = new SetOperationInput(null, (SelectExpression) p.first);
            }
            newRightInputs.add(new SetOperationRight(right.getSetOpType(), right.setSemantics(), newRightInput));
        }
        SelectSetOperation newSelectSetOperation = new SelectSetOperation(newLeftInput, newRightInputs);
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(newSelectSetOperation, env);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(SelectExpression selectExpression,
            VariableSubstitutionEnvironment env) throws AsterixException {
        boolean subquery = selectExpression.isSubquery();
        List<LetClause> newLetList = new ArrayList<LetClause>();
        SelectSetOperation newSelectSetOperation = null;
        OrderbyClause newOrderbyClause = null;
        LimitClause newLimitClause = null;

        VariableSubstitutionEnvironment currentEnv = env;
        Pair<ILangExpression, VariableSubstitutionEnvironment> p = null;
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                p = letClause.accept(this, currentEnv);
                newLetList.add(letClause);
                currentEnv = p.second;
            }
        }

        p = selectExpression.getSelectSetOperation().accept(this, env);
        newSelectSetOperation = (SelectSetOperation) p.first;
        currentEnv = p.second;

        if (selectExpression.hasOrderby()) {
            p = selectExpression.getOrderbyClause().accept(this, env);
            newOrderbyClause = (OrderbyClause) p.first;
            currentEnv = p.second;
        }

        if (selectExpression.hasLimit()) {
            p = selectExpression.getLimitClause().accept(this, env);
            newLimitClause = (LimitClause) p.first;
            currentEnv = p.second;
        }
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(
                new SelectExpression(newLetList, newSelectSetOperation, newOrderbyClause, newLimitClause, subquery),
                currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(HavingClause havingClause,
            VariableSubstitutionEnvironment env) throws AsterixException {
        Pair<ILangExpression, VariableSubstitutionEnvironment> p = havingClause.getFilterExpression().accept(this, env);
        HavingClause newHavingClause = new HavingClause((Expression) p.first);
        return new Pair<ILangExpression, VariableSubstitutionEnvironment>(newHavingClause, p.second);
    }

}