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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Clause.ClauseType;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.util.VariableCloneAndSubstitutionUtil;
import org.apache.asterix.lang.common.visitor.CloneAndSubstituteVariablesVisitor;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.ClusterbyClause;
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
import org.apache.asterix.lang.sqlpp.expression.CaseExpression;
import org.apache.asterix.lang.sqlpp.expression.ChangeExpression;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.struct.TimeTravelSpec;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.commons.lang3.tuple.Pair;

public class SqlppCloneAndSubstituteVariablesVisitor extends CloneAndSubstituteVariablesVisitor implements
        ISqlppVisitor<Pair<ILangExpression, VariableSubstitutionEnvironment>, VariableSubstitutionEnvironment> {

    private LangRewritingContext context;

    public SqlppCloneAndSubstituteVariablesVisitor(LangRewritingContext context) {
        super(context);
        this.context = context;
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(FromClause fromClause,
            VariableSubstitutionEnvironment env) throws CompilationException {
        VariableSubstitutionEnvironment currentEnv = new VariableSubstitutionEnvironment(env);
        List<FromTerm> newFromTerms = new ArrayList<>();
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            Pair<ILangExpression, VariableSubstitutionEnvironment> p = fromTerm.accept(this, currentEnv);
            newFromTerms.add((FromTerm) p.getLeft());
            // A right from term could be correlated from a left from term,
            // therefore we propagate the substitution environment.
            currentEnv = p.getRight();
        }
        FromClause newFromClause = new FromClause(newFromTerms);
        newFromClause.setSourceLocation(fromClause.getSourceLocation());
        return Pair.of(newFromClause, currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(FromTerm fromTerm,
            VariableSubstitutionEnvironment env) throws CompilationException {
        VariableExpr leftVar = fromTerm.getLeftVariable();
        VariableExpr newLeftVar = generateNewVariable(context, leftVar);
        VariableExpr newLeftPosVar = fromTerm.hasPositionalVariable()
                ? generateNewVariable(context, fromTerm.getPositionalVariable()) : null;
        Expression newLeftExpr = (Expression) visitUnnestBindingExpression(fromTerm.getLeftExpression(), env).getLeft();
        List<AbstractBinaryCorrelateClause> newCorrelateClauses = new ArrayList<>();

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
                currentEnv = p.getRight();
                newCorrelateClauses.add((AbstractBinaryCorrelateClause) p.getLeft());
            } else {
                // The right-hand-side of join and nest could not be correlated with the left side,
                // therefore we propagate the original substitution environment.
                newCorrelateClauses.add((AbstractBinaryCorrelateClause) correlateClause.accept(this, env).getLeft());
                // Join binding variables should be removed for further traversal.
                currentEnv.removeSubstitution(correlateClause.getRightVariable());
                if (correlateClause.hasPositionalVariable()) {
                    currentEnv.removeSubstitution(correlateClause.getPositionalVariable());
                }
            }
        }
        FromTerm newFromTerm = new FromTerm(newLeftExpr, newLeftVar, newLeftPosVar, newCorrelateClauses,
                copyTimeTravel(fromTerm.getTimeTravel(), env));
        newFromTerm.setSourceLocation(fromTerm.getSourceLocation());
        return Pair.of(newFromTerm, currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(JoinClause joinClause,
            VariableSubstitutionEnvironment env) throws CompilationException {
        VariableExpr rightVar = joinClause.getRightVariable();
        VariableExpr newRightVar = generateNewVariable(context, rightVar);
        VariableExpr newRightPosVar = joinClause.hasPositionalVariable()
                ? generateNewVariable(context, joinClause.getPositionalVariable()) : null;

        // Visits the right expression.
        Expression newRightExpr =
                (Expression) visitUnnestBindingExpression(joinClause.getRightExpression(), env).getLeft();

        // Visits the condition.
        VariableSubstitutionEnvironment currentEnv = new VariableSubstitutionEnvironment(env);
        currentEnv.removeSubstitution(newRightVar);
        if (newRightPosVar != null) {
            currentEnv.removeSubstitution(newRightPosVar);
        }
        // The condition can refer to the newRightVar and newRightPosVar.
        Expression conditionExpr = (Expression) joinClause.getConditionExpression().accept(this, currentEnv).getLeft();

        JoinClause newJoinClause =
                new JoinClause(joinClause.getJoinType(), newRightExpr, newRightVar, newRightPosVar, conditionExpr,
                        joinClause.getOuterJoinMissingValueType(), copyTimeTravel(joinClause.getTimeTravel(), env));
        newJoinClause.setSourceLocation(joinClause.getSourceLocation());
        return Pair.of(newJoinClause, currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(NestClause nestClause,
            VariableSubstitutionEnvironment env) throws CompilationException {
        VariableExpr rightVar = nestClause.getRightVariable();
        VariableExpr newRightVar = generateNewVariable(context, rightVar);
        VariableExpr newRightPosVar = nestClause.hasPositionalVariable()
                ? generateNewVariable(context, nestClause.getPositionalVariable()) : null;

        // Visits the right expression.
        Expression rightExpr = (Expression) nestClause.getRightExpression().accept(this, env).getLeft();

        // Visits the condition.
        VariableSubstitutionEnvironment currentEnv = new VariableSubstitutionEnvironment(env);
        currentEnv.removeSubstitution(newRightVar);
        if (newRightPosVar != null) {
            currentEnv.removeSubstitution(newRightPosVar);
        }
        // The condition can refer to the newRightVar and newRightPosVar.
        Expression conditionExpr = (Expression) nestClause.getConditionExpression().accept(this, currentEnv).getLeft();

        NestClause newNestClause =
                new NestClause(nestClause.getNestType(), rightExpr, newRightVar, newRightPosVar, conditionExpr);
        newNestClause.setSourceLocation(nestClause.getSourceLocation());
        return Pair.of(newNestClause, currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(UnnestClause unnestClause,
            VariableSubstitutionEnvironment env) throws CompilationException {
        VariableExpr rightVar = unnestClause.getRightVariable();
        VariableExpr newRightVar = generateNewVariable(context, rightVar);
        VariableExpr newRightPosVar = unnestClause.hasPositionalVariable()
                ? generateNewVariable(context, unnestClause.getPositionalVariable()) : null;

        // Visits the right expression.
        Expression rightExpr =
                (Expression) visitUnnestBindingExpression(unnestClause.getRightExpression(), env).getLeft();

        // Visits the condition.
        VariableSubstitutionEnvironment currentEnv = new VariableSubstitutionEnvironment(env);
        currentEnv.removeSubstitution(newRightVar);
        if (newRightPosVar != null) {
            currentEnv.removeSubstitution(newRightPosVar);
        }
        // The condition can refer to the newRightVar and newRightPosVar.
        UnnestClause newUnnestClause = new UnnestClause(unnestClause.getUnnestType(), rightExpr, newRightVar,
                newRightPosVar, unnestClause.getOuterUnnestMissingValueType());
        newUnnestClause.setSourceLocation(unnestClause.getSourceLocation());
        return Pair.of(newUnnestClause, currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(Projection projection,
            VariableSubstitutionEnvironment env) throws CompilationException {
        Projection newProjection = new Projection(projection.getKind(),
                projection.hasExpression() ? (Expression) projection.getExpression().accept(this, env).getLeft() : null,
                projection.getName());
        newProjection.setSourceLocation(projection.getSourceLocation());
        return Pair.of(newProjection, env);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(SelectBlock selectBlock,
            VariableSubstitutionEnvironment env) throws CompilationException {
        Pair<ILangExpression, VariableSubstitutionEnvironment> newFrom = null;
        Pair<ILangExpression, VariableSubstitutionEnvironment> newLetWhere;
        Pair<ILangExpression, VariableSubstitutionEnvironment> newGroupby = null;
        Pair<ILangExpression, VariableSubstitutionEnvironment> newLetHaving;
        Pair<ILangExpression, VariableSubstitutionEnvironment> newSelect;
        List<AbstractClause> newLetWhereClauses = new ArrayList<>();
        List<AbstractClause> newLetHavingClausesAfterGby = new ArrayList<>();
        VariableSubstitutionEnvironment currentEnv = new VariableSubstitutionEnvironment(env);

        if (selectBlock.hasFromClause()) {
            newFrom = selectBlock.getFromClause().accept(this, currentEnv);
            currentEnv = newFrom.getRight();
        }

        if (selectBlock.hasLetWhereClauses()) {
            for (AbstractClause letWhereClause : selectBlock.getLetWhereList()) {
                newLetWhere = letWhereClause.accept(this, currentEnv);
                currentEnv = newLetWhere.getRight();
                newLetWhereClauses.add((AbstractClause) newLetWhere.getLeft());
            }
        }

        if (selectBlock.hasGroupbyClause()) {
            newGroupby = selectBlock.getGroupbyClause().accept(this, currentEnv);
            currentEnv = newGroupby.getRight();
            if (selectBlock.hasLetHavingClausesAfterGroupby()) {
                for (AbstractClause letHavingClauseAfterGby : selectBlock.getLetHavingListAfterGroupby()) {
                    newLetHaving = letHavingClauseAfterGby.accept(this, currentEnv);
                    currentEnv = newLetHaving.getRight();
                    newLetHavingClausesAfterGby.add((AbstractClause) newLetHaving.getLeft());
                }
            }
        }

        Pair<ILangExpression, VariableSubstitutionEnvironment> newClusterby = null;
        if (selectBlock.hasClusterbyClause()) {
            // Before the SELECT, which reads the variables the clause binds.
            newClusterby = selectBlock.getClusterbyClause().accept(this, currentEnv);
            currentEnv = newClusterby.getRight();
        }

        newSelect = selectBlock.getSelectClause().accept(this, currentEnv);
        currentEnv = newSelect.getRight();
        FromClause fromClause = newFrom == null ? null : (FromClause) newFrom.getLeft();
        GroupbyClause groupbyClause = newGroupby == null ? null : (GroupbyClause) newGroupby.getLeft();
        SelectBlock newSelectBlock = new SelectBlock((SelectClause) newSelect.getLeft(), fromClause, newLetWhereClauses,
                groupbyClause, newLetHavingClausesAfterGby);
        if (newClusterby != null) {
            newSelectBlock.setClusterbyClause((ClusterbyClause) newClusterby.getLeft());
        }
        newSelectBlock.setSourceLocation(selectBlock.getSourceLocation());
        return Pair.of(newSelectBlock, currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(ClusterbyClause cc,
            VariableSubstitutionEnvironment env) throws CompilationException {
        Expression newExpr = (Expression) cc.getClusteringExpression().accept(this, env).getLeft();
        // The clause binds variables the rest of the block reads: the descriptor and members the user named,
        // and the output variables the rewrite resolved the descriptor's fields to. All are renamed here and
        // the renames are returned, so the SELECT cloned after this clause follows them.
        VariableSubstitutionEnvironment newEnv = new VariableSubstitutionEnvironment(env);
        VariableExpr newDescVar =
                cc.hasClusterDescriptorVar() ? renameBound(cc.getClusterDescriptorVar(), newEnv) : null;
        VariableExpr newMembersVar = cc.hasClusterMembersVar() ? renameBound(cc.getClusterMembersVar(), newEnv) : null;
        List<Pair<Expression, Identifier>> newClusterFieldList = cc.hasClusterFieldList()
                ? VariableCloneAndSubstitutionUtil.substInFieldList(cc.getClusterFieldList(), env, this) : null;
        RecordConstructor newWith =
                cc.hasWithOptions() ? (RecordConstructor) cc.getWithOptions().accept(this, env).getLeft() : null;
        ClusterbyClause newClusterbyClause =
                new ClusterbyClause(newExpr, newDescVar, newMembersVar, newClusterFieldList, newWith);
        newClusterbyClause.setSourceLocation(cc.getSourceLocation());
        // The resolved state survives the clone: a view or function body is rewritten once, then inlined.
        // Shared by reference: the holder is immutable.
        newClusterbyClause.setResolvedOptions(cc.getResolvedOptions());
        if (cc.hasDecorList()) {
            // The expression reads the variable before the clause; the decoration binds it after it.
            List<GbyVariableExpressionPair> decorList = new ArrayList<>();
            for (GbyVariableExpressionPair pair : cc.getDecorPairList()) {
                Expression newDecorExpr = (Expression) pair.getExpr().accept(this, env).getLeft();
                decorList.add(new GbyVariableExpressionPair(renameBound(pair.getVar(), newEnv), newDecorExpr));
            }
            newClusterbyClause.setDecorPairList(decorList);
        }
        if (cc.getClusterIdVar() != null) {
            newClusterbyClause.setClusterIdVar(renameBound(cc.getClusterIdVar(), newEnv));
        }
        if (cc.getCentroidVar() != null) {
            newClusterbyClause.setCentroidVar(renameBound(cc.getCentroidVar(), newEnv));
        }
        return Pair.of(newClusterbyClause, newEnv);
    }

    private VariableExpr renameBound(VariableExpr var, VariableSubstitutionEnvironment env) {
        VariableExpr renamed = generateNewVariable(context, var);
        env.addSubstituion(var, renamed);
        return renamed;
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(SelectClause selectClause,
            VariableSubstitutionEnvironment env) throws CompilationException {
        boolean distinct = selectClause.distinct();
        if (selectClause.selectElement()) {
            Pair<ILangExpression, VariableSubstitutionEnvironment> newSelectElement =
                    selectClause.getSelectElement().accept(this, env);
            SelectClause newSelectClause = new SelectClause((SelectElement) newSelectElement.getLeft(), null, distinct);
            newSelectClause.setSourceLocation(selectClause.getSourceLocation());
            return Pair.of(newSelectClause, newSelectElement.getRight());
        } else {
            Pair<ILangExpression, VariableSubstitutionEnvironment> newSelectRegular =
                    selectClause.getSelectRegular().accept(this, env);
            List<List<String>> fieldExclusions = new ArrayList<>();
            if (!selectClause.getFieldExclusions().isEmpty()) {
                for (List<String> fieldExclusion : selectClause.getFieldExclusions()) {
                    List<String> fieldExclusionCopy = new ArrayList<>(fieldExclusion);
                    fieldExclusions.add(fieldExclusionCopy);
                }
            }
            SelectClause newSelectClause =
                    new SelectClause(null, (SelectRegular) newSelectRegular.getLeft(), fieldExclusions, distinct);
            newSelectClause.setSourceLocation(selectClause.getSourceLocation());
            return Pair.of(newSelectClause, newSelectRegular.getRight());
        }
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(SelectElement selectElement,
            VariableSubstitutionEnvironment env) throws CompilationException {
        Pair<ILangExpression, VariableSubstitutionEnvironment> newExpr =
                selectElement.getExpression().accept(this, env);
        SelectElement newSelectElement = new SelectElement((Expression) newExpr.getLeft());
        newSelectElement.setSourceLocation(selectElement.getSourceLocation());
        return Pair.of(newSelectElement, newExpr.getRight());
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(SelectRegular selectRegular,
            VariableSubstitutionEnvironment env) throws CompilationException {
        List<Projection> newProjections = new ArrayList<>();
        for (Projection projection : selectRegular.getProjections()) {
            newProjections.add((Projection) projection.accept(this, env).getLeft());
        }
        SelectRegular newSelectRegular = new SelectRegular(newProjections);
        newSelectRegular.setSourceLocation(selectRegular.getSourceLocation());
        return Pair.of(newSelectRegular, env);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(SelectSetOperation selectSetOperation,
            VariableSubstitutionEnvironment env) throws CompilationException {
        SetOperationInput leftInput = selectSetOperation.getLeftInput();
        SetOperationInput newLeftInput;

        Pair<ILangExpression, VariableSubstitutionEnvironment> leftResult;
        // Sets the left input.
        if (leftInput.selectBlock()) {
            leftResult = leftInput.getSelectBlock().accept(this, env);
            newLeftInput = new SetOperationInput((SelectBlock) leftResult.getLeft(), null);
        } else {
            leftResult = leftInput.getSubquery().accept(this, env);
            newLeftInput = new SetOperationInput(null, (SelectExpression) leftResult.getLeft());
        }

        // Sets the right input
        List<SetOperationRight> newRightInputs = new ArrayList<>();
        if (selectSetOperation.hasRightInputs()) {
            for (SetOperationRight right : selectSetOperation.getRightInputs()) {
                SetOperationInput newRightInput;
                SetOperationInput rightInput = right.getSetOperationRightInput();
                if (rightInput.selectBlock()) {
                    Pair<ILangExpression, VariableSubstitutionEnvironment> rightResult =
                            rightInput.getSelectBlock().accept(this, env);
                    newRightInput = new SetOperationInput((SelectBlock) rightResult.getLeft(), null);
                } else {
                    Pair<ILangExpression, VariableSubstitutionEnvironment> rightResult =
                            rightInput.getSubquery().accept(this, env);
                    newRightInput = new SetOperationInput(null, (SelectExpression) rightResult.getLeft());
                }
                newRightInputs.add(new SetOperationRight(right.getSetOpType(), right.isSetSemantics(), newRightInput));
            }
        }
        SelectSetOperation newSelectSetOperation = new SelectSetOperation(newLeftInput, newRightInputs);
        newSelectSetOperation.setSourceLocation(selectSetOperation.getSourceLocation());
        return Pair.of(newSelectSetOperation, selectSetOperation.hasRightInputs() ? env : leftResult.getRight());
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(SelectExpression selectExpression,
            VariableSubstitutionEnvironment env) throws CompilationException {
        boolean subquery = selectExpression.isSubquery();
        List<LetClause> newLetList = new ArrayList<>();
        SelectSetOperation newSelectSetOperation;
        OrderbyClause newOrderbyClause = null;
        LimitClause newLimitClause = null;

        VariableSubstitutionEnvironment currentEnv = env;
        Pair<ILangExpression, VariableSubstitutionEnvironment> p;
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                p = letClause.accept(this, currentEnv);
                newLetList.add((LetClause) p.getLeft());
                currentEnv = p.getRight();
            }
        }

        p = selectExpression.getSelectSetOperation().accept(this, env);
        newSelectSetOperation = (SelectSetOperation) p.getLeft();
        currentEnv = p.getRight();

        if (selectExpression.hasOrderby()) {
            p = selectExpression.getOrderbyClause().accept(this, currentEnv);
            newOrderbyClause = (OrderbyClause) p.getLeft();
            currentEnv = p.getRight();
        }

        if (selectExpression.hasLimit()) {
            p = selectExpression.getLimitClause().accept(this, currentEnv);
            newLimitClause = (LimitClause) p.getLeft();
            currentEnv = p.getRight();
        }
        SelectExpression newSelectExpression =
                new SelectExpression(newLetList, newSelectSetOperation, newOrderbyClause, newLimitClause, subquery);
        newSelectExpression.setSourceLocation(selectExpression.getSourceLocation());
        return Pair.of(newSelectExpression, currentEnv);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(HavingClause havingClause,
            VariableSubstitutionEnvironment env) throws CompilationException {
        Pair<ILangExpression, VariableSubstitutionEnvironment> p = havingClause.getFilterExpression().accept(this, env);
        HavingClause newHavingClause = new HavingClause((Expression) p.getLeft());
        newHavingClause.setSourceLocation(havingClause.getSourceLocation());
        return Pair.of(newHavingClause, p.getRight());
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(CaseExpression caseExpr,
            VariableSubstitutionEnvironment env) throws CompilationException {
        Expression conditionExpr = (Expression) caseExpr.getConditionExpr().accept(this, env).getLeft();
        List<Expression> whenExprList =
                VariableCloneAndSubstitutionUtil.visitAndCloneExprList(caseExpr.getWhenExprs(), env, this);
        List<Expression> thenExprList =
                VariableCloneAndSubstitutionUtil.visitAndCloneExprList(caseExpr.getThenExprs(), env, this);
        Expression elseExpr = (Expression) caseExpr.getElseExpr().accept(this, env).getLeft();
        CaseExpression newCaseExpr = new CaseExpression(conditionExpr, whenExprList, thenExprList, elseExpr);
        newCaseExpr.setSourceLocation(caseExpr.getSourceLocation());
        return Pair.of(newCaseExpr, env);
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(ChangeExpression changeExpr,
            VariableSubstitutionEnvironment env) throws CompilationException {
        // ChangeExpression is changed to select expression before getting to this visitor.
        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, changeExpr.getSourceLocation());
    }

    @Override
    public Pair<ILangExpression, VariableSubstitutionEnvironment> visit(WindowExpression winExpr,
            VariableSubstitutionEnvironment env) throws CompilationException {
        List<Expression> newExprList =
                VariableCloneAndSubstitutionUtil.visitAndCloneExprList(winExpr.getExprList(), env, this);
        Expression newAggFilterExpr = winExpr.hasAggregateFilterExpr()
                ? (Expression) winExpr.getAggregateFilterExpr().accept(this, env).getLeft() : null;
        List<Expression> newPartitionList = winExpr.hasPartitionList()
                ? VariableCloneAndSubstitutionUtil.visitAndCloneExprList(winExpr.getPartitionList(), env, this) : null;
        List<Expression> newOrderbyList = winExpr.hasOrderByList()
                ? VariableCloneAndSubstitutionUtil.visitAndCloneExprList(winExpr.getOrderbyList(), env, this) : null;
        List<OrderbyClause.OrderModifier> newOrderbyModifierList =
                winExpr.hasOrderByList() ? new ArrayList<>(winExpr.getOrderbyModifierList()) : null;
        List<OrderbyClause.NullOrderModifier> newOrderbyNullModifierList =
                winExpr.hasOrderByList() ? new ArrayList<>(winExpr.getOrderbyNullModifierList()) : null;
        Expression newFrameStartExpr = winExpr.hasFrameStartExpr()
                ? (Expression) winExpr.getFrameStartExpr().accept(this, env).getLeft() : null;
        Expression newFrameEndExpr =
                winExpr.hasFrameEndExpr() ? (Expression) winExpr.getFrameEndExpr().accept(this, env).getLeft() : null;
        VariableExpr newWindowVar =
                winExpr.hasWindowVar() ? (VariableExpr) winExpr.getWindowVar().accept(this, env).getLeft() : null;
        List<Pair<Expression, Identifier>> newWindowFieldList = winExpr.hasWindowFieldList()
                ? VariableCloneAndSubstitutionUtil.substInFieldList(winExpr.getWindowFieldList(), env, this) : null;
        WindowExpression newWinExpr = new WindowExpression(winExpr.getFunctionSignature(), newExprList,
                newAggFilterExpr, newPartitionList, newOrderbyList, newOrderbyModifierList, newOrderbyNullModifierList,
                winExpr.getFrameMode(), winExpr.getFrameStartKind(), newFrameStartExpr, winExpr.getFrameEndKind(),
                newFrameEndExpr, winExpr.getFrameExclusionKind(), newWindowVar, newWindowFieldList,
                winExpr.getIgnoreNulls(), winExpr.getFromLast());
        newWinExpr.setSourceLocation(winExpr.getSourceLocation());
        newWinExpr.addHints(winExpr.getHints());
        return Pair.of(newWinExpr, env);
    }

    /**
     * Clones a time travel specification in {@code env}, the scope the value is resolved in: the enclosing one,
     * before the clause's own binding variables. A value that only resolves to a constant later can still
     * reference variables here -- a function parameter while a function body is being inlined -- and they must
     * be substituted like anywhere else, or the clone refers to a variable that no longer exists.
     */
    private TimeTravelSpec copyTimeTravel(TimeTravelSpec timeTravel, VariableSubstitutionEnvironment env)
            throws CompilationException {
        if (timeTravel == null) {
            return null;
        }
        TimeTravelSpec copy = new TimeTravelSpec(
                (Expression) timeTravel.getValueExpression().accept(this, env).getLeft(), timeTravel.getType());
        copy.setSourceLocation(timeTravel.getSourceLocation());
        return copy;
    }
}
