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
package org.apache.asterix.translator;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslator;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Clause.ClauseType;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.util.FunctionUtil;
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
import org.apache.asterix.lang.sqlpp.expression.CaseExpression;
import org.apache.asterix.lang.sqlpp.expression.IndependentSubquery;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.optype.SetOpType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestNonMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;

/**
 * Each visit returns a pair of an operator and a variable. The variable
 * corresponds to the new column, if any, added to the tuple flow. E.g., for
 * Unnest, the column is the variable bound to the elements in the list, for
 * Subplan it is null. The first argument of a visit method is the expression
 * which is translated. The second argument of a visit method is the tuple
 * source for the current subtree.
 */
class SqlppExpressionToPlanTranslator extends LangExpressionToPlanTranslator implements ILangExpressionToPlanTranslator,
        ISqlppVisitor<Pair<ILogicalOperator, LogicalVariable>, Mutable<ILogicalOperator>> {
    private static final String ERR_MSG = "Translator should never enter this method!";
    private Deque<Mutable<ILogicalOperator>> uncorrelatedLeftBranchStack = new ArrayDeque<>();

    public SqlppExpressionToPlanTranslator(MetadataProvider metadataProvider, int currentVarCounter)
            throws AlgebricksException {
        super(metadataProvider, currentVarCounter);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(Query q, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Expression queryBody = q.getBody();
        if (queryBody.getKind() == Kind.SELECT_EXPRESSION) {
            SelectExpression selectExpr = (SelectExpression) queryBody;
            if (q.isTopLevel()) {
                selectExpr.setSubquery(false);
            }
            return queryBody.accept(this, tupSource);
        } else {
            LogicalVariable var = context.newVar();
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(queryBody, tupSource);
            AssignOperator assignOp = new AssignOperator(var, new MutableObject<ILogicalExpression>(eo.first));
            assignOp.getInputs().add(eo.second);
            ProjectOperator projectOp = new ProjectOperator(var);
            projectOp.getInputs().add(new MutableObject<ILogicalOperator>(assignOp));
            return new Pair<>(projectOp, var);
        }
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectExpression selectExpression,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        if (selectExpression.isSubquery()) {
            context.enterSubplan();
        }
        Mutable<ILogicalOperator> currentOpRef = tupSource;
        if (selectExpression.hasLetClauses()) {
            for (LetClause letClause : selectExpression.getLetList()) {
                currentOpRef = new MutableObject<>(letClause.accept(this, currentOpRef).first);
            }
        }
        Pair<ILogicalOperator, LogicalVariable> select =
                selectExpression.getSelectSetOperation().accept(this, currentOpRef);
        currentOpRef = new MutableObject<>(select.first);
        if (selectExpression.hasOrderby()) {
            currentOpRef = new MutableObject<>(selectExpression.getOrderbyClause().accept(this, currentOpRef).first);
        }
        if (selectExpression.hasLimit()) {
            currentOpRef = new MutableObject<>(selectExpression.getLimitClause().accept(this, currentOpRef).first);
        }
        Pair<ILogicalOperator, LogicalVariable> result =
                produceSelectPlan(selectExpression.isSubquery(), currentOpRef, select.second);
        if (selectExpression.isSubquery()) {
            context.exitSubplan();
        }
        return result;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(IndependentSubquery independentSubquery,
            Mutable<ILogicalOperator> tupleSource) throws AsterixException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo =
                langExprToAlgExpression(independentSubquery.getExpr(), tupleSource);
        LogicalVariable var = context.newVar();
        AssignOperator assignOp = new AssignOperator(var, new MutableObject<ILogicalExpression>(eo.first));
        assignOp.getInputs().add(eo.second);
        return new Pair<>(assignOp, var);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectSetOperation selectSetOperation,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        SetOperationInput leftInput = selectSetOperation.getLeftInput();
        if (!selectSetOperation.hasRightInputs()) {
            return leftInput.accept(this, tupSource);
        }
        List<ILangExpression> inputExprs = new ArrayList<>();
        inputExprs.add(leftInput.selectBlock()
                ? new SelectExpression(null, new SelectSetOperation(leftInput, null), null, null, true)
                : leftInput.getSubquery());
        for (SetOperationRight setOperationRight : selectSetOperation.getRightInputs()) {
            SetOpType setOpType = setOperationRight.getSetOpType();
            if (setOpType != SetOpType.UNION || setOperationRight.isSetSemantics()) {
                throw new AsterixException("Operation " + setOpType
                        + (setOperationRight.isSetSemantics() ? " with set semantics" : "ALL") + " is not supported.");
            }
            SetOperationInput rightInput = setOperationRight.getSetOperationRightInput();
            inputExprs.add(rightInput.selectBlock()
                    ? new SelectExpression(null, new SelectSetOperation(rightInput, null), null, null, true)
                    : rightInput.getSubquery());
        }
        return translateUnionAllFromInputExprs(inputExprs, tupSource);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectBlock selectBlock, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Mutable<ILogicalOperator> currentOpRef = tupSource;
        if (selectBlock.hasFromClause()) {
            currentOpRef = new MutableObject<>(selectBlock.getFromClause().accept(this, currentOpRef).first);
        }
        if (selectBlock.hasLetClauses()) {
            for (LetClause letClause : selectBlock.getLetList()) {
                currentOpRef = new MutableObject<>(letClause.accept(this, currentOpRef).first);
            }
        }
        if (selectBlock.hasWhereClause()) {
            currentOpRef = new MutableObject<>(selectBlock.getWhereClause().accept(this, currentOpRef).first);
        }
        if (selectBlock.hasGroupbyClause()) {
            currentOpRef = new MutableObject<>(selectBlock.getGroupbyClause().accept(this, currentOpRef).first);
        }
        if (selectBlock.hasLetClausesAfterGroupby()) {
            for (LetClause letClause : selectBlock.getLetListAfterGroupby()) {
                currentOpRef = new MutableObject<>(letClause.accept(this, currentOpRef).first);
            }
        }
        if (selectBlock.hasHavingClause()) {
            currentOpRef = new MutableObject<>(selectBlock.getHavingClause().accept(this, currentOpRef).first);
        }
        return processSelectClause(selectBlock, currentOpRef);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FromClause fromClause, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        Mutable<ILogicalOperator> inputSrc = arg;
        Pair<ILogicalOperator, LogicalVariable> topUnnest = null;
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            topUnnest = fromTerm.accept(this, inputSrc);
            inputSrc = new MutableObject<>(topUnnest.first);
        }
        return topUnnest;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FromTerm fromTerm, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        LogicalVariable fromVar = context.newVar(fromTerm.getLeftVariable());
        Expression fromExpr = fromTerm.getLeftExpression();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(fromExpr, tupSource);
        ILogicalOperator unnestOp;
        if (fromTerm.hasPositionalVariable()) {
            LogicalVariable pVar = context.newVar(fromTerm.getPositionalVariable());
            // We set the positional variable type as INT64 type.
            unnestOp =
                    new UnnestOperator(fromVar, new MutableObject<ILogicalExpression>(makeUnnestExpression(eo.first)),
                            pVar, BuiltinType.AINT64, new AqlPositionWriter());
        } else {
            unnestOp =
                    new UnnestOperator(fromVar, new MutableObject<ILogicalExpression>(makeUnnestExpression(eo.first)));
        }
        unnestOp.getInputs().add(eo.second);

        // Processes joins, unnests, and nests.
        Mutable<ILogicalOperator> topOpRef = new MutableObject<>(unnestOp);
        if (fromTerm.hasCorrelateClauses()) {
            for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
                if (correlateClause.getClauseType() == ClauseType.UNNEST_CLAUSE) {
                    // Correlation is allowed.
                    topOpRef = new MutableObject<>(correlateClause.accept(this, topOpRef).first);
                } else {
                    // Correlation is dis-allowed.
                    uncorrelatedLeftBranchStack.push(topOpRef);
                    topOpRef = new MutableObject<>(correlateClause.accept(this, tupSource).first);
                }
            }
        }
        return new Pair<>(topOpRef.getValue(), fromVar);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(JoinClause joinClause, Mutable<ILogicalOperator> inputRef)
            throws AsterixException {
        Mutable<ILogicalOperator> leftInputRef = uncorrelatedLeftBranchStack.pop();
        if (joinClause.getJoinType() == JoinType.INNER) {
            Pair<ILogicalOperator, LogicalVariable> rightBranch =
                    generateUnnestForBinaryCorrelateRightBranch(joinClause, inputRef, true);
            // A join operator with condition TRUE.
            AbstractBinaryJoinOperator joinOperator =
                    new InnerJoinOperator(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE), leftInputRef,
                            new MutableObject<ILogicalOperator>(rightBranch.first));
            Mutable<ILogicalOperator> joinOpRef = new MutableObject<>(joinOperator);

            // Add an additional filter operator.
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> conditionExprOpPair =
                    langExprToAlgExpression(joinClause.getConditionExpression(), joinOpRef);
            SelectOperator filter =
                    new SelectOperator(new MutableObject<ILogicalExpression>(conditionExprOpPair.first), false, null);
            filter.getInputs().add(conditionExprOpPair.second);
            return new Pair<>(filter, rightBranch.second);
        } else {
            // Creates a subplan operator.
            SubplanOperator subplanOp = new SubplanOperator();
            Mutable<ILogicalOperator> ntsRef =
                    new MutableObject<>(new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(subplanOp)));
            subplanOp.getInputs().add(leftInputRef);

            // Enters the translation for a subplan.
            context.enterSubplan();

            // Adds an unnest operator to unnest to right expression.
            Pair<ILogicalOperator, LogicalVariable> rightBranch =
                    generateUnnestForBinaryCorrelateRightBranch(joinClause, ntsRef, true);
            AbstractUnnestNonMapOperator rightUnnestOp = (AbstractUnnestNonMapOperator) rightBranch.first;

            // Adds an additional filter operator for the join condition.
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> conditionExprOpPair = langExprToAlgExpression(
                    joinClause.getConditionExpression(), new MutableObject<ILogicalOperator>(rightUnnestOp));
            SelectOperator filter =
                    new SelectOperator(new MutableObject<ILogicalExpression>(conditionExprOpPair.first), false, null);
            filter.getInputs().add(conditionExprOpPair.second);

            ILogicalOperator currentTopOp = filter;
            LogicalVariable varToListify;
            boolean hasRightPosVar = rightUnnestOp.getPositionalVariable() != null;
            if (hasRightPosVar) {
                // Creates record to get correlation between the two aggregate variables.
                ScalarFunctionCallExpression recordCreationFunc = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR),
                        // Field name for the listified right unnest var.
                        new MutableObject<ILogicalExpression>(
                                new ConstantExpression(new AsterixConstantValue(new AString("unnestvar")))),
                        // The listified right unnest var
                        new MutableObject<ILogicalExpression>(
                                new VariableReferenceExpression(rightUnnestOp.getVariable())),
                        // Field name for the listified right unnest positional var.
                        new MutableObject<ILogicalExpression>(
                                new ConstantExpression(new AsterixConstantValue(new AString("posvar")))),
                        // The listified right unnest positional var.
                        new MutableObject<ILogicalExpression>(
                                new VariableReferenceExpression(rightUnnestOp.getPositionalVariable())));

                // Assigns the record constructor function to a record variable.
                LogicalVariable recordVar = context.newVar();
                AssignOperator assignOp =
                        new AssignOperator(recordVar, new MutableObject<ILogicalExpression>(recordCreationFunc));
                assignOp.getInputs().add(new MutableObject<ILogicalOperator>(currentTopOp));

                // Sets currentTopOp and varToListify for later usages.
                currentTopOp = assignOp;
                varToListify = recordVar;
            } else {
                varToListify = rightUnnestOp.getVariable();
            }

            // Adds an aggregate operator to listfy unnest variables.
            AggregateFunctionCallExpression fListify = AsterixBuiltinFunctions
                    .makeAggregateFunctionExpression(AsterixBuiltinFunctions.LISTIFY, mkSingletonArrayList(
                            new MutableObject<ILogicalExpression>(new VariableReferenceExpression(varToListify))));

            LogicalVariable aggVar = context.newSubplanOutputVar();
            AggregateOperator aggOp = new AggregateOperator(mkSingletonArrayList(aggVar),
                    mkSingletonArrayList(new MutableObject<ILogicalExpression>(fListify)));
            aggOp.getInputs().add(new MutableObject<ILogicalOperator>(currentTopOp));

            // Exits the translation of a subplan.
            context.exitSubplan();

            // Sets the nested subplan of the subplan operator.
            ILogicalPlan subplan = new ALogicalPlanImpl(new MutableObject<ILogicalOperator>(aggOp));
            subplanOp.getNestedPlans().add(subplan);

            // Outer unnest the aggregated var from the subplan.
            LogicalVariable outerUnnestVar = context.newVar();
            LeftOuterUnnestOperator outerUnnestOp =
                    new LeftOuterUnnestOperator(outerUnnestVar, new MutableObject<ILogicalExpression>(
                            makeUnnestExpression(new VariableReferenceExpression(aggVar))));
            outerUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(subplanOp));
            currentTopOp = outerUnnestOp;

            if (hasRightPosVar) {
                ScalarFunctionCallExpression fieldAccessForRightUnnestVar = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX),
                        new MutableObject<ILogicalExpression>(new VariableReferenceExpression(outerUnnestVar)),
                        new MutableObject<ILogicalExpression>(
                                new ConstantExpression(new AsterixConstantValue(new AInt32(0)))));
                ScalarFunctionCallExpression fieldAccessForRightPosVar = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX),
                        new MutableObject<ILogicalExpression>(new VariableReferenceExpression(outerUnnestVar)),
                        new MutableObject<ILogicalExpression>(
                                new ConstantExpression(new AsterixConstantValue(new AInt32(1)))));

                // Creates variables for assign.
                LogicalVariable rightUnnestVar = context.newVar();
                LogicalVariable rightPosVar = context.newVar();

                // Relate the assigned variables to the variable expression in AST.
                context.setVar(joinClause.getRightVariable(), rightUnnestVar);
                context.setVar(joinClause.getPositionalVariable(), rightPosVar);

                // Varaibles to assign.
                List<LogicalVariable> assignVars = new ArrayList<>();
                assignVars.add(rightUnnestVar);
                assignVars.add(rightPosVar);

                // Expressions for assign.
                List<Mutable<ILogicalExpression>> assignExprs = new ArrayList<>();
                assignExprs.add(new MutableObject<ILogicalExpression>(fieldAccessForRightUnnestVar));
                assignExprs.add(new MutableObject<ILogicalExpression>(fieldAccessForRightPosVar));

                // Creates the assign operator.
                AssignOperator assignOp = new AssignOperator(assignVars, assignExprs);
                assignOp.getInputs().add(new MutableObject<ILogicalOperator>(currentTopOp));
                currentTopOp = assignOp;
            } else {
                context.setVar(joinClause.getRightVariable(), outerUnnestVar);
            }
            return new Pair<>(currentTopOp, null);
        }
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(NestClause nestClause, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        throw new NotImplementedException("Nest clause has not been implemented");
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(UnnestClause unnestClause,
            Mutable<ILogicalOperator> inputOpRef) throws AsterixException {
        return generateUnnestForBinaryCorrelateRightBranch(unnestClause, inputOpRef,
                unnestClause.getJoinType() == JoinType.INNER);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(HavingClause havingClause, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p =
                langExprToAlgExpression(havingClause.getFilterExpression(), tupSource);
        SelectOperator s = new SelectOperator(new MutableObject<ILogicalExpression>(p.first), false, null);
        s.getInputs().add(p.second);
        return new Pair<>(s, null);
    }

    private Pair<ILogicalOperator, LogicalVariable> generateUnnestForBinaryCorrelateRightBranch(
            AbstractBinaryCorrelateClause binaryCorrelate, Mutable<ILogicalOperator> inputOpRef, boolean innerUnnest)
            throws AsterixException {
        LogicalVariable rightVar = context.newVar(binaryCorrelate.getRightVariable());
        Expression rightExpr = binaryCorrelate.getRightExpression();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(rightExpr, inputOpRef);
        ILogicalOperator unnestOp;
        if (binaryCorrelate.hasPositionalVariable()) {
            LogicalVariable pVar = context.newVar(binaryCorrelate.getPositionalVariable());
            // We set the positional variable type as INT64 type.
            unnestOp = innerUnnest
                    ? new UnnestOperator(rightVar, new MutableObject<>(makeUnnestExpression(eo.first)), pVar,
                            BuiltinType.AINT64, new AqlPositionWriter())
                    : new LeftOuterUnnestOperator(rightVar, new MutableObject<>(makeUnnestExpression(eo.first)), pVar,
                            BuiltinType.AINT64, new AqlPositionWriter());
        } else {
            unnestOp = innerUnnest ? new UnnestOperator(rightVar, new MutableObject<>(makeUnnestExpression(eo.first)))
                    : new LeftOuterUnnestOperator(rightVar, new MutableObject<>(makeUnnestExpression(eo.first)));
        }
        unnestOp.getInputs().add(eo.second);
        return new Pair<>(unnestOp, rightVar);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectClause selectClause, Mutable<ILogicalOperator> tupSrc)
            throws AsterixException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectElement selectElement, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectRegular selectRegular, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(Projection projection, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(CaseExpression caseExpression,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        //Creates a series of subplan operators, one for each branch.
        Mutable<ILogicalOperator> currentOpRef = tupSource;
        ILogicalOperator currentOperator = null;
        List<Expression> whenExprList = caseExpression.getWhenExprs();
        List<Expression> thenExprList = caseExpression.getThenExprs();
        List<ILogicalExpression> branchCondVarReferences = new ArrayList<>();
        List<ILogicalExpression> allVarReferences = new ArrayList<>();
        for (int index = 0; index < whenExprList.size(); ++index) {
            Pair<ILogicalOperator, LogicalVariable> whenExprResult = whenExprList.get(index).accept(this, currentOpRef);
            currentOperator = whenExprResult.first;
            // Variable whenConditionVar is corresponds to the current "WHEN" condition.
            LogicalVariable whenConditionVar = whenExprResult.second;
            Mutable<ILogicalExpression> branchEntraceConditionExprRef =
                    new MutableObject<>(new VariableReferenceExpression(whenConditionVar));

            // Constructs an expression that filters data based on preceding "WHEN" conditions
            // and the current "WHEN" condition. Note that only one "THEN" expression can be run
            // even though multiple "WHEN" conditions can be satisfied.
            if (!branchCondVarReferences.isEmpty()) {
                // The additional filter generated here makes sure the the tuple has not
                // entered other matched "WHEN...THEN" case.
                List<Mutable<ILogicalExpression>> andArgs = new ArrayList<>();
                andArgs.add(generateNoMatchedPrecedingWhenBranchesFilter(branchCondVarReferences));
                andArgs.add(branchEntraceConditionExprRef);

                // A "THEN" branch can be entered only when the tuple has not enter any other preceding
                // branches and the current "WHEN" condition is TRUE.
                branchEntraceConditionExprRef = new MutableObject<>(new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.AND), andArgs));
            }

            // Translates the corresponding "THEN" expression.
            Pair<ILogicalOperator, LogicalVariable> opAndVarForThen = constructSubplanOperatorForBranch(currentOperator,
                    branchEntraceConditionExprRef, thenExprList.get(index));

            branchCondVarReferences.add(new VariableReferenceExpression(whenConditionVar));
            allVarReferences.add(new VariableReferenceExpression(whenConditionVar));
            allVarReferences.add(new VariableReferenceExpression(opAndVarForThen.second));
            currentOperator = opAndVarForThen.first;
            currentOpRef = new MutableObject<>(currentOperator);
        }

        // Creates a subplan for the "ELSE" branch.
        Mutable<ILogicalExpression> elseCondExprRef =
                generateNoMatchedPrecedingWhenBranchesFilter(branchCondVarReferences);
        Pair<ILogicalOperator, LogicalVariable> opAndVarForElse =
                constructSubplanOperatorForBranch(currentOperator, elseCondExprRef, caseExpression.getElseExpr());

        // Uses switch-case function to select the results of two branches.
        LogicalVariable selectVar = context.newVar();
        List<Mutable<ILogicalExpression>> arguments = new ArrayList<>();
        arguments.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE))));
        for (ILogicalExpression argVar : allVarReferences) {
            arguments.add(new MutableObject<>(argVar));
        }
        arguments.add(new MutableObject<>(new VariableReferenceExpression(opAndVarForElse.second)));
        AbstractFunctionCallExpression swithCaseExpr = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.SWITCH_CASE), arguments);
        AssignOperator assignOp = new AssignOperator(selectVar, new MutableObject<>(swithCaseExpr));
        assignOp.getInputs().add(new MutableObject<>(opAndVarForElse.first));

        // Unnests the selected (a "THEN" or "ELSE" branch) result.
        LogicalVariable unnestVar = context.newVar();
        UnnestOperator unnestOp = new UnnestOperator(unnestVar,
                new MutableObject<>(new UnnestingFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.SCAN_COLLECTION), Collections
                                .singletonList(new MutableObject<>(new VariableReferenceExpression(selectVar))))));
        unnestOp.getInputs().add(new MutableObject<>(assignOp));

        // Produces the final assign operator.
        LogicalVariable resultVar = context.newVar();
        AssignOperator finalAssignOp =
                new AssignOperator(resultVar, new MutableObject<>(new VariableReferenceExpression(unnestVar)));
        finalAssignOp.getInputs().add(new MutableObject<>(unnestOp));
        return new Pair<>(finalAssignOp, resultVar);
    }

    private Pair<ILogicalOperator, LogicalVariable> produceSelectPlan(boolean isSubquery,
            Mutable<ILogicalOperator> returnOpRef, LogicalVariable resVar) {
        if (isSubquery) {
            return aggListifyForSubquery(resVar, returnOpRef, false);
        } else {
            ProjectOperator pr = new ProjectOperator(resVar);
            pr.getInputs().add(returnOpRef);
            return new Pair<>(pr, resVar);
        }
    }

    // Recursively replaces nested tuple source with empty tuple source
    // in the operator tree under opRef.
    private void replaceNtsWithEtsTopDown(Mutable<ILogicalOperator> opRef) {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            opRef.setValue(new EmptyTupleSourceOperator());
        }
        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            replaceNtsWithEtsTopDown(childRef);
        }
    }

    // Generates the return expression for a select clause.
    private Pair<ILogicalOperator, LogicalVariable> processSelectClause(SelectBlock selectBlock,
            Mutable<ILogicalOperator> tupSrc) throws AsterixException {
        SelectClause selectClause = selectBlock.getSelectClause();
        Expression returnExpr;
        if (selectClause.selectElement()) {
            returnExpr = selectClause.getSelectElement().getExpression();
        } else {
            returnExpr = generateReturnExpr(selectClause, selectBlock);
        }
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(returnExpr, tupSrc);
        LogicalVariable returnVar;
        ILogicalOperator returnOperator;
        if (returnExpr.getKind() == Kind.VARIABLE_EXPRESSION) {
            VariableExpr varExpr = (VariableExpr) returnExpr;
            returnOperator = eo.second.getValue();
            returnVar = context.getVar(varExpr.getVar().getId());
        } else {
            returnVar = context.newVar();
            returnOperator = new AssignOperator(returnVar, new MutableObject<ILogicalExpression>(eo.first));
            returnOperator.getInputs().add(eo.second);
        }
        if (selectClause.distinct()) {
            DistinctOperator distinctOperator = new DistinctOperator(mkSingletonArrayList(
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(returnVar))));
            distinctOperator.getInputs().add(new MutableObject<ILogicalOperator>(returnOperator));
            return new Pair<>(distinctOperator, returnVar);
        } else {
            return new Pair<>(returnOperator, returnVar);
        }
    }

    // Generates the return expression for a select clause.
    private Expression generateReturnExpr(SelectClause selectClause, SelectBlock selectBlock) {
        SelectRegular selectRegular = selectClause.getSelectRegular();
        List<FieldBinding> fieldBindings = new ArrayList<>();
        List<Projection> projections = selectRegular.getProjections();
        for (Projection projection : projections) {
            if (projection.star()) {
                if (selectBlock.hasGroupbyClause()) {
                    fieldBindings.addAll(getGroupBindings(selectBlock.getGroupbyClause()));
                } else if (selectBlock.hasFromClause()) {
                    fieldBindings.addAll(getFromBindings(selectBlock.getFromClause()));
                }
            } else {
                fieldBindings.add(new FieldBinding(new LiteralExpr(new StringLiteral(projection.getName())),
                        projection.getExpression()));
            }
        }
        return new RecordConstructor(fieldBindings);
    }

    // Generates all field bindings according to the from clause.
    private List<FieldBinding> getFromBindings(FromClause fromClause) {
        List<FieldBinding> fieldBindings = new ArrayList<>();
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            fieldBindings.add(getFieldBinding(fromTerm.getLeftVariable()));
            if (fromTerm.hasPositionalVariable()) {
                fieldBindings.add(getFieldBinding(fromTerm.getPositionalVariable()));
            }
            if (!fromTerm.hasCorrelateClauses()) {
                continue;
            }
            for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
                fieldBindings.add(getFieldBinding(correlateClause.getRightVariable()));
                if (correlateClause.hasPositionalVariable()) {
                    fieldBindings.add(getFieldBinding(correlateClause.getPositionalVariable()));
                }
            }
        }
        return fieldBindings;
    }

    // Generates all field bindings according to the from clause.
    private List<FieldBinding> getGroupBindings(GroupbyClause groupbyClause) {
        List<FieldBinding> fieldBindings = new ArrayList<>();
        for (GbyVariableExpressionPair pair : groupbyClause.getGbyPairList()) {
            fieldBindings.add(getFieldBinding(pair.getVar()));
        }
        if (groupbyClause.hasWithMap() && groupbyClause.hasGroupVar()) {
            // Makes sure that we add the re-mapped group variable which refers to a collection.
            fieldBindings.add(getFieldBinding(groupbyClause.getWithVarMap().get(groupbyClause.getGroupVar())));
        }
        return fieldBindings;
    }

    // Generates a field binding for a variable.
    private FieldBinding getFieldBinding(VariableExpr var) {
        LiteralExpr fieldName = new LiteralExpr(
                new StringLiteral(SqlppVariableUtil.variableNameToDisplayedFieldName(var.getVar().getValue())));
        return new FieldBinding(fieldName, var);
    }

}
