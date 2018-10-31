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
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslator;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Clause.ClauseType;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.struct.VarIdentifier;
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
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
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
import org.apache.asterix.om.base.IACollection;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Each visit returns a pair of an operator and a variable. The variable
 * corresponds to the new column, if any, added to the tuple flow. E.g., for
 * Unnest, the column is the variable bound to the elements in the list, for
 * Subplan it is null. The first argument of a visit method is the expression
 * which is translated. The second argument of a visit method is the tuple
 * source for the current subtree.
 */
public class SqlppExpressionToPlanTranslator extends LangExpressionToPlanTranslator
        implements ILangExpressionToPlanTranslator,
        ISqlppVisitor<Pair<ILogicalOperator, LogicalVariable>, Mutable<ILogicalOperator>> {

    private static final String ERR_MSG = "Translator should never enter this method!";

    public static final String REWRITE_IN_AS_OR_OPTION = "rewrite_in_as_or";
    private static final boolean REWRITE_IN_AS_OR_OPTION_DEFAULT = true;

    private Deque<Mutable<ILogicalOperator>> uncorrelatedLeftBranchStack = new ArrayDeque<>();
    private final Map<VarIdentifier, IAObject> externalVars;
    private final boolean translateInAsOr;

    public SqlppExpressionToPlanTranslator(MetadataProvider metadataProvider, int currentVarCounter,
            Map<VarIdentifier, IAObject> externalVars) throws AlgebricksException {
        super(metadataProvider, currentVarCounter);
        this.externalVars = externalVars != null ? externalVars : Collections.emptyMap();
        translateInAsOr = metadataProvider.getBooleanProperty(REWRITE_IN_AS_OR_OPTION, REWRITE_IN_AS_OR_OPTION_DEFAULT);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(Query q, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        Expression queryBody = q.getBody();
        SourceLocation sourceLoc = queryBody.getSourceLocation();
        if (queryBody.getKind() == Kind.SELECT_EXPRESSION) {
            SelectExpression selectExpr = (SelectExpression) queryBody;
            if (q.isTopLevel()) {
                selectExpr.setSubquery(false);
            }
            return queryBody.accept(this, tupSource);
        } else {
            LogicalVariable var = context.newVar();
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(queryBody, tupSource);
            AssignOperator assignOp = new AssignOperator(var, new MutableObject<>(eo.first));
            assignOp.getInputs().add(eo.second);
            assignOp.setSourceLocation(sourceLoc);
            ProjectOperator projectOp = new ProjectOperator(var);
            projectOp.getInputs().add(new MutableObject<>(assignOp));
            projectOp.setSourceLocation(sourceLoc);
            return new Pair<>(projectOp, var);
        }
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectExpression selectExpression,
            Mutable<ILogicalOperator> tupSource) throws CompilationException {
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
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectSetOperation selectSetOperation,
            Mutable<ILogicalOperator> tupSource) throws CompilationException {
        SetOperationInput leftInput = selectSetOperation.getLeftInput();
        if (!selectSetOperation.hasRightInputs()) {
            return leftInput.accept(this, tupSource);
        }
        List<ILangExpression> inputExprs = new ArrayList<>();
        SelectExpression leftInputExpr;
        if (leftInput.selectBlock()) {
            leftInputExpr = new SelectExpression(null, new SelectSetOperation(leftInput, null), null, null, true);
            leftInputExpr.setSourceLocation(leftInput.getSelectBlock().getSourceLocation());
        } else {
            leftInputExpr = leftInput.getSubquery();
        }
        inputExprs.add(leftInputExpr);
        for (SetOperationRight setOperationRight : selectSetOperation.getRightInputs()) {
            SetOpType setOpType = setOperationRight.getSetOpType();
            if (setOpType != SetOpType.UNION || setOperationRight.isSetSemantics()) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, selectSetOperation.getSourceLocation(),
                        "Operation " + setOpType + (setOperationRight.isSetSemantics() ? " with set semantics" : "ALL")
                                + " is not supported.");
            }
            SetOperationInput rightInput = setOperationRight.getSetOperationRightInput();
            SelectExpression rightInputExpr;
            if (rightInput.selectBlock()) {
                rightInputExpr = new SelectExpression(null, new SelectSetOperation(rightInput, null), null, null, true);
                rightInputExpr.setSourceLocation(rightInput.getSelectBlock().getSourceLocation());
            } else {
                rightInputExpr = rightInput.getSubquery();
            }
            inputExprs.add(rightInputExpr);
        }
        return translateUnionAllFromInputExprs(inputExprs, tupSource, selectSetOperation.getSourceLocation());
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectBlock selectBlock, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
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
            throws CompilationException {
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
            throws CompilationException {
        SourceLocation sourceLoc = fromTerm.getSourceLocation();
        LogicalVariable fromVar = context.newVarFromExpression(fromTerm.getLeftVariable());
        Expression fromExpr = fromTerm.getLeftExpression();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(fromExpr, tupSource);
        UnnestOperator unnestOp;
        if (fromTerm.hasPositionalVariable()) {
            LogicalVariable pVar = context.newVarFromExpression(fromTerm.getPositionalVariable());
            // We set the positional variable type as BIGINT type.
            unnestOp = new UnnestOperator(fromVar, new MutableObject<>(makeUnnestExpression(eo.first)), pVar,
                    BuiltinType.AINT64, new PositionWriter());
        } else {
            unnestOp = new UnnestOperator(fromVar, new MutableObject<>(makeUnnestExpression(eo.first)));
        }
        unnestOp.getInputs().add(eo.second);
        unnestOp.setSourceLocation(sourceLoc);

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
            throws CompilationException {
        SourceLocation sourceLoc = joinClause.getSourceLocation();
        Mutable<ILogicalOperator> leftInputRef = uncorrelatedLeftBranchStack.pop();
        if (joinClause.getJoinType() == JoinType.INNER) {
            Pair<ILogicalOperator, LogicalVariable> rightBranch =
                    generateUnnestForBinaryCorrelateRightBranch(joinClause, inputRef, true);
            // A join operator with condition TRUE.
            AbstractBinaryJoinOperator joinOperator = new InnerJoinOperator(
                    new MutableObject<>(ConstantExpression.TRUE), leftInputRef, new MutableObject<>(rightBranch.first));
            joinOperator.setSourceLocation(sourceLoc);
            Mutable<ILogicalOperator> joinOpRef = new MutableObject<>(joinOperator);

            // Add an additional filter operator.
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> conditionExprOpPair =
                    langExprToAlgExpression(joinClause.getConditionExpression(), joinOpRef);
            SelectOperator filter = new SelectOperator(new MutableObject<>(conditionExprOpPair.first), false, null);
            filter.getInputs().add(conditionExprOpPair.second);
            filter.setSourceLocation(conditionExprOpPair.first.getSourceLocation());
            return new Pair<>(filter, rightBranch.second);
        } else {
            // Creates a subplan operator.
            SubplanOperator subplanOp = new SubplanOperator();
            subplanOp.getInputs().add(leftInputRef);
            subplanOp.setSourceLocation(sourceLoc);
            NestedTupleSourceOperator ntsOp = new NestedTupleSourceOperator(new MutableObject<>(subplanOp));
            ntsOp.setSourceLocation(sourceLoc);
            Mutable<ILogicalOperator> ntsRef = new MutableObject<>(ntsOp);

            // Enters the translation for a subplan.
            context.enterSubplan();

            // Adds an unnest operator to unnest to right expression.
            Pair<ILogicalOperator, LogicalVariable> rightBranch =
                    generateUnnestForBinaryCorrelateRightBranch(joinClause, ntsRef, true);
            AbstractUnnestNonMapOperator rightUnnestOp = (AbstractUnnestNonMapOperator) rightBranch.first;

            // Adds an additional filter operator for the join condition.
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> conditionExprOpPair =
                    langExprToAlgExpression(joinClause.getConditionExpression(), new MutableObject<>(rightUnnestOp));
            SelectOperator filter = new SelectOperator(new MutableObject<>(conditionExprOpPair.first), false, null);
            filter.getInputs().add(conditionExprOpPair.second);
            filter.setSourceLocation(conditionExprOpPair.first.getSourceLocation());

            ILogicalOperator currentTopOp = filter;
            LogicalVariable varToListify;
            boolean hasRightPosVar = rightUnnestOp.getPositionalVariable() != null;
            if (hasRightPosVar) {
                // Creates record to get correlation between the two aggregate variables.
                VariableReferenceExpression rightUnnestVarRef =
                        new VariableReferenceExpression(rightUnnestOp.getVariable());
                rightUnnestVarRef.setSourceLocation(joinClause.getRightVariable().getSourceLocation());
                VariableReferenceExpression rightUnnestPosVarRef =
                        new VariableReferenceExpression(rightUnnestOp.getPositionalVariable());
                rightUnnestPosVarRef.setSourceLocation(joinClause.getPositionalVariable().getSourceLocation());
                ScalarFunctionCallExpression recordCreationFunc = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR),
                        // Field name for the listified right unnest var.
                        new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AString("unnestvar")))),
                        // The listified right unnest var
                        new MutableObject<>(rightUnnestVarRef),
                        // Field name for the listified right unnest positional var.
                        new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AString("posvar")))),
                        // The listified right unnest positional var.
                        new MutableObject<>(rightUnnestPosVarRef));
                recordCreationFunc.setSourceLocation(joinClause.getRightVariable().getSourceLocation());

                // Assigns the record constructor function to a record variable.
                LogicalVariable recordVar = context.newVar();
                AssignOperator assignOp = new AssignOperator(recordVar, new MutableObject<>(recordCreationFunc));
                assignOp.getInputs().add(new MutableObject<>(currentTopOp));
                assignOp.setSourceLocation(joinClause.getRightVariable().getSourceLocation());

                // Sets currentTopOp and varToListify for later usages.
                currentTopOp = assignOp;
                varToListify = recordVar;
            } else {
                varToListify = rightUnnestOp.getVariable();
            }

            // Adds an aggregate operator to listfy unnest variables.
            VariableReferenceExpression varToListifyRef = new VariableReferenceExpression(varToListify);
            varToListifyRef.setSourceLocation(currentTopOp.getSourceLocation());
            AggregateFunctionCallExpression fListify = BuiltinFunctions.makeAggregateFunctionExpression(
                    BuiltinFunctions.LISTIFY, mkSingletonArrayList(new MutableObject<>(varToListifyRef)));
            fListify.setSourceLocation(currentTopOp.getSourceLocation());

            LogicalVariable aggVar = context.newSubplanOutputVar();
            AggregateOperator aggOp = new AggregateOperator(mkSingletonArrayList(aggVar),
                    mkSingletonArrayList(new MutableObject<>(fListify)));
            aggOp.getInputs().add(new MutableObject<>(currentTopOp));
            aggOp.setSourceLocation(fListify.getSourceLocation());

            // Exits the translation of a subplan.
            context.exitSubplan();

            // Sets the nested subplan of the subplan operator.
            ILogicalPlan subplan = new ALogicalPlanImpl(new MutableObject<>(aggOp));
            subplanOp.getNestedPlans().add(subplan);

            // Outer unnest the aggregated var from the subplan.
            LogicalVariable outerUnnestVar = context.newVar();
            VariableReferenceExpression aggVarRefExpr = new VariableReferenceExpression(aggVar);
            aggVarRefExpr.setSourceLocation(aggOp.getSourceLocation());
            LeftOuterUnnestOperator outerUnnestOp = new LeftOuterUnnestOperator(outerUnnestVar,
                    new MutableObject<>(makeUnnestExpression(aggVarRefExpr)));
            outerUnnestOp.getInputs().add(new MutableObject<>(subplanOp));
            outerUnnestOp.setSourceLocation(aggOp.getSourceLocation());
            currentTopOp = outerUnnestOp;

            if (hasRightPosVar) {
                VariableReferenceExpression outerUnnestVarRef1 = new VariableReferenceExpression(outerUnnestVar);
                outerUnnestVarRef1.setSourceLocation(joinClause.getRightVariable().getSourceLocation());
                ScalarFunctionCallExpression fieldAccessForRightUnnestVar = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_INDEX),
                        new MutableObject<>(outerUnnestVarRef1),
                        new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(0)))));
                fieldAccessForRightUnnestVar.setSourceLocation(joinClause.getRightVariable().getSourceLocation());

                VariableReferenceExpression outerUnnestVarRef2 = new VariableReferenceExpression(outerUnnestVar);
                outerUnnestVarRef2.setSourceLocation(joinClause.getPositionalVariable().getSourceLocation());
                ScalarFunctionCallExpression fieldAccessForRightPosVar = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_INDEX),
                        new MutableObject<>(outerUnnestVarRef2),
                        new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(1)))));
                fieldAccessForRightPosVar.setSourceLocation(joinClause.getPositionalVariable().getSourceLocation());

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
                assignExprs.add(new MutableObject<>(fieldAccessForRightUnnestVar));
                assignExprs.add(new MutableObject<>(fieldAccessForRightPosVar));

                // Creates the assign operator.
                AssignOperator assignOp = new AssignOperator(assignVars, assignExprs);
                assignOp.getInputs().add(new MutableObject<>(currentTopOp));
                assignOp.setSourceLocation(joinClause.getRightVariable().getSourceLocation());
                currentTopOp = assignOp;
            } else {
                context.setVar(joinClause.getRightVariable(), outerUnnestVar);
            }
            return new Pair<>(currentTopOp, null);
        }
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(NestClause nestClause, Mutable<ILogicalOperator> arg)
            throws CompilationException {
        throw new CompilationException(ErrorCode.COMPILATION_ERROR, nestClause.getSourceLocation(),
                "Nest clause has not been implemented");
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(UnnestClause unnestClause,
            Mutable<ILogicalOperator> inputOpRef) throws CompilationException {
        return generateUnnestForBinaryCorrelateRightBranch(unnestClause, inputOpRef,
                unnestClause.getJoinType() == JoinType.INNER);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(HavingClause havingClause, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p =
                langExprToAlgExpression(havingClause.getFilterExpression(), tupSource);
        SelectOperator s = new SelectOperator(new MutableObject<>(p.first), false, null);
        s.getInputs().add(p.second);
        return new Pair<>(s, null);
    }

    private Pair<ILogicalOperator, LogicalVariable> generateUnnestForBinaryCorrelateRightBranch(
            AbstractBinaryCorrelateClause binaryCorrelate, Mutable<ILogicalOperator> inputOpRef, boolean innerUnnest)
            throws CompilationException {
        LogicalVariable rightVar = context.newVarFromExpression(binaryCorrelate.getRightVariable());
        Expression rightExpr = binaryCorrelate.getRightExpression();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(rightExpr, inputOpRef);
        AbstractUnnestOperator unnestOp;
        if (binaryCorrelate.hasPositionalVariable()) {
            LogicalVariable pVar = context.newVarFromExpression(binaryCorrelate.getPositionalVariable());
            // We set the positional variable type as BIGINT type.
            unnestOp = innerUnnest
                    ? new UnnestOperator(rightVar, new MutableObject<>(makeUnnestExpression(eo.first)), pVar,
                            BuiltinType.AINT64, new PositionWriter())
                    : new LeftOuterUnnestOperator(rightVar, new MutableObject<>(makeUnnestExpression(eo.first)), pVar,
                            BuiltinType.AINT64, new PositionWriter());
        } else {
            unnestOp = innerUnnest ? new UnnestOperator(rightVar, new MutableObject<>(makeUnnestExpression(eo.first)))
                    : new LeftOuterUnnestOperator(rightVar, new MutableObject<>(makeUnnestExpression(eo.first)));
        }
        unnestOp.getInputs().add(eo.second);
        unnestOp.setSourceLocation(binaryCorrelate.getRightVariable().getSourceLocation());
        return new Pair<>(unnestOp, rightVar);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectClause selectClause, Mutable<ILogicalOperator> tupSrc)
            throws CompilationException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectElement selectElement, Mutable<ILogicalOperator> arg)
            throws CompilationException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectRegular selectRegular, Mutable<ILogicalOperator> arg)
            throws CompilationException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(Projection projection, Mutable<ILogicalOperator> arg)
            throws CompilationException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(CaseExpression caseExpression,
            Mutable<ILogicalOperator> tupSource) throws CompilationException {
        //Creates a series of subplan operators, one for each branch.
        Mutable<ILogicalOperator> currentOpRef = tupSource;
        ILogicalOperator currentOperator = null;
        List<Expression> whenExprList = caseExpression.getWhenExprs();
        List<Expression> thenExprList = caseExpression.getThenExprs();
        List<ILogicalExpression> branchCondVarReferences = new ArrayList<>();
        List<ILogicalExpression> allVarReferences = new ArrayList<>();
        for (int index = 0; index < whenExprList.size(); ++index) {
            Expression whenExpr = whenExprList.get(index);
            Pair<ILogicalOperator, LogicalVariable> whenExprResult = whenExpr.accept(this, currentOpRef);
            currentOperator = whenExprResult.first;
            // Variable whenConditionVar is corresponds to the current "WHEN" condition.
            LogicalVariable whenConditionVar = whenExprResult.second;
            VariableReferenceExpression whenConditionVarRef1 = new VariableReferenceExpression(whenConditionVar);
            whenConditionVarRef1.setSourceLocation(whenExpr.getSourceLocation());
            Mutable<ILogicalExpression> branchEntraceConditionExprRef = new MutableObject<>(whenConditionVarRef1);

            // Constructs an expression that filters data based on preceding "WHEN" conditions
            // and the current "WHEN" condition. Note that only one "THEN" expression can be run
            // even though multiple "WHEN" conditions can be satisfied.
            if (!branchCondVarReferences.isEmpty()) {
                // The additional filter generated here makes sure the the tuple has not
                // entered other matched "WHEN...THEN" case.
                List<Mutable<ILogicalExpression>> andArgs = new ArrayList<>();
                andArgs.add(generateNoMatchedPrecedingWhenBranchesFilter(branchCondVarReferences,
                        caseExpression.getSourceLocation()));
                andArgs.add(branchEntraceConditionExprRef);

                // A "THEN" branch can be entered only when the tuple has not enter any other preceding
                // branches and the current "WHEN" condition is TRUE.
                ScalarFunctionCallExpression andExpr =
                        new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.AND), andArgs);
                andExpr.setSourceLocation(whenExpr.getSourceLocation());
                branchEntraceConditionExprRef = new MutableObject<>(andExpr);
            }

            // Translates the corresponding "THEN" expression.
            Expression thenExpr = thenExprList.get(index);
            Pair<ILogicalOperator, LogicalVariable> opAndVarForThen =
                    constructSubplanOperatorForBranch(currentOperator, branchEntraceConditionExprRef, thenExpr);

            VariableReferenceExpression whenConditionVarRef2 = new VariableReferenceExpression(whenConditionVar);
            whenConditionVarRef2.setSourceLocation(whenExpr.getSourceLocation());
            branchCondVarReferences.add(whenConditionVarRef2);

            VariableReferenceExpression whenConditionVarRef3 = new VariableReferenceExpression(whenConditionVar);
            whenConditionVarRef3.setSourceLocation(whenExpr.getSourceLocation());
            allVarReferences.add(whenConditionVarRef3);

            VariableReferenceExpression thenVarRef = new VariableReferenceExpression(opAndVarForThen.second);
            thenVarRef.setSourceLocation(thenExpr.getSourceLocation());
            allVarReferences.add(thenVarRef);

            currentOperator = opAndVarForThen.first;
            currentOpRef = new MutableObject<>(currentOperator);
        }

        // Creates a subplan for the "ELSE" branch.
        Mutable<ILogicalExpression> elseCondExprRef = generateNoMatchedPrecedingWhenBranchesFilter(
                branchCondVarReferences, caseExpression.getSourceLocation());
        Expression elseExpr = caseExpression.getElseExpr();
        Pair<ILogicalOperator, LogicalVariable> opAndVarForElse =
                constructSubplanOperatorForBranch(currentOperator, elseCondExprRef, elseExpr);

        // Uses switch-case function to select the results of two branches.
        LogicalVariable selectVar = context.newVar();
        List<Mutable<ILogicalExpression>> arguments = new ArrayList<>();
        arguments.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE))));
        for (ILogicalExpression argVar : allVarReferences) {
            arguments.add(new MutableObject<>(argVar));
        }
        VariableReferenceExpression varForElseRef = new VariableReferenceExpression(opAndVarForElse.second);
        varForElseRef.setSourceLocation(elseExpr.getSourceLocation());
        arguments.add(new MutableObject<>(varForElseRef));
        AbstractFunctionCallExpression swithCaseExpr =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.SWITCH_CASE), arguments);
        swithCaseExpr.setSourceLocation(caseExpression.getSourceLocation());
        AssignOperator assignOp = new AssignOperator(selectVar, new MutableObject<>(swithCaseExpr));
        assignOp.getInputs().add(new MutableObject<>(opAndVarForElse.first));
        assignOp.setSourceLocation(caseExpression.getSourceLocation());

        // Unnests the selected (a "THEN" or "ELSE" branch) result.
        LogicalVariable unnestVar = context.newVar();
        VariableReferenceExpression selectVarRef = new VariableReferenceExpression(selectVar);
        selectVarRef.setSourceLocation(caseExpression.getSourceLocation());
        UnnestingFunctionCallExpression scanCollectionExpr =
                new UnnestingFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.SCAN_COLLECTION),
                        Collections.singletonList(new MutableObject<>(selectVarRef)));
        scanCollectionExpr.setSourceLocation(caseExpression.getSourceLocation());
        UnnestOperator unnestOp = new UnnestOperator(unnestVar, new MutableObject<>(scanCollectionExpr));
        unnestOp.getInputs().add(new MutableObject<>(assignOp));
        unnestOp.setSourceLocation(caseExpression.getSourceLocation());

        // Produces the final assign operator.
        LogicalVariable resultVar = context.newVar();
        VariableReferenceExpression unnestVarRef = new VariableReferenceExpression(unnestVar);
        unnestVarRef.setSourceLocation(caseExpression.getSourceLocation());
        AssignOperator finalAssignOp = new AssignOperator(resultVar, new MutableObject<>(unnestVarRef));
        finalAssignOp.getInputs().add(new MutableObject<>(unnestOp));
        finalAssignOp.setSourceLocation(caseExpression.getSourceLocation());
        return new Pair<>(finalAssignOp, resultVar);
    }

    @Override
    protected ILogicalExpression translateVariableRef(VariableExpr varExpr) throws CompilationException {
        VarIdentifier varId = varExpr.getVar();
        if (SqlppVariableUtil.isExternalVariableIdentifier(varId)) {
            SourceLocation sourceLoc = varExpr.getSourceLocation();
            IAObject value = getExternalVariableValue(varId, sourceLoc);
            return translateConstantValue(value, sourceLoc);
        }

        return super.translateVariableRef(varExpr);
    }

    private IAObject getExternalVariableValue(VarIdentifier varId, SourceLocation sourceLoc)
            throws CompilationException {
        IAObject value = externalVars.get(varId);
        if (value == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc, varId.toString());
        }
        return value;
    }

    private ILogicalExpression translateConstantValue(IAObject value, SourceLocation sourceLoc)
            throws CompilationException {
        ConstantExpression constExpr = new ConstantExpression(new AsterixConstantValue(value));
        constExpr.setSourceLocation(sourceLoc);

        IAType valueType = value.getType();
        if (valueType.getTypeTag().isDerivedType()) {
            ScalarFunctionCallExpression castExpr =
                    new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.CAST_TYPE));
            castExpr.setSourceLocation(sourceLoc);
            castExpr.getArguments().add(new MutableObject<>(constExpr));
            TypeCastUtils.setRequiredAndInputTypes(castExpr, BuiltinType.ANY, valueType);
            return castExpr;
        } else {
            return constExpr;
        }
    }

    private Pair<ILogicalOperator, LogicalVariable> produceSelectPlan(boolean isSubquery,
            Mutable<ILogicalOperator> returnOpRef, LogicalVariable resVar) {
        if (isSubquery) {
            return aggListifyForSubquery(resVar, returnOpRef, false);
        } else {
            ProjectOperator pr = new ProjectOperator(resVar);
            pr.getInputs().add(returnOpRef);
            pr.setSourceLocation(returnOpRef.getValue().getSourceLocation());
            return new Pair<>(pr, resVar);
        }
    }

    // Generates the return expression for a select clause.
    private Pair<ILogicalOperator, LogicalVariable> processSelectClause(SelectBlock selectBlock,
            Mutable<ILogicalOperator> tupSrc) throws CompilationException {
        SelectClause selectClause = selectBlock.getSelectClause();
        Expression returnExpr = selectClause.selectElement() ? selectClause.getSelectElement().getExpression()
                : generateReturnExpr(selectClause.getSelectRegular(), selectBlock);
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(returnExpr, tupSrc);
        LogicalVariable returnVar;
        ILogicalOperator returnOperator;
        SourceLocation sourceLoc = returnExpr.getSourceLocation();
        if (returnExpr.getKind() == Kind.VARIABLE_EXPRESSION
                && eo.first.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            VariableExpr varExpr = (VariableExpr) returnExpr;
            returnOperator = eo.second.getValue();
            returnVar = context.getVar(varExpr.getVar().getId());
        } else {
            returnVar = context.newVar();
            AssignOperator assignOp = new AssignOperator(returnVar, new MutableObject<>(eo.first));
            assignOp.getInputs().add(eo.second);
            assignOp.setSourceLocation(sourceLoc);
            returnOperator = assignOp;
        }
        if (selectClause.distinct()) {
            VariableReferenceExpression returnVarRef = new VariableReferenceExpression(returnVar);
            returnVarRef.setSourceLocation(sourceLoc);
            DistinctOperator distinctOperator =
                    new DistinctOperator(mkSingletonArrayList(new MutableObject<>(returnVarRef)));
            distinctOperator.getInputs().add(new MutableObject<>(returnOperator));
            distinctOperator.setSourceLocation(returnOperator.getSourceLocation());
            return new Pair<>(distinctOperator, returnVar);
        } else {
            return new Pair<>(returnOperator, returnVar);
        }
    }

    // Generates the return expression for a regular select clause.
    private Expression generateReturnExpr(SelectRegular selectRegular, SelectBlock selectBlock)
            throws CompilationException {
        List<Expression> recordExprs = new ArrayList<>();
        List<FieldBinding> fieldBindings = new ArrayList<>();
        Set<String> fieldNames = new HashSet<>();

        for (Projection projection : selectRegular.getProjections()) {
            if (projection.varStar()) {
                if (!fieldBindings.isEmpty()) {
                    RecordConstructor recordConstr = new RecordConstructor(new ArrayList<>(fieldBindings));
                    recordConstr.setSourceLocation(selectRegular.getSourceLocation());
                    recordExprs.add(recordConstr);
                    fieldBindings.clear();
                }
                Expression projectionExpr = projection.getExpression();
                SourceLocation sourceLoc = projection.getSourceLocation();
                CallExpr toObjectExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.TO_OBJECT),
                        Collections.singletonList(projectionExpr));
                toObjectExpr.setSourceLocation(sourceLoc);
                CallExpr ifMissingOrNullExpr = new CallExpr(new FunctionSignature(BuiltinFunctions.IF_MISSING_OR_NULL),
                        Arrays.asList(toObjectExpr, new RecordConstructor(Collections.emptyList())));
                ifMissingOrNullExpr.setSourceLocation(sourceLoc);
                recordExprs.add(ifMissingOrNullExpr);
            } else if (projection.star()) {
                if (selectBlock.hasGroupbyClause()) {
                    getGroupBindings(selectBlock.getGroupbyClause(), fieldBindings, fieldNames);
                    if (selectBlock.hasLetClausesAfterGroupby()) {
                        getLetBindings(selectBlock.getLetListAfterGroupby(), fieldBindings, fieldNames);
                    }
                } else if (selectBlock.hasFromClause()) {
                    getFromBindings(selectBlock.getFromClause(), fieldBindings, fieldNames);
                    if (selectBlock.hasLetClauses()) {
                        getLetBindings(selectBlock.getLetList(), fieldBindings, fieldNames);
                    }
                } else if (selectBlock.hasLetClauses()) {
                    getLetBindings(selectBlock.getLetList(), fieldBindings, fieldNames);
                }
            } else if (projection.hasName()) {
                fieldBindings.add(getFieldBinding(projection, fieldNames));
            } else {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, projection.getSourceLocation());
            }
        }
        if (!fieldBindings.isEmpty()) {
            RecordConstructor recordConstr = new RecordConstructor(fieldBindings);
            recordConstr.setSourceLocation(selectRegular.getSourceLocation());
            recordExprs.add(recordConstr);
        }

        if (recordExprs.size() == 1) {
            return recordExprs.get(0);
        } else {
            CallExpr recordConcatExpr =
                    new CallExpr(new FunctionSignature(BuiltinFunctions.RECORD_CONCAT_STRICT), recordExprs);
            recordConcatExpr.setSourceLocation(selectRegular.getSourceLocation());
            return recordConcatExpr;
        }
    }

    // Generates all field bindings according to the from clause.
    private void getFromBindings(FromClause fromClause, List<FieldBinding> outFieldBindings, Set<String> outFieldNames)
            throws CompilationException {
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            outFieldBindings.add(getFieldBinding(fromTerm.getLeftVariable(), outFieldNames));
            if (fromTerm.hasPositionalVariable()) {
                outFieldBindings.add(getFieldBinding(fromTerm.getPositionalVariable(), outFieldNames));
            }
            if (!fromTerm.hasCorrelateClauses()) {
                continue;
            }
            for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
                outFieldBindings.add(getFieldBinding(correlateClause.getRightVariable(), outFieldNames));
                if (correlateClause.hasPositionalVariable()) {
                    outFieldBindings.add(getFieldBinding(correlateClause.getPositionalVariable(), outFieldNames));
                }
            }
        }
    }

    // Generates all field bindings according to the from clause.
    private void getGroupBindings(GroupbyClause groupbyClause, List<FieldBinding> outFieldBindings,
            Set<String> outFieldNames) throws CompilationException {
        for (GbyVariableExpressionPair pair : groupbyClause.getGbyPairList()) {
            outFieldBindings.add(getFieldBinding(pair.getVar(), outFieldNames));
        }
        if (groupbyClause.hasGroupVar()) {
            outFieldBindings.add(getFieldBinding(groupbyClause.getGroupVar(), outFieldNames));
        }
        if (groupbyClause.hasWithMap()) {
            // no WITH in SQLPP
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, groupbyClause.getSourceLocation(),
                    groupbyClause.getWithVarMap().values().toString());
        }
    }

    // Generates all field bindings according to the let clause.
    private void getLetBindings(List<LetClause> letClauses, List<FieldBinding> outFieldBindings,
            Set<String> outFieldNames) throws CompilationException {
        for (LetClause letClause : letClauses) {
            outFieldBindings.add(getFieldBinding(letClause.getVarExpr(), outFieldNames));
        }
    }

    // Generates a field binding for a variable.
    private FieldBinding getFieldBinding(VariableExpr varExpr, Set<String> outFieldNames) throws CompilationException {
        String fieldName = SqlppVariableUtil.variableNameToDisplayedFieldName(varExpr.getVar().getValue());
        return generateFieldBinding(fieldName, varExpr, outFieldNames, varExpr.getSourceLocation());
    }

    // Generates a field binding for a named projection.
    private FieldBinding getFieldBinding(Projection projection, Set<String> outFieldNames) throws CompilationException {
        String fieldName = projection.getName();
        Expression fieldValueExpr = projection.getExpression();
        return generateFieldBinding(fieldName, fieldValueExpr, outFieldNames, projection.getSourceLocation());
    }

    private FieldBinding generateFieldBinding(String fieldName, Expression fieldValueExpr, Set<String> outFieldNames,
            SourceLocation sourceLoc) throws CompilationException {
        if (!outFieldNames.add(fieldName)) {
            throw new CompilationException(ErrorCode.DUPLICATE_FIELD_NAME, sourceLoc, fieldName);
        }
        return new FieldBinding(new LiteralExpr(new StringLiteral(fieldName)), fieldValueExpr);
    }

    @Override
    protected boolean expressionNeedsNoNesting(Expression expr) {
        return super.expressionNeedsNoNesting(expr) || (translateInAsOr && expr.getKind() == Kind.QUANTIFIED_EXPRESSION
                && isInOperatorWithStaticList((QuantifiedExpression) expr));
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(QuantifiedExpression qe, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        return translateInAsOr && isInOperatorWithStaticList(qe) ? translateInOperatorWithStaticList(qe, tupSource)
                : super.visit(qe, tupSource);
    }

    // At this point "$x in list_expr" is a quantified expression:
    // "some $y in list_expr satisfies $x = $y"
    // Look for such quantified expression with a constant list_expr ([e1, e2, ... eN])
    // and translate it into "$x=e1 || $x=e2 || ... || $x=eN"
    private boolean isInOperatorWithStaticList(QuantifiedExpression qe) {
        if (qe.getQuantifier() != QuantifiedExpression.Quantifier.SOME) {
            return false;
        }
        List<QuantifiedPair> qpList = qe.getQuantifiedList();
        if (qpList.size() != 1) {
            return false;
        }
        QuantifiedPair qp = qpList.get(0);

        Expression condExpr = qe.getSatisfiesExpr();
        if (condExpr.getKind() != Kind.OP_EXPRESSION) {
            return false;
        }
        OperatorExpr opExpr = (OperatorExpr) condExpr;
        if (opExpr.getOpList().get(0) != OperatorType.EQ) {
            return false;
        }
        List<Expression> operandExprs = opExpr.getExprList();
        if (operandExprs.size() != 2) {
            return false;
        }
        int varPos = operandExprs.indexOf(qp.getVarExpr());
        if (varPos < 0) {
            return false;
        }
        Expression inExpr = qp.getExpr();
        switch (inExpr.getKind()) {
            case LIST_CONSTRUCTOR_EXPRESSION:
                ListConstructor listExpr = (ListConstructor) inExpr;
                List<Expression> itemExprs = listExpr.getExprList();
                if (itemExprs.isEmpty()) {
                    return false;
                }
                for (Expression itemExpr : itemExprs) {
                    boolean isConst = itemExpr.getKind() == Kind.LITERAL_EXPRESSION
                            || (itemExpr.getKind() == Kind.VARIABLE_EXPRESSION
                                    && SqlppVariableUtil.isExternalVariableReference((VariableExpr) itemExpr));
                    if (!isConst) {
                        return false;
                    }
                }
                return true;
            case VARIABLE_EXPRESSION:
                VarIdentifier inVarId = ((VariableExpr) inExpr).getVar();
                if (!SqlppVariableUtil.isExternalVariableIdentifier(inVarId)) {
                    return false;
                }
                IAObject inValue = externalVars.get(inVarId);
                return inValue != null && inValue.getType().getTypeTag().isListType()
                        && ((IACollection) inValue).size() > 0;
            default:
                return false;
        }
    }

    private Pair<ILogicalOperator, LogicalVariable> translateInOperatorWithStaticList(QuantifiedExpression qe,
            Mutable<ILogicalOperator> tupSource) throws CompilationException {
        SourceLocation sourceLoc = qe.getSourceLocation();

        QuantifiedPair qp = qe.getQuantifiedList().get(0);
        VariableExpr varExpr = qp.getVarExpr();
        List<Expression> operandExprs = ((OperatorExpr) qe.getSatisfiesExpr()).getExprList();
        int varIdx = operandExprs.indexOf(varExpr);
        Expression operandExpr = operandExprs.get(1 - varIdx);

        Mutable<ILogicalOperator> topOp = tupSource;

        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo1 = langExprToAlgExpression(operandExpr, topOp);
        topOp = eo1.second;

        LogicalVariable operandVar = context.newVar();
        AssignOperator operandAssign = new AssignOperator(operandVar, new MutableObject<>(eo1.first));
        operandAssign.getInputs().add(topOp);
        operandAssign.setSourceLocation(sourceLoc);
        topOp = new MutableObject<>(operandAssign);

        List<MutableObject<ILogicalExpression>> disjuncts = new ArrayList<>();
        Expression inExpr = qp.getExpr();
        switch (inExpr.getKind()) {
            case LIST_CONSTRUCTOR_EXPRESSION:
                ListConstructor listExpr = (ListConstructor) inExpr;
                for (Expression itemExpr : listExpr.getExprList()) {
                    IAObject inValue;
                    switch (itemExpr.getKind()) {
                        case LITERAL_EXPRESSION:
                            inValue = ConstantHelper.objectFromLiteral(((LiteralExpr) itemExpr).getValue());
                            break;
                        case VARIABLE_EXPRESSION:
                            inValue = getExternalVariableValue(((VariableExpr) itemExpr).getVar(), sourceLoc);
                            break;
                        default:
                            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc,
                                    itemExpr.getKind());
                    }
                    ILogicalExpression eqExpr = createEqExpr(operandVar, inValue, sourceLoc);
                    disjuncts.add(new MutableObject<>(eqExpr));
                }
                break;
            case VARIABLE_EXPRESSION:
                VarIdentifier inVarId = ((VariableExpr) inExpr).getVar();
                IAObject inVarValue = externalVars.get(inVarId);
                IACursor inVarCursor = ((IACollection) inVarValue).getCursor();
                inVarCursor.reset();
                while (inVarCursor.next()) {
                    IAObject inValue = inVarCursor.get();
                    ILogicalExpression eqExpr = createEqExpr(operandVar, inValue, sourceLoc);
                    disjuncts.add(new MutableObject<>(eqExpr));
                }
                break;
            default:
                throw new IllegalStateException(String.valueOf(inExpr.getKind()));
        }

        MutableObject<ILogicalExpression> condExpr;
        if (disjuncts.size() == 1) {
            condExpr = disjuncts.get(0);
        } else {
            AbstractFunctionCallExpression orExpr =
                    createFunctionCallExpressionForBuiltinOperator(OperatorType.OR, sourceLoc);
            orExpr.getArguments().addAll(disjuncts);
            condExpr = new MutableObject<>(orExpr);
        }

        LogicalVariable assignVar = context.newVar();
        AssignOperator assignOp = new AssignOperator(assignVar, condExpr);
        assignOp.getInputs().add(topOp);
        assignOp.setSourceLocation(sourceLoc);
        return new Pair<>(assignOp, assignVar);
    }

    private ILogicalExpression createEqExpr(LogicalVariable lhsVar, IAObject rhsValue, SourceLocation sourceLoc)
            throws CompilationException {
        VariableReferenceExpression lhsExpr = new VariableReferenceExpression(lhsVar);
        lhsExpr.setSourceLocation(sourceLoc);
        ILogicalExpression rhsExpr = translateConstantValue(rhsValue, sourceLoc);
        AbstractFunctionCallExpression opExpr =
                createFunctionCallExpressionForBuiltinOperator(OperatorType.EQ, sourceLoc);
        opExpr.getArguments().add(new MutableObject<>(lhsExpr));
        opExpr.getArguments().add(new MutableObject<>(rhsExpr));
        return opExpr;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(WindowExpression winExpr, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        SourceLocation sourceLoc = winExpr.getSourceLocation();
        Mutable<ILogicalOperator> currentOpRef = tupSource;

        List<Mutable<ILogicalExpression>> partExprListOut = null;
        if (winExpr.hasPartitionList()) {
            List<Expression> partExprList = winExpr.getPartitionList();
            partExprListOut = new ArrayList<>(partExprList.size());
            for (Expression partExpr : partExprList) {
                Pair<ILogicalOperator, LogicalVariable> partExprResult = partExpr.accept(this, currentOpRef);
                VariableReferenceExpression partExprOut = new VariableReferenceExpression(partExprResult.second);
                partExprOut.setSourceLocation(partExpr.getSourceLocation());
                partExprListOut.add(new MutableObject<>(partExprOut));
                currentOpRef = new MutableObject<>(partExprResult.first);
            }
        }

        List<Expression> orderExprList = winExpr.getOrderbyList();
        List<OrderbyClause.OrderModifier> orderModifierList = winExpr.getOrderbyModifierList();
        List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExprListOut =
                new ArrayList<>(orderExprList.size());
        for (int i = 0, ln = orderExprList.size(); i < ln; i++) {
            Expression orderExpr = orderExprList.get(i);
            OrderbyClause.OrderModifier orderModifier = orderModifierList.get(i);
            Pair<ILogicalOperator, LogicalVariable> orderExprResult = orderExpr.accept(this, currentOpRef);
            VariableReferenceExpression orderExprOut = new VariableReferenceExpression(orderExprResult.second);
            orderExprOut.setSourceLocation(orderExpr.getSourceLocation());
            OrderOperator.IOrder orderModifierOut = translateOrderModifier(orderModifier);
            orderExprListOut.add(new Pair<>(orderModifierOut, new MutableObject<>(orderExprOut)));
            currentOpRef = new MutableObject<>(orderExprResult.first);
        }

        Expression expr = winExpr.getExpr();
        Pair<ILogicalOperator, LogicalVariable> exprResult = expr.accept(this, currentOpRef);
        ILogicalOperator exprOp = exprResult.first;
        if (exprOp.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc);
        }
        AssignOperator exprAssignOp = (AssignOperator) exprOp;
        currentOpRef = exprAssignOp.getInputs().get(0);
        List<LogicalVariable> exprAssignVars = exprAssignOp.getVariables();
        if (exprAssignVars.size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc);
        }
        LogicalVariable exprAssignVar = exprAssignVars.get(0);
        List<Mutable<ILogicalExpression>> exprAssignExprs = exprAssignOp.getExpressions();
        ILogicalExpression exprAssignExpr = exprAssignExprs.get(0).getValue();
        if (exprAssignExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new CompilationException(ErrorCode.COMPILATION_EXPECTED_FUNCTION_CALL, sourceLoc);
        }
        AbstractFunctionCallExpression callExpr = (AbstractFunctionCallExpression) exprAssignExpr;
        if (BuiltinFunctions.windowFunctionRequiresOrderArgs(callExpr.getFunctionIdentifier())) {
            List<Mutable<ILogicalExpression>> callArgs = callExpr.getArguments();
            for (Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>> p : orderExprListOut) {
                callArgs.add(new MutableObject<>(p.second.getValue().cloneExpression()));
            }
        }

        WindowOperator winOp = new WindowOperator(partExprListOut, orderExprListOut, exprAssignVars, exprAssignExprs);
        winOp.setSourceLocation(sourceLoc);
        winOp.getInputs().add(currentOpRef);

        // must return ASSIGN
        LogicalVariable assignVar = context.newVar();
        AssignOperator assignOp =
                new AssignOperator(assignVar, new MutableObject<>(new VariableReferenceExpression(exprAssignVar)));
        assignOp.setSourceLocation(sourceLoc);
        assignOp.getInputs().add(new MutableObject<>(winOp));
        return new Pair<>(assignOp, assignVar);
    }
}
