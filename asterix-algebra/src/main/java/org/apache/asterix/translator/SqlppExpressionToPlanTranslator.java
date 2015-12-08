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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslator;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.FieldBinding;
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
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
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
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OuterUnnestOperator;
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

    public SqlppExpressionToPlanTranslator(AqlMetadataProvider metadataProvider, int currentVarCounter)
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
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(queryBody, tupSource);
            AssignOperator assignOp = new AssignOperator(var, new MutableObject<ILogicalExpression>(eo.first));
            assignOp.getInputs().add(eo.second);
            ProjectOperator projectOp = new ProjectOperator(var);
            projectOp.getInputs().add(new MutableObject<ILogicalOperator>(assignOp));
            return new Pair<ILogicalOperator, LogicalVariable>(projectOp, var);
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
                currentOpRef = new MutableObject<ILogicalOperator>(letClause.accept(this, currentOpRef).first);
            }
        }
        Pair<ILogicalOperator, LogicalVariable> select = selectExpression.getSelectSetOperation().accept(this,
                currentOpRef);
        currentOpRef = new MutableObject<ILogicalOperator>(select.first);
        if (selectExpression.hasOrderby()) {
            currentOpRef = new MutableObject<ILogicalOperator>(
                    selectExpression.getOrderbyClause().accept(this, currentOpRef).first);
        }
        if (selectExpression.hasLimit()) {
            currentOpRef = new MutableObject<ILogicalOperator>(
                    selectExpression.getLimitClause().accept(this, currentOpRef).first);
        }
        Pair<ILogicalOperator, LogicalVariable> result = produceSelectPlan(selectExpression.isSubquery(), currentOpRef,
                select.second);
        if (selectExpression.isSubquery()) {
            context.existSubplan();
        }
        return result;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectSetOperation selectSetOperation,
            Mutable<ILogicalOperator> tupSource) throws AsterixException {
        Mutable<ILogicalOperator> currentOpRef = tupSource;
        Pair<ILogicalOperator, LogicalVariable> currentResult = selectSetOperation.getLeftInput().accept(this,
                currentOpRef);
        if (selectSetOperation.hasRightInputs()) {
            throw new NotImplementedException();
        }
        return currentResult;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectBlock selectBlock, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Mutable<ILogicalOperator> currentOpRef = tupSource;
        if (selectBlock.hasFromClause()) {
            currentOpRef = new MutableObject<ILogicalOperator>(
                    selectBlock.getFromClause().accept(this, currentOpRef).first);
        }
        if (selectBlock.hasLetClauses()) {
            for (LetClause letClause : selectBlock.getLetList()) {
                currentOpRef = new MutableObject<ILogicalOperator>(letClause.accept(this, currentOpRef).first);
            }
        }
        if (selectBlock.hasWhereClause()) {
            currentOpRef = new MutableObject<ILogicalOperator>(
                    selectBlock.getWhereClause().accept(this, currentOpRef).first);
        }
        if (selectBlock.hasGroupbyClause()) {
            currentOpRef = new MutableObject<ILogicalOperator>(
                    selectBlock.getGroupbyClause().accept(this, currentOpRef).first);
        }
        if (selectBlock.hasLetClausesAfterGroupby()) {
            for (LetClause letClause : selectBlock.getLetListAfterGroupby()) {
                currentOpRef = new MutableObject<ILogicalOperator>(letClause.accept(this, currentOpRef).first);
            }
        }
        if (selectBlock.hasHavingClause()) {
            currentOpRef = new MutableObject<ILogicalOperator>(
                    selectBlock.getHavingClause().accept(this, currentOpRef).first);
        }
        return selectBlock.getSelectClause().accept(this, currentOpRef);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FromClause fromClause, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        Mutable<ILogicalOperator> inputSrc = arg;
        Pair<ILogicalOperator, LogicalVariable> topUnnest = null;
        for (FromTerm fromTerm : fromClause.getFromTerms()) {
            topUnnest = fromTerm.accept(this, inputSrc);
            inputSrc = new MutableObject<ILogicalOperator>(topUnnest.first);
        }
        return topUnnest;
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(FromTerm fromTerm, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        LogicalVariable fromVar = context.newVar(fromTerm.getLeftVariable());
        Expression fromExpr = fromTerm.getLeftExpression();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(fromExpr, tupSource);
        ILogicalOperator unnestOp;
        if (fromTerm.hasPositionalVariable()) {
            LogicalVariable pVar = context.newVar(fromTerm.getPositionalVariable());
            // We set the positional variable type as INT64 type.
            unnestOp = new UnnestOperator(fromVar,
                    new MutableObject<ILogicalExpression>(makeUnnestExpression(eo.first)), pVar, BuiltinType.AINT64,
                    new AqlPositionWriter());
        } else {
            unnestOp = new UnnestOperator(fromVar,
                    new MutableObject<ILogicalExpression>(makeUnnestExpression(eo.first)));
        }
        unnestOp.getInputs().add(eo.second);

        // Processes joins, unnests, and nests.
        Mutable<ILogicalOperator> topOpRef = new MutableObject<ILogicalOperator>(unnestOp);
        if (fromTerm.hasCorrelateClauses()) {
            for (AbstractBinaryCorrelateClause correlateClause : fromTerm.getCorrelateClauses()) {
                topOpRef = new MutableObject<ILogicalOperator>(correlateClause.accept(this, topOpRef).first);
            }
        }
        return new Pair<ILogicalOperator, LogicalVariable>(topOpRef.getValue(), fromVar);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(JoinClause joinClause, Mutable<ILogicalOperator> inputRef)
            throws AsterixException {
        if (joinClause.getJoinType() == JoinType.INNER) {
            Pair<ILogicalOperator, LogicalVariable> rightBranch = generateUnnestForBinaryCorrelateRightBranch(
                    joinClause, new MutableObject<ILogicalOperator>(new EmptyTupleSourceOperator()));
            // A join operator with condition TRUE.
            AbstractBinaryJoinOperator joinOperator = new InnerJoinOperator(
                    new MutableObject<ILogicalExpression>(ConstantExpression.TRUE), inputRef,
                    new MutableObject<ILogicalOperator>(rightBranch.first));
            Mutable<ILogicalOperator> joinOpRef = new MutableObject<ILogicalOperator>(joinOperator);

            // Add an additional filter operator.
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> conditionExprOpPair = aqlExprToAlgExpression(
                    joinClause.getConditionExpression(), joinOpRef);
            SelectOperator filter = new SelectOperator(new MutableObject<ILogicalExpression>(conditionExprOpPair.first),
                    false, null);
            filter.getInputs().add(conditionExprOpPair.second);
            return new Pair<ILogicalOperator, LogicalVariable>(filter, rightBranch.second);
        } else {
            // Creates a subplan operator.
            SubplanOperator subplanOp = new SubplanOperator();
            Mutable<ILogicalOperator> ntsRef = new MutableObject<ILogicalOperator>(
                    new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(subplanOp)));
            subplanOp.getInputs().add(inputRef);

            // Enters the translation for a subplan.
            context.enterSubplan();

            // Adds an unnest operator to unnest to right expression.
            Pair<ILogicalOperator, LogicalVariable> rightBranch = generateUnnestForBinaryCorrelateRightBranch(
                    joinClause, ntsRef);
            UnnestOperator rightUnnestOp = (UnnestOperator) rightBranch.first;

            // Adds an additional filter operator for the join condition.
            Pair<ILogicalExpression, Mutable<ILogicalOperator>> conditionExprOpPair = aqlExprToAlgExpression(
                    joinClause.getConditionExpression(), new MutableObject<ILogicalOperator>(rightUnnestOp));
            SelectOperator filter = new SelectOperator(new MutableObject<ILogicalExpression>(conditionExprOpPair.first),
                    false, null);
            filter.getInputs().add(conditionExprOpPair.second);

            ILogicalOperator currentTopOp = filter;
            LogicalVariable varToListify;
            boolean hasRightPosVar = rightUnnestOp.getPositionalVariable() != null;
            if (hasRightPosVar) {
                // Creates record to get correlation between the two aggregate variables.
                @SuppressWarnings("unchecked")
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
                AssignOperator assignOp = new AssignOperator(recordVar,
                        new MutableObject<ILogicalExpression>(recordCreationFunc));
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
            context.existSubplan();

            // Sets the nested subplan of the subplan operator.
            ILogicalPlan subplan = new ALogicalPlanImpl(new MutableObject<ILogicalOperator>(aggOp));
            subplanOp.getNestedPlans().add(subplan);

            // Outer unnest the aggregated var from the subplan.
            LogicalVariable outerUnnestVar = context.newVar();
            OuterUnnestOperator outerUnnestOp = new OuterUnnestOperator(outerUnnestVar,
                    new MutableObject<ILogicalExpression>(
                            makeUnnestExpression(new VariableReferenceExpression(aggVar))));
            outerUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(subplanOp));
            currentTopOp = outerUnnestOp;

            if (hasRightPosVar) {
                @SuppressWarnings("unchecked")
                ScalarFunctionCallExpression fieldAccessForRightUnnestVar = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX),
                        new MutableObject<ILogicalExpression>(new VariableReferenceExpression(outerUnnestVar)),
                        new MutableObject<ILogicalExpression>(
                                new ConstantExpression(new AsterixConstantValue(new AInt32(0)))));

                @SuppressWarnings("unchecked")
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
                List<LogicalVariable> assignVars = new ArrayList<LogicalVariable>();
                assignVars.add(rightUnnestVar);
                assignVars.add(rightPosVar);

                // Expressions for assign.
                List<Mutable<ILogicalExpression>> assignExprs = new ArrayList<Mutable<ILogicalExpression>>();
                assignExprs.add(new MutableObject<ILogicalExpression>(fieldAccessForRightUnnestVar));
                assignExprs.add(new MutableObject<ILogicalExpression>(fieldAccessForRightPosVar));

                // Creates the assign operator.
                AssignOperator assignOp = new AssignOperator(assignVars, assignExprs);
                assignOp.getInputs().add(new MutableObject<ILogicalOperator>(currentTopOp));
                currentTopOp = assignOp;
            } else {
                context.setVar(joinClause.getRightVariable(), outerUnnestVar);
            }
            return new Pair<ILogicalOperator, LogicalVariable>(currentTopOp, null);
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
        return generateUnnestForBinaryCorrelateRightBranch(unnestClause, inputOpRef);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(HavingClause havingClause, Mutable<ILogicalOperator> tupSource)
            throws AsterixException {
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> p = aqlExprToAlgExpression(
                havingClause.getFilterExpression(), tupSource);
        SelectOperator s = new SelectOperator(new MutableObject<ILogicalExpression>(p.first), false, null);
        s.getInputs().add(p.second);
        return new Pair<ILogicalOperator, LogicalVariable>(s, null);
    }

    private Pair<ILogicalOperator, LogicalVariable> generateUnnestForBinaryCorrelateRightBranch(
            AbstractBinaryCorrelateClause binaryCorrelate, Mutable<ILogicalOperator> inputOpRef)
                    throws AsterixException {
        LogicalVariable rightVar = context.newVar(binaryCorrelate.getRightVariable());
        Expression rightExpr = binaryCorrelate.getRightExpression();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(rightExpr, inputOpRef);
        ILogicalOperator unnestOp;
        if (binaryCorrelate.hasPositionalVariable()) {
            LogicalVariable pVar = context.newVar(binaryCorrelate.getPositionalVariable());
            // We set the positional variable type as INT64 type.
            unnestOp = new UnnestOperator(rightVar,
                    new MutableObject<ILogicalExpression>(makeUnnestExpression(eo.first)), pVar, BuiltinType.AINT64,
                    new AqlPositionWriter());
        } else {
            unnestOp = new UnnestOperator(rightVar,
                    new MutableObject<ILogicalExpression>(makeUnnestExpression(eo.first)));
        }
        unnestOp.getInputs().add(eo.second);
        return new Pair<ILogicalOperator, LogicalVariable>(unnestOp, rightVar);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectClause selectClause, Mutable<ILogicalOperator> tupSrc)
            throws AsterixException {
        Expression returnExpr;
        if (selectClause.selectElement()) {
            returnExpr = selectClause.getSelectElement().getExpression();
        } else {
            List<Projection> projections = selectClause.getSelectRegular().getProjections();
            List<FieldBinding> fieldBindings = new ArrayList<FieldBinding>();
            for (Projection projection : projections) {
                fieldBindings.add(new FieldBinding(new LiteralExpr(new StringLiteral(projection.getName())),
                        projection.getExpression()));
            }
            returnExpr = new RecordConstructor(fieldBindings);
        }
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = aqlExprToAlgExpression(returnExpr, tupSrc);
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
            return new Pair<ILogicalOperator, LogicalVariable>(distinctOperator, returnVar);
        } else {
            return new Pair<ILogicalOperator, LogicalVariable>(returnOperator, returnVar);
        }
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectElement selectElement, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        throw new IllegalStateException("Translator should never enter this method!");
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(SelectRegular selectRegular, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        throw new IllegalStateException("Translator should never enter this method!");
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(Projection projection, Mutable<ILogicalOperator> arg)
            throws AsterixException {
        throw new IllegalStateException("Translator should never enter this method!");
    }

    private Pair<ILogicalOperator, LogicalVariable> produceSelectPlan(boolean isSubquery,
            Mutable<ILogicalOperator> returnOpRef, LogicalVariable resVar) {
        if (isSubquery) {
            return aggListifyForSubquery(resVar, returnOpRef, false);
        } else {
            ProjectOperator pr = new ProjectOperator(resVar);
            pr.getInputs().add(returnOpRef);
            return new Pair<ILogicalOperator, LogicalVariable>(pr, resVar);
        }
    }

}
