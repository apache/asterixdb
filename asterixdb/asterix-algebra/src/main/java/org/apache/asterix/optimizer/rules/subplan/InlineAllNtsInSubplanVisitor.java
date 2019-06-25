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
package org.apache.asterix.optimizer.rules.subplan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.rules.util.EquivalenceClassUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.LogicalOperatorDeepCopyWithNewVariablesVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.IQueryOperatorVisitor;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 *   This visitor inlines all nested tuple source operators in the query
 *   plan rooted at the operator being visited, with a deep copy of the query
 *   plan rooted at the input <code>subplanInputOperator</code>.
 *
 *   The visitor ensures that the variables used to correlate between the
 *   query plan rooted at <code>subplanInputOperator</code> are propagated
 *   to the operator being visited.
 *
 *   ----------------------------------
 *   Here is an abstract example.
 *   The original query plan:
 *   --Op1
 *     --Subplan{
 *       --AggregateOp
 *         --NestedOp
 *           .....
 *             --Nested-Tuple-Source
 *       }
 *       --InputOp
 *         .....
 *
 *   After we call NestedOp.accept(....) with this visitor. We will get an
 *   intermediate plan that looks like:
 *   --Op1
 *     --Subplan{
 *       --AggregateOp
 *         --NestedOp
 *           .....
 *             --InputOp'
 *               ....
 *       }
 *       --InputOp
 *         .....
 *    The plan rooted at InputOp' is a deep copy of the plan rooted at InputOp
 *    with a different set of variables.
 *
 */
class InlineAllNtsInSubplanVisitor implements IQueryOperatorVisitor<ILogicalOperator, Void> {
    // The optimization context.
    private final IOptimizationContext context;

    // The target SubplanOperator.
    private final ILogicalOperator subplanOperator;

    // The input operator to the subplan.
    private final ILogicalOperator subplanInputOperator;

    // Maps live variables at <code>subplanInputOperator</code> to variables in
    // the flattened nested plan.
    private final LinkedHashMap<LogicalVariable, LogicalVariable> subplanInputVarToCurrentVarMap =
            new LinkedHashMap<>();

    // Maps variables in the flattened nested plan to live variables at
    // <code>subplannputOperator</code>.
    private final Map<LogicalVariable, LogicalVariable> currentVarToSubplanInputVarMap = new HashMap<>();

    // The set of key variables at the current operator that is being visited.
    private final Set<LogicalVariable> correlatedKeyVars = new HashSet<>();

    // The list of variables determining the ordering.
    private final List<Pair<IOrder, Mutable<ILogicalExpression>>> orderingExprs = new ArrayList<>();

    // Maps variables in the flattened nested plan to live variables at
    // <code>subplannputOperator</code>.
    private final List<Pair<LogicalVariable, LogicalVariable>> varMapIntroducedByRewriting = new ArrayList<>();

    /**
     * @param context
     *            the optimization context
     * @param subplanOperator
     *            the input operator to the target subplan operator, which is to
     *            be inlined.
     * @throws AlgebricksException
     */
    public InlineAllNtsInSubplanVisitor(IOptimizationContext context, ILogicalOperator subplanOperator)
            throws AlgebricksException {
        this.context = context;
        this.subplanOperator = subplanOperator;
        this.subplanInputOperator = subplanOperator.getInputs().get(0).getValue();
    }

    public Map<LogicalVariable, LogicalVariable> getInputVariableToOutputVariableMap() {
        return subplanInputVarToCurrentVarMap;
    }

    public List<Pair<LogicalVariable, LogicalVariable>> getVariableMapHistory() {
        return varMapIntroducedByRewriting;
    }

    public List<Pair<IOrder, Mutable<ILogicalExpression>>> getOrderingExpressions() {
        return orderingExprs;
    }

    @Override
    public ILogicalOperator visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
        return visitAggregateOperator(op);
    }

    @Override
    public ILogicalOperator visitRunningAggregateOperator(RunningAggregateOperator op, Void arg)
            throws AlgebricksException {
        return visitAggregateOperator(op);
    }

    @Override
    public ILogicalOperator visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg)
            throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        visitSingleInputOperator(op);
        Set<LogicalVariable> groupKeyVars = new HashSet<>();
        // Maps group by key variables if the corresponding expressions are
        // VariableReferenceExpressions.
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> keyVarExprRef : op.getGroupByList()) {
            ILogicalExpression expr = keyVarExprRef.second.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varExpr = (VariableReferenceExpression) expr;
                LogicalVariable sourceVar = varExpr.getVariableReference();
                updateInputToOutputVarMapping(sourceVar, keyVarExprRef.first, false);
                groupKeyVars.add(keyVarExprRef.first);
            }
        }

        // Add correlated key variables into group-by keys.
        Map<LogicalVariable, LogicalVariable> addedGroupKeyMapping = new HashMap<>();
        for (LogicalVariable keyVar : correlatedKeyVars) {
            if (!groupKeyVars.contains(keyVar)) {
                LogicalVariable newVar = context.newVar();
                VariableReferenceExpression keyVarRef = new VariableReferenceExpression(keyVar);
                keyVarRef.setSourceLocation(op.getSourceLocation());
                op.getGroupByList().add(new Pair<>(newVar, new MutableObject<>(keyVarRef)));
                addedGroupKeyMapping.put(keyVar, newVar);
            }
        }

        // Updates decor list.
        Iterator<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decorExprIter = op.getDecorList().iterator();
        while (decorExprIter.hasNext()) {
            ILogicalExpression expr = decorExprIter.next().second.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varExpr = (VariableReferenceExpression) expr;
                if (correlatedKeyVars.contains(varExpr.getVariableReference())) {
                    decorExprIter.remove();
                }
            }
        }

        // Updates the var mapping for added group-by keys.
        addedGroupKeyMapping.forEach((key, value) -> updateInputToOutputVarMapping(key, value, false));
        return op;
    }

    @Override
    public ILogicalOperator visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        // Processes its input operator.
        visitSingleInputOperator(op);
        if (correlatedKeyVars.isEmpty()) {
            return op;
        }

        // Get live variables before limit.
        Set<LogicalVariable> inputLiveVars = new HashSet<LogicalVariable>();
        VariableUtilities.getSubplanLocalLiveVariables(op.getInputs().get(0).getValue(), inputLiveVars);

        // Creates a record construction assign operator.
        Pair<ILogicalOperator, LogicalVariable> assignOpAndRecordVar =
                createRecordConstructorAssignOp(inputLiveVars, op.getSourceLocation());
        ILogicalOperator assignOp = assignOpAndRecordVar.first;
        LogicalVariable recordVar = assignOpAndRecordVar.second;
        ILogicalOperator inputOp = op.getInputs().get(0).getValue();
        assignOp.getInputs().add(new MutableObject<>(inputOp));

        // Rewrites limit to a group-by with limit as its nested operator.
        Pair<ILogicalOperator, LogicalVariable> gbyOpAndAggVar = wrapLimitInGroupBy(op, recordVar, inputLiveVars);
        ILogicalOperator gbyOp = gbyOpAndAggVar.first;
        LogicalVariable aggVar = gbyOpAndAggVar.second;
        gbyOp.getInputs().add(new MutableObject<>(assignOp));

        // Adds an unnest operators on top of the group-by operator.
        Pair<ILogicalOperator, LogicalVariable> unnestOpAndUnnestVar =
                createUnnestForAggregatedList(aggVar, op.getSourceLocation());
        ILogicalOperator unnestOp = unnestOpAndUnnestVar.first;
        LogicalVariable unnestVar = unnestOpAndUnnestVar.second;
        unnestOp.getInputs().add(new MutableObject<>(gbyOp));

        // Adds field accesses to recover input live variables.
        ILogicalOperator fieldAccessAssignOp =
                createFieldAccessAssignOperator(unnestVar, inputLiveVars, op.getSourceLocation());
        fieldAccessAssignOp.getInputs().add(new MutableObject<>(unnestOp));

        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(fieldAccessAssignOp, context);
        return fieldAccessAssignOp;
    }

    private Pair<ILogicalOperator, LogicalVariable> createRecordConstructorAssignOp(Set<LogicalVariable> inputLiveVars,
            SourceLocation sourceLoc) {
        // Creates a nested record.
        List<Mutable<ILogicalExpression>> recordConstructorArgs = new ArrayList<>();
        for (LogicalVariable inputLiveVar : inputLiveVars) {
            if (!correlatedKeyVars.contains(inputLiveVar)) {
                recordConstructorArgs.add(new MutableObject<>(new ConstantExpression(
                        new AsterixConstantValue(new AString(Integer.toString(inputLiveVar.getId()))))));
                VariableReferenceExpression inputLiveVarRef = new VariableReferenceExpression(inputLiveVar);
                inputLiveVarRef.setSourceLocation(sourceLoc);
                recordConstructorArgs.add(new MutableObject<>(inputLiveVarRef));
            }
        }
        LogicalVariable recordVar = context.newVar();
        ScalarFunctionCallExpression openRecConstr = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR), recordConstructorArgs);
        openRecConstr.setSourceLocation(sourceLoc);
        Mutable<ILogicalExpression> recordExprRef = new MutableObject<ILogicalExpression>(openRecConstr);
        AssignOperator assignOp = new AssignOperator(recordVar, recordExprRef);
        assignOp.setSourceLocation(sourceLoc);
        return new Pair<>(assignOp, recordVar);
    }

    private Pair<ILogicalOperator, LogicalVariable> wrapLimitInGroupBy(ILogicalOperator op, LogicalVariable recordVar,
            Set<LogicalVariable> inputLiveVars) throws AlgebricksException {
        SourceLocation sourceLoc = op.getSourceLocation();
        GroupByOperator gbyOp = new GroupByOperator();
        gbyOp.setSourceLocation(sourceLoc);
        List<Pair<LogicalVariable, LogicalVariable>> keyVarNewVarPairs = new ArrayList<>();
        for (LogicalVariable keyVar : correlatedKeyVars) {
            // This limits the visitor can only be applied to a nested logical
            // plan inside a Subplan operator,
            // where the keyVarsToEnforce forms a candidate key which can
            // uniquely identify a tuple out of the nested-tuple-source.
            LogicalVariable newVar = context.newVar();
            VariableReferenceExpression keyVarRef = new VariableReferenceExpression(keyVar);
            keyVarRef.setSourceLocation(sourceLoc);
            gbyOp.getGroupByList().add(new Pair<>(newVar, new MutableObject<>(keyVarRef)));
            keyVarNewVarPairs.add(new Pair<>(keyVar, newVar));
        }

        // Creates an aggregate operator doing LISTIFY, as the root of the
        // nested plan of the added group-by operator.
        List<LogicalVariable> aggVarList = new ArrayList<LogicalVariable>();
        List<Mutable<ILogicalExpression>> aggExprList = new ArrayList<Mutable<ILogicalExpression>>();
        LogicalVariable aggVar = context.newVar();
        List<Mutable<ILogicalExpression>> aggArgList = new ArrayList<>();
        aggVarList.add(aggVar);
        // Creates an aggregation function expression.
        VariableReferenceExpression recordVarRef = new VariableReferenceExpression(recordVar);
        recordVarRef.setSourceLocation(sourceLoc);
        aggArgList.add(new MutableObject<>(recordVarRef));
        AggregateFunctionCallExpression aggExpr = new AggregateFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.LISTIFY), false, aggArgList);
        aggExpr.setSourceLocation(sourceLoc);
        aggExprList.add(new MutableObject<>(aggExpr));
        AggregateOperator aggOp = new AggregateOperator(aggVarList, aggExprList);
        aggOp.setSourceLocation(sourceLoc);

        // Adds the original limit operator as the input operator to the added
        // aggregate operator.
        aggOp.getInputs().add(new MutableObject<>(op));
        op.getInputs().clear();
        ILogicalOperator currentOp = op;
        if (!orderingExprs.isEmpty()) {
            OrderOperator orderOp = new OrderOperator(OperatorManipulationUtil.cloneOrderExpressions(orderingExprs));
            orderOp.setSourceLocation(sourceLoc);
            op.getInputs().add(new MutableObject<>(orderOp));
            currentOp = orderOp;
        }

        // Adds a nested tuple source operator as the input operator to the
        // limit operator.
        NestedTupleSourceOperator nts = new NestedTupleSourceOperator(new MutableObject<>(gbyOp));
        nts.setSourceLocation(sourceLoc);
        currentOp.getInputs().add(new MutableObject<>(nts));

        // Sets the root of the added nested plan to the aggregate operator.
        ILogicalPlan nestedPlan = new ALogicalPlanImpl();
        nestedPlan.getRoots().add(new MutableObject<>(aggOp));

        // Sets the nested plan for the added group-by operator.
        gbyOp.getNestedPlans().add(nestedPlan);

        // Updates variable mapping for ancestor operators.
        for (Pair<LogicalVariable, LogicalVariable> keyVarNewVar : keyVarNewVarPairs) {
            updateInputToOutputVarMapping(keyVarNewVar.first, keyVarNewVar.second, false);
        }
        return new Pair<>(gbyOp, aggVar);
    }

    private Pair<ILogicalOperator, LogicalVariable> createUnnestForAggregatedList(LogicalVariable aggVar,
            SourceLocation sourceLoc) {
        LogicalVariable unnestVar = context.newVar();
        // Creates an unnest function expression.
        VariableReferenceExpression aggVarRef = new VariableReferenceExpression(aggVar);
        aggVarRef.setSourceLocation(sourceLoc);
        Mutable<ILogicalExpression> unnestArg = new MutableObject<>(aggVarRef);
        List<Mutable<ILogicalExpression>> unnestArgList = new ArrayList<>();
        unnestArgList.add(unnestArg);
        UnnestingFunctionCallExpression unnestExpr = new UnnestingFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.SCAN_COLLECTION), unnestArgList);
        unnestExpr.setSourceLocation(sourceLoc);
        UnnestOperator unnestOp = new UnnestOperator(unnestVar, new MutableObject<>(unnestExpr));
        unnestOp.setSourceLocation(sourceLoc);
        return new Pair<>(unnestOp, unnestVar);
    }

    private ILogicalOperator createFieldAccessAssignOperator(LogicalVariable recordVar,
            Set<LogicalVariable> inputLiveVars, SourceLocation sourceLoc) {
        List<LogicalVariable> fieldAccessVars = new ArrayList<>();
        List<Mutable<ILogicalExpression>> fieldAccessExprs = new ArrayList<>();
        // Adds field access by name.
        for (LogicalVariable inputLiveVar : inputLiveVars) {
            if (!correlatedKeyVars.contains(inputLiveVar)) {
                // field Var
                LogicalVariable newVar = context.newVar();
                fieldAccessVars.add(newVar);
                // fieldAcess expr
                List<Mutable<ILogicalExpression>> argRefs = new ArrayList<>();
                VariableReferenceExpression recordVarRef = new VariableReferenceExpression(recordVar);
                recordVarRef.setSourceLocation(sourceLoc);
                argRefs.add(new MutableObject<>(recordVarRef));
                argRefs.add(new MutableObject<>(new ConstantExpression(
                        new AsterixConstantValue(new AString(Integer.toString(inputLiveVar.getId()))))));
                ScalarFunctionCallExpression faExpr = new ScalarFunctionCallExpression(
                        FunctionUtil.getFunctionInfo(BuiltinFunctions.FIELD_ACCESS_BY_NAME), argRefs);
                faExpr.setSourceLocation(sourceLoc);
                fieldAccessExprs.add(new MutableObject<>(faExpr));
                // Updates variable mapping for ancestor operators.
                updateInputToOutputVarMapping(inputLiveVar, newVar, false);
            }
        }
        AssignOperator assignOp = new AssignOperator(fieldAccessVars, fieldAccessExprs);
        assignOp.setSourceLocation(sourceLoc);
        return assignOp;
    }

    @Override
    public ILogicalOperator visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
        return visitMultiInputOperator(op);
    }

    @Override
    public ILogicalOperator visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
        return visitMultiInputOperator(op);
    }

    @Override
    public ILogicalOperator visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg)
            throws AlgebricksException {
        if (op.getDataSourceReference().getValue() != subplanOperator) {
            return op;
        }
        LogicalOperatorDeepCopyWithNewVariablesVisitor deepCopyVisitor =
                new LogicalOperatorDeepCopyWithNewVariablesVisitor(context, context);
        ILogicalOperator copiedInputOperator = deepCopyVisitor.deepCopy(subplanInputOperator);

        // Updates the primary key info in the copied plan segment.
        Map<LogicalVariable, LogicalVariable> varMap = deepCopyVisitor.getInputToOutputVariableMapping();
        addPrimaryKeys(varMap);
        Pair<ILogicalOperator, Set<LogicalVariable>> primaryOpAndVars =
                EquivalenceClassUtils.findOrCreatePrimaryKeyOpAndVariables(copiedInputOperator, true, context);
        correlatedKeyVars.clear();
        correlatedKeyVars.addAll(primaryOpAndVars.second);
        // Update key variables and input-output-var mapping.
        varMap.forEach((oldVar, newVar) -> {
            if (correlatedKeyVars.contains(oldVar)) {
                correlatedKeyVars.remove(oldVar);
                correlatedKeyVars.add(newVar);
            }
            updateInputToOutputVarMapping(oldVar, newVar, true);
        });
        return primaryOpAndVars.first;
    }

    @Override
    public ILogicalOperator visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
        visitSingleInputOperator(op);
        if (correlatedKeyVars.isEmpty()) {
            return op;
        }

        orderingExprs.clear();
        orderingExprs.addAll(OperatorManipulationUtil.cloneOrderExpressions(op.getOrderExpressions()));

        List<Pair<IOrder, Mutable<ILogicalExpression>>> orderExprList = new ArrayList<>();
        // Adds keyVars to the prefix of sorting columns.
        for (LogicalVariable keyVar : correlatedKeyVars) {
            VariableReferenceExpression keyVarRef = new VariableReferenceExpression(keyVar);
            keyVarRef.setSourceLocation(op.getSourceLocation());
            orderExprList.add(new Pair<>(OrderOperator.ASC_ORDER, new MutableObject<>(keyVarRef)));
        }
        orderExprList.addAll(op.getOrderExpressions());

        // Creates an order operator with the new expression list.
        OrderOperator orderOp = new OrderOperator(orderExprList);
        orderOp.setSourceLocation(op.getSourceLocation());
        orderOp.getInputs().addAll(op.getInputs());
        context.computeAndSetTypeEnvironmentForOperator(orderOp);
        return orderOp;
    }

    @Override
    public ILogicalOperator visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
        visitSingleInputOperator(op);
        List<Mutable<ILogicalExpression>> assignedExprRefs = op.getExpressions();
        List<LogicalVariable> assignedVars = op.getVariables();
        // Maps assigning variables if assignment expressions are
        // VariableReferenceExpressions.
        for (int index = 0; index < assignedVars.size(); ++index) {
            ILogicalExpression expr = assignedExprRefs.get(index).getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varExpr = (VariableReferenceExpression) expr;
                LogicalVariable sourceVar = varExpr.getVariableReference();
                updateInputToOutputVarMapping(sourceVar, assignedVars.get(index), false);
            }
        }
        return op;
    }

    @Override
    public ILogicalOperator visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
        visitSingleInputOperator(op);
        // Adds all missing variables that should propagates up.
        for (LogicalVariable keyVar : correlatedKeyVars) {
            if (!op.getVariables().contains(keyVar)) {
                op.getVariables().add(keyVar);
            }
        }
        return op;

    }

    @Override
    public ILogicalOperator visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
        throw new UnsupportedOperationException("Script operators in a subplan are not supported!");
    }

    @Override
    public ILogicalOperator visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
        visitMultiInputOperator(op);
        // Update the variable mappings
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varTriples = op.getVariableMappings();
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> triple : varTriples) {
            updateInputToOutputVarMapping(triple.first, triple.third, false);
            updateInputToOutputVarMapping(triple.second, triple.third, false);
        }
        return op;
    }

    @Override
    public ILogicalOperator visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
        visitMultiInputOperator(op);

        int nInput = op.getNumInput();
        boolean hasExtraVars = op.hasExtraVariables();
        List<LogicalVariable> outputCompareVars = op.getOutputCompareVariables();
        List<LogicalVariable> outputExtraVars = op.getOutputExtraVariables();
        for (int i = 0; i < nInput; i++) {
            updateInputToOutputVarMappings(op.getInputCompareVariables(i), outputCompareVars, false,
                    op.getSourceLocation());
            if (hasExtraVars) {
                updateInputToOutputVarMappings(op.getInputExtraVariables(i), outputExtraVars, false,
                        op.getSourceLocation());
            }
        }
        return op;
    }

    @Override
    public ILogicalOperator visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg)
            throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
        visitSingleInputOperator(op);
        Set<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(op, liveVars);
        if (!liveVars.containsAll(correlatedKeyVars)) {
            op.setPropagatesInput(true);
        }
        return op;
    }

    @Override
    public ILogicalOperator visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg)
            throws AlgebricksException {
        throw new CompilationException(ErrorCode.COMPILATION_ERROR, op.getSourceLocation(),
                "The subquery de-correlation rule should always be applied before index-access-method related rules.");
    }

    @Override
    public ILogicalOperator visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
        visitSingleInputOperator(op);
        List<LogicalVariable> distinctVarList = op.getDistinctByVarList();
        for (LogicalVariable keyVar : correlatedKeyVars) {
            if (!distinctVarList.contains(keyVar)) {
                distinctVarList.add(keyVar);
            }
        }
        context.computeAndSetTypeEnvironmentForOperator(op);
        return op;
    }

    @Override
    public ILogicalOperator visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    @Override
    public ILogicalOperator visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
        throw new CompilationException(ErrorCode.COMPILATION_ERROR, op.getSourceLocation(),
                "Forward operator should have been disqualified for this rewriting!");
    }

    @Override
    public ILogicalOperator visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
        return visitSingleInputOperator(op);
    }

    /**
     * Wraps an AggregateOperator or RunningAggregateOperator with a group-by
     * operator where the group-by keys are variables in keyVarsToEnforce. Note
     * that the function here prevents this visitor being used to rewrite
     * arbitrary query plans. Instead, it could only be used for rewriting a
     * nested plan within a subplan operator.
     *
     * @param op
     *            the logical operator for aggregate or running aggregate.
     * @return the wrapped group-by operator if {@code keyVarsToEnforce} is not
     *         empty, and {@code op} otherwise.
     * @throws AlgebricksException
     */
    private ILogicalOperator visitAggregateOperator(ILogicalOperator op) throws AlgebricksException {
        visitSingleInputOperator(op);
        if (correlatedKeyVars.isEmpty()) {
            return op;
        }
        SourceLocation sourceLoc = op.getSourceLocation();
        GroupByOperator gbyOp = new GroupByOperator();
        gbyOp.setSourceLocation(sourceLoc);
        // Creates a copy of correlatedKeyVars, to fix the ConcurrentModificationException in ASTERIXDB-1581.
        List<LogicalVariable> copyOfCorrelatedKeyVars = new ArrayList<>(correlatedKeyVars);
        for (LogicalVariable keyVar : copyOfCorrelatedKeyVars) {
            // This limits the visitor can only be applied to a nested logical
            // plan inside a Subplan operator,
            // where the keyVarsToEnforce forms a candidate key which can
            // uniquely identify a tuple out of the nested-tuple-source.
            LogicalVariable newVar = context.newVar();
            VariableReferenceExpression keyVarRef = new VariableReferenceExpression(keyVar);
            keyVarRef.setSourceLocation(sourceLoc);
            gbyOp.getGroupByList().add(new Pair<>(newVar, new MutableObject<>(keyVarRef)));
            updateInputToOutputVarMapping(keyVar, newVar, false);
        }

        ILogicalOperator inputOp = op.getInputs().get(0).getValue();
        gbyOp.getInputs().add(new MutableObject<>(inputOp));

        NestedTupleSourceOperator nts = new NestedTupleSourceOperator(new MutableObject<>(gbyOp));
        nts.setSourceLocation(sourceLoc);
        op.getInputs().clear();
        op.getInputs().add(new MutableObject<>(nts));

        ILogicalPlan nestedPlan = new ALogicalPlanImpl();
        nestedPlan.getRoots().add(new MutableObject<>(op));
        gbyOp.getNestedPlans().add(nestedPlan);

        OperatorManipulationUtil.computeTypeEnvironmentBottomUp(gbyOp, context);
        return op;
    }

    private ILogicalOperator visitMultiInputOperator(ILogicalOperator op) throws AlgebricksException {
        orderingExprs.clear();
        Set<LogicalVariable> keyVarsForCurrentBranch = new HashSet<LogicalVariable>();
        for (int i = op.getInputs().size() - 1; i >= 0; --i) {
            // Stores key variables for the previous branch.
            keyVarsForCurrentBranch.addAll(correlatedKeyVars);
            correlatedKeyVars.clear();

            // Deals with single input operators.
            ILogicalOperator newChild = op.getInputs().get(i).getValue().accept(this, null);
            op.getInputs().get(i).setValue(newChild);

            if (correlatedKeyVars.isEmpty()) {
                correlatedKeyVars.addAll(keyVarsForCurrentBranch);
            }
            keyVarsForCurrentBranch.clear();
        }
        subtituteVariables(op);
        return op;
    }

    private ILogicalOperator visitSingleInputOperator(ILogicalOperator op) throws AlgebricksException {
        if (op.getInputs().size() == 1) {
            // Deals with single input operators.
            ILogicalOperator newChild = op.getInputs().get(0).getValue().accept(this, null);
            op.getInputs().get(0).setValue(newChild);
        }
        subtituteVariables(op);
        return op;
    }

    private void subtituteVariables(ILogicalOperator op) throws AlgebricksException {
        VariableUtilities.substituteVariables(op, subplanInputVarToCurrentVarMap, context);
        VariableUtilities.substituteVariables(op, varMapIntroducedByRewriting, context);
    }

    private void updateInputToOutputVarMapping(LogicalVariable oldVar, LogicalVariable newVar, boolean inNts) {
        if (correlatedKeyVars.contains(oldVar)) {
            correlatedKeyVars.remove(oldVar);
            correlatedKeyVars.add(newVar);
        }

        for (Pair<IOrder, Mutable<ILogicalExpression>> orderExpr : orderingExprs) {
            orderExpr.second.getValue().substituteVar(oldVar, newVar);
        }

        if (currentVarToSubplanInputVarMap.containsKey(oldVar)) {
            // Find the original mapped var.
            oldVar = currentVarToSubplanInputVarMap.get(oldVar);
        }
        if (subplanInputVarToCurrentVarMap.containsKey(oldVar) || inNts) {
            subplanInputVarToCurrentVarMap.put(oldVar, newVar);
            currentVarToSubplanInputVarMap.put(newVar, oldVar);
        } else {
            varMapIntroducedByRewriting.add(new Pair<>(oldVar, newVar));
        }
    }

    private void updateInputToOutputVarMappings(List<LogicalVariable> oldVarList, List<LogicalVariable> newVarList,
            boolean inNts, SourceLocation sourceLoc) throws CompilationException {
        int oldVarCount = oldVarList.size();
        if (oldVarCount != newVarList.size()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "The cardinality of input and output are not equal");
        }
        for (int i = 0; i < oldVarCount; i++) {
            updateInputToOutputVarMapping(oldVarList.get(i), newVarList.get(i), inNts);
        }
    }

    private void addPrimaryKeys(Map<LogicalVariable, LogicalVariable> varMap) {
        for (Entry<LogicalVariable, LogicalVariable> entry : varMap.entrySet()) {
            List<LogicalVariable> dependencyVars = context.findPrimaryKey(entry.getKey());
            if (dependencyVars == null) {
                // No key dependencies
                continue;
            }
            List<LogicalVariable> newDependencies = new ArrayList<>();
            for (LogicalVariable dependencyVar : dependencyVars) {
                LogicalVariable newDependencyVar = varMap.get(dependencyVar);
                if (newDependencyVar == null) {
                    continue;
                }
                newDependencies.add(newDependencyVar);
            }
            context.addPrimaryKey(
                    new FunctionalDependency(newDependencies, Collections.singletonList(entry.getValue())));
        }
    }

}
