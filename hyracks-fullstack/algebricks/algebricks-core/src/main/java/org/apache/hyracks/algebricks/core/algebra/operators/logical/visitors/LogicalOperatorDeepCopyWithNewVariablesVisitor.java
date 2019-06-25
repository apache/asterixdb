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
package org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IVariableContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
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
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.IQueryOperatorVisitor;

/**
 * This visitor deep-copies a query plan but uses a new set of variables. Method
 * getInputToOutputVariableMapping() will return a map that maps input variables
 * to their corresponding output variables.
 */
public class LogicalOperatorDeepCopyWithNewVariablesVisitor
        implements IQueryOperatorVisitor<ILogicalOperator, ILogicalOperator> {
    private final ITypingContext typeContext;
    private final IVariableContext varContext;
    private final LogicalExpressionDeepCopyWithNewVariablesVisitor exprDeepCopyVisitor;

    // Key: Variable in the original plan. Value: New variable replacing the
    // original one in the copied plan.
    private final LinkedHashMap<LogicalVariable, LogicalVariable> inputVarToOutputVarMapping;

    // Key: New variable in the new plan. Value: The old variable in the
    // original plan.
    private final LinkedHashMap<LogicalVariable, LogicalVariable> outputVarToInputVarMapping = new LinkedHashMap<>();

    // Free variables: variables that shouldn't be deep copied, i.e., mapped.
    private final Set<LogicalVariable> freeVars = new HashSet<>();

    // Whether free variables in the given plan subtree should be reused.
    private final boolean reuseFreeVars;

    /**
     * @param varContext
     *            , the variable context.
     * @param typeContext
     *            the type context.
     */
    public LogicalOperatorDeepCopyWithNewVariablesVisitor(IVariableContext varContext, ITypingContext typeContext) {
        this(varContext, typeContext, new LinkedHashMap<>(), false);
    }

    /**
     * @param varContext
     *            , the variable context.
     * @param typeContext
     *            the type context.
     * @param reuseFreeVars
     *            whether free variables in the given plan tree should be reused.
     */
    public LogicalOperatorDeepCopyWithNewVariablesVisitor(IVariableContext varContext, ITypingContext typeContext,
            boolean reuseFreeVars) {
        this(varContext, typeContext, new LinkedHashMap<>(), reuseFreeVars);
    }

    /**
     * @param varContext
     *            , the variable context.
     * @param typeContext
     *            the type context.
     * @param inVarMapping
     *            Variable mapping keyed by variables in the original plan.
     *            Those variables are replaced by their corresponding value in
     *            the map in the copied plan.
     * @param reuseFreeVars
     *            whether free variables in the given plan tree should be reused.
     */
    public LogicalOperatorDeepCopyWithNewVariablesVisitor(IVariableContext varContext, ITypingContext typeContext,
            LinkedHashMap<LogicalVariable, LogicalVariable> inVarMapping, boolean reuseFreeVars) {
        this.varContext = varContext;
        this.typeContext = typeContext;
        this.inputVarToOutputVarMapping = inVarMapping;
        this.exprDeepCopyVisitor = new LogicalExpressionDeepCopyWithNewVariablesVisitor(varContext, inVarMapping,
                inputVarToOutputVarMapping, freeVars);
        this.reuseFreeVars = reuseFreeVars;
    }

    private void copyAnnotations(ILogicalOperator src, ILogicalOperator dest) {
        dest.getAnnotations().putAll(src.getAnnotations());
    }

    private void copySourceLocation(ILogicalOperator src, AbstractLogicalOperator dest) {
        dest.setSourceLocation(src.getSourceLocation());
    }

    public ILogicalOperator deepCopy(ILogicalOperator op) throws AlgebricksException {
        // The deep copy call outside this visitor always has a null argument.
        return deepCopy(op, null);
    }

    private ILogicalOperator deepCopy(ILogicalOperator op, ILogicalOperator arg) throws AlgebricksException {
        if (op == null) {
            return null;
        }
        if (reuseFreeVars) {
            // If the reuseFreeVars flag is set, we collect all free variables in the
            // given operator subtree and do not re-map them in the deep-copied plan.
            OperatorPropertiesUtil.getFreeVariablesInSelfOrDesc((AbstractLogicalOperator) op, freeVars);
        }
        ILogicalOperator opCopy = op.accept(this, arg);
        if (typeContext != null) {
            OperatorManipulationUtil.computeTypeEnvironmentBottomUp(opCopy, typeContext);
        }
        return opCopy;
    }

    private void deepCopyInputs(ILogicalOperator src, ILogicalOperator dest, ILogicalOperator arg)
            throws AlgebricksException {
        List<Mutable<ILogicalOperator>> inputs = src.getInputs();
        List<Mutable<ILogicalOperator>> inputsCopy = dest.getInputs();
        for (Mutable<ILogicalOperator> input : inputs) {
            inputsCopy.add(deepCopyOperatorReference(input, arg));
        }
    }

    private Mutable<ILogicalOperator> deepCopyOperatorReference(Mutable<ILogicalOperator> opRef, ILogicalOperator arg)
            throws AlgebricksException {
        return new MutableObject<>(deepCopy(opRef.getValue(), arg));
    }

    private List<Mutable<ILogicalOperator>> deepCopyOperatorReferenceList(List<Mutable<ILogicalOperator>> list,
            ILogicalOperator arg) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> listCopy = new ArrayList<Mutable<ILogicalOperator>>(list.size());
        for (Mutable<ILogicalOperator> opRef : list) {
            listCopy.add(deepCopyOperatorReference(opRef, arg));
        }
        return listCopy;
    }

    private IOrder deepCopyOrder(IOrder order) {
        switch (order.getKind()) {
            case ASC:
            case DESC:
                return order;
            case FUNCTIONCALL:
            default:
                throw new UnsupportedOperationException();
        }
    }

    private List<Pair<IOrder, Mutable<ILogicalExpression>>> deepCopyOrderExpressionReferencePairList(
            List<Pair<IOrder, Mutable<ILogicalExpression>>> list) throws AlgebricksException {
        ArrayList<Pair<IOrder, Mutable<ILogicalExpression>>> listCopy =
                new ArrayList<Pair<IOrder, Mutable<ILogicalExpression>>>(list.size());
        for (Pair<IOrder, Mutable<ILogicalExpression>> pair : list) {
            listCopy.add(new Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>(deepCopyOrder(pair.first),
                    exprDeepCopyVisitor.deepCopyExpressionReference(pair.second)));
        }
        return listCopy;
    }

    private ILogicalPlan deepCopyPlan(ILogicalPlan plan, ILogicalOperator arg) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> rootsCopy = deepCopyOperatorReferenceList(plan.getRoots(), arg);
        ILogicalPlan planCopy = new ALogicalPlanImpl(rootsCopy);
        return planCopy;
    }

    private List<ILogicalPlan> deepCopyPlanList(List<ILogicalPlan> list, List<ILogicalPlan> listCopy,
            ILogicalOperator arg) throws AlgebricksException {
        for (ILogicalPlan plan : list) {
            listCopy.add(deepCopyPlan(plan, arg));
        }
        return listCopy;
    }

    private LogicalVariable deepCopyVariable(LogicalVariable var) {
        if (var == null) {
            return null;
        }
        if (freeVars.contains(var)) {
            return var;
        }
        LogicalVariable givenVarReplacement = outputVarToInputVarMapping.get(var);
        if (givenVarReplacement != null) {
            inputVarToOutputVarMapping.put(var, givenVarReplacement);
            return givenVarReplacement;
        }
        LogicalVariable varCopy = inputVarToOutputVarMapping.get(var);
        if (varCopy == null) {
            varCopy = varContext.newVar();
            inputVarToOutputVarMapping.put(var, varCopy);
        }
        return varCopy;
    }

    private List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> deepCopyVariableExpressionReferencePairList(
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> list) throws AlgebricksException {
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> listCopy =
                new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>(list.size());
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> pair : list) {
            listCopy.add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(deepCopyVariable(pair.first),
                    exprDeepCopyVisitor.deepCopyExpressionReference(pair.second)));
        }
        return listCopy;
    }

    private List<LogicalVariable> deepCopyVariableList(List<LogicalVariable> list) {
        ArrayList<LogicalVariable> listCopy = new ArrayList<LogicalVariable>(list.size());
        for (LogicalVariable var : list) {
            listCopy.add(deepCopyVariable(var));
        }
        return listCopy;
    }

    private void deepCopyInputsAnnotationsAndExecutionMode(ILogicalOperator op, ILogicalOperator arg,
            AbstractLogicalOperator opCopy) throws AlgebricksException {
        deepCopyInputs(op, opCopy, arg);
        copyAnnotations(op, opCopy);
        copySourceLocation(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
    }

    public void reset() {
        freeVars.clear();
        inputVarToOutputVarMapping.clear();
        outputVarToInputVarMapping.clear();
    }

    public void updatePrimaryKeys(IOptimizationContext context) {
        inputVarToOutputVarMapping.forEach((key, value) -> {
            List<LogicalVariable> primaryKey = context.findPrimaryKey(key);
            if (primaryKey != null) {
                List<LogicalVariable> head = new ArrayList<>();
                for (LogicalVariable variable : primaryKey) {
                    head.add(inputVarToOutputVarMapping.get(variable));
                }
                List<LogicalVariable> tail = new ArrayList<>(1);
                tail.add(value);
                context.addPrimaryKey(new FunctionalDependency(head, tail));
            }
        });
    }

    public LogicalVariable varCopy(LogicalVariable var) throws AlgebricksException {
        return inputVarToOutputVarMapping.get(var);
    }

    @Override
    public ILogicalOperator visitAggregateOperator(AggregateOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        AggregateOperator opCopy = new AggregateOperator(deepCopyVariableList(op.getVariables()),
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getExpressions()));
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitAssignOperator(AssignOperator op, ILogicalOperator arg) throws AlgebricksException {
        AssignOperator opCopy = new AssignOperator(deepCopyVariableList(op.getVariables()),
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getExpressions()));
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitDataScanOperator(DataSourceScanOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        Mutable<ILogicalExpression> newSelectCondition = op.getSelectCondition() != null
                ? exprDeepCopyVisitor.deepCopyExpressionReference(op.getSelectCondition()) : null;
        DataSourceScanOperator opCopy = new DataSourceScanOperator(deepCopyVariableList(op.getVariables()),
                op.getDataSource(), newSelectCondition, op.getOutputLimit());
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitDistinctOperator(DistinctOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        DistinctOperator opCopy =
                new DistinctOperator(exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getExpressions()));
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, ILogicalOperator arg) {
        EmptyTupleSourceOperator opCopy = new EmptyTupleSourceOperator();
        copySourceLocation(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitExchangeOperator(ExchangeOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        ExchangeOperator opCopy = new ExchangeOperator();
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitGroupByOperator(GroupByOperator op, ILogicalOperator arg) throws AlgebricksException {
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByListCopy =
                deepCopyVariableExpressionReferencePairList(op.getGroupByList());
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decorListCopy =
                deepCopyVariableExpressionReferencePairList(op.getDecorList());
        List<ILogicalPlan> nestedPlansCopy = new ArrayList<ILogicalPlan>();

        GroupByOperator opCopy = new GroupByOperator(groupByListCopy, decorListCopy, nestedPlansCopy, op.isGroupAll());
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        deepCopyPlanList(op.getNestedPlans(), nestedPlansCopy, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitInnerJoinOperator(InnerJoinOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        InnerJoinOperator opCopy =
                new InnerJoinOperator(exprDeepCopyVisitor.deepCopyExpressionReference(op.getCondition()),
                        deepCopyOperatorReference(op.getInputs().get(0), arg),
                        deepCopyOperatorReference(op.getInputs().get(1), arg));
        copyAnnotations(op, opCopy);
        copySourceLocation(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitLeftOuterJoinOperator(LeftOuterJoinOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        LeftOuterJoinOperator opCopy =
                new LeftOuterJoinOperator(exprDeepCopyVisitor.deepCopyExpressionReference(op.getCondition()),
                        deepCopyOperatorReference(op.getInputs().get(0), arg),
                        deepCopyOperatorReference(op.getInputs().get(1), arg));
        copyAnnotations(op, opCopy);
        copySourceLocation(op, opCopy);
        opCopy.setExecutionMode(op.getExecutionMode());
        return opCopy;
    }

    @Override
    public ILogicalOperator visitLimitOperator(LimitOperator op, ILogicalOperator arg) throws AlgebricksException {
        LimitOperator opCopy = new LimitOperator(exprDeepCopyVisitor.deepCopy(op.getMaxObjects().getValue()),
                exprDeepCopyVisitor.deepCopy(op.getOffset().getValue()), op.isTopmostLimitOp());
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitNestedTupleSourceOperator(NestedTupleSourceOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        Mutable<ILogicalOperator> dataSourceReference =
                arg == null ? op.getDataSourceReference() : new MutableObject<>(arg);
        NestedTupleSourceOperator opCopy = new NestedTupleSourceOperator(dataSourceReference);
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitOrderOperator(OrderOperator op, ILogicalOperator arg) throws AlgebricksException {
        OrderOperator opCopy = new OrderOperator(deepCopyOrderExpressionReferencePairList(op.getOrderExpressions()));
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitProjectOperator(ProjectOperator op, ILogicalOperator arg) throws AlgebricksException {
        ProjectOperator opCopy = new ProjectOperator(deepCopyVariableList(op.getVariables()));
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitReplicateOperator(ReplicateOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        boolean[] outputMatFlags = op.getOutputMaterializationFlags();
        boolean[] copiedOutputMatFlags = new boolean[outputMatFlags.length];
        System.arraycopy(outputMatFlags, 0, copiedOutputMatFlags, 0, outputMatFlags.length);
        ReplicateOperator opCopy = new ReplicateOperator(op.getOutputArity(), copiedOutputMatFlags);
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitSplitOperator(SplitOperator op, ILogicalOperator arg) throws AlgebricksException {
        SplitOperator opCopy = new SplitOperator(op.getOutputArity(), op.getBranchingExpression());
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitMaterializeOperator(MaterializeOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        MaterializeOperator opCopy = new MaterializeOperator();
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitRunningAggregateOperator(RunningAggregateOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        RunningAggregateOperator opCopy = new RunningAggregateOperator(deepCopyVariableList(op.getVariables()),
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getExpressions()));
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitScriptOperator(ScriptOperator op, ILogicalOperator arg) throws AlgebricksException {
        ScriptOperator opCopy = new ScriptOperator(op.getScriptDescription(),
                deepCopyVariableList(op.getInputVariables()), deepCopyVariableList(op.getOutputVariables()));
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitSelectOperator(SelectOperator op, ILogicalOperator arg) throws AlgebricksException {
        SelectOperator opCopy = new SelectOperator(exprDeepCopyVisitor.deepCopyExpressionReference(op.getCondition()),
                op.getRetainMissing(), deepCopyVariable(op.getMissingPlaceholderVariable()));
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitSubplanOperator(SubplanOperator op, ILogicalOperator arg) throws AlgebricksException {
        List<ILogicalPlan> nestedPlansCopy = new ArrayList<ILogicalPlan>();
        SubplanOperator opCopy = new SubplanOperator(nestedPlansCopy);
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        deepCopyPlanList(op.getNestedPlans(), nestedPlansCopy, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitUnionOperator(UnionAllOperator op, ILogicalOperator arg) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> copiedInputs = new ArrayList<>();
        for (Mutable<ILogicalOperator> childRef : op.getInputs()) {
            copiedInputs.add(deepCopyOperatorReference(childRef, null));
        }
        List<List<LogicalVariable>> liveVarsInInputs = new ArrayList<>();
        for (Mutable<ILogicalOperator> inputOpRef : copiedInputs) {
            List<LogicalVariable> liveVars = new ArrayList<>();
            VariableUtilities.getLiveVariables(inputOpRef.getValue(), liveVars);
            liveVarsInInputs.add(liveVars);
        }
        List<LogicalVariable> liveVarsInLeftInput = liveVarsInInputs.get(0);
        List<LogicalVariable> liveVarsInRightInput = liveVarsInInputs.get(1);
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> copiedTriples = new ArrayList<>();
        int index = 0;
        for (Triple<LogicalVariable, LogicalVariable, LogicalVariable> triple : op.getVariableMappings()) {
            LogicalVariable producedVar = deepCopyVariable(triple.third);
            Triple<LogicalVariable, LogicalVariable, LogicalVariable> copiedTriple =
                    new Triple<>(liveVarsInLeftInput.get(index), liveVarsInRightInput.get(index), producedVar);
            copiedTriples.add(copiedTriple);
            ++index;
        }
        UnionAllOperator opCopy = new UnionAllOperator(copiedTriples);
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitIntersectOperator(IntersectOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        int nInput = op.getNumInput();
        List<LogicalVariable> outputCompareVarsCopy = deepCopyVariableList(op.getOutputCompareVariables());
        boolean hasExtraVars = op.hasExtraVariables();
        List<LogicalVariable> outputExtraVarsCopy =
                hasExtraVars ? deepCopyVariableList(op.getOutputExtraVariables()) : null;
        List<List<LogicalVariable>> inputCompareVarsCopy = new ArrayList<>(nInput);
        List<List<LogicalVariable>> inputExtraVarsCopy = hasExtraVars ? new ArrayList<>(nInput) : null;
        for (int i = 0; i < nInput; i++) {
            inputCompareVarsCopy.add(deepCopyVariableList(op.getInputCompareVariables(i)));
            if (hasExtraVars) {
                inputExtraVarsCopy.add(deepCopyVariableList(op.getInputExtraVariables(i)));
            }
        }
        IntersectOperator opCopy = new IntersectOperator(outputCompareVarsCopy, outputExtraVarsCopy,
                inputCompareVarsCopy, inputExtraVarsCopy);
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitUnnestMapOperator(UnnestMapOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        Mutable<ILogicalExpression> newSelectCondition = op.getSelectCondition() != null
                ? exprDeepCopyVisitor.deepCopyExpressionReference(op.getSelectCondition()) : null;
        UnnestMapOperator opCopy = new UnnestMapOperator(deepCopyVariableList(op.getVariables()),
                exprDeepCopyVisitor.deepCopyExpressionReference(op.getExpressionRef()), op.getVariableTypes(),
                op.propagatesInput(), newSelectCondition, op.getOutputLimit());
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        LeftOuterUnnestMapOperator opCopy = new LeftOuterUnnestMapOperator(deepCopyVariableList(op.getVariables()),
                exprDeepCopyVisitor.deepCopyExpressionReference(op.getExpressionRef()), op.getVariableTypes(),
                op.propagatesInput());
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitUnnestOperator(UnnestOperator op, ILogicalOperator arg) throws AlgebricksException {
        UnnestOperator opCopy = new UnnestOperator(deepCopyVariable(op.getVariable()),
                exprDeepCopyVisitor.deepCopyExpressionReference(op.getExpressionRef()),
                deepCopyVariable(op.getPositionalVariable()), op.getPositionalVariableType(), op.getPositionWriter());
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitTokenizeOperator(TokenizeOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        TokenizeOperator opCopy = new TokenizeOperator(op.getDataSourceIndex(),
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getPrimaryKeyExpressions()),
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getSecondaryKeyExpressions()),
                this.deepCopyVariableList(op.getTokenizeVars()),
                exprDeepCopyVisitor.deepCopyExpressionReference(op.getFilterExpression()), op.getOperation(),
                op.isBulkload(), op.isPartitioned(), op.getTokenizeVarTypes());
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitForwardOperator(ForwardOperator op, ILogicalOperator arg) throws AlgebricksException {
        ForwardOperator opCopy = new ForwardOperator(op.getSideDataKey(),
                exprDeepCopyVisitor.deepCopyExpressionReference(op.getSideDataExpression()));
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitDelegateOperator(DelegateOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILogicalOperator visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, ILogicalOperator arg)
            throws AlgebricksException {
        LeftOuterUnnestOperator opCopy = new LeftOuterUnnestOperator(deepCopyVariable(op.getVariable()),
                exprDeepCopyVisitor.deepCopyExpressionReference(op.getExpressionRef()),
                deepCopyVariable(op.getPositionalVariable()), op.getPositionalVariableType(), op.getPositionWriter());
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        return opCopy;
    }

    @Override
    public ILogicalOperator visitWindowOperator(WindowOperator op, ILogicalOperator arg) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> partitionExprCopy =
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getPartitionExpressions());
        List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> orderExprCopy =
                deepCopyOrderExpressionReferencePairList(op.getOrderExpressions());
        List<Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> frameValueExprCopy =
                deepCopyOrderExpressionReferencePairList(op.getFrameValueExpressions());
        List<Mutable<ILogicalExpression>> frameStartExprCopy =
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getFrameStartExpressions());
        List<Mutable<ILogicalExpression>> frameStartValidationExprCopy =
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getFrameStartValidationExpressions());
        List<Mutable<ILogicalExpression>> frameEndExprCopy =
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getFrameEndExpressions());
        List<Mutable<ILogicalExpression>> frameEndValidationExprCopy =
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getFrameEndValidationExpressions());
        List<Mutable<ILogicalExpression>> frameExcludeExprCopy =
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getFrameExcludeExpressions());
        ILogicalExpression frameExcludeUnaryExprCopy =
                exprDeepCopyVisitor.deepCopy(op.getFrameExcludeUnaryExpression().getValue());
        ILogicalExpression frameOffsetExprCopy = exprDeepCopyVisitor.deepCopy(op.getFrameOffsetExpression().getValue());
        List<LogicalVariable> varCopy = deepCopyVariableList(op.getVariables());
        List<Mutable<ILogicalExpression>> exprCopy =
                exprDeepCopyVisitor.deepCopyExpressionReferenceList(op.getExpressions());
        List<ILogicalPlan> nestedPlansCopy = new ArrayList<>();
        WindowOperator opCopy = new WindowOperator(partitionExprCopy, orderExprCopy, frameValueExprCopy,
                frameStartExprCopy, frameStartValidationExprCopy, frameEndExprCopy, frameEndValidationExprCopy,
                frameExcludeExprCopy, op.getFrameExcludeNegationStartIdx(), frameExcludeUnaryExprCopy,
                frameOffsetExprCopy, op.getFrameMaxObjects(), varCopy, exprCopy, nestedPlansCopy);
        deepCopyInputsAnnotationsAndExecutionMode(op, arg, opCopy);
        deepCopyPlanList(op.getNestedPlans(), nestedPlansCopy, opCopy);
        return opCopy;
    }

    public LinkedHashMap<LogicalVariable, LogicalVariable> getInputToOutputVariableMapping() {
        return inputVarToOutputVarMapping;
    }
}
