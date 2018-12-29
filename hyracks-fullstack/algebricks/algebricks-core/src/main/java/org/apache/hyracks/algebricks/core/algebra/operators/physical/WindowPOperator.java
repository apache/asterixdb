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

package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.StatefulFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.win.AbstractWindowRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.win.WindowNestedPlansRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.win.WindowSimpleRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.win.WindowAggregatorDescriptorFactory;
import org.apache.hyracks.algebricks.runtime.operators.win.WindowMaterializingRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;

public class WindowPOperator extends AbstractPhysicalOperator {

    private final List<LogicalVariable> partitionColumns;

    private final boolean partitionMaterialization;

    private final List<OrderColumn> orderColumns;

    public WindowPOperator(List<LogicalVariable> partitionColumns, boolean partitionMaterialization,
            List<OrderColumn> orderColumns) {
        this.partitionColumns = partitionColumns;
        this.partitionMaterialization = partitionMaterialization;
        this.orderColumns = orderColumns;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.WINDOW;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) throws AlgebricksException {
        IPartitioningProperty pp;
        switch (op.getExecutionMode()) {
            case PARTITIONED:
                pp = new UnorderedPartitionedProperty(new ListSet<>(partitionColumns),
                        context.getComputationNodeDomain());
                break;
            case UNPARTITIONED:
                pp = IPartitioningProperty.UNPARTITIONED;
                break;
            case LOCAL:
                pp = null;
                break;
            default:
                throw new IllegalStateException(op.getExecutionMode().name());
        }

        // require local order property [pc1, ... pcN, oc1, ... ocN]
        // accounting for cases where there's an overlap between order and partition columns
        // TODO replace with required local grouping on partition columns + local order on order columns
        List<OrderColumn> lopColumns = new ArrayList<>();
        ListSet<LogicalVariable> pcVars = new ListSet<>();
        pcVars.addAll(partitionColumns);
        for (int oIdx = 0, ln = orderColumns.size(); oIdx < ln; oIdx++) {
            OrderColumn oc = orderColumns.get(oIdx);
            LogicalVariable ocVar = oc.getColumn();
            if (!pcVars.remove(ocVar) && containsAny(orderColumns, oIdx + 1, pcVars)) {
                throw new AlgebricksException(ErrorCode.HYRACKS, ErrorCode.UNSUPPORTED_WINDOW_SPEC,
                        op.getSourceLocation(), String.valueOf(partitionColumns), String.valueOf(orderColumns));
            }
            lopColumns.add(new OrderColumn(oc.getColumn(), oc.getOrder()));
        }
        int pIdx = 0;
        for (LogicalVariable pColumn : pcVars) {
            lopColumns.add(pIdx++, new OrderColumn(pColumn, OrderOperator.IOrder.OrderKind.ASC));
        }
        List<ILocalStructuralProperty> localProps =
                lopColumns.isEmpty() ? null : Collections.singletonList(new LocalOrderProperty(lopColumns));

        return new PhysicalRequirements(
                new StructuralPropertiesVector[] { new StructuralPropertiesVector(pp, localProps) },
                IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        WindowOperator winOp = (WindowOperator) op;

        int[] partitionColumnsList = JobGenHelper.projectVariables(inputSchemas[0], partitionColumns);

        IVariableTypeEnvironment opTypeEnv = context.getTypeEnvironment(op);
        IBinaryComparatorFactory[] partitionComparatorFactories =
                JobGenHelper.variablesToAscBinaryComparatorFactories(partitionColumns, opTypeEnv, context);

        //TODO not all functions need order comparators
        IBinaryComparatorFactory[] orderComparatorFactories =
                JobGenHelper.variablesToBinaryComparatorFactories(orderColumns, opTypeEnv, context);

        IVariableTypeEnvironment inputTypeEnv = context.getTypeEnvironment(op.getInputs().get(0).getValue());
        IExpressionRuntimeProvider exprRuntimeProvider = context.getExpressionRuntimeProvider();
        IBinaryComparatorFactoryProvider binaryComparatorFactoryProvider = context.getBinaryComparatorFactoryProvider();

        IScalarEvaluatorFactory[] frameStartExprEvals = createEvaluatorFactories(winOp.getFrameStartExpressions(),
                inputSchemas, inputTypeEnv, exprRuntimeProvider, context);

        IScalarEvaluatorFactory[] frameEndExprEvals = createEvaluatorFactories(winOp.getFrameEndExpressions(),
                inputSchemas, inputTypeEnv, exprRuntimeProvider, context);

        Pair<IScalarEvaluatorFactory[], IBinaryComparatorFactory[]> frameValueExprEvalsAndComparators =
                createEvaluatorAndComparatorFactories(winOp.getFrameValueExpressions(), Pair::getSecond, Pair::getFirst,
                        inputSchemas, inputTypeEnv, exprRuntimeProvider, binaryComparatorFactoryProvider, context);

        Pair<IScalarEvaluatorFactory[], IBinaryComparatorFactory[]> frameExcludeExprEvalsAndComparators =
                createEvaluatorAndComparatorFactories(winOp.getFrameExcludeExpressions(), v -> v,
                        v -> OrderOperator.ASC_ORDER, inputSchemas, inputTypeEnv, exprRuntimeProvider,
                        binaryComparatorFactoryProvider, context);

        IScalarEvaluatorFactory frameOffsetExprEval = null;
        ILogicalExpression frameOffsetExpr = winOp.getFrameOffset().getValue();
        if (frameOffsetExpr != null) {
            frameOffsetExprEval =
                    exprRuntimeProvider.createEvaluatorFactory(frameOffsetExpr, inputTypeEnv, inputSchemas, context);
        }

        int[] projectionColumnsExcludingSubplans = JobGenHelper.projectAllVariables(opSchema);

        int[] runningAggOutColumns = JobGenHelper.projectVariables(opSchema, winOp.getVariables());

        List<Mutable<ILogicalExpression>> runningAggExprs = winOp.getExpressions();
        int runningAggExprCount = runningAggExprs.size();
        IRunningAggregateEvaluatorFactory[] runningAggFactories =
                new IRunningAggregateEvaluatorFactory[runningAggExprCount];
        for (int i = 0; i < runningAggExprCount; i++) {
            StatefulFunctionCallExpression expr = (StatefulFunctionCallExpression) runningAggExprs.get(i).getValue();
            runningAggFactories[i] = exprRuntimeProvider.createRunningAggregateFunctionFactory(expr, inputTypeEnv,
                    inputSchemas, context);
        }

        AbstractWindowRuntimeFactory runtime;
        if (winOp.hasNestedPlans()) {
            int opSchemaSizePreSubplans = opSchema.getSize();
            AlgebricksPipeline[] subplans = compileSubplans(inputSchemas[0], winOp, opSchema, context);
            int aggregatorOutputSchemaSize = opSchema.getSize() - opSchemaSizePreSubplans;
            WindowAggregatorDescriptorFactory nestedAggFactory = new WindowAggregatorDescriptorFactory(subplans);
            nestedAggFactory.setSourceLocation(winOp.getSourceLocation());
            runtime = new WindowNestedPlansRuntimeFactory(partitionColumnsList, partitionComparatorFactories,
                    orderComparatorFactories, frameValueExprEvalsAndComparators.first,
                    frameValueExprEvalsAndComparators.second, frameStartExprEvals, frameEndExprEvals,
                    frameExcludeExprEvalsAndComparators.first, winOp.getFrameExcludeNegationStartIdx(),
                    frameExcludeExprEvalsAndComparators.second, frameOffsetExprEval,
                    context.getBinaryIntegerInspectorFactory(), winOp.getFrameMaxObjects(),
                    projectionColumnsExcludingSubplans, runningAggOutColumns, runningAggFactories,
                    aggregatorOutputSchemaSize, nestedAggFactory);
        } else if (partitionMaterialization) {
            runtime = new WindowMaterializingRuntimeFactory(partitionColumnsList, partitionComparatorFactories,
                    orderComparatorFactories, projectionColumnsExcludingSubplans, runningAggOutColumns,
                    runningAggFactories);
        } else {
            runtime = new WindowSimpleRuntimeFactory(partitionColumnsList, partitionComparatorFactories,
                    orderComparatorFactories, projectionColumnsExcludingSubplans, runningAggOutColumns,
                    runningAggFactories);
        }
        runtime.setSourceLocation(winOp.getSourceLocation());

        // contribute one Asterix framewriter
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(opTypeEnv, opSchema, context);
        builder.contributeMicroOperator(winOp, runtime, recDesc);
        // and contribute one edge from its child
        ILogicalOperator src = winOp.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, winOp, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }

    public boolean isPartitionMaterialization() {
        return partitionMaterialization;
    }

    private IScalarEvaluatorFactory[] createEvaluatorFactories(List<Mutable<ILogicalExpression>> exprList,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment inputTypeEnv,
            IExpressionRuntimeProvider exprRuntimeProvider, JobGenContext context) throws AlgebricksException {
        if (exprList.isEmpty()) {
            return null;
        }
        int ln = exprList.size();
        IScalarEvaluatorFactory[] evals = new IScalarEvaluatorFactory[ln];
        for (int i = 0; i < ln; i++) {
            ILogicalExpression expr = exprList.get(i).getValue();
            evals[i] = exprRuntimeProvider.createEvaluatorFactory(expr, inputTypeEnv, inputSchemas, context);
        }
        return evals;
    }

    private <T> Pair<IScalarEvaluatorFactory[], IBinaryComparatorFactory[]> createEvaluatorAndComparatorFactories(
            List<T> exprList, Function<T, Mutable<ILogicalExpression>> exprGetter,
            Function<T, OrderOperator.IOrder> orderGetter, IOperatorSchema[] inputSchemas,
            IVariableTypeEnvironment inputTypeEnv, IExpressionRuntimeProvider exprRuntimeProvider,
            IBinaryComparatorFactoryProvider binaryComparatorFactoryProvider, JobGenContext context)
            throws AlgebricksException {
        if (exprList.isEmpty()) {
            return new Pair<>(null, null);
        }
        int ln = exprList.size();
        IScalarEvaluatorFactory[] evals = new IScalarEvaluatorFactory[ln];
        IBinaryComparatorFactory[] comparators = new IBinaryComparatorFactory[ln];
        for (int i = 0; i < ln; i++) {
            T exprObj = exprList.get(i);
            ILogicalExpression expr = exprGetter.apply(exprObj).getValue();
            OrderOperator.IOrder order = orderGetter.apply(exprObj);
            evals[i] = exprRuntimeProvider.createEvaluatorFactory(expr, inputTypeEnv, inputSchemas, context);
            comparators[i] = binaryComparatorFactoryProvider.getBinaryComparatorFactory(inputTypeEnv.getType(expr),
                    order.getKind() == OrderOperator.IOrder.OrderKind.ASC);
        }
        return new Pair<>(evals, comparators);
    }

    private boolean containsAny(List<OrderColumn> ocList, int startIdx, Set<LogicalVariable> varSet) {
        for (int i = startIdx, ln = ocList.size(); i < ln; i++) {
            if (varSet.contains(ocList.get(i).getColumn())) {
                return true;
            }
        }
        return false;
    }
}
