/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.ListSet;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IPartialAggregationTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.LocalGroupingProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.OrderColumn;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.OperatorSchemaImpl;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.aggreg.SimpleAlgebricksAccumulatingAggregatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.sort.SortGroupByOperatorDescriptor;

public class SortGroupByPOperator extends AbstractPhysicalOperator {

    private final int frameLimit;
    private final OrderColumn[] orderColumns;
    private final List<LogicalVariable> columnSet = new ArrayList<LogicalVariable>();

    public SortGroupByPOperator(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyList, int frameLimit,
            OrderColumn[] orderColumns) {
        this.frameLimit = frameLimit;
        this.orderColumns = orderColumns;
        computeColumnSet(gbyList);
    }

    private void computeColumnSet(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyList) {
        columnSet.clear();
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gbyList) {
            ILogicalExpression expr = p.second.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression v = (VariableReferenceExpression) expr;
                columnSet.add(v.getVariableReference());
            }
        }
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.SORT_GROUP_BY;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + columnSet;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    private List<LogicalVariable> getGbyColumns() {
        return columnSet;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        List<ILocalStructuralProperty> propsLocal = new LinkedList<ILocalStructuralProperty>();

        GroupByOperator gOp = (GroupByOperator) op;
        Set<LogicalVariable> columnSet = new ListSet<LogicalVariable>();
        List<OrderColumn> ocs = new ArrayList<OrderColumn>();

        if (!columnSet.isEmpty()) {
            propsLocal.add(new LocalGroupingProperty(columnSet));
        }
        for (OrderColumn oc : orderColumns) {
            ocs.add(oc);
        }
        propsLocal.add(new LocalOrderProperty(ocs));
        for (ILogicalPlan p : gOp.getNestedPlans()) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                ILogicalOperator rOp = r.getValue();
                propsLocal.addAll(rOp.getDeliveredPhysicalProperties().getLocalProperties());
            }
        }

        ILogicalOperator op2 = op.getInputs().get(0).getValue();
        IPhysicalPropertiesVector childProp = op2.getDeliveredPhysicalProperties();
        deliveredProperties = new StructuralPropertiesVector(childProp.getPartitioningProperty(), propsLocal);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        return emptyUnaryRequirements();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        List<LogicalVariable> gbyCols = getGbyColumns();
        int keys[] = JobGenHelper.variablesToFieldIndexes(gbyCols, inputSchemas[0]);
        GroupByOperator gby = (GroupByOperator) op;
        int numFds = gby.getDecorList().size();
        int fdColumns[] = new int[numFds];
        int j = 0;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gby.getDecorList()) {
            ILogicalExpression expr = p.second.getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                throw new AlgebricksException("Sort group-by expects variable references.");
            }
            VariableReferenceExpression v = (VariableReferenceExpression) expr;
            LogicalVariable decor = v.getVariableReference();
            fdColumns[j++] = inputSchemas[0].findVariable(decor);
        }

        if (gby.getNestedPlans().size() != 1) {
            throw new AlgebricksException(
                    "Sort group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        ILogicalPlan p0 = gby.getNestedPlans().get(0);
        if (p0.getRoots().size() != 1) {
            throw new AlgebricksException(
                    "Sort group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        Mutable<ILogicalOperator> r0 = p0.getRoots().get(0);
        AggregateOperator aggOp = (AggregateOperator) r0.getValue();

        IPartialAggregationTypeComputer partialAggregationTypeComputer = context.getPartialAggregationTypeComputer();
        List<Object> intermediateTypes = new ArrayList<Object>();
        int n = aggOp.getExpressions().size();
        IAggregateEvaluatorFactory[] aff = new IAggregateEvaluatorFactory[n];
        int i = 0;
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        IVariableTypeEnvironment aggOpInputEnv = context.getTypeEnvironment(aggOp.getInputs().get(0).getValue());
        IVariableTypeEnvironment outputEnv = context.getTypeEnvironment(op);
        for (Mutable<ILogicalExpression> exprRef : aggOp.getExpressions()) {
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) exprRef.getValue();
            aff[i++] = expressionRuntimeProvider.createAggregateFunctionFactory(aggFun, aggOpInputEnv, inputSchemas,
                    context);
            intermediateTypes.add(partialAggregationTypeComputer.getType(aggFun, aggOpInputEnv,
                    context.getMetadataProvider()));
        }

        int[] keyAndDecFields = new int[keys.length + fdColumns.length];
        for (i = 0; i < keys.length; ++i) {
            keyAndDecFields[i] = keys[i];
        }
        for (i = 0; i < fdColumns.length; i++) {
            keyAndDecFields[keys.length + i] = fdColumns[i];
        }

        List<LogicalVariable> keyAndDecVariables = new ArrayList<LogicalVariable>();
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gby.getGroupByList()) {
            keyAndDecVariables.add(p.first);
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gby.getDecorList()) {
            keyAndDecVariables.add(GroupByOperator.getDecorVariable(p));
        }

        for (LogicalVariable var : keyAndDecVariables) {
            aggOpInputEnv.setVarType(var, outputEnv.getVarType(var));
        }

        compileSubplans(inputSchemas[0], gby, opSchema, context);
        IOperatorDescriptorRegistry spec = builder.getJobSpec();

        IBinaryComparatorFactory[] compFactories = new IBinaryComparatorFactory[gbyCols.size()];
        IBinaryComparatorFactoryProvider bcfProvider = context.getBinaryComparatorFactoryProvider();
        i = 0;
        for (LogicalVariable v : gbyCols) {
            Object type = aggOpInputEnv.getVarType(v);
            if (orderColumns[i].getOrder() == OrderKind.ASC) {
                compFactories[i] = bcfProvider.getBinaryComparatorFactory(type, true);
            } else {
                compFactories[i] = bcfProvider.getBinaryComparatorFactory(type, false);
            }
            i++;
        }
        RecordDescriptor recordDescriptor = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema,
                context);

        IAggregateEvaluatorFactory[] merges = new IAggregateEvaluatorFactory[n];
        List<LogicalVariable> usedVars = new ArrayList<LogicalVariable>();
        IOperatorSchema[] localInputSchemas = new IOperatorSchema[1];
        localInputSchemas[0] = new OperatorSchemaImpl();
        for (i = 0; i < n; i++) {
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) aggOp.getMergeExpressions()
                    .get(i).getValue();
            aggFun.getUsedVariables(usedVars);
        }
        i = 0;
        for (Object type : intermediateTypes) {
            aggOpInputEnv.setVarType(usedVars.get(i++), type);
        }
        for (LogicalVariable keyVar : keyAndDecVariables) {
            localInputSchemas[0].addVariable(keyVar);
        }
        for (LogicalVariable usedVar : usedVars) {
            localInputSchemas[0].addVariable(usedVar);
        }
        for (i = 0; i < n; i++) {
            AggregateFunctionCallExpression mergeFun = (AggregateFunctionCallExpression) aggOp.getMergeExpressions()
                    .get(i).getValue();
            merges[i] = expressionRuntimeProvider.createAggregateFunctionFactory(mergeFun, aggOpInputEnv,
                    localInputSchemas, context);
        }
        RecordDescriptor partialAggRecordDescriptor = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op),
                localInputSchemas[0], context);

        IAggregatorDescriptorFactory aggregatorFactory = new SimpleAlgebricksAccumulatingAggregatorFactory(aff,
                keyAndDecFields);
        IAggregatorDescriptorFactory mergeFactory = new SimpleAlgebricksAccumulatingAggregatorFactory(merges,
                keyAndDecFields);

        INormalizedKeyComputerFactory normalizedKeyFactory = null;
        INormalizedKeyComputerFactoryProvider nkcfProvider = context.getNormalizedKeyComputerFactoryProvider();
        if (nkcfProvider == null) {
            normalizedKeyFactory = null;
        }
        Object type = aggOpInputEnv.getVarType(gbyCols.get(0));
        normalizedKeyFactory = orderColumns[0].getOrder() == OrderKind.ASC ? nkcfProvider
                .getNormalizedKeyComputerFactory(type, true) : nkcfProvider
                .getNormalizedKeyComputerFactory(type, false);
        SortGroupByOperatorDescriptor gbyOpDesc = new SortGroupByOperatorDescriptor(spec, frameLimit, keys,
                keyAndDecFields, normalizedKeyFactory, compFactories, aggregatorFactory, mergeFactory,
                partialAggRecordDescriptor, recordDescriptor, false);

        contributeOpDesc(builder, gby, gbyOpDesc);
        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    @Override
    public Pair<int[], int[]> getInputOutputDependencyLabels(ILogicalOperator op) {
        int[] inputDependencyLabels = new int[] { 0 };
        int[] outputDependencyLabels = new int[] { 1 };
        return new Pair<int[], int[]>(inputDependencyLabels, outputDependencyLabels);
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }
}
