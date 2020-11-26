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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.IPartialAggregationTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalGroupingProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.core.jobgen.impl.OperatorSchemaImpl;
import org.apache.hyracks.algebricks.runtime.base.ISerializedAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.SerializableAggregatorDescriptorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.group.AbstractAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.HashSpillableTableFactory;
import org.apache.hyracks.dataflow.std.group.external.ExternalGroupOperatorDescriptor;

public class ExternalGroupByPOperator extends AbstractGroupByPOperator {

    public ExternalGroupByPOperator(List<LogicalVariable> columnList) {
        super(columnList);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.EXTERNAL_GROUP_BY;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        List<ILocalStructuralProperty> propsLocal = new LinkedList<ILocalStructuralProperty>();

        GroupByOperator gOp = (GroupByOperator) op;
        Set<LogicalVariable> columnSet = new ListSet<LogicalVariable>();

        if (!columnSet.isEmpty()) {
            propsLocal.add(new LocalGroupingProperty(columnSet));
        }
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
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        AbstractLogicalOperator aop = (AbstractLogicalOperator) op;
        if (aop.getExecutionMode() == ExecutionMode.PARTITIONED) {
            StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
            pv[0] = new StructuralPropertiesVector(new UnorderedPartitionedProperty(
                    new ListSet<LogicalVariable>(columnList), context.getComputationNodeDomain()), null);
            return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
        } else {
            return emptyUnaryRequirements();
        }
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        GroupByOperator gby = (GroupByOperator) op;
        checkGroupAll(gby);
        List<LogicalVariable> gbyCols = getGroupByColumns();
        int[] gbyColumns = JobGenHelper.variablesToFieldIndexes(gbyCols, inputSchemas[0]);
        int[] fdColumns = getFdColumns(gby, inputSchemas[0]);

        if (gby.getNestedPlans().size() != 1) {
            throw new AlgebricksException(
                    "External group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        ILogicalPlan p0 = gby.getNestedPlans().get(0);
        if (p0.getRoots().size() != 1) {
            throw new AlgebricksException(
                    "External group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        Mutable<ILogicalOperator> r0 = p0.getRoots().get(0);
        AggregateOperator aggOp = (AggregateOperator) r0.getValue();

        compileSubplans(inputSchemas[0], gby, opSchema, context);

        IPartialAggregationTypeComputer partialAggregationTypeComputer = context.getPartialAggregationTypeComputer();
        List<Object> intermediateTypes = new ArrayList<Object>();
        int n = aggOp.getExpressions().size();
        ISerializedAggregateEvaluatorFactory[] aff = new ISerializedAggregateEvaluatorFactory[n];
        int i = 0;
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        ILogicalOperator aggOpInput = aggOp.getInputs().get(0).getValue();
        IOperatorSchema aggOpInputSchema = context.getSchema(aggOpInput);
        IOperatorSchema[] aggOpInputSchemas = new IOperatorSchema[] { aggOpInputSchema };
        IVariableTypeEnvironment aggOpInputEnv = context.getTypeEnvironment(aggOpInput);
        IVariableTypeEnvironment outputEnv = context.getTypeEnvironment(op);
        for (Mutable<ILogicalExpression> exprRef : aggOp.getExpressions()) {
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) exprRef.getValue();
            aff[i++] = expressionRuntimeProvider.createSerializableAggregateFunctionFactory(aggFun, aggOpInputEnv,
                    aggOpInputSchemas, context);
            intermediateTypes
                    .add(partialAggregationTypeComputer.getType(aggFun, aggOpInputEnv, context.getMetadataProvider()));
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

        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        IBinaryComparatorFactory[] comparatorFactories =
                JobGenHelper.variablesToAscBinaryComparatorFactories(gbyCols, aggOpInputEnv, context);
        RecordDescriptor recordDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);
        IBinaryHashFunctionFamily[] hashFunctionFactories =
                JobGenHelper.variablesToBinaryHashFunctionFamilies(gbyCols, aggOpInputEnv, context);

        ISerializedAggregateEvaluatorFactory[] merges = new ISerializedAggregateEvaluatorFactory[n];
        List<LogicalVariable> usedVars = new ArrayList<LogicalVariable>();
        IOperatorSchema[] localInputSchemas = new IOperatorSchema[1];
        localInputSchemas[0] = new OperatorSchemaImpl();
        for (i = 0; i < n; i++) {
            AggregateFunctionCallExpression aggFun =
                    (AggregateFunctionCallExpression) aggOp.getMergeExpressions().get(i).getValue();
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
            AggregateFunctionCallExpression mergeFun =
                    (AggregateFunctionCallExpression) aggOp.getMergeExpressions().get(i).getValue();
            merges[i] = expressionRuntimeProvider.createSerializableAggregateFunctionFactory(mergeFun, aggOpInputEnv,
                    localInputSchemas, context);
        }
        AbstractAggregatorDescriptorFactory aggregatorFactory = new SerializableAggregatorDescriptorFactory(aff);
        aggregatorFactory.setSourceLocation(gby.getSourceLocation());
        AbstractAggregatorDescriptorFactory mergeFactory = new SerializableAggregatorDescriptorFactory(merges);
        mergeFactory.setSourceLocation(gby.getSourceLocation());

        INormalizedKeyComputerFactory normalizedKeyFactory =
                JobGenHelper.variablesToAscNormalizedKeyComputerFactory(gbyCols, aggOpInputEnv, context);

        // Calculates the hash table size (# of unique hash values) based on the budget and a tuple size.
        int frameSize = context.getFrameSize();
        long memoryBudgetInBytes = localMemoryRequirements.getMemoryBudgetInBytes(frameSize);
        int allColumns = gbyColumns.length + fdColumns.length;
        int hashTableSize = ExternalGroupOperatorDescriptor.calculateGroupByTableCardinality(memoryBudgetInBytes,
                allColumns, frameSize);

        int framesLimit = localMemoryRequirements.getMemoryBudgetInFrames();
        long inputSize = framesLimit * (long) frameSize;
        ExternalGroupOperatorDescriptor gbyOpDesc = new ExternalGroupOperatorDescriptor(spec, hashTableSize, inputSize,
                gbyColumns, fdColumns, framesLimit, comparatorFactories, normalizedKeyFactory, aggregatorFactory,
                mergeFactory, recordDescriptor, recordDescriptor, new HashSpillableTableFactory(hashFunctionFactories));
        gbyOpDesc.setSourceLocation(gby.getSourceLocation());
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
}
