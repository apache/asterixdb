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
import java.util.List;
import java.util.Set;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalGroupingProperty;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.TuplePairEvaluatorFactory;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoinOperatorDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HybridHashJoinPOperator extends AbstractHashJoinPOperator {

    private final int maxInputBuildSizeInFrames;
    private final double fudgeFactor;

    private static final Logger LOGGER = LogManager.getLogger();

    public HybridHashJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeftOfEqualities, List<LogicalVariable> sideRightOfEqualities,
            int maxInputSizeInFrames, int aveRecordsPerFrame, double fudgeFactor) {
        super(kind, partitioningType, sideLeftOfEqualities, sideRightOfEqualities);
        this.maxInputBuildSizeInFrames = maxInputSizeInFrames;
        this.fudgeFactor = fudgeFactor;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("HybridHashJoinPOperator constructed with: JoinKind=" + kind + ", JoinPartitioningType="
                    + partitioningType + ", List<LogicalVariable>=" + sideLeftOfEqualities + ", List<LogicalVariable>="
                    + sideRightOfEqualities + ", int maxInputSize0InFrames=" + maxInputSizeInFrames
                    + ", int aveRecordsPerFrame=" + aveRecordsPerFrame + ", double fudgeFactor=" + fudgeFactor + ".");
        }
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.HYBRID_HASH_JOIN;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    public double getFudgeFactor() {
        return fudgeFactor;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + " " + keysLeftBranch + keysRightBranch;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        validateNumKeys(keysLeftBranch, keysRightBranch);
        int[] keysLeft = JobGenHelper.variablesToFieldIndexes(keysLeftBranch, inputSchemas[0]);
        int[] keysRight = JobGenHelper.variablesToFieldIndexes(keysRightBranch, inputSchemas[1]);
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        IBinaryHashFunctionFamily[] leftHashFunFamilies =
                JobGenHelper.variablesToBinaryHashFunctionFamilies(keysLeftBranch, env, context);
        IBinaryHashFunctionFamily[] rightHashFunFamilies =
                JobGenHelper.variablesToBinaryHashFunctionFamilies(keysRightBranch, env, context);

        IPredicateEvaluatorFactoryProvider predEvaluatorFactoryProvider =
                context.getPredicateEvaluatorFactoryProvider();
        IPredicateEvaluatorFactory predEvaluatorFactory = predEvaluatorFactoryProvider == null ? null
                : predEvaluatorFactoryProvider.getPredicateEvaluatorFactory(keysLeft, keysRight);

        RecordDescriptor recDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);
        IOperatorSchema[] conditionInputSchemas = new IOperatorSchema[1];
        conditionInputSchemas[0] = propagatedSchema;
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        IScalarEvaluatorFactory cond = expressionRuntimeProvider.createEvaluatorFactory(
                joinOp.getCondition().getValue(), context.getTypeEnvironment(op), conditionInputSchemas, context);
        ITuplePairComparatorFactory comparatorFactory =
                new TuplePairEvaluatorFactory(cond, false, context.getBinaryBooleanInspectorFactory());
        ITuplePairComparatorFactory reverseComparatorFactory =
                new TuplePairEvaluatorFactory(cond, true, context.getBinaryBooleanInspectorFactory());
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        IOperatorDescriptor opDesc;

        opDesc = generateOptimizedHashJoinRuntime(context, inputSchemas, keysLeft, keysRight, leftHashFunFamilies,
                rightHashFunFamilies, comparatorFactory, reverseComparatorFactory, predEvaluatorFactory, recDescriptor,
                spec);
        opDesc.setSourceLocation(op.getSourceLocation());
        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);

        ILogicalOperator src1 = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src1, 0, op, 0);
        ILogicalOperator src2 = op.getInputs().get(1).getValue();
        builder.contributeGraphEdge(src2, 0, op, 1);
    }

    private IOperatorDescriptor generateOptimizedHashJoinRuntime(JobGenContext context, IOperatorSchema[] inputSchemas,
            int[] keysLeft, int[] keysRight, IBinaryHashFunctionFamily[] leftHashFunFamilies,
            IBinaryHashFunctionFamily[] rightHashFunFamilies, ITuplePairComparatorFactory comparatorFactory,
            ITuplePairComparatorFactory reverseComparatorFactory, IPredicateEvaluatorFactory predEvaluatorFactory,
            RecordDescriptor recDescriptor, IOperatorDescriptorRegistry spec) {
        int memSizeInFrames = localMemoryRequirements.getMemoryBudgetInFrames();
        switch (kind) {
            case INNER:
                return new OptimizedHybridHashJoinOperatorDescriptor(spec, memSizeInFrames, maxInputBuildSizeInFrames,
                        getFudgeFactor(), keysLeft, keysRight, leftHashFunFamilies, rightHashFunFamilies, recDescriptor,
                        comparatorFactory, reverseComparatorFactory, predEvaluatorFactory);
            case LEFT_OUTER:
                IMissingWriterFactory[] nonMatchWriterFactories = new IMissingWriterFactory[inputSchemas[1].getSize()];
                for (int j = 0; j < nonMatchWriterFactories.length; j++) {
                    nonMatchWriterFactories[j] = context.getMissingWriterFactory();
                }
                return new OptimizedHybridHashJoinOperatorDescriptor(spec, memSizeInFrames, maxInputBuildSizeInFrames,
                        getFudgeFactor(), keysLeft, keysRight, leftHashFunFamilies, rightHashFunFamilies, recDescriptor,
                        comparatorFactory, reverseComparatorFactory, predEvaluatorFactory, true,
                        nonMatchWriterFactories);
            default:
                throw new NotImplementedException();
        }
    }

    @Override
    protected List<ILocalStructuralProperty> deliveredLocalProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        List<ILocalStructuralProperty> deliveredLocalProperties = new ArrayList<>();
        // Inner join can kick off the "role reversal" optimization, which can kill data properties for the probe side.
        if (kind != JoinKind.LEFT_OUTER) {
            return deliveredLocalProperties;
        }
        AbstractLogicalOperator probeOp = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        IPhysicalPropertiesVector probeSideProperties = probeOp.getPhysicalOperator().getDeliveredProperties();
        List<ILocalStructuralProperty> probeSideLocalProperties = probeSideProperties.getLocalProperties();
        if (probeSideLocalProperties != null) {
            // The local grouping property in the probe side will be maintained
            // and the local ordering property in the probe side will be turned into a local grouping property
            // if the grouping variables (or sort columns) in the local property contain all the join key variables
            // for the left branch:
            // 1. in case spilling is not kicked off, the ordering property is maintained and hence local grouping
            // property is maintained.
            // 2. if spilling is kicked off, the grouping property is still maintained though the ordering property
            // is destroyed.
            for (ILocalStructuralProperty property : probeSideLocalProperties) {
                Set<LogicalVariable> groupingVars = new ListSet<>();
                Set<LogicalVariable> leftBranchVars = new ListSet<>();
                property.getVariables(groupingVars);
                leftBranchVars.addAll(getKeysLeftBranch());
                if (groupingVars.containsAll(leftBranchVars)) {
                    deliveredLocalProperties.add(new LocalGroupingProperty(groupingVars));
                }
            }
        }
        return deliveredLocalProperties;
    }
}
