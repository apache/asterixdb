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
import java.util.logging.Logger;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalGroupingProperty;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.join.HybridHashJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoinOperatorDescriptor;

public class HybridHashJoinPOperator extends AbstractHashJoinPOperator {

    // The maximum number of in-memory frames that this hash join can use.
    private final int memSizeInFrames;
    private final int maxInputBuildSizeInFrames;
    private final int aveRecordsPerFrame;
    private final double fudgeFactor;

    private static final Logger LOGGER = Logger.getLogger(HybridHashJoinPOperator.class.getName());

    public HybridHashJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeftOfEqualities, List<LogicalVariable> sideRightOfEqualities,
            int memSizeInFrames, int maxInputSizeInFrames, int aveRecordsPerFrame, double fudgeFactor) {
        super(kind, partitioningType, sideLeftOfEqualities, sideRightOfEqualities);
        this.memSizeInFrames = memSizeInFrames;
        this.maxInputBuildSizeInFrames = maxInputSizeInFrames;
        this.aveRecordsPerFrame = aveRecordsPerFrame;
        this.fudgeFactor = fudgeFactor;

        LOGGER.fine("HybridHashJoinPOperator constructed with: JoinKind=" + kind + ", JoinPartitioningType="
                + partitioningType + ", List<LogicalVariable>=" + sideLeftOfEqualities + ", List<LogicalVariable>="
                + sideRightOfEqualities + ", int memSizeInFrames=" + memSizeInFrames + ", int maxInputSize0InFrames="
                + maxInputSizeInFrames + ", int aveRecordsPerFrame=" + aveRecordsPerFrame + ", double fudgeFactor="
                + fudgeFactor + ".");
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

    public int getMemSizeInFrames() {
        return memSizeInFrames;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + " " + keysLeftBranch + keysRightBranch;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        int[] keysLeft = JobGenHelper.variablesToFieldIndexes(keysLeftBranch, inputSchemas[0]);
        int[] keysRight = JobGenHelper.variablesToFieldIndexes(keysRightBranch, inputSchemas[1]);
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        IBinaryHashFunctionFactory[] hashFunFactories = JobGenHelper
                .variablesToBinaryHashFunctionFactories(keysLeftBranch, env, context);
        IBinaryHashFunctionFamily[] hashFunFamilies = JobGenHelper.variablesToBinaryHashFunctionFamilies(keysLeftBranch,
                env, context);
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[keysLeft.length];
        int i = 0;
        IBinaryComparatorFactoryProvider bcfp = context.getBinaryComparatorFactoryProvider();
        for (LogicalVariable v : keysLeftBranch) {
            Object t = env.getVarType(v);
            comparatorFactories[i++] = bcfp.getBinaryComparatorFactory(t, true);
        }

        IPredicateEvaluatorFactoryProvider predEvaluatorFactoryProvider = context
                .getPredicateEvaluatorFactoryProvider();
        IPredicateEvaluatorFactory predEvaluatorFactory = predEvaluatorFactoryProvider == null ? null
                : predEvaluatorFactoryProvider.getPredicateEvaluatorFactory(keysLeft, keysRight);

        RecordDescriptor recDescriptor = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op),
                propagatedSchema, context);
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        IOperatorDescriptor opDesc;
        boolean optimizedHashJoin = true;
        for (IBinaryHashFunctionFamily family : hashFunFamilies) {
            if (family == null) {
                optimizedHashJoin = false;
                break;
            }
        }

        if (optimizedHashJoin) {
            opDesc = generateOptimizedHashJoinRuntime(context, inputSchemas, keysLeft, keysRight, hashFunFamilies,
                    comparatorFactories, predEvaluatorFactory, recDescriptor, spec);
        } else {
            opDesc = generateHashJoinRuntime(context, inputSchemas, keysLeft, keysRight, hashFunFactories,
                    comparatorFactories, predEvaluatorFactory, recDescriptor, spec);
        }
        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);

        ILogicalOperator src1 = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src1, 0, op, 0);
        ILogicalOperator src2 = op.getInputs().get(1).getValue();
        builder.contributeGraphEdge(src2, 0, op, 1);
    }

    private IOperatorDescriptor generateHashJoinRuntime(JobGenContext context, IOperatorSchema[] inputSchemas,
            int[] keysLeft, int[] keysRight, IBinaryHashFunctionFactory[] hashFunFactories,
            IBinaryComparatorFactory[] comparatorFactories, IPredicateEvaluatorFactory predEvaluatorFactory,
            RecordDescriptor recDescriptor, IOperatorDescriptorRegistry spec) throws AlgebricksException {
        IOperatorDescriptor opDesc;
        try {
            switch (kind) {
                case INNER:
                    opDesc = new HybridHashJoinOperatorDescriptor(spec, getMemSizeInFrames(), maxInputBuildSizeInFrames,
                            aveRecordsPerFrame, getFudgeFactor(), keysLeft, keysRight, hashFunFactories,
                            comparatorFactories, recDescriptor, predEvaluatorFactory, false, null);
                    break;
                case LEFT_OUTER:
                    IMissingWriterFactory[] nonMatchWriterFactories = new IMissingWriterFactory[inputSchemas[1]
                            .getSize()];
                    for (int j = 0; j < nonMatchWriterFactories.length; j++) {
                        nonMatchWriterFactories[j] = context.getMissingWriterFactory();
                    }
                    opDesc = new HybridHashJoinOperatorDescriptor(spec, getMemSizeInFrames(), maxInputBuildSizeInFrames,
                            aveRecordsPerFrame, getFudgeFactor(), keysLeft, keysRight, hashFunFactories,
                            comparatorFactories, recDescriptor, predEvaluatorFactory, true, nonMatchWriterFactories);
                    break;
                default:
                    throw new NotImplementedException();
            }
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        return opDesc;
    }

    private IOperatorDescriptor generateOptimizedHashJoinRuntime(JobGenContext context, IOperatorSchema[] inputSchemas,
            int[] keysLeft, int[] keysRight, IBinaryHashFunctionFamily[] hashFunFamilies,
            IBinaryComparatorFactory[] comparatorFactories, IPredicateEvaluatorFactory predEvaluatorFactory,
            RecordDescriptor recDescriptor, IOperatorDescriptorRegistry spec) throws AlgebricksException {
        IOperatorDescriptor opDesc;
        try {
            switch (kind) {
                case INNER:
                    opDesc = new OptimizedHybridHashJoinOperatorDescriptor(spec, getMemSizeInFrames(),
                            maxInputBuildSizeInFrames, getFudgeFactor(), keysLeft, keysRight, hashFunFamilies,
                            comparatorFactories, recDescriptor,
                            new JoinMultiComparatorFactory(comparatorFactories, keysLeft, keysRight),
                            new JoinMultiComparatorFactory(comparatorFactories, keysRight, keysLeft),
                            predEvaluatorFactory);
                    break;
                case LEFT_OUTER:
                    IMissingWriterFactory[] nonMatchWriterFactories = new IMissingWriterFactory[inputSchemas[1]
                            .getSize()];
                    for (int j = 0; j < nonMatchWriterFactories.length; j++) {
                        nonMatchWriterFactories[j] = context.getMissingWriterFactory();
                    }
                    opDesc = new OptimizedHybridHashJoinOperatorDescriptor(spec, getMemSizeInFrames(),
                            maxInputBuildSizeInFrames, getFudgeFactor(), keysLeft, keysRight, hashFunFamilies,
                            comparatorFactories, recDescriptor,
                            new JoinMultiComparatorFactory(comparatorFactories, keysLeft, keysRight),
                            new JoinMultiComparatorFactory(comparatorFactories, keysRight, keysLeft),
                            predEvaluatorFactory, true, nonMatchWriterFactories);
                    break;
                default:
                    throw new NotImplementedException();
            }
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        return opDesc;
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

/**
 * {@ ITuplePairComparatorFactory} implementation for optimized hybrid hash join.
 */
class JoinMultiComparatorFactory implements ITuplePairComparatorFactory {
    private static final long serialVersionUID = 1L;

    private final IBinaryComparatorFactory[] binaryComparatorFactories;
    private final int[] keysLeft;
    private final int[] keysRight;

    public JoinMultiComparatorFactory(IBinaryComparatorFactory[] binaryComparatorFactory, int[] keysLeft,
            int[] keysRight) {
        this.binaryComparatorFactories = binaryComparatorFactory;
        this.keysLeft = keysLeft;
        this.keysRight = keysRight;
    }

    @Override
    public ITuplePairComparator createTuplePairComparator(IHyracksTaskContext ctx) {
        IBinaryComparator[] binaryComparators = new IBinaryComparator[binaryComparatorFactories.length];
        for (int i = 0; i < binaryComparators.length; i++) {
            binaryComparators[i] = binaryComparatorFactories[i].createBinaryComparator();
        }
        return new JoinMultiComparator(binaryComparators, keysLeft, keysRight);
    }
}

/**
 * {@ ITuplePairComparator} implementation for optimized hybrid hash join.
 * The comparator applies multiple binary comparators, one for each key pairs
 */
class JoinMultiComparator implements ITuplePairComparator {
    private final IBinaryComparator[] binaryComparators;
    private final int[] keysLeft;
    private final int[] keysRight;

    public JoinMultiComparator(IBinaryComparator[] bComparator, int[] keysLeft, int[] keysRight) {
        this.binaryComparators = bComparator;
        this.keysLeft = keysLeft;
        this.keysRight = keysRight;
    }

    @Override
    public int compare(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        int tStart0 = accessor0.getTupleStartOffset(tIndex0);
        int fStartOffset0 = accessor0.getFieldSlotsLength() + tStart0;

        int tStart1 = accessor1.getTupleStartOffset(tIndex1);
        int fStartOffset1 = accessor1.getFieldSlotsLength() + tStart1;

        for (int i = 0; i < binaryComparators.length; i++) {
            int fStart0 = accessor0.getFieldStartOffset(tIndex0, keysLeft[i]);
            int fEnd0 = accessor0.getFieldEndOffset(tIndex0, keysLeft[i]);
            int fLen0 = fEnd0 - fStart0;

            int fStart1 = accessor1.getFieldStartOffset(tIndex1, keysRight[i]);
            int fEnd1 = accessor1.getFieldEndOffset(tIndex1, keysRight[i]);
            int fLen1 = fEnd1 - fStart1;

            int c = binaryComparators[i].compare(accessor0.getBuffer().array(), fStart0 + fStartOffset0, fLen0,
                    accessor1.getBuffer().array(), fStart1 + fStartOffset1, fLen1);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }
}
