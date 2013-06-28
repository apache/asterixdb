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

import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.join.HybridHashJoinOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.join.OptimizedHybridHashJoinOperatorDescriptor;

public class HybridHashJoinPOperator extends AbstractHashJoinPOperator {

    private final int memSizeInFrames;
    private final int maxInputBuildSizeInFrames;
    private final int aveRecordsPerFrame;
    private final double fudgeFactor;

    public HybridHashJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeftOfEqualities, List<LogicalVariable> sideRightOfEqualities,
            int memSizeInFrames, int maxInputSize0InFrames, int aveRecordsPerFrame, double fudgeFactor) {
        super(kind, partitioningType, sideLeftOfEqualities, sideRightOfEqualities);
        this.memSizeInFrames = memSizeInFrames;
        this.maxInputBuildSizeInFrames = maxInputSize0InFrames;
        this.aveRecordsPerFrame = aveRecordsPerFrame;
        this.fudgeFactor = fudgeFactor;
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
        IBinaryHashFunctionFactory[] hashFunFactories = JobGenHelper.variablesToBinaryHashFunctionFactories(
                keysLeftBranch, env, context);
        IBinaryHashFunctionFamily[] hashFunFamilies = JobGenHelper.variablesToBinaryHashFunctionFamilies(
                keysLeftBranch, env, context);
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[keysLeft.length];
        int i = 0;
        IBinaryComparatorFactoryProvider bcfp = context.getBinaryComparatorFactoryProvider();
        for (LogicalVariable v : keysLeftBranch) {
            Object t = env.getVarType(v);
            comparatorFactories[i++] = bcfp.getBinaryComparatorFactory(t, true);
        }
        
        IPredicateEvaluatorFactoryProvider predEvaluatorFactoryProvider = context.getPredicateEvaluatorFactoryProvider();
        IPredicateEvaluatorFactory predEvaluatorFactory = ( predEvaluatorFactoryProvider == null ? null : predEvaluatorFactoryProvider.getPredicateEvaluatorFactory(keysLeft, keysRight));
        
        RecordDescriptor recDescriptor = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op),
                propagatedSchema, context);
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        IOperatorDescriptor opDesc = null;

        boolean optimizedHashJoin = true;
        for (IBinaryHashFunctionFamily family : hashFunFamilies) {
            if (family == null) {
                optimizedHashJoin = false;
                break;
            }
        }

        if (!optimizedHashJoin) {
            try {
                switch (kind) {
                    case INNER: {
                        opDesc = new HybridHashJoinOperatorDescriptor(spec, getMemSizeInFrames(),
                                maxInputBuildSizeInFrames, aveRecordsPerFrame, getFudgeFactor(), keysLeft, keysRight,
                                hashFunFactories, comparatorFactories, recDescriptor, predEvaluatorFactory);
                        break;
                    }
                    case LEFT_OUTER: {
                        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[inputSchemas[1].getSize()];
                        for (int j = 0; j < nullWriterFactories.length; j++) {
                            nullWriterFactories[j] = context.getNullWriterFactory();
                        }
                        opDesc = new HybridHashJoinOperatorDescriptor(spec, getMemSizeInFrames(),
                                maxInputBuildSizeInFrames, aveRecordsPerFrame, getFudgeFactor(), keysLeft, keysRight,
                                hashFunFactories, comparatorFactories, recDescriptor, predEvaluatorFactory, true, nullWriterFactories);
                        break;
                    }
                    default: {
                        throw new NotImplementedException();
                    }
                }
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
        } else {
            try {
                switch (kind) {
                    case INNER: {
                        opDesc = new OptimizedHybridHashJoinOperatorDescriptor(spec, getMemSizeInFrames(),
                                maxInputBuildSizeInFrames, getFudgeFactor(), keysLeft, keysRight, hashFunFamilies,
                                comparatorFactories, recDescriptor, new JoinMultiComparatorFactory(comparatorFactories,
                                        keysLeft, keysRight), new JoinMultiComparatorFactory(comparatorFactories,
                                        keysRight, keysLeft), predEvaluatorFactory);
                        break;
                    }
                    case LEFT_OUTER: {
                        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[inputSchemas[1].getSize()];
                        for (int j = 0; j < nullWriterFactories.length; j++) {
                            nullWriterFactories[j] = context.getNullWriterFactory();
                        }
                        opDesc = new OptimizedHybridHashJoinOperatorDescriptor(spec, getMemSizeInFrames(),
                                maxInputBuildSizeInFrames, getFudgeFactor(), keysLeft, keysRight, hashFunFamilies,
                                comparatorFactories, recDescriptor, new JoinMultiComparatorFactory(comparatorFactories,
                                        keysLeft, keysRight), new JoinMultiComparatorFactory(comparatorFactories,
                                        keysRight, keysLeft), predEvaluatorFactory, true, nullWriterFactories);
                        break;
                    }
                    default: {
                        throw new NotImplementedException();
                    }
                }
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
        }
        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);

        ILogicalOperator src1 = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src1, 0, op, 0);
        ILogicalOperator src2 = op.getInputs().get(1).getValue();
        builder.contributeGraphEdge(src2, 0, op, 1);
    }

    @Override
    protected List<ILocalStructuralProperty> deliveredLocalProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        return new LinkedList<ILocalStructuralProperty>();
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
    public int compare(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1) {
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
