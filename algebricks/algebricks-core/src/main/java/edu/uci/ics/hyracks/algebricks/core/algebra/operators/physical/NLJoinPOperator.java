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
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspector;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.join.NestedLoopJoinOperatorDescriptor;

/**
 * <<<<<<< .working
 * Left input is broadcast and preserves its local properties. Right input can
 * be partitioned in any way.
 * =======
 * Left input is broadcast and preserves its local properties.
 * Right input can be partitioned in any way.
 * >>>>>>> .merge-right.r3014
 */
public class NLJoinPOperator extends AbstractJoinPOperator {

    private final int memSize;

    public NLJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType, int memSize) {
        super(kind, partitioningType);
        this.memSize = memSize;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.NESTED_LOOP;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator iop, IOptimizationContext context) {
        if (partitioningType != JoinPartitioningType.BROADCAST) {
            throw new NotImplementedException(partitioningType + " nested loop joins are not implemented.");
        }

        IPartitioningProperty pp;

        AbstractLogicalOperator op = (AbstractLogicalOperator) iop;

        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
            AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(1).getValue();
            IPhysicalPropertiesVector pv1 = op2.getPhysicalOperator().getDeliveredProperties();
            if (pv1 == null) {
                pp = null;
            } else {
                pp = pv1.getPartitioningProperty();
            }
        } else {
            pp = IPartitioningProperty.UNPARTITIONED;
        }

        List<ILocalStructuralProperty> localProps = new LinkedList<ILocalStructuralProperty>();
        this.deliveredProperties = new StructuralPropertiesVector(pp, localProps);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        if (partitioningType != JoinPartitioningType.BROADCAST) {
            throw new NotImplementedException(partitioningType + " nested loop joins are not implemented.");
        }

        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[2];
        pv[0] = new StructuralPropertiesVector(new BroadcastPartitioningProperty(null), null);
        pv[1] = new StructuralPropertiesVector(null, null);
        return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) op;
        RecordDescriptor recDescriptor = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op),
                propagatedSchema, context);
        IOperatorSchema[] conditionInputSchemas = new IOperatorSchema[1];
        conditionInputSchemas[0] = propagatedSchema;
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        IScalarEvaluatorFactory cond = expressionRuntimeProvider.createEvaluatorFactory(join.getCondition().getValue(),
                context.getTypeEnvironment(op), conditionInputSchemas, context);
        ITuplePairComparatorFactory comparatorFactory = new TuplePairEvaluatorFactory(cond,
                context.getBinaryBooleanInspectorFactory());
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        IOperatorDescriptor opDesc = null;

        switch (kind) {
            case INNER: {
                opDesc = new NestedLoopJoinOperatorDescriptor(spec, comparatorFactory, recDescriptor, memSize, false,
                        null);
                break;
            }
            case LEFT_OUTER: {
                INullWriterFactory[] nullWriterFactories = new INullWriterFactory[inputSchemas[1].getSize()];
                for (int j = 0; j < nullWriterFactories.length; j++) {
                    nullWriterFactories[j] = context.getNullWriterFactory();
                }
                opDesc = new NestedLoopJoinOperatorDescriptor(spec, comparatorFactory, recDescriptor, memSize, true,
                        nullWriterFactories);
                break;
            }
            default: {
                throw new NotImplementedException();
            }
        }
        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);

        ILogicalOperator src1 = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src1, 0, op, 0);
        ILogicalOperator src2 = op.getInputs().get(1).getValue();
        builder.contributeGraphEdge(src2, 0, op, 1);
    }

    public static class TuplePairEvaluatorFactory implements ITuplePairComparatorFactory {

        private static final long serialVersionUID = 1L;
        private final IScalarEvaluatorFactory cond;
        private final IBinaryBooleanInspectorFactory binaryBooleanInspectorFactory;

        public TuplePairEvaluatorFactory(IScalarEvaluatorFactory cond,
                IBinaryBooleanInspectorFactory binaryBooleanInspectorFactory) {
            this.cond = cond;
            this.binaryBooleanInspectorFactory = binaryBooleanInspectorFactory;
        }

        @Override
        public synchronized ITuplePairComparator createTuplePairComparator(IHyracksTaskContext ctx) {
            return new TuplePairEvaluator(ctx, cond, binaryBooleanInspectorFactory.createBinaryBooleanInspector(ctx));
        }
    }

    public static class TuplePairEvaluator implements ITuplePairComparator {
        private final IHyracksTaskContext ctx;
        private IScalarEvaluator condEvaluator;
        private final IScalarEvaluatorFactory condFactory;
        private final IPointable p;
        private final CompositeFrameTupleReference compositeTupleRef;
        private final FrameTupleReference leftRef;
        private final FrameTupleReference rightRef;
        private final IBinaryBooleanInspector binaryBooleanInspector;

        public TuplePairEvaluator(IHyracksTaskContext ctx, IScalarEvaluatorFactory condFactory,
                IBinaryBooleanInspector binaryBooleanInspector) {
            this.ctx = ctx;
            this.condFactory = condFactory;
            this.binaryBooleanInspector = binaryBooleanInspector;
            this.leftRef = new FrameTupleReference();
            this.p = VoidPointable.FACTORY.createPointable();
            this.rightRef = new FrameTupleReference();
            this.compositeTupleRef = new CompositeFrameTupleReference(leftRef, rightRef);
        }

        @Override
        public int compare(IFrameTupleAccessor outerAccessor, int outerIndex, IFrameTupleAccessor innerAccessor,
                int innerIndex) throws HyracksDataException {
            if (condEvaluator == null) {
                try {
                    this.condEvaluator = condFactory.createScalarEvaluator(ctx);
                } catch (AlgebricksException ae) {
                    throw new HyracksDataException(ae);
                }
            }
            compositeTupleRef.reset(outerAccessor, outerIndex, innerAccessor, innerIndex);
            try {
                condEvaluator.evaluate(compositeTupleRef, p);
            } catch (AlgebricksException ae) {
                throw new HyracksDataException(ae);
            }
            boolean result = binaryBooleanInspector
                    .getBooleanValue(p.getByteArray(), p.getStartOffset(), p.getLength());
            if (result)
                return 0;
            else
                return 1;
        }
    }

    public static class CompositeFrameTupleReference implements IFrameTupleReference {

        private final FrameTupleReference refLeft;
        private final FrameTupleReference refRight;

        public CompositeFrameTupleReference(FrameTupleReference refLeft, FrameTupleReference refRight) {
            this.refLeft = refLeft;
            this.refRight = refRight;
        }

        public void reset(IFrameTupleAccessor outerAccessor, int outerIndex, IFrameTupleAccessor innerAccessor,
                int innerIndex) {
            refLeft.reset(outerAccessor, outerIndex);
            refRight.reset(innerAccessor, innerIndex);
        }

        @Override
        public int getFieldCount() {
            return refLeft.getFieldCount() + refRight.getFieldCount();
        }

        @Override
        public byte[] getFieldData(int fIdx) {
            int leftFieldCount = refLeft.getFieldCount();
            if (fIdx < leftFieldCount)
                return refLeft.getFieldData(fIdx);
            else
                return refRight.getFieldData(fIdx - leftFieldCount);
        }

        @Override
        public int getFieldStart(int fIdx) {
            int leftFieldCount = refLeft.getFieldCount();
            if (fIdx < leftFieldCount)
                return refLeft.getFieldStart(fIdx);
            else
                return refRight.getFieldStart(fIdx - leftFieldCount);
        }

        @Override
        public int getFieldLength(int fIdx) {
            int leftFieldCount = refLeft.getFieldCount();
            if (fIdx < leftFieldCount)
                return refLeft.getFieldLength(fIdx);
            else
                return refRight.getFieldLength(fIdx - leftFieldCount);
        }

        @Override
        public IFrameTupleAccessor getFrameTupleAccessor() {
            throw new NotImplementedException();
        }

        @Override
        public int getTupleIndex() {
            throw new NotImplementedException();
        }

    }
}