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
package org.apache.asterix.algebra.operators.physical;

import java.util.HashSet;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.SpatialPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.TuplePairEvaluatorFactory;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.join.NestedLoopJoinOperatorDescriptor;

public class SpatialJoinPOperator extends AbstractJoinPOperator {
    private final List<LogicalVariable> keysLeftBranch;
    private final List<LogicalVariable> keysRightBranch;

    public SpatialJoinPOperator(AbstractBinaryJoinOperator.JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> keysLeftBranch, List<LogicalVariable> keysRightBranch) {
        super(kind, partitioningType);
        this.keysLeftBranch = keysLeftBranch;
        this.keysRightBranch = keysRightBranch;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.SPATIAL_JOIN;
    }

    @Override
    public String toString() {
        return "SPATIAL_JOIN" + " " + keysLeftBranch + " " + keysRightBranch;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator iop,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) throws AlgebricksException {
        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[2];
        AbstractLogicalOperator op = (AbstractLogicalOperator) iop;
        INodeDomain targetNodeDomain = context.getComputationNodeDomain();

        IPartitioningProperty ppLeft = null;
        IPartitioningProperty ppRight = null;

        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
            ppLeft = new SpatialPartitionedProperty(new HashSet<>(keysLeftBranch), targetNodeDomain);
            ppRight = new SpatialPartitionedProperty(new HashSet<>(keysRightBranch), targetNodeDomain);
        }

        //        List<ILocalStructuralProperty> ispLeft = new ArrayList<>();
        //        List<ILocalStructuralProperty> ispRight = new ArrayList<>();

        pv[0] = new StructuralPropertiesVector(ppLeft, null);
        pv[1] = new StructuralPropertiesVector(ppRight, null);
        IPartitioningRequirementsCoordinator prc = IPartitioningRequirementsCoordinator.NO_COORDINATION;

        return new PhysicalRequirements(pv, prc);
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        IPartitioningProperty pp =
                new SpatialPartitionedProperty(new ListSet<>(keysLeftBranch), context.getComputationNodeDomain());
        deliveredProperties = new StructuralPropertiesVector(pp, null);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) op;
        RecordDescriptor recDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);
        IOperatorSchema[] conditionInputSchemas = new IOperatorSchema[1];
        conditionInputSchemas[0] = propagatedSchema;
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        IScalarEvaluatorFactory cond = expressionRuntimeProvider.createEvaluatorFactory(join.getCondition().getValue(),
                context.getTypeEnvironment(op), conditionInputSchemas, context);
        ITuplePairComparatorFactory comparatorFactory =
                new TuplePairEvaluatorFactory(cond, false, context.getBinaryBooleanInspectorFactory());
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        IOperatorDescriptor opDesc;

        int memSize = localMemoryRequirements.getMemoryBudgetInFrames();
        switch (kind) {
            case INNER:
                opDesc = new NestedLoopJoinOperatorDescriptor(spec, comparatorFactory, recDescriptor, memSize, false,
                        null);
                break;
            case LEFT_OUTER:
                IMissingWriterFactory[] nonMatchWriterFactories = new IMissingWriterFactory[inputSchemas[1].getSize()];
                for (int j = 0; j < nonMatchWriterFactories.length; j++) {
                    nonMatchWriterFactories[j] = context.getMissingWriterFactory();
                }
                opDesc = new NestedLoopJoinOperatorDescriptor(spec, comparatorFactory, recDescriptor, memSize, true,
                        nonMatchWriterFactories);
                break;
            default:
                throw new NotImplementedException();
        }

        opDesc.setSourceLocation(join.getSourceLocation());
        contributeOpDesc(builder, join, opDesc);

        ILogicalOperator src1 = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src1, 0, op, 0);
        ILogicalOperator src2 = op.getInputs().get(1).getValue();
        builder.contributeGraphEdge(src2, 0, op, 1);
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }
}
