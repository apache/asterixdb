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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractForwardOperatorDescriptor;

/**
 * <pre>
 * {@see {@link ForwardOperator} and {@link AbstractForwardOperatorDescriptor}}
 * idx0: Input data source    --
 *                              |-- forward op.
 * idx1: Side activity output --
 * </pre>
 */
public abstract class AbstractForwardPOperator extends AbstractPhysicalOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.FORWARD;
    }

    /**
     * Get the correct Forward Operator Descriptor
     * @param builder Hyracks job builder
     * @param forwardOp Forward Operator
     * @param  dataInputDescriptor Data input descriptor
     * @return return the correct operator descriptor
     */
    public abstract AbstractForwardOperatorDescriptor getOperatorDescriptor(IHyracksJobBuilder builder,
            ForwardOperator forwardOp, RecordDescriptor dataInputDescriptor);

    /**
     * Forward operator requires that the global aggregate operator broadcasts side activity output.
     * No required properties at the data source input.
     * @param op {@see {@link ForwardOperator}}
     * @param requiredByParent parent's requirements, which are not enforced for now, as we only explore one plan
     * @param context the optimization context
     * @return broadcast requirement at input 1; empty requirements at input 0; No coordination between the two.
     */
    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector requiredByParent, IOptimizationContext context) {
        // broadcast the side activity output to the cluster node domain
        INodeDomain targetDomain = context.getComputationNodeDomain();
        List<ILocalStructuralProperty> noProp = new ArrayList<>();
        StructuralPropertiesVector[] requiredAtInputs = new StructuralPropertiesVector[2];
        requiredAtInputs[0] = StructuralPropertiesVector.EMPTY_PROPERTIES_VECTOR;
        requiredAtInputs[1] = new StructuralPropertiesVector(new BroadcastPartitioningProperty(targetDomain), noProp);
        return new PhysicalRequirements(requiredAtInputs, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    /**
     * Forward operator delivers whatever properties delivered by the input located at index = 0 (tuples source op).
     * Subtree at index 0 must compute its delivered properties before any call to this method
     * @param op forward logical operator
     * @param context {@link IOptimizationContext}
     * @throws AlgebricksException
     */
    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator dataSourceOperator = op.getInputs().get(0).getValue();
        deliveredProperties = dataSourceOperator.getDeliveredPhysicalProperties().clone();
    }

    /**
     * The output record descriptor of forward operator is the same as the output record descriptor of the data source
     * which is located at index 0.
     * @param builder Hyracks job builder
     * @param context job generation context
     * @param op {@see {@link ForwardOperator}}
     * @param propagatedSchema not used
     * @param inputSchemas schemas of all inputs
     * @param outerPlanSchema not used
     * @throws AlgebricksException
     */
    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        ForwardOperator forwardOp = (ForwardOperator) op;
        RecordDescriptor dataInputDescriptor = JobGenHelper.mkRecordDescriptor(
                context.getTypeEnvironment(forwardOp.getInputs().get(0).getValue()), inputSchemas[0], context);
        AbstractForwardOperatorDescriptor forwardDescriptor =
                getOperatorDescriptor(builder, forwardOp, dataInputDescriptor);
        builder.contributeHyracksOperator(forwardOp, forwardDescriptor);
        ILogicalOperator dataSource = forwardOp.getInputs().get(0).getValue();
        builder.contributeGraphEdge(dataSource, 0, forwardOp, 0);
        ILogicalOperator sideDataSource = forwardOp.getInputs().get(1).getValue();
        builder.contributeGraphEdge(sideDataSource, 0, forwardOp, 1);
    }

    @Override
    public Pair<int[], int[]> getInputOutputDependencyLabels(ILogicalOperator op) {
        int[] outputDependencyLabels = new int[] { 1 };
        int[] inputDependencyLabels = new int[] { 1, 0 };
        return new Pair<>(inputDependencyLabels, outputDependencyLabels);
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }
}
