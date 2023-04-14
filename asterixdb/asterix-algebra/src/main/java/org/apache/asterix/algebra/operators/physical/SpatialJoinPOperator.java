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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.asterix.runtime.operators.joins.spatial.PlaneSweepJoinOperatorDescriptor;
import org.apache.asterix.runtime.operators.joins.spatial.utils.ISpatialJoinUtilFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
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
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;

/**
 * The right input is broadcast and the left input can be partitioned in any way.
 */
public class SpatialJoinPOperator extends AbstractJoinPOperator {

    private final List<LogicalVariable> keysLeftBranch;
    private final List<LogicalVariable> keysRightBranch;

    protected final ISpatialJoinUtilFactory mjcf;
    private final int memSizeInFrames;

    public SpatialJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> keysLeftBranch, List<LogicalVariable> keysRightBranch, int memSizeInFrames,
            ISpatialJoinUtilFactory mjcf) {
        super(kind, partitioningType);
        this.keysLeftBranch = keysLeftBranch;
        this.keysRightBranch = keysRightBranch;
        this.mjcf = mjcf;
        this.memSizeInFrames = memSizeInFrames;
    }

    public List<LogicalVariable> getKeysLeftBranch() {
        return keysLeftBranch;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.SPATIAL_JOIN;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public String toString() {
        return "SPATIAL_JOIN" + " " + keysLeftBranch + " " + keysRightBranch;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator iop, IOptimizationContext context)
            throws AlgebricksException {
        IPartitioningProperty pp;
        AbstractLogicalOperator op = (AbstractLogicalOperator) iop;

        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.LOCAL) {
            AbstractLogicalOperator op0 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
            IPhysicalPropertiesVector pv0 = op0.getPhysicalOperator().getDeliveredProperties();
            AbstractLogicalOperator op1 = (AbstractLogicalOperator) op.getInputs().get(1).getValue();
            IPhysicalPropertiesVector pv1 = op1.getPhysicalOperator().getDeliveredProperties();

            if (pv0 == null || pv1 == null) {
                pp = null;
            } else {
                pp = pv0.getPartitioningProperty();
            }
        } else {
            pp = IPartitioningProperty.UNPARTITIONED;
        }
        this.deliveredProperties = new StructuralPropertiesVector(pp, deliveredLocalProperties(iop, context));
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        List<LogicalVariable> keysLeftBranchTileId = new ArrayList<>();
        keysLeftBranchTileId.add(keysLeftBranch.get(0));
        List<LogicalVariable> keysRightBranchTileId = new ArrayList<>();
        keysRightBranchTileId.add(keysRightBranch.get(0));
        IPartitioningProperty pp1 = UnorderedPartitionedProperty.of(new ListSet<>(keysLeftBranchTileId),
                context.getComputationNodeDomain());
        IPartitioningProperty pp2 = UnorderedPartitionedProperty.of(new ListSet<>(keysRightBranchTileId),
                context.getComputationNodeDomain());

        List<ILocalStructuralProperty> localProperties1 = new ArrayList<>();
        List<OrderColumn> orderColumns1 = new ArrayList<OrderColumn>();
        orderColumns1.add(new OrderColumn(keysLeftBranch.get(0), OrderOperator.IOrder.OrderKind.ASC));
        orderColumns1.add(new OrderColumn(keysLeftBranch.get(1), OrderOperator.IOrder.OrderKind.ASC));
        localProperties1.add(new LocalOrderProperty(orderColumns1));

        List<ILocalStructuralProperty> localProperties2 = new ArrayList<>();
        List<OrderColumn> orderColumns2 = new ArrayList<OrderColumn>();
        orderColumns2.add(new OrderColumn(keysRightBranch.get(0), OrderOperator.IOrder.OrderKind.ASC));
        orderColumns2.add(new OrderColumn(keysRightBranch.get(1), OrderOperator.IOrder.OrderKind.ASC));
        localProperties2.add(new LocalOrderProperty(orderColumns2));

        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[2];
        pv[0] = new StructuralPropertiesVector(pp1, localProperties1);
        pv[1] = new StructuralPropertiesVector(pp2, localProperties2);

        return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        int[] keysBuild = JobGenHelper.variablesToFieldIndexes(keysLeftBranch, inputSchemas[0]);
        int[] keysProbe = JobGenHelper.variablesToFieldIndexes(keysRightBranch, inputSchemas[1]);

        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        RecordDescriptor recordDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);

        IOperatorDescriptor opDesc = new PlaneSweepJoinOperatorDescriptor(spec, memSizeInFrames, keysBuild, keysProbe,
                recordDescriptor, mjcf);
        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);

        ILogicalOperator src1 = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src1, 0, op, 0);
        ILogicalOperator src2 = op.getInputs().get(1).getValue();
        builder.contributeGraphEdge(src2, 0, op, 1);
    }

    protected List<ILocalStructuralProperty> deliveredLocalProperties(ILogicalOperator op,
            IOptimizationContext context) {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        IPhysicalPropertiesVector pv0 = op0.getPhysicalOperator().getDeliveredProperties();
        List<ILocalStructuralProperty> lp0 = pv0.getLocalProperties();
        if (lp0 != null) {
            // maintains the local properties on the probe side
            return new LinkedList<>(lp0);
        }
        return new LinkedList<>();
    }
}
