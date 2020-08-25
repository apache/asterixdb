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
import java.util.List;

import org.apache.asterix.optimizer.rules.util.IntervalPartitions;
import org.apache.asterix.runtime.operators.joins.interval.IntervalMergeJoinOperatorDescriptor;
import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtilFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.PartialBroadcastOrderedFollowingProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.PartialBroadcastOrderedIntersectProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

public class IntervalMergeJoinPOperator extends AbstractJoinPOperator {

    private final List<LogicalVariable> keysLeftBranch;
    private final List<LogicalVariable> keysRightBranch;
    protected final IIntervalJoinUtilFactory mjcf;
    protected final IntervalPartitions intervalPartitions;

    private final int memSizeInFrames;

    public IntervalMergeJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeftOfEqualities, List<LogicalVariable> sideRightOfEqualities,
            int memSizeInFrames, IIntervalJoinUtilFactory mjcf, IntervalPartitions intervalPartitions) {
        super(kind, partitioningType);
        this.keysLeftBranch = sideLeftOfEqualities;
        this.keysRightBranch = sideRightOfEqualities;
        this.mjcf = mjcf;
        this.intervalPartitions = intervalPartitions;
        this.memSizeInFrames = memSizeInFrames;
    }

    public IIntervalJoinUtilFactory getIntervalMergeJoinCheckerFactory() {
        return mjcf;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.INTERVAL_MERGE_JOIN;
    }

    @Override
    public String toString() {
        return "INTERVAL_MERGE_JOIN" + " " + keysLeftBranch + " " + keysRightBranch;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator iop, IOptimizationContext context) {
        List<OrderColumn> order = intervalPartitions.getLeftStartColumn();
        IPartitioningProperty pp = new OrderedPartitionedProperty(order, null, intervalPartitions.getRangeMap());
        List<ILocalStructuralProperty> propsLocal = new ArrayList<>();
        propsLocal.add(new LocalOrderProperty(intervalPartitions.getLeftStartColumn()));
        deliveredProperties = new StructuralPropertiesVector(pp, propsLocal);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator iop,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[2];
        AbstractLogicalOperator op = (AbstractLogicalOperator) iop;

        //Create Left Local Order Column
        IPartitioningProperty ppLeft = null;
        List<ILocalStructuralProperty> ispLeft = new ArrayList<>();
        ArrayList<OrderColumn> leftLocalOrderColumn = new ArrayList<>();
        for (LogicalVariable v : keysLeftBranch) {
            leftLocalOrderColumn.add(new OrderColumn(v, intervalPartitions.getLeftIntervalColumn().get(0).getOrder()));
        }
        ispLeft.add(new LocalOrderProperty(leftLocalOrderColumn));

        //Create Right Local Order Column
        IPartitioningProperty ppRight = null;
        List<ILocalStructuralProperty> ispRight = new ArrayList<>();
        ArrayList<OrderColumn> rightLocalOrderColumn = new ArrayList<>();
        for (LogicalVariable v : keysRightBranch) {
            rightLocalOrderColumn
                    .add(new OrderColumn(v, intervalPartitions.getRightIntervalColumn().get(0).getOrder()));
        }
        ispRight.add(new LocalOrderProperty(rightLocalOrderColumn));

        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
            INodeDomain targetNodeDomain = context.getComputationNodeDomain();

            RangeMap rangeMapHint = intervalPartitions.getRangeMap();

            //Assign Property
            switch (intervalPartitions.getLeftPartitioningType()) {
                case ORDERED_PARTITIONED:
                    ppLeft = new OrderedPartitionedProperty(intervalPartitions.getLeftStartColumn(), targetNodeDomain,
                            rangeMapHint);
                    break;
                case PARTIAL_BROADCAST_ORDERED_FOLLOWING:
                    ppLeft = new PartialBroadcastOrderedFollowingProperty(intervalPartitions.getLeftStartColumn(),
                            targetNodeDomain, rangeMapHint);
                    break;
                case PARTIAL_BROADCAST_ORDERED_INTERSECT:
                    ppLeft = new PartialBroadcastOrderedIntersectProperty(intervalPartitions.getLeftIntervalColumn(),
                            targetNodeDomain, rangeMapHint);
                    break;
            }
            switch (intervalPartitions.getRightPartitioningType()) {
                case ORDERED_PARTITIONED:
                    ppRight = new OrderedPartitionedProperty(intervalPartitions.getRightStartColumn(), targetNodeDomain,
                            rangeMapHint);
                    break;
                case PARTIAL_BROADCAST_ORDERED_FOLLOWING:
                    ppRight = new PartialBroadcastOrderedFollowingProperty(intervalPartitions.getRightStartColumn(),
                            targetNodeDomain, rangeMapHint);
                    break;
                case PARTIAL_BROADCAST_ORDERED_INTERSECT:
                    ppRight = new PartialBroadcastOrderedIntersectProperty(intervalPartitions.getRightIntervalColumn(),
                            targetNodeDomain, rangeMapHint);
                    break;
            }
        }
        pv[0] = new StructuralPropertiesVector(ppLeft, ispLeft);
        pv[1] = new StructuralPropertiesVector(ppRight, ispRight);
        IPartitioningRequirementsCoordinator prc = IPartitioningRequirementsCoordinator.NO_COORDINATION;
        return new PhysicalRequirements(pv, prc);
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        int[] keysBuild = JobGenHelper.variablesToFieldIndexes(keysLeftBranch, inputSchemas[0]);
        int[] keysProbe = JobGenHelper.variablesToFieldIndexes(keysRightBranch, inputSchemas[1]);

        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        RecordDescriptor recordDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), opSchema, context);

        IOperatorDescriptor opDesc = getIntervalOperatorDescriptor(keysBuild, keysProbe, spec, recordDescriptor, mjcf);
        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);

        ILogicalOperator src1 = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src1, 0, op, 0);
        ILogicalOperator src2 = op.getInputs().get(1).getValue();
        builder.contributeGraphEdge(src2, 0, op, 1);
    }

    IOperatorDescriptor getIntervalOperatorDescriptor(int[] keysBuild, int[] keysProbe,
            IOperatorDescriptorRegistry spec, RecordDescriptor recordDescriptor, IIntervalJoinUtilFactory mjcf) {
        return new IntervalMergeJoinOperatorDescriptor(spec, memSizeInFrames, keysBuild, keysProbe, recordDescriptor,
                mjcf);
    }
}
