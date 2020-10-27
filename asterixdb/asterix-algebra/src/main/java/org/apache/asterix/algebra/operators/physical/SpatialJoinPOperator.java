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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.SpatialPartitionedProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SpatialJoinPOperator extends AbstractJoinPOperator {
    private final List<LogicalVariable> keysLeftBranch;
    private final List<LogicalVariable> keysRightBranch;

    public SpatialJoinPOperator(AbstractBinaryJoinOperator.JoinKind kind, JoinPartitioningType partitioningType, List<LogicalVariable> keysLeftBranch, List<LogicalVariable> keysRightBranch) {
        super(kind, partitioningType);
        this.keysLeftBranch = keysLeftBranch;
        this.keysRightBranch = keysRightBranch;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return null;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator iop, IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) throws AlgebricksException {
        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[2];
        AbstractLogicalOperator op = (AbstractLogicalOperator) iop;
        INodeDomain targetNodeDomain = context.getComputationNodeDomain();

        IPartitioningProperty ppLeft = null;
        IPartitioningProperty ppRight = null;

        if (op.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED) {
            ppLeft = new SpatialPartitionedProperty(new HashSet<>(keysLeftBranch), targetNodeDomain);
            ppRight = new SpatialPartitionedProperty(new HashSet<>(keysRightBranch), targetNodeDomain);
        }

        List<ILocalStructuralProperty> ispLeft = new ArrayList<>();
        List<ILocalStructuralProperty> ispRight = new ArrayList<>();

        pv[0] = new StructuralPropertiesVector(ppLeft, ispLeft);
        pv[1] = new StructuralPropertiesVector(ppRight, ispRight);
        IPartitioningRequirementsCoordinator prc = IPartitioningRequirementsCoordinator.NO_COORDINATION;

        return new PhysicalRequirements(pv, prc);
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) throws AlgebricksException {

    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op, IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema) throws AlgebricksException {

    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }
}
