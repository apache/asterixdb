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

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.KMeansStageOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * What the two self-iterating k-means stages share. Both take the vectors at input 0 and a pool at input 1,
 * and both are realized as a sub-graph rather than a single descriptor: a Hyracks job graph is acyclic, so an
 * operator that iterates cannot feed its own input. The iteration is carried instead by a permit and a shared
 * run file between co-located partitions, which needs several operators wired into a fixed chain -- hence a
 * physical operator that contributes a graph rather than one descriptor.
 * <p>
 * This class settles the inputs; a subclass builds its own chain.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public abstract class AbstractKMeansLoopPOperator extends AbstractKMeansStagePOperator {

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        // The vectors (input 0) and the broadcast pool (input 1). A loop chain carries an absolute partition
        // constraint -- one instance per compute partition, so the co-located stages can share per-partition
        // state -- which means the vectors have to arrive at that same width. Stating a partitioning
        // requirement over the computation node domain is what makes the property enforcer insert a
        // repartition when the child does not already deliver one; with no requirement, a child of a different
        // width (anything below a global LIMIT, say) is joined to this fixed-width consumer by a one-to-one
        // connector, which then addresses a producer partition that does not exist. RANDOM is the weakest
        // requirement that still settles the width: k-means does not care which vector lands in which
        // partition, only that each one lands somewhere. The domain is the storage domain the chain is pinned
        // to. A scan already delivers it so a dataset input is never repartitioned.
        INodeDomain domain = stageDomain(context);
        StructuralPropertiesVector[] pv = new StructuralPropertiesVector[] {
                new StructuralPropertiesVector(new RandomPartitioningProperty(domain), null),
                new StructuralPropertiesVector(new BroadcastPartitioningProperty(domain), null) };
        return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
    }

    @Override
    public final void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        KMeansStageOperator kop = (KMeansStageOperator) op;
        RecordDescriptor recDesc =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);
        int vectorColumn = resolveSingleColumn(inputSchemas[0], kop.getVectorVariable());
        int poolColumn = resolveSingleColumn(inputSchemas[1], kop.getPoolVariable());
        String[] clusterLocations =
                ((MetadataProvider) context.getMetadataProvider()).getClusterLocations().getLocations();
        contributeLoop(builder, kop, (AbstractLogicalOperator) op, recDesc, vectorColumn, poolColumn, clusterLocations,
                op.getInputs().get(0).getValue(), op.getInputs().get(1).getValue());
    }

    /**
     * Build this loop's chain onto the job spec. The implementation registers its head descriptor (so the
     * builder wires both plan inputs into it), pins the stages that hold per-partition state to
     * {@code clusterLocations} and the reducing stages to a single partition, connects the chain, and adds the
     * final sink as a job root so its branch is scheduled.
     */
    protected abstract void contributeLoop(IHyracksJobBuilder builder, KMeansStageOperator kop,
            AbstractLogicalOperator op, RecordDescriptor recDesc, int vectorColumn, int poolColumn,
            String[] clusterLocations, ILogicalOperator src0, ILogicalOperator src1) throws AlgebricksException;
}
