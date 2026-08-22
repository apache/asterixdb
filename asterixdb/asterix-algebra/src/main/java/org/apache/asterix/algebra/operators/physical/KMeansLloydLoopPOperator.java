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

import org.apache.asterix.runtime.operators.kmeans.KMeansCentroidMergeOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansLloydControllerOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansLloydReleaseOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansLoopIO;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.KMeansStageOperator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.MToNBroadcastConnectorDescriptor;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * One Lloyd refinement run as a three-operator systolic chain -- the same shape as the oversampling loop with
 * one reduce instead of two, because a Lloyd iteration all-reduces only the per-centroid (count, sum) partials.
 * The Controller is the registered head, so the builder wires the vectors (input 0) and the initial centroids
 * (input 1) into it; Controller and Release are pinned to the SAME cluster locations so partition i of each
 * co-locates and shares that NC's permit and run files, while the centroid merge is single-partition. Release
 * is a sink dead-end, so it is registered as a job root to ensure its branch is scheduled.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public class KMeansLloydLoopPOperator extends AbstractKMeansLoopPOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.KMEANS_LLOYD_LOOP;
    }

    @Override
    protected void contributeLoop(IHyracksJobBuilder builder, KMeansStageOperator kop, AbstractLogicalOperator op,
            RecordDescriptor centroidRecDesc, int vectorColumn, int centroidColumn, String[] clusterLocations,
            ILogicalOperator src0, ILogicalOperator src1) throws AlgebricksException {
        JobSpecification spec = builder.getJobSpec();
        // Unique + stable per loop instance, and distinct from any oversampling loop's key in the same job.
        String loopKey = "kmeansLloydLoop#" + kop.getCandidateVariable();
        int participants = clusterLocations.length;

        KMeansLloydControllerOperatorDescriptor op1 = new KMeansLloydControllerOperatorDescriptor(spec, centroidRecDesc,
                KMeansLoopIO.PARTIAL_RD, loopKey, vectorColumn, centroidColumn, kop.getLoopRounds(), kop.getTopCount(),
                framesLimit(), kop.getDimension());
        contributeOpDesc(builder, op, op1);
        builder.contributeGraphEdge(src0, 0, op, 0);
        builder.contributeGraphEdge(src1, 0, op, 1);

        KMeansCentroidMergeOperatorDescriptor op2 =
                new KMeansCentroidMergeOperatorDescriptor(spec, KMeansLoopIO.DRAW_RD, participants, framesLimit());
        KMeansLloydReleaseOperatorDescriptor op3 = new KMeansLloydReleaseOperatorDescriptor(spec, loopKey);

        AlgebricksAbsolutePartitionConstraint coLocated = new AlgebricksAbsolutePartitionConstraint(clusterLocations);
        builder.contributeAlgebricksPartitionConstraint(op1, coLocated);
        builder.contributeAlgebricksPartitionConstraint(op3, coLocated);
        builder.contributeAlgebricksPartitionConstraint(op2, new AlgebricksCountPartitionConstraint(1));

        // Internal pipelined broadcast edges: Op1.partials -> CentroidMerge -> Release.
        // (Broadcast into a single-partition merge is a CONCURRENT M-to-1; never the sequential merging
        // connector, which would deadlock against the permit-paced producers.)
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), op1, 1, op2, 0);
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), op2, 0, op3, 0);

        spec.addRoot(op3);
    }
}
