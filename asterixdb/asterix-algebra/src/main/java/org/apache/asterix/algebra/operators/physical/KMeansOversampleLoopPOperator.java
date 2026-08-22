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

import org.apache.asterix.runtime.operators.kmeans.KMeansCostControllerOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansLoopIO;
import org.apache.asterix.runtime.operators.kmeans.KMeansPhiMergeOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansPoolMergeOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansReleaseOperatorDescriptor;
import org.apache.asterix.runtime.operators.kmeans.KMeansSampleOperatorDescriptor;
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
 * The k-means|| oversampling initialization as a five-operator systolic chain. Cost/Controller is the
 * registered head, so the builder wires the vectors (input 0) and the seed (input 1) into it and the
 * downstream RECLUSTER reads the weighed pool from its output 0; the per-round potential (output 1) and the
 * remaining stages are internal edges wired here with pipelined broadcast connectors. Cost, Sample and
 * Release are pinned to the SAME cluster locations, so partition i of each co-locates on one NC and can share
 * that NC's permit and pool/vector run files through joblet state; the two merges are single-partition.
 * Release is a sink dead-end, so it is registered as a job root to ensure its branch is scheduled.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public class KMeansOversampleLoopPOperator extends AbstractKMeansLoopPOperator {

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.KMEANS_OVERSAMPLE_LOOP;
    }

    @Override
    protected void contributeLoop(IHyracksJobBuilder builder, KMeansStageOperator kop, AbstractLogicalOperator op,
            RecordDescriptor poolEnvelopeRecDesc, int vectorColumn, int seedColumn, String[] clusterLocations,
            ILogicalOperator src0, ILogicalOperator src1) throws AlgebricksException {
        JobSpecification spec = builder.getJobSpec();
        // Unique + stable per loop instance (one per query); baked into all five descriptors so every partition
        // and NC agrees on the joblet-state keys.
        String loopKey = "kmeansSystolicLoop#" + kop.getCandidateVariable();
        int participants = clusterLocations.length;

        // Op1 Cost/Controller — the registered descriptor (inputs land here; output 0 carries the weighed
        // partials the downstream RECLUSTER reduces).
        KMeansCostControllerOperatorDescriptor op1 =
                new KMeansCostControllerOperatorDescriptor(spec, poolEnvelopeRecDesc, KMeansLoopIO.SIGMA_RD, loopKey,
                        vectorColumn, seedColumn, kop.getLoopRounds(), framesLimit(), kop.getDimension());
        contributeOpDesc(builder, op, op1);
        builder.contributeGraphEdge(src0, 0, op, 0);
        builder.contributeGraphEdge(src1, 0, op, 1);

        // The internal loop operators.
        // Its INPUT descriptor comes from op1's declared output (SIGMA_RD, {round, part, localSigma}) via the
        // connector; the argument here is its OUTPUT, {round, phi}.
        KMeansPhiMergeOperatorDescriptor op2 =
                new KMeansPhiMergeOperatorDescriptor(spec, KMeansLoopIO.SCALAR_RD, participants);
        KMeansSampleOperatorDescriptor op3 = new KMeansSampleOperatorDescriptor(spec, KMeansLoopIO.DRAW_RD, loopKey,
                kop.getTopCount(), kop.getSeed());
        KMeansPoolMergeOperatorDescriptor op4 =
                new KMeansPoolMergeOperatorDescriptor(spec, KMeansLoopIO.DRAW_RD, participants, framesLimit());
        KMeansReleaseOperatorDescriptor op5 = new KMeansReleaseOperatorDescriptor(spec, loopKey);

        // Partition constraints (registered via the builder so the job-gen finalizer does not double-assign a
        // default). Op1/Op3/Op5 share identical absolute locations -> co-located per partition; merges single-node.
        AlgebricksAbsolutePartitionConstraint coLocated = new AlgebricksAbsolutePartitionConstraint(clusterLocations);
        builder.contributeAlgebricksPartitionConstraint(op1, coLocated);
        builder.contributeAlgebricksPartitionConstraint(op3, coLocated);
        builder.contributeAlgebricksPartitionConstraint(op5, coLocated);
        builder.contributeAlgebricksPartitionConstraint(op2, new AlgebricksCountPartitionConstraint(1));
        builder.contributeAlgebricksPartitionConstraint(op4, new AlgebricksCountPartitionConstraint(1));

        // Internal pipelined broadcast edges: Op1.localSigma -> PhiMerge -> Sample -> PoolMerge -> Release.
        // (Broadcast into a single-partition merge is a CONCURRENT M-to-1; never the sequential merging connector,
        // which would deadlock against the permit-paced producers.)
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), op1, 1, op2, 0);
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), op2, 0, op3, 0);
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), op3, 0, op4, 0);
        spec.connect(new MToNBroadcastConnectorDescriptor(spec), op4, 0, op5, 0);

        // Release is a sink dead-end (its "output" is side-effects: pool append + permit release), not upstream of
        // the main result, so register it as a root or its branch would not be scheduled.
        spec.addRoot(op5);
    }
}
