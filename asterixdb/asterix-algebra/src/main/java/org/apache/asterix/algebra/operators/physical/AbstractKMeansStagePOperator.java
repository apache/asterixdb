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

import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.KMeansStageOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.DefaultNodeGroupDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.api.exceptions.ErrorCode;

/**
 * What the CLUSTER BY k-means|| stages share, whatever they compute: they are blocking, they emit only the
 * candidate variable, and each of their inputs delivers exactly one column. The stage a query actually runs is
 * chosen by {@code SetAsterixPhysicalOperatorsRule} from the logical operator's mode, one subclass per
 * stage as the join and group-by families do, so an input arity or a partitioning requirement is a property
 * of the class.
 */
public abstract class AbstractKMeansStagePOperator extends AbstractPhysicalOperator {

    /** Mirrors {@code OptimizationConfUtil.MIN_FRAME_LIMIT_FOR_CLUSTER_BY}, which asterix-common owns. */
    public static final int MIN_FRAME_LIMIT_FOR_CLUSTER_BY = 4;

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return true;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        // The output is partitioned like input 0 but carries ONLY the candidate variable: claiming the
        // child's delivered properties would advertise partitioning on variables this operator drops,
        // forcing the enforcer to insert bogus re-partitioning (e.g. hashing the candidate array).
        // An unpartitioned stage delivers UNPARTITIONED, as GROUP BY over an unpartitioned input would.
        deliveredProperties =
                unpartitioned(op) ? new StructuralPropertiesVector(IPartitioningProperty.UNPARTITIONED, null)
                        : new StructuralPropertiesVector(new RandomPartitioningProperty(stageDomain(context)), null);
    }

    /**
     * Whether this stage runs on a single partition. The expansion derives a stage's execution mode from its
     * input, so an unpartitioned input (a global LIMIT, a constant array) makes an unpartitioned stage that
     * runs one loop instance where its rows are, as GROUP BY follows the partitioning it is given.
     */
    protected static boolean unpartitioned(ILogicalOperator op) {
        return ((AbstractLogicalOperator) op).getExecutionMode() == AbstractLogicalOperator.ExecutionMode.UNPARTITIONED;
    }

    /**
     * The node domain of the stage. It is the storage domain because the stage is pinned to the cluster
     * locations at job-gen time.
     * <p>
     * Do not use the computation domain here. If compiler.parallelism differs from the number of storage
     * partitions the two domains differ. A scan delivers the storage domain and cannot satisfy a requirement
     * on the computation domain. The enforcer then inserts a RANDOM_PARTITION_EXCHANGE whose partitioner is
     * unseeded and the clustering result changes on every run.
     */
    protected static INodeDomain stageDomain(IOptimizationContext context) {
        return new DefaultNodeGroupDomain(((MetadataProvider) context.getMetadataProvider()).getClusterLocations());
    }

    /**
     * Declares a <em>variable</em> budget with the minimum as its floor. Without this the operator keeps the
     * default {@code fixedMemoryBudget(1)}, whose {@code setMemoryBudgetInFrames} throws for any value other
     * than 1, so the rule assigning a budget would fail.
     */
    @Override
    public void createLocalMemoryRequirements(ILogicalOperator op) {
        localMemoryRequirements = LocalMemoryRequirements.variableMemoryBudget(MIN_FRAME_LIMIT_FOR_CLUSTER_BY);
    }

    /**
     * The frame budget this stage may use, from {@link SetMemoryRequirementsRule} via
     * {@code PhysicalOptimizationConfig.getMaxFramesForClusterBy()}. It sizes the block of vectors held while
     * scoring and the sort runs the merges spill through.
     * <p>
     * Falls back to the declared minimum when nothing was assigned, since a stage given a single frame
     * cannot make progress.
     */
    protected int framesLimit() {
        LocalMemoryRequirements req = getLocalMemoryRequirements();
        int budget = req == null ? 0 : req.getMemoryBudgetInFrames();
        return Math.max(budget, MIN_FRAME_LIMIT_FOR_CLUSTER_BY);
    }

    /**
     * Resolves the column an input branch delivers the stage's variable in. The branch is an ASSIGN the
     * expansion rule builds over the shared input, so the schema may carry the row's other columns as well;
     * only the variable identifies the column. A schema without it is a broken plan, so it raises.
     */
    protected static int resolveSingleColumn(IOperatorSchema schema, LogicalVariable var) throws AlgebricksException {
        int col = schema.findVariable(var);
        if (col >= 0) {
            return col;
        }
        throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE, "kmeans-stage input schema",
                String.valueOf(schema.getSize()));
    }

    /**
     * The metric the stage measures with. The rewrite validates the alias and the expansion rule always sets
     * it, so the fallback is defensive only.
     */
    protected static VectorSimilarityMetric metricOf(KMeansStageOperator kop) {
        VectorSimilarityMetric metric = VectorSimilarityMetric.fromAlias(kop.getMetric());
        return metric == null ? VectorSimilarityMetric.EUCLIDEAN_SQUARED : metric;
    }
}
