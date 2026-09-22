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
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.NonPropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * One stage of the distributed k-means|| plan expansion (CLUSTER BY), selected by {@link Mode}: RECLUSTER is a
 * single-input reduction over the broadcast partials (see the runtime operators); OVERSAMPLE_LOOP and
 * LLOYD_LOOP are self-iterating loops, each realized as a systolic sub-graph by the physical
 * operator. Blocking; produces a single new variable (the stage's output vector/envelope); input variables
 * are NOT propagated. Semantics are opaque to generic rewrite rules by design: expressing these stages as
 * SELECT/ORDER BY/LIMIT/GROUP BY algebra regressed with optimizer context (lost topK pushdown, nested-plan
 * in-memory sorts).
 * <p>
 * The vector input (input 0) is present for OVERSAMPLE_LOOP and LLOYD_LOOP; it is ABSENT (a single pool
 * input) for the RECLUSTER merge, so {@link #getVectorVariable()} is null in that mode.
 */
public class KMeansStageOperator extends AbstractLogicalOperator {

    /**
     * What this instance computes. RECLUSTER merges the broadcast partials and reduces the weighted candidate
     * pool to the {@code topCount} initial centroids (C0). The two loop modes are self-iterating: the physical
     * operator realizes each as an injected systolic sub-graph, never through a per-mode emit here.
     */
    public enum Mode {
        RECLUSTER("recluster"),
        // The exact Bernoulli oversampling init (Bahmani et al. VLDB'12, Algorithm 2) as ONE operator that
        // iterates internally: each of loopRounds iterations does a local cost + all-reduce to the global
        // potential phi + a local Bernoulli sample + an all-reduce union into the next pool; the final pool is
        // weighed and emitted for RECLUSTER. The physical operator injects this as a pipelined systolic
        // sub-graph (correct on any topology). See the operator descriptor / physical operator.
        OVERSAMPLE_LOOP("oversample-loop"),
        // The Lloyd refinement as ONE operator that iterates internally: each of loopRounds iterations
        // assigns every resident vector to its nearest current centroid and all-reduces the per-centroid
        // (count, sum) partials into the next centroid set. The physical operator injects this as a
        // pipelined systolic sub-graph, as for OVERSAMPLE_LOOP. Emits the final centroids as plain vectors.
        LLOYD_LOOP("lloyd-loop");

        private final String label;

        Mode(String label) {
            this.label = label;
        }

        /** The name this mode prints under in a plan. */
        public String getLabel() {
            return label;
        }
    }

    // References to the vector-valued variable of input 0 (the qualified points) and of input 1 (the
    // pool). Held as EXPRESSIONS (exposed via acceptExpressionTransform) so variable-substitution and
    // pruning rules see them; plain LogicalVariable fields silently drift through renames.
    // vectorRef is NULL for the single-input RECLUSTER merge: it reads only the broadcast partials, so there
    // is no vector input and the pool is the operator's sole (index-0) input.
    private final Mutable<ILogicalExpression> vectorRef;
    private final Mutable<ILogicalExpression> poolRef;
    // The single produced variable: a candidate vector, typed open by the expansion rule that builds the stage.
    private LogicalVariable candidateVar;
    private final Object candidateVarType;
    // RECLUSTER: k, the number of initial centroids to keep. Always non-negative.
    private final int topCount;
    private final Mode mode;
    // Base seed for the mode's RNG (the loops draw per round and per partition; RECLUSTER's roulette draws
    // once). LLOYD_LOOP draws nothing and takes 0.
    private final long seed;
    // The loop modes only: how many rounds or iterations the operator runs internally. RECLUSTER takes 0.
    private final int loopRounds;
    // Loop stages only: the declared vector width, enforced by the operators' decoder (a predicate could be
    // pushed into the columnar reader and evaluated per array element). Unused by RECLUSTER.
    private final int dimension;
    // The metric's canonical name; a String since the metric enum lives above Algebricks.
    private final String metric;

    public KMeansStageOperator(Mutable<ILogicalExpression> vectorRef, Mutable<ILogicalExpression> poolRef,
            LogicalVariable candidateVar, Object candidateVarType, int topCount, Mode mode, long seed, int loopRounds,
            int dimension, String metric) {
        this.vectorRef = vectorRef;
        this.poolRef = poolRef;
        this.candidateVar = candidateVar;
        this.candidateVarType = candidateVarType;
        this.topCount = topCount;
        this.mode = mode;
        this.seed = seed;
        this.loopRounds = loopRounds;
        this.dimension = dimension;
        this.metric = metric;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.KMEANS_STAGE;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitKMeansStageOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        // Blocking: input 0 is fully materialized before any candidate is emitted.
        return false;
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        // Only the candidate variable is live downstream; input tuples are consumed, not propagated.
        schema = new ArrayList<>();
        schema.add(candidateVar);
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                target.addVariable(candidateVar);
            }
        };
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        // vectorRef is null only for RECLUSTER, the one mode with a pool input and no vector input.
        boolean changed = vectorRef != null && visitor.transform(vectorRef);
        changed |= visitor.transform(poolRef);
        return changed;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        // Non-propagating, to agree with recomputeSchema and the propagation policy: the input tuples are
        // consumed, and the candidate variable is the only thing live downstream. Propagating the inputs here
        // would advertise types for variables the schema says are gone. Same shape as AggregateOperator.
        IVariableTypeEnvironment env =
                new NonPropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMetadataProvider());
        env.setVarType(candidateVar, candidateVarType);
        return env;
    }

    /** The vector input variable, or null for RECLUSTER, the only mode without a vector input. */
    public LogicalVariable getVectorVariable() {
        return vectorRef == null ? null : ((VariableReferenceExpression) vectorRef.getValue()).getVariableReference();
    }

    public LogicalVariable getPoolVariable() {
        return ((VariableReferenceExpression) poolRef.getValue()).getVariableReference();
    }

    public Mutable<ILogicalExpression> getVectorRef() {
        return vectorRef;
    }

    public Mutable<ILogicalExpression> getPoolRef() {
        return poolRef;
    }

    public LogicalVariable getCandidateVariable() {
        return candidateVar;
    }

    public Object getCandidateVarType() {
        return candidateVarType;
    }

    public void setCandidateVariable(LogicalVariable v) {
        this.candidateVar = v;
    }

    public int getTopCount() {
        return topCount;
    }

    public Mode getMode() {
        return mode;
    }

    /** Base seed for the mode's RNG; 0 for LLOYD_LOOP, which draws nothing. */
    public long getSeed() {
        return seed;
    }

    /** The loop modes only: how many rounds or iterations the operator runs internally. */
    public int getLoopRounds() {
        return loopRounds;
    }

    /** The loop stages only: the declared vector width the decoder admits. */
    public int getDimension() {
        return dimension;
    }

    public String getMetric() {
        return metric;
    }
}
