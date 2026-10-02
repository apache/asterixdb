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
import java.util.Collection;
import java.util.List;

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
 * <p>
 * Exception: LLOYD_LOOP ends by labelling its rows, emitting the {@link #getRowVariables() row variables} with
 * the cluster id (and optionally the centroid). Under k-means|| those rows come from another stage's store,
 * not its own input, so their types are kept on the operator.
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
        // pipelined systolic sub-graph, as for OVERSAMPLE_LOOP. Emits every row labelled with its cluster id.
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
    // A candidate vector, typed open; for LLOYD_LOOP the cluster id.
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
    // The vector store shared by the two loops of one expansion, named by the OVERSAMPLE_LOOP's candidate
    // variable: that loop keeps its file, the LLOYD_LOOP reads it. Null when a stage keeps its own store.
    private LogicalVariable vectorStoreVar;
    // Columns stored beside each vector and emitted by LLOYD_LOOP, in that order, with their types.
    private final List<LogicalVariable> rowVars = new ArrayList<>();
    private final List<Object> rowVarTypes = new ArrayList<>();
    // LLOYD_LOOP only: the row's centroid, or null when nothing reads it.
    private LogicalVariable labelCentroidVar;
    private Object labelCentroidVarType;

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

    /** Whether this stage emits labelled rows rather than a candidate set. */
    public boolean emitsRows() {
        return mode == Mode.LLOYD_LOOP;
    }

    /** Whether this stage writes the row store from its input (only a storing stage reads the row columns). */
    public boolean storesRows() {
        return mode == Mode.OVERSAMPLE_LOOP || (mode == Mode.LLOYD_LOOP && vectorStoreVar == null);
    }

    /** The variables this stage emits, in column order. */
    public void getOutputVariables(Collection<LogicalVariable> vars) {
        if (emitsRows()) {
            vars.addAll(rowVars);
        }
        vars.add(candidateVar);
        if (emitsRows() && labelCentroidVar != null) {
            vars.add(labelCentroidVar);
        }
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        // Input tuples are consumed; only what the stage emits is live downstream.
        schema = new ArrayList<>();
        getOutputVariables(schema);
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                List<LogicalVariable> vars = new ArrayList<>();
                getOutputVariables(vars);
                for (LogicalVariable v : vars) {
                    target.addVariable(v);
                }
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
        // consumed, and only what the stage emits is live downstream. Propagating the inputs here
        // would advertise types for variables the schema says are gone. Same shape as AggregateOperator.
        IVariableTypeEnvironment env =
                new NonPropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMetadataProvider());
        if (emitsRows()) {
            for (int i = 0; i < rowVars.size(); i++) {
                env.setVarType(rowVars.get(i), rowVarTypes.get(i));
            }
            if (labelCentroidVar != null) {
                env.setVarType(labelCentroidVar, labelCentroidVarType);
            }
        }
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

    public LogicalVariable getVectorStoreVariable() {
        return vectorStoreVar;
    }

    public void setVectorStoreVariable(LogicalVariable v) {
        this.vectorStoreVar = v;
    }

    /** Mutable. */
    public List<LogicalVariable> getRowVariables() {
        return rowVars;
    }

    /** Parallel to {@link #getRowVariables()}. */
    public List<Object> getRowVariableTypes() {
        return rowVarTypes;
    }

    public void addRowVariable(LogicalVariable v, Object type) {
        rowVars.add(v);
        rowVarTypes.add(type);
    }

    public LogicalVariable getLabelCentroidVariable() {
        return labelCentroidVar;
    }

    public Object getLabelCentroidVarType() {
        return labelCentroidVarType;
    }

    public void setLabelCentroidVariable(LogicalVariable v, Object type) {
        this.labelCentroidVar = v;
        this.labelCentroidVarType = type;
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
