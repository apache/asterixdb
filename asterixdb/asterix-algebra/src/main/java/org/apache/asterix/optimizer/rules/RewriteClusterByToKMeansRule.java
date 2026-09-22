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
package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.clustering.ClusterByOptions;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ClusterByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.KMeansStageOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.NestedLoopJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StableSortPOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Expands a {@link ClusterByOperator} into the chain of stages that implements its algorithm, the way the
 * combiner rules expand one group-by into a local and a global one.
 * <p>
 * Everything the query said is on the operator; everything about how the algorithm is carried out is decided
 * here. That split is the point: a second algorithm is another {@link Expansion}, not another path through
 * the language layer.
 * <p>
 * Runs at the head of the physical phase -- after every logical rule, so they all see one opaque node with one
 * ordinary input, and before physical-operator assignment and property enforcement, which need the stages.
 */
public class RewriteClusterByToKMeansRule implements IAlgebraicRewriteRule {

    /** k-means||: rounds of oversampling, and the pool width drawn per round as a multiple of k. */
    private static final int OVERSAMPLING_ROUNDS = 5;
    private static final int OVERSAMPLING_FACTOR_PER_K = 2;
    /** Refinement iterations after seeding, when the query's 'num_iterations' names none. */
    private static final int LLOYD_ITERATIONS_DEFAULT = 3;
    private static final int LLOYD_ITERATIONS_MAX = 20;
    // Base for the per-round sampling seed (seed_r = base + r); the descriptor mixes in the partition id.
    // A prime, which keeps neighbouring (round, partition) pairs from starting correlated generator states.
    private static final long SEED_BASE = 1_000_003L;
    // RECLUSTER's roulette seed when the query supplies none. The value carries no meaning.
    private static final long RECLUSTER_SEED_DEFAULT = 12345L;

    private static final String ALGORITHM_KMEANS = ClusterByOptions.ALGORITHM_KMEANS;
    private static final String INIT_MODE_RANDOM = ClusterByOptions.INIT_MODE_RANDOM;

    /** The operator carries its options opaquely; this rule is the layer that knows their structure. */
    private static ClusterByOptions options(ClusterByOperator cop) {
        return (ClusterByOptions) cop.getOptions();
    }

    private static ClusterByOptions.KmeansOptions kmeans(ClusterByOperator cop) {
        return options(cop).getKmeans();
    }

    /**
     * How many Lloyd iterations to run: what the query asked for, capped at {@link #LLOYD_ITERATIONS_MAX}, or
     * {@link #LLOYD_ITERATIONS_DEFAULT} when it asked for nothing. Over the cap is clamped rather than
     * refused -- the request is for more refinement, and the ceiling is this rule's, not the query's.
     */
    private static int lloydIterations(ClusterByOperator cop) {
        Integer requested = kmeans(cop).getNumIterations();
        return requested == null ? LLOYD_ITERATIONS_DEFAULT : Math.min(requested, LLOYD_ITERATIONS_MAX);
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.CLUSTER_BY) {
            return false;
        }
        ClusterByOperator cop = (ClusterByOperator) op;
        opRef.setValue(expansionFor(cop).expand(cop, context));
        return true;
    }

    /**
     * The extension point. An algorithm is a method that turns this node into the stages implementing it;
     * adding one is adding a case here and accepting its name in the rewrite's option validation. Nothing
     * above this rule -- grammar, rewrite, translator, logical plan -- learns that the algorithm exists.
     */
    private Expansion expansionFor(ClusterByOperator cop) throws AlgebricksException {
        if (ALGORITHM_KMEANS.equals(options(cop).getAlgorithm())) {
            return this::expandKMeans;
        }
        // The rewrite rejects unknown algorithms; reaching this means a name was accepted without an expansion.
        throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE, cop.getSourceLocation(),
                "no expansion for CLUSTER BY algorithm " + options(cop).getAlgorithm());
    }

    @FunctionalInterface
    private interface Expansion {
        ILogicalOperator expand(ClusterByOperator cop, IOptimizationContext context) throws AlgebricksException;
    }

    /**
     * k-means: seed, then refine. {@code kmeans_parallel} grows an oversampled pool from a single centre and
     * reduces it to k before refining; {@code random} (Forgy) takes k starting points and refines them directly.
     * The input is read once: one REPLICATE feeds the seed draw, the loops and the labelling.
     */
    private ILogicalOperator expandKMeans(ClusterByOperator cop, IOptimizationContext context)
            throws AlgebricksException {
        SourceLocation loc = cop.getSourceLocation();
        LogicalVariable vectorVar = cop.getVectorVariable();
        boolean forgy = INIT_MODE_RANDOM.equals(kmeans(cop).getInitMode());

        // One REPLICATE over the input, built as IntroduceSecondaryIndexInsertDeleteRule builds its fan-out;
        // the enforcer adds the exchanges and FixReplicateOperatorOutputsRule re-points the outputs before
        // job generation. The seed draw and the loops store their input before they run, so their outputs
        // stream. The labelling join's probe side would run in the input's activity cluster while its build
        // side depends on that same cluster, which is a dependency cycle, so that last output is materialized
        // to give the probe its own cluster (ExtractCommonOperatorsRule.requiresMaterialization is the same
        // test).
        int outputArity = forgy ? 3 : 4;
        boolean[] materialize = new boolean[outputArity];
        materialize[outputArity - 1] = true;
        ReplicateOperator shared = new ReplicateOperator(outputArity, materialize);
        shared.setSourceLocation(loc);
        // The labelling branch below builds the real definition of the assignment centroid, so the
        // translator's placeholder is spliced out; the translator always creates it, and a miss is a bug.
        if (!spliceOutAssign(cop.getInputs().get(0), cop.getAssignedCentroidVariable(), context)) {
            throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE, loc,
                    "CLUSTER BY assignment-centroid placeholder not found below the operator");
        }
        shared.getInputs().add(cop.getInputs().get(0));
        finish(shared, context);

        // Forgy seeds with k centres and refines them directly; k-means|| grows a pool from one.
        Pair<Mutable<ILogicalOperator>, LogicalVariable> seedInput = branchOf(shared, vectorVar, context, loc);
        Mutable<ILogicalOperator> centroidsIn =
                seedOf(seedInput.first, seedInput.second, forgy ? kmeans(cop).getNumClusters() : 1,
                        options(cop).getDimension(), kmeans(cop).getSeed(), context, loc);
        LogicalVariable centroidsVar = seedInput.second;
        if (!forgy) {
            KMeansStageOperator recluster =
                    oversampleAndRecluster(cop, shared, centroidsIn, centroidsVar, context, loc);
            centroidsIn = new MutableObject<>(recluster);
            centroidsVar = recluster.getCandidateVariable();
        }

        KMeansStageOperator lloyd = refine(cop, shared, centroidsIn, centroidsVar, context, loc);
        AggregateOperator finalSet = centroidList(lloyd, context, loc);
        LogicalVariable cFinal = finalSet.getVariables().get(0);

        Labelled rows = label(cop, shared, finalSet, cFinal, context, loc);
        return clustersOf(cop, rows.op, rows.cid, context, loc);
    }

    /** k-means||: oversample a pool from the seed, then reduce it to k centres. */
    private KMeansStageOperator oversampleAndRecluster(ClusterByOperator cop, ReplicateOperator shared,
            Mutable<ILogicalOperator> seed, LogicalVariable seedVar, IOptimizationContext context, SourceLocation loc)
            throws AlgebricksException {
        Pair<Mutable<ILogicalOperator>, LogicalVariable> input =
                branchOf(shared, cop.getVectorVariable(), context, loc);
        KMeansStageOperator oversample = stage(cop, KMeansStageOperator.Mode.OVERSAMPLE_LOOP, context,
                ref(input.second), ref(seedVar), oversamplingWidth(cop),
                kmeans(cop).getSeed() == null ? SEED_BASE : kmeans(cop).getSeed(), OVERSAMPLING_ROUNDS);
        oversample.getInputs().add(input.first);
        oversample.getInputs().add(seed);
        finish(oversample, context);

        KMeansStageOperator recluster = stage(cop, KMeansStageOperator.Mode.RECLUSTER, context, null,
                ref(oversample.getCandidateVariable()), kmeans(cop).getNumClusters(),
                kmeans(cop).getSeed() == null ? RECLUSTER_SEED_DEFAULT : kmeans(cop).getSeed(), 0);
        recluster.getInputs().add(new MutableObject<>(oversample));
        finish(recluster, context);
        return recluster;
    }

    /** The refinement loop: emits the k final centroids and nothing else. */
    private KMeansStageOperator refine(ClusterByOperator cop, ReplicateOperator shared,
            Mutable<ILogicalOperator> centroidsIn, LogicalVariable centroidsVar, IOptimizationContext context,
            SourceLocation loc) throws AlgebricksException {
        Pair<Mutable<ILogicalOperator>, LogicalVariable> input =
                branchOf(shared, cop.getVectorVariable(), context, loc);
        KMeansStageOperator lloyd = stage(cop, KMeansStageOperator.Mode.LLOYD_LOOP, context, ref(input.second),
                ref(centroidsVar), kmeans(cop).getNumClusters(), 0L, lloydIterations(cop));
        lloyd.getInputs().add(input.first);
        lloyd.getInputs().add(centroidsIn);
        // The execution mode is derived from the input, as GROUP BY's is: a partitioned input gives one loop
        // instance per partition, an unpartitioned input a single instance. The physical operators read it
        // (AbstractKMeansStagePOperator.unpartitioned) to size and place the loop.
        finish(lloyd, context);
        return lloyd;
    }

    /**
     * The final centroid set as one list, ordered by centroid value: a cluster id is the position of the
     * nearest centroid in this list, and the loop's output order varies run to run. The aggregate is global, so
     * the enforcer sort-merges the partitions' streams into it.
     */
    private AggregateOperator centroidList(KMeansStageOperator lloyd, IOptimizationContext context, SourceLocation loc)
            throws AlgebricksException {
        LogicalVariable centroidVar = lloyd.getCandidateVariable();
        OrderOperator byValue = new OrderOperator();
        byValue.setSourceLocation(loc);
        byValue.getOrderExpressions().add(new Pair<>(OrderOperator.ASC_ORDER, ref(centroidVar)));
        byValue.getInputs().add(new MutableObject<>(lloyd));
        finish(byValue, context);

        AggregateFunctionCallExpression listify = BuiltinFunctions
                .makeAggregateFunctionExpression(BuiltinFunctions.LISTIFY, new ArrayList<>(List.of(ref(centroidVar))));
        listify.setSourceLocation(loc);
        AggregateOperator finalSet = new AggregateOperator(new ArrayList<>(List.of(context.newVar())),
                new ArrayList<>(List.of(new MutableObject<>(listify))));
        finalSet.setSourceLocation(loc);
        finalSet.setGlobal(true);
        finalSet.getInputs().add(new MutableObject<>(byValue));
        finish(finalSet, context);
        return finalSet;
    }

    /** The labelled rows: the operator at the top and the cluster-id variable. */
    private static final class Labelled {
        final ILogicalOperator op;
        final LogicalVariable cid;

        Labelled(ILogicalOperator op, LogicalVariable cid) {
            this.op = op;
            this.cid = cid;
        }
    }

    /**
     * Labels every row. The last replicate branch carries the rows under their original variables, and the
     * single-tuple centroid list is attached to each row by a nested-loop join on TRUE with the list side
     * broadcast. A row the labelling cannot place, where nearest-centroid returns NULL with a warning, is
     * dropped so it cannot form a (k+1)-th NULL-keyed cluster.
     */
    private Labelled label(ClusterByOperator cop, ReplicateOperator shared, AggregateOperator finalSet,
            LogicalVariable cFinal, IOptimizationContext context, SourceLocation loc) throws AlgebricksException {
        LogicalVariable vectorVar = cop.getVectorVariable();
        Mutable<ILogicalOperator> rows = new MutableObject<>(shared);
        shared.getOutputs().add(rows);
        InnerJoinOperator attach = new InnerJoinOperator(
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE))), rows,
                new MutableObject<>(finalSet));
        attach.setSourceLocation(loc);
        attach.setPhysicalOperator(new NestedLoopJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER,
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
        finish(attach, context);

        Mutable<ILogicalExpression> metric = new MutableObject<>(
                new ConstantExpression(new AsterixConstantValue(new AString(kmeans(cop).getMetric()))));
        LogicalVariable rowCid = context.newVar();
        ScalarFunctionCallExpression nearest = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.NEAREST_CENTROID), ref(vectorVar), ref(cFinal),
                metric);
        nearest.setSourceLocation(loc);
        AssignOperator labelOp = new AssignOperator(rowCid, new MutableObject<>(nearest));
        labelOp.setSourceLocation(loc);
        labelOp.getInputs().add(new MutableObject<>(attach));
        finish(labelOp, context);

        // The row's assignment centroid: its entry in the final list. Constant within a group, so the
        // reported centroid aggregates it with FIRST, and a pushed members-subquery reads it per row.
        ScalarFunctionCallExpression pick = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.GET_ITEM), ref(cFinal), ref(rowCid));
        pick.setSourceLocation(loc);
        AssignOperator assignedOp = new AssignOperator(cop.getAssignedCentroidVariable(), new MutableObject<>(pick));
        assignedOp.setSourceLocation(loc);
        assignedOp.getInputs().add(new MutableObject<>(labelOp));
        finish(assignedOp, context);

        ScalarFunctionCallExpression unknown = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.IS_UNKNOWN), ref(rowCid));
        unknown.setSourceLocation(loc);
        ScalarFunctionCallExpression placed = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.NOT), new MutableObject<>(unknown));
        placed.setSourceLocation(loc);
        SelectOperator labelled = new SelectOperator(new MutableObject<>(placed));
        labelled.setSourceLocation(loc);
        labelled.getInputs().add(new MutableObject<>(assignedOp));
        finish(labelled, context);
        return new Labelled(labelled, rowCid);
    }

    /**
     * Removes the definition of {@code var} from the chain below {@code top}. A single-variable assign is
     * spliced out whole; a multi-variable assign, which ConsolidateAssignsRule can produce by merging,
     * sheds just that variable and its expression.
     */
    private static boolean spliceOutAssign(Mutable<ILogicalOperator> top, LogicalVariable var,
            IOptimizationContext context) throws AlgebricksException {
        List<ILogicalOperator> above = new ArrayList<>();
        Mutable<ILogicalOperator> ref = top;
        while (ref.getValue().getInputs().size() == 1) {
            ILogicalOperator op = ref.getValue();
            if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN
                    && ((AssignOperator) op).getVariables().contains(var)) {
                AssignOperator assign = (AssignOperator) op;
                if (assign.getVariables().size() == 1) {
                    ref.setValue(op.getInputs().get(0).getValue());
                } else {
                    int idx = assign.getVariables().indexOf(var);
                    assign.getVariables().remove(idx);
                    assign.getExpressions().remove(idx);
                    assign.recomputeSchema();
                    context.computeAndSetTypeEnvironmentForOperator(assign);
                }
                // The operators the walk passed sit above the removed definition, where a projection may list
                // the variable and every cached schema still carries it. Either would make jobgen compile a
                // pass-through against a missing column, so drop the variable from project lists and
                // recompute the schemas bottom-up.
                for (int i = above.size() - 1; i >= 0; i--) {
                    ILogicalOperator a = above.get(i);
                    if (a.getOperatorTag() == LogicalOperatorTag.PROJECT) {
                        ((ProjectOperator) a).getVariables().remove(var);
                    }
                    a.recomputeSchema();
                    context.computeAndSetTypeEnvironmentForOperator(a);
                }
                return true;
            }
            above.add(op);
            ref = op.getInputs().get(0);
        }
        return false;
    }

    /** Sets what a rule-built operator must set itself: execution mode, schema, type environment. */
    private static void finish(AbstractLogicalOperator op, IOptimizationContext context) throws AlgebricksException {
        OperatorManipulationUtil.setOperatorMode(op);
        op.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(op);
    }

    /**
     * Turns the labelled rows into one tuple per cluster.
     * <p>
     * This is an ordinary GROUP BY on the assignment, with the pieces a cluster is made of hanging off it
     * as nested aggregates. It lives in the expansion since, from the optimizer's side, CLUSTER BY is one
     * operator and how it is carried out is this rule's business.
     */
    private ILogicalOperator clustersOf(ClusterByOperator cop, ILogicalOperator labelled, LogicalVariable rowCid,
            IOptimizationContext context, SourceLocation loc) throws AlgebricksException {
        GroupByOperator gby = new GroupByOperator();
        gby.setSourceLocation(loc);
        gby.addGbyExpression(cop.getClusterIdVariable(), ref(rowCid).getValue());
        // The decorations ride on every labelled row; the GROUP BY carries them out as the operator promised.
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : cop.getDecorList()) {
            gby.addDecorExpression(p.first, p.second.getValue().cloneExpression());
        }
        gby.getInputs().add(new MutableObject<>(labelled));

        // members -- or whatever the aggregate-pushdown rule rewrote the listify into: the operator's own
        // nested plans, moved wholesale onto the GROUP BY. Re-pointing each NESTED_TUPLE_SOURCE at the
        // GROUP BY is all the move needs; the fallback listify covers an operator without nested plans.
        if (cop.getNestedPlans().isEmpty()) {
            gby.getNestedPlans().add(aggregate(gby, cop.getMembersVariable(), BuiltinFunctions.LISTIFY,
                    cop.getMemberRecordRef().getValue(), context, loc));
        } else {
            for (ILogicalPlan nestedPlan : cop.getNestedPlans()) {
                for (Mutable<ILogicalOperator> root : nestedPlan.getRoots()) {
                    adoptNestedBranch(root, gby, context);
                }
                gby.getNestedPlans().add(nestedPlan);
            }
        }
        // The reported centroid IS the assignment centroid: constant within the group, picked with FIRST.
        gby.getNestedPlans().add(aggregate(gby, cop.getCentroidVariable(), BuiltinFunctions.FIRST_ELEMENT,
                ref(cop.getAssignedCentroidVariable()).getValue(), context, loc));
        // Mode and schema are set explicitly: nothing walks the plan afterwards filling them in.
        finish(gby, context);
        return gby;
    }

    /** Re-points a moved nested branch's NESTED_TUPLE_SOURCE at {@code gby} and refreshes the branch. */
    private void adoptNestedBranch(Mutable<ILogicalOperator> root, GroupByOperator gby, IOptimizationContext context)
            throws AlgebricksException {
        List<AbstractLogicalOperator> chain = new ArrayList<>();
        ILogicalOperator cur = root.getValue();
        chain.add((AbstractLogicalOperator) cur);
        while (!cur.getInputs().isEmpty()) {
            cur = cur.getInputs().get(0).getValue();
            chain.add((AbstractLogicalOperator) cur);
        }
        if (cur.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            ((NestedTupleSourceOperator) cur).getDataSourceReference().setValue(gby);
        }
        for (int i = chain.size() - 1; i >= 0; i--) {
            finish(chain.get(i), context);
        }
    }

    /** One nested aggregate over the group: {@code out <- fid(arg)}. */
    private ILogicalPlan aggregate(GroupByOperator gby, LogicalVariable out, FunctionIdentifier fid,
            ILogicalExpression arg, IOptimizationContext context, SourceLocation loc) throws AlgebricksException {
        NestedTupleSourceOperator nts = new NestedTupleSourceOperator(new MutableObject<>(gby));
        nts.setSourceLocation(loc);
        AggregateFunctionCallExpression call = BuiltinFunctions.makeAggregateFunctionExpression(fid,
                new ArrayList<>(List.of(new MutableObject<>(arg.cloneExpression()))));
        call.setSourceLocation(loc);
        AggregateOperator agg = new AggregateOperator(new ArrayList<>(List.of(out)),
                new ArrayList<>(List.of(new MutableObject<>(call))));
        agg.setSourceLocation(loc);
        // A nested tuple source without a schema fails at job generation.
        finish(nts, context);
        agg.getInputs().add(new MutableObject<>(nts));
        finish(agg, context);
        return new ALogicalPlanImpl(new MutableObject<>(agg));
    }

    /**
     * The {@code n} starting points, drawn uniformly from the vectors: order on a shuffle key and take the
     * first n.
     * <p>
     * The key is {@code random(vec[0])}, since ordering by the vector's value would return the n most
     * similar points and seat every centre in one corner of the data, a fixed point refinement cannot escape.
     */
    private Mutable<ILogicalOperator> seedOf(Mutable<ILogicalOperator> vectors, LogicalVariable vectorVar, int n,
            int dimension, Integer querySeed, IOptimizationContext context, SourceLocation loc)
            throws AlgebricksException {
        // Only usable vectors may be drawn, since a rejected draw loses the whole answer and a row with no
        // vector makes random(v[0]) unknown, which orders first. The guard uses total functions the columnar
        // filter pushdown refuses (is-array, sql-count), so no conjunct can be pushed into the scan where it
        // would be evaluated per array element.
        ScalarFunctionCallExpression isArray = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.IS_ARRAY), ref(vectorVar));
        isArray.setSourceLocation(loc);
        ScalarFunctionCallExpression width = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.SCALAR_SQL_COUNT), ref(vectorVar));
        width.setSourceLocation(loc);
        ScalarFunctionCallExpression widthOk =
                new ScalarFunctionCallExpression(BuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.EQ),
                        new MutableObject<>(width), constant((long) dimension));
        widthOk.setSourceLocation(loc);
        ScalarFunctionCallExpression usable = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND), new MutableObject<>(isArray),
                new MutableObject<>(widthOk));
        usable.setSourceLocation(loc);
        SelectOperator guard = new SelectOperator(new MutableObject<>(usable));
        guard.setSourceLocation(loc);
        guard.getInputs().add(vectors);
        finish(guard, context);

        ScalarFunctionCallExpression firstComponent = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.GET_ITEM), ref(vectorVar), constant(0L));
        firstComponent.setSourceLocation(loc);
        // A query seed shifts the draw key, so different seeds draw different rows from the same data.
        ILogicalExpression keyArg = firstComponent;
        if (querySeed != null) {
            ScalarFunctionCallExpression shifted = new ScalarFunctionCallExpression(
                    BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.NUMERIC_ADD),
                    new MutableObject<>(firstComponent), constant(querySeed.longValue()));
            shifted.setSourceLocation(loc);
            keyArg = shifted;
        }
        ScalarFunctionCallExpression key = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.RANDOM_WITH_SEED),
                new MutableObject<>(keyArg));
        key.setSourceLocation(loc);

        LogicalVariable keyVar = context.newVar();
        AssignOperator assign = new AssignOperator(keyVar, new MutableObject<>(key));
        assign.setSourceLocation(loc);
        assign.getInputs().add(new MutableObject<>(guard));
        finish(assign, context);

        // A top-n sort: PushLimitIntoOrderByRule, which would fuse a sort and a limit, has already run.
        OrderOperator order = new OrderOperator(new ArrayList<>(), n);
        order.setSourceLocation(loc);
        order.getOrderExpressions().add(new Pair<>(OrderOperator.ASC_ORDER, ref(keyVar)));
        order.getInputs().add(new MutableObject<>(assign));
        // The pass that fuses a LIMIT into a top-n sort ran before this rule, so the bound is set here
        // directly, the way the labelling join sets its broadcast NLJ. Property enforcement then derives a
        // local sort from this one and inherits the bound onto it.
        order.setPhysicalOperator(new StableSortPOperator(n));
        finish(order, context);

        // The top-n bounds the sort; the limit bounds the stream.
        LimitOperator limit = new LimitOperator(constant((long) n).getValue());
        limit.setSourceLocation(loc);
        limit.getInputs().add(new MutableObject<>(order));
        finish(limit, context);
        return new MutableObject<>(limit);
    }

    private static Mutable<ILogicalExpression> constant(long v) {
        return new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt64(v))));
    }

    private KMeansStageOperator stage(ClusterByOperator cop, KMeansStageOperator.Mode mode,
            IOptimizationContext context, Mutable<ILogicalExpression> vectorRef, Mutable<ILogicalExpression> poolRef,
            int topCount, long seed, int loopRounds) {
        // Every stage emits vectors (a pool, the k candidates, the k centroids), typed open: their width is enforced
        // by the decoders, not by the type. The loop stages admit only numeric arrays of the declared width;
        // RECLUSTER reads decoded envelopes.
        KMeansStageOperator stage = new KMeansStageOperator(vectorRef, poolRef, context.newVar(), BuiltinType.ANY,
                topCount, mode, seed, loopRounds, options(cop).getDimension(), kmeans(cop).getMetric());
        stage.setSourceLocation(cop.getSourceLocation());
        return stage;
    }

    /** l = factor * k, the pool width per oversampling round. */
    private int oversamplingWidth(ClusterByOperator cop) throws AlgebricksException {
        try {
            return Math.multiplyExact(OVERSAMPLING_FACTOR_PER_K, kmeans(cop).getNumClusters());
        } catch (ArithmeticException e) {
            throw AlgebricksException.create(ErrorCode.ILLEGAL_STATE, cop.getSourceLocation(),
                    "CLUSTER BY num_clusters is too large: " + OVERSAMPLING_FACTOR_PER_K + " * "
                            + kmeans(cop).getNumClusters() + " overflows a 32-bit integer");
        }
    }

    private static Mutable<ILogicalExpression> ref(LogicalVariable v) {
        return new MutableObject<>(new VariableReferenceExpression(v));
    }

    /**
     * One consumer's branch off the shared input: an ASSIGN giving the vector a variable of the branch's own,
     * so no stage sees the same variable on two of its inputs (the seed stream feeds the oversample loop, whose
     * other input is the vectors).
     */
    private Pair<Mutable<ILogicalOperator>, LogicalVariable> branchOf(ReplicateOperator shared,
            LogicalVariable vectorVar, IOptimizationContext context, SourceLocation loc) throws AlgebricksException {
        LogicalVariable branchVar = context.newVar();
        AssignOperator rename = new AssignOperator(branchVar, ref(vectorVar));
        rename.setSourceLocation(loc);
        Mutable<ILogicalOperator> fromShared = new MutableObject<>(shared);
        shared.getOutputs().add(fromShared);
        rename.getInputs().add(fromShared);
        finish(rename, context);
        // Only the vector rides into a training branch: the member record the shared replicate carries is
        // projected away here, so the seed sort and the stages move vector-wide tuples, not whole rows.
        ProjectOperator thin = new ProjectOperator(new ArrayList<>(List.of(branchVar)));
        thin.setSourceLocation(loc);
        thin.getInputs().add(new MutableObject<>(rename));
        finish(thin, context);
        return new Pair<>(new MutableObject<>(thin), branchVar);
    }
}
