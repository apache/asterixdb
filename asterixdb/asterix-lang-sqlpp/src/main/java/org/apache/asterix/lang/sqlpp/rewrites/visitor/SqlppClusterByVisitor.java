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

package org.apache.asterix.lang.sqlpp.rewrites.visitor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.clustering.ClusterByOptions;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.QuantifiedPair;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.ConfigurationUtil;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.lang.sqlpp.clause.ClusterbyClause;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.JoinType;
import org.apache.asterix.lang.sqlpp.optype.UnnestType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.util.SqlppRewriteUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppSimpleExpressionVisitor;
import org.apache.asterix.object.base.AdmArrayNode;
import org.apache.asterix.object.base.AdmBigIntNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Validates a {@code CLUSTER BY} block and resolves its descriptor. A block
 *
 * <pre>
 *   FROM src AS t
 *   CLUSTER BY t.vec AS sc [CLUSTER AS members]
 *   WITH { "num_clusters": k, ... }
 *   SELECT ... sc.cluster_id ... sc.centroid ... members ...
 * </pre>
 *
 * keeps its clause: the translator emits one {@code CLUSTER_BY} logical operator, expanded into the k-means
 * stages by {@code RewriteClusterByToKMeansRule} in the physical phase. This pass checks the block's shape
 * (one CLUSTER BY, no GROUP BY, inner joins and inner UNNEST only, no set operation), validates the WITH
 * options and records the resolved ones on the clause, names the output variables, and substitutes the
 * descriptor's field accesses with those variables.
 * <p>
 * {@code CLUSTER AS} members hold the block's FROM and LET bindings, one field per binding, as
 * {@code GROUP AS} does. Supports K-Means only, with the {@code kmeans_parallel} (default) and
 * {@code random} init modes, the Euclidean(-squared) metrics, and an optional {@code num_iterations} count
 * of Lloyd iterations (3 by default, capped by the rule that expands the operator).
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public class SqlppClusterByVisitor extends AbstractSqlppSimpleExpressionVisitor {

    // WITH option keys, compared case-insensitively; named as the vector index's WITH options are.
    // These two are mandatory for every algorithm.
    private static final String OPT_ALGORITHM = "clustering_algorithm";
    private static final String OPT_DIMENSION = "dimension";
    // K-Means options. num_clusters is mandatory; 'seed' pins every randomized initialization choice.
    private static final String OPT_NUM_CLUSTERS = "num_clusters";
    private static final String OPT_INIT_MODE = "init_mode";
    private static final String OPT_SEED = "seed";
    private static final String OPT_NUM_ITERATIONS = "num_iterations";
    private static final String OPT_SIMILARITY = "similarity";
    private static final String OPT_CROSS_POLLINATION = "cross_pollination";

    // Variables bound by the enclosing query blocks, subquery LETs and quantifiers of the expression being
    // visited, innermost first; a CLUSTER BY block may not read any of them.
    private final Deque<Set<String>> enclosingBindings = new ArrayDeque<>();

    // Practical ceiling on the requested cluster count; also keeps the k-means|| pool width inside an int.
    private static final int MAX_NUM_CLUSTERS = 65536;

    private static final Set<String> KNOWN_OPTIONS = Set.of(OPT_ALGORITHM, OPT_NUM_CLUSTERS, OPT_SIMILARITY,
            OPT_CROSS_POLLINATION, OPT_INIT_MODE, OPT_DIMENSION, OPT_SEED, OPT_NUM_ITERATIONS);
    // Listed on an unknown-option error; a literal because Set.of iterates in a per-JVM salted order.
    private static final String KNOWN_OPTIONS_DISPLAY = "clustering_algorithm, dimension, num_clusters, init_mode, "
            + "seed, similarity, cross_pollination, num_iterations";
    // The two fields the cluster descriptor exposes; their accesses substitute to the output variables.
    private static final String SC_CLUSTER_ID = ClusterByOptions.FIELD_CLUSTER_ID;
    private static final String SC_CENTROID = ClusterByOptions.FIELD_CENTROID;

    // Only K-Means is supported.
    private static final String ALGORITHM_KMEANS = ClusterByOptions.ALGORITHM_KMEANS;
    private static final Set<String> KNOWN_ALGORITHMS = Set.of("k-means", ALGORITHM_KMEANS);
    // Metrics with a usable centroid update, which is the arithmetic mean for the Euclidean family; cosine
    // and dot would need a spherical update and are refused.
    private static final Set<VectorSimilarityMetric> SUPPORTED_METRICS =
            Set.of(VectorSimilarityMetric.EUCLIDEAN, VectorSimilarityMetric.EUCLIDEAN_SQUARED);
    // Listed on an unsupported value; sorted so the message does not depend on Set iteration order.
    private static final String SUPPORTED_METRICS_DISPLAY = SUPPORTED_METRICS.stream()
            .map(m -> m.canonical().toUpperCase(Locale.ROOT)).sorted().collect(Collectors.joining(", "));
    // "kmeans_parallel" (default) = k-means|| oversampling, drawing each point with probability
    // p_x = l * d^2(x, pool) / phi. "random" = k uniformly drawn vectors.
    private static final String INIT_MODE_KMEANS_PARALLEL = ClusterByOptions.INIT_MODE_KMEANS_PARALLEL;
    // Deprecated spelling of kmeans_parallel; accepted and canonicalised.
    private static final String INIT_MODE_KMEANSPP_DEPRECATED = "kmeanspp";
    private static final String INIT_MODE_RANDOM = ClusterByOptions.INIT_MODE_RANDOM;
    private static final Set<String> KNOWN_INIT_MODES =
            Set.of(INIT_MODE_KMEANS_PARALLEL, INIT_MODE_KMEANSPP_DEPRECATED, INIT_MODE_RANDOM);
    private final LangRewritingContext context;

    // Set for the post-inlining pass, which runs only the enclosing-variable rejection.
    private final boolean checkOnly;

    public SqlppClusterByVisitor(LangRewritingContext context) {
        this(context, false);
    }

    public SqlppClusterByVisitor(LangRewritingContext context, boolean checkOnly) {
        this.context = context;
        this.checkOnly = checkOnly;
    }

    @Override
    public Expression visit(SelectExpression selectExpression, ILangExpression arg) throws CompilationException {
        SelectBlock clusterBlock = clusterByBlockOf(selectExpression);
        if (clusterBlock != null) {
            if (checkOnly) {
                rejectEnclosingVariables(selectExpression, clusterBlock);
            } else {
                resolveClusterBy(selectExpression, clusterBlock);
            }
        }
        // A LET of the query itself is single-valued; a LET of a subquery is per row of the enclosing block.
        Set<String> subqueryLetVars = new HashSet<>();
        if (!enclosingBindings.isEmpty() && selectExpression.hasLetClauses()) {
            addNames(subqueryLetVars, SqlppVariableUtil.getLetBindingVariables(selectExpression.getLetList()));
        }
        enclosingBindings.push(subqueryLetVars);
        try {
            // Recurse: a subquery may carry a CLUSTER BY of its own.
            return super.visit(selectExpression, arg);
        } finally {
            enclosingBindings.pop();
        }
    }

    @Override
    public Expression visit(SelectBlock selectBlock, ILangExpression arg) throws CompilationException {
        Set<String> blockVars = new HashSet<>();
        addNames(blockVars, SqlppVariableUtil.getBindingVariables(selectBlock.getFromClause()));
        addNames(blockVars, SqlppVariableUtil.getLetBindingVariables(selectBlock.getLetWhereList()));
        if (selectBlock.hasGroupbyClause()) {
            addNames(blockVars, SqlppVariableUtil.getBindingVariables(selectBlock.getGroupbyClause()));
        }
        addNames(blockVars, SqlppVariableUtil.getLetBindingVariables(selectBlock.getLetHavingListAfterGroupby()));
        if (selectBlock.hasClusterbyClause()) {
            addNames(blockVars, SqlppVariableUtil.getBindingVariables(selectBlock.getClusterbyClause()));
        }
        enclosingBindings.push(blockVars);
        try {
            return super.visit(selectBlock, arg);
        } finally {
            enclosingBindings.pop();
        }
    }

    @Override
    public Expression visit(QuantifiedExpression qe, ILangExpression arg) throws CompilationException {
        Set<String> quantifiedVars = new HashSet<>();
        addNames(quantifiedVars,
                qe.getQuantifiedList().stream().map(QuantifiedPair::getVarExpr).collect(Collectors.toList()));
        enclosingBindings.push(quantifiedVars);
        try {
            return super.visit(qe, arg);
        } finally {
            enclosingBindings.pop();
        }
    }

    /** Adds the names of the given variables; a binding that has no variable (an unnamed GROUP AS) is skipped. */
    private static void addNames(Set<String> names, Collection<VariableExpr> vars) {
        for (VariableExpr v : vars) {
            if (v != null && v.getVar() != null) {
                names.add(v.getVar().getValue());
            }
        }
    }

    /**
     * A CLUSTER BY block runs once over its input; a subquery that depends on the enclosing query's row would
     * have to run once per row, which the operator cannot do (and the subplan flattening would otherwise nest the
     * expansion inside a GROUP BY, where it cannot be executed). So nothing in the block, before or after the
     * clause, may read a variable bound by an enclosing query block, subquery LET or quantifier.
     */
    private void rejectEnclosingVariables(SelectExpression selectExpression, SelectBlock selectBlock)
            throws CompilationException {
        Set<String> enclosing = new HashSet<>();
        for (Set<String> bindings : enclosingBindings) {
            enclosing.addAll(bindings);
        }
        if (enclosing.isEmpty()) {
            return;
        }
        Set<VariableExpr> free = new HashSet<>(SqlppVariableUtil.getFreeVariables(selectBlock));
        if (selectExpression.hasLetClauses()) {
            // The query's own WITH/LET: single-valued unless it reads the enclosing row itself.
            for (LetClause letClause : selectExpression.getLetList()) {
                free.addAll(SqlppVariableUtil.getFreeVariables(letClause.getBindingExpr()));
            }
        }
        if (selectExpression.hasOrderby()) {
            free.addAll(SqlppVariableUtil.getFreeVariables(selectExpression.getOrderbyClause()));
        }
        if (selectExpression.hasLimit()) {
            free.addAll(SqlppVariableUtil.getFreeVariables(selectExpression.getLimitClause()));
        }
        for (VariableExpr freeVar : free) {
            String name = freeVar.getVar().getValue();
            if (enclosing.contains(name)) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, freeVar.getSourceLocation(),
                        "CLUSTER BY: '" + SqlppVariableUtil.toUserDefinedName(name)
                                + "' belongs to an enclosing query; a query block with CLUSTER BY cannot depend on"
                                + " the enclosing query's row yet.");
            }
        }
    }

    /** The left select block of {@code selectExpression} iff it carries a CLUSTER BY clause; else null. */
    private SelectBlock clusterByBlockOf(SelectExpression selectExpression) {
        SelectSetOperation setOp = selectExpression.getSelectSetOperation();
        SelectBlock leftBlock = blockWithClusterby(setOp.getLeftInput());
        if (leftBlock != null) {
            return leftBlock;
        }
        if (setOp.hasRightInputs()) {
            for (SetOperationRight right : setOp.getRightInputs()) {
                SelectBlock rightBlock = blockWithClusterby(right.getSetOperationRightInput());
                if (rightBlock != null) {
                    return rightBlock;
                }
            }
        }
        return null;
    }

    private static SelectBlock blockWithClusterby(SetOperationInput input) {
        if (!input.selectBlock()) {
            return null;
        }
        SelectBlock selectBlock = input.getSelectBlock();
        return selectBlock != null && selectBlock.hasClusterbyClause() ? selectBlock : null;
    }

    /**
     * Resolves a CLUSTER BY clause in place, so the translator can build an operator from it.
     * <p>
     * The clause stays on the block. This validates the WITH options, records them on the clause, names the
     * variables the operator produces, and points the descriptor's fields at them. Nothing is copied and
     * nothing is rebuilt: the operator consumes the block's own pipeline and emits one tuple per cluster.
     */
    private void resolveClusterBy(SelectExpression selectExpression, SelectBlock selectBlock)
            throws CompilationException {
        ClusterbyClause cbc = selectBlock.getClusterbyClause();
        SourceLocation loc = cbc.getSourceLocation();
        rejectEnclosingVariables(selectExpression, selectBlock);
        rejectUnsupportedShapes(selectExpression, selectBlock, loc);
        cbc.setResolvedOptions(resolveOptions(cbc));

        // The declared width is not a WHERE on the block: the columnar filter pushdown would split it from its
        // is-array guard and evaluate len() per array element inside the scan. The stages' decoders enforce it
        // on the assembled value, skipping a row of the wrong shape with a warning; the expansion guards its
        // seed draws with non-pushable functions.

        // The operator's output. members always exists, whether or not the query named it (CLUSTER AS is
        // optional in the grammar): it is what carries the rows through, and the centroid is derived from the
        // same assignment.
        cbc.setClusterIdVar(newVar(loc));
        cbc.setCentroidVar(newVar(loc));
        if (!cbc.hasClusterMembersVar()) {
            cbc.setClusterMembersVar(newVar(loc));
        }
        // What a member looks like: one field per FROM binding and per LET of the block, mirroring
        // SqlppGroupByVisitor.createGroupFieldList. The block's pipeline is translated once below the operator,
        // so a LET variable is as available to the clause and to the member record as a FROM variable.
        if (!cbc.hasClusterFieldList()) {
            List<Pair<Expression, Identifier>> memberFields = new ArrayList<>();
            for (VariableExpr fromVarExpr : SqlppVariableUtil.getBindingVariables(selectBlock.getFromClause())) {
                SqlppVariableUtil.addToFieldVariableList(fromVarExpr, memberFields);
            }
            for (VariableExpr letVarExpr : SqlppVariableUtil.getLetBindingVariables(selectBlock.getLetWhereList())) {
                SqlppVariableUtil.addToFieldVariableList(letVarExpr, memberFields);
            }
            cbc.setClusterFieldList(memberFields);
        }

        // Detached for the substitution, then put back: SqlppSubstituteExpressionVisitor refuses to replace an
        // expression whose free variables are live in the current scope, and the clause is what keeps the
        // descriptor variable live.
        selectBlock.setClusterbyClause(null);
        if (cbc.hasClusterDescriptorVar()) {
            substituteDescriptorFields(selectExpression, cbc, loc);
        }
        selectBlock.setClusterbyClause(cbc);
    }

    /** Rejects the block shapes the operator does not support. */
    private void rejectUnsupportedShapes(SelectExpression selectExpression, SelectBlock selectBlock, SourceLocation loc)
            throws CompilationException {
        // This rewrite is per-SelectExpression, not per-branch: clusterByBlockOf returns the first CLUSTER BY
        // block it finds and runs once, so a second branch's clause would survive unresolved.
        if (selectExpression.getSelectSetOperation().hasRightInputs()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY is not supported with set operations (UNION/INTERSECT/EXCEPT).");
        }
        // Several FROM terms and correlate clauses are fine: the block's pipeline is translated once below
        // the operator, whatever its shape.
        FromClause fromClause = selectBlock.getFromClause();
        // Both are defensive. The grammar requires a FROM clause here, and in practice every term carries a
        // variable: an unaliased source would swallow CLUSTER as its alias and fail to parse.
        if (fromClause == null || fromClause.getFromTerms().isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc, "CLUSTER BY requires a FROM clause.");
        }
        for (FromTerm term : fromClause.getFromTerms()) {
            if (term.getLeftVariable() == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                        "CLUSTER BY requires every FROM term to bind a variable.");
            }
            // An unmatched row leaves the clustering expression MISSING, and every stage downstream assumes
            // a real vector. UnnestClause is a sibling of JoinClause, so each needs its own guard.
            for (AbstractBinaryCorrelateClause correlate : term.getCorrelateClauses()) {
                if (correlate instanceof JoinClause && ((JoinClause) correlate).getJoinType() != JoinType.INNER) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                            "CLUSTER BY currently supports inner joins only; an outer join can leave the "
                                    + "clustering expression MISSING.");
                }
                if (correlate instanceof UnnestClause
                        && ((UnnestClause) correlate).getUnnestType() != UnnestType.INNER) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                            "CLUSTER BY currently supports inner UNNEST only; an outer UNNEST can leave the "
                                    + "clustering expression MISSING.");
                }
            }
        }
    }

    /**
     * Replaces every descriptor read with the variable the operator produces: each {@code <descriptor>.<field>}
     * by its own, and the descriptor read as a whole value by a record of them all. Every value is a variable
     * reference, so a read is safe at any depth -- inside a members subquery included.
     * <p>
     * One map covers both, so the descriptor reads like any other record: substitution matches
     * outermost-first, so a known field access is replaced whole and an unknown one falls through to the bare
     * variable inside it, becoming a field of the record -- MISSING, as {@code r.nosuchfield} is anywhere.
     */
    private void substituteDescriptorFields(SelectExpression selectExpression, ClusterbyClause cbc, SourceLocation loc)
            throws CompilationException {
        VariableExpr scVar = cbc.getClusterDescriptorVar();
        // Named once, so a new field reaches both the field accesses and the whole-value record.
        Map<String, VarIdentifier> fields = new LinkedHashMap<>();
        fields.put(SC_CLUSTER_ID, cbc.getClusterIdVar().getVar());
        fields.put(SC_CENTROID, cbc.getCentroidVar().getVar());

        List<FieldBinding> fbList = new ArrayList<>(fields.size());
        Map<Expression, Expression> subst = new HashMap<>();
        for (Map.Entry<String, VarIdentifier> field : fields.entrySet()) {
            subst.put(fieldAccess(scVar, field.getKey(), loc), new VariableExpr(field.getValue()));
            LiteralExpr nameLit = new LiteralExpr(new StringLiteral(field.getKey()));
            nameLit.setSourceLocation(loc);
            fbList.add(new FieldBinding(nameLit, new VariableExpr(field.getValue())));
        }
        RecordConstructor descriptorRecord = new RecordConstructor(fbList);
        descriptorRecord.setSourceLocation(loc);
        subst.put(new VariableExpr(scVar.getVar()), descriptorRecord);
        SqlppRewriteUtil.substituteExpression(selectExpression, subst, context);
    }

    private FieldAccessor fieldAccess(VariableExpr recordVar, String field, SourceLocation loc) {
        FieldAccessor fa = new FieldAccessor(new VariableExpr(recordVar.getVar()), new Identifier(field));
        fa.setSourceLocation(loc);
        return fa;
    }

    private VariableExpr newVar(SourceLocation loc) {
        return varRef(context.newVariable(), loc);
    }

    private VariableExpr varRef(VarIdentifier var, SourceLocation loc) {
        VariableExpr ref = new VariableExpr(var);
        ref.setSourceLocation(loc);
        return ref;
    }

    /**
     * Resolves the WITH record into the finished {@link ClusterByOptions}: one walk collects the raw values,
     * rejecting unknown keys, then each option is validated in an order that lets every error name its own
     * option. {@code dimension} stays a raw node (it may be an array); the scalar options flatten to strings.
     */
    private ClusterByOptions resolveOptions(ClusterbyClause cbc) throws CompilationException {
        SourceLocation loc = cbc.getSourceLocation();
        Map<String, String> opts = new HashMap<>();
        IAdmNode dimensionNode = null;
        if (cbc.hasWithOptions()) {
            for (Map.Entry<String, IAdmNode> e : ExpressionUtils.toNode(cbc.getWithOptions()).getFields()) {
                String key = e.getKey().toLowerCase();
                if (!KNOWN_OPTIONS.contains(key)) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                            "Unknown CLUSTER BY option '" + key + "'. Known options: " + KNOWN_OPTIONS_DISPLAY);
                }
                if (OPT_DIMENSION.equals(key)) {
                    dimensionNode = e.getValue();
                } else {
                    opts.put(key, ConfigurationUtil.getStringValue(e.getValue()));
                }
            }
        }
        // The algorithm first: it decides which of the options below apply. Everything after it is K-Means';
        // a second algorithm brings its own set.
        String algorithm = opts.get(OPT_ALGORITHM);
        if (algorithm == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY requires the 'clustering_algorithm' option. Supported: K-Means.");
        }
        if (!KNOWN_ALGORITHMS.contains(algorithm.toLowerCase())) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "Unsupported CLUSTER BY 'clustering_algorithm' '" + algorithm + "'. Supported: K-Means.");
        }
        int k = numClustersOf(opts, loc);
        rejectCrossPollination(opts, loc);
        String metric = metricOf(opts, loc);
        Integer seed = seedOf(opts, loc);
        Integer numIterations = numIterationsOf(opts, loc);
        String initMode = initModeOf(opts, loc);
        int dimension = dimensionOf(dimensionNode, loc);
        return new ClusterByOptions(ALGORITHM_KMEANS, dimension,
                new ClusterByOptions.KmeansOptions(k, initMode, metric, seed, numIterations));
    }

    /** {@code num_clusters}: mandatory for K-Means, a positive integer, at most {@link #MAX_NUM_CLUSTERS}. */
    private static int numClustersOf(Map<String, String> opts, SourceLocation loc) throws CompilationException {
        String numClusters = opts.get(OPT_NUM_CLUSTERS);
        if (numClusters == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY with K-Means requires the 'num_clusters' option.");
        }
        int k;
        try {
            k = Integer.parseInt(numClusters.trim());
            if (k <= 0) {
                throw new NumberFormatException(numClusters);
            }
        } catch (NumberFormatException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY 'num_clusters' must be a positive integer, but was: " + numClusters);
        }
        if (k > MAX_NUM_CLUSTERS) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY 'num_clusters' must be at most " + MAX_NUM_CLUSTERS + ", but was: " + k);
        }
        return k;
    }

    /**
     * Cross-pollination (overlapping clusters) is not implemented, but only a request to turn it ON is an
     * error: false asks for the disjoint clusters this release already produces. Accepting a true would
     * silently hand back disjoint clusters to a query that asked for overlapping ones.
     */
    private static void rejectCrossPollination(Map<String, String> opts, SourceLocation loc)
            throws CompilationException {
        String crossPollination = opts.get(OPT_CROSS_POLLINATION);
        if (crossPollination == null) {
            return;
        }
        String value = crossPollination.trim();
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY 'cross_pollination' must be true or false, but was: " + crossPollination);
        }
        if (Boolean.parseBoolean(value)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY cross-pollination is currently not enabled; clusters are always disjoint.");
        }
    }

    /**
     * The canonical name of the metric every stage measures with. {@code similarity} is optional (absent
     * means squared Euclidean) and resolved through the same taxonomy the vector index resolves its own
     * similarity option through; unknown names and metrics without a matching centroid update (dot) are
     * rejected.
     * <p>
     * EUCLIDEAN normalizes to EUCLIDEAN_SQUARED: they name the same clustering, since a cluster assignment is
     * an argmin and squaring is monotone, but the oversampling draw probability is defined on d^2, so the two
     * spellings would otherwise sample differently. The squared form is the one both mean.
     */
    private static String metricOf(Map<String, String> opts, SourceLocation loc) throws CompilationException {
        String similarity = opts.get(OPT_SIMILARITY);
        VectorSimilarityMetric metric = similarity == null ? VectorSimilarityMetric.EUCLIDEAN_SQUARED
                : VectorSimilarityMetric.fromAlias(similarity);
        if (metric == null || !SUPPORTED_METRICS.contains(metric)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc, "CLUSTER BY 'similarity' '" + similarity
                    + "' is not supported. Supported: " + SUPPORTED_METRICS_DISPLAY + ".");
        }
        if (metric == VectorSimilarityMetric.EUCLIDEAN) {
            metric = VectorSimilarityMetric.EUCLIDEAN_SQUARED;
        }
        return metric.canonical();
    }

    /** The query's {@code seed}, a 32-bit integer, or null when absent -- each consumer of a null applies its
     * own built-in default. */
    private static Integer seedOf(Map<String, String> opts, SourceLocation loc) throws CompilationException {
        String seed = opts.get(OPT_SEED);
        if (seed == null) {
            return null;
        }
        try {
            return Integer.valueOf(seed.trim());
        } catch (NumberFormatException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY 'seed' must be a 32-bit integer, but was: " + seed);
        }
    }

    /**
     * The query's {@code num_iterations}, a positive integer, or null when absent -- the rule that expands the
     * operator applies its own default and caps what the query asked for. Optional: how many refinement
     * iterations to run is a quality/cost trade the query may make, within a bound it need not know.
     */
    private static Integer numIterationsOf(Map<String, String> opts, SourceLocation loc) throws CompilationException {
        String numIterations = opts.get(OPT_NUM_ITERATIONS);
        if (numIterations == null) {
            return null;
        }
        try {
            int iterations = Integer.parseInt(numIterations.trim());
            if (iterations <= 0) {
                throw new NumberFormatException(numIterations);
            }
            return iterations;
        } catch (NumberFormatException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY 'num_iterations' must be a positive integer, but was: " + numIterations);
        }
    }

    /** The validated {@code init_mode}, canonicalised ({@link #INIT_MODE_KMEANS_PARALLEL} default). */
    private static String initModeOf(Map<String, String> opts, SourceLocation loc) throws CompilationException {
        String mode = opts.get(OPT_INIT_MODE);
        if (mode == null) {
            return INIT_MODE_KMEANS_PARALLEL;
        }
        String lower = mode.toLowerCase();
        if (!KNOWN_INIT_MODES.contains(lower)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "Unknown CLUSTER BY 'init_mode' '" + mode + "'. Supported: kmeans_parallel, random.");
        }
        return INIT_MODE_KMEANSPP_DEPRECATED.equals(lower) ? INIT_MODE_KMEANS_PARALLEL : lower;
    }

    /**
     * The declared vector width, required whatever the algorithm: an open-type dataset carries no schema to
     * infer it from, and inferring it from the first row would make the plan depend on which row happened to
     * arrive first.
     * <p>
     * An array, so that clustering on several fields can declare one width each. How many it must hold is the
     * algorithm's to say: k-means clusters a single field (the grammar admits only one clustering expression),
     * so it allows exactly one. Since that array always holds exactly one element, a plain positive integer is
     * accepted as well and means the same thing: {@code "dimension": 384} is {@code "dimension": [384]}.
     */
    private static int dimensionOf(IAdmNode node, SourceLocation loc) throws CompilationException {
        if (node == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY requires the 'dimension' option: the width of each clustering vector, as a "
                            + "positive integer or a one-element array, e.g. \"dimension\": 384.");
        }
        IAdmNode dimNode;
        if (node.getType() == ATypeTag.ARRAY) {
            AdmArrayNode dims = (AdmArrayNode) node;
            if (dims.size() != 1) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                        "CLUSTER BY with K-Means clusters a single field, so 'dimension' must hold exactly one "
                                + "element, but held " + dims.size() + ".");
            }
            dimNode = dims.get(0);
            if (dimNode.getType() != ATypeTag.BIGINT) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                        "CLUSTER BY 'dimension' must contain integers, but contained " + dimNode.getType() + ".");
            }
        } else {
            dimNode = node;
            if (dimNode.getType() != ATypeTag.BIGINT) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                        "CLUSTER BY 'dimension' must be a positive integer or an array of positive integers, "
                                + "e.g. 384 or [384], but was: " + dimNode.getType() + ".");
            }
        }
        long dim = ((AdmBigIntNode) dimNode).get();
        if (dim <= 0 || dim > Integer.MAX_VALUE) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, loc,
                    "CLUSTER BY 'dimension' must be a positive integer, but was: " + dim + ".");
        }
        return (int) dim;
    }
}
