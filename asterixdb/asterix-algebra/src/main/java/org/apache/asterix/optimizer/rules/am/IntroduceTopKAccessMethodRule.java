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
package org.apache.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.asterix.common.annotations.AnnSearchPreferenceAnnotation;
import org.apache.asterix.common.annotations.SkipSecondaryIndexSearchExpressionAnnotation;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.metadata.declared.IIndexProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.cost.VectorIndexGeometry;
import org.apache.asterix.optimizer.rules.VectorIncludeFilterPushdown;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Optimization rule for introducing vector index access for top-k ANN queries.
 *
 * Pattern: LIMIT k → ORDER BY ANN_DISTANCE(vectorField, queryVector, metric) → ... → DATASOURCE_SCAN
 *
 * Transformation:
 * - If vector index exists: Replaces DATASOURCE_SCAN with UNNEST-MAP(vector_index_search)
 *   → Returns candidate tuples for approximate ANN search
 * - If no vector index: Leaves plan unchanged
 *   → Falls back to exact KNN search (exhaustive distance computation on all tuples)
 *
 * The ORDER BY ANN_DISTANCE operator handles distance computation in both cases.
 *
 * <h2>Why this rule does not push the WHERE into the index search</h2>
 *
 * A {@code WHERE} over the index's {@code INCLUDE} columns is evaluated by the vector cursor itself, which
 * reads it from the unnest-map's {@code selectCondition}. This rule never sets that condition, even for the
 * index-only plan, which it could: it stops at rebinding the predicate and leaves it in its {@code SELECT}
 * for {@link org.apache.asterix.optimizer.rules.PushFilterIntoVectorSearchRule} to move in during
 * {@code physicalRewritesTopLevel}. That split is deliberate.
 * <p>
 * A {@code selectCondition} is the one expression on an operator that reads variables the operator
 * <em>produces</em> rather than variables from its input. Rules that walk an operator's expressions
 * reasonably type them against the input environment, so a condition present while the logical rewrites are
 * still running breaks them. Two rules in {@code buildPlanCleanupRuleCollection} — which runs immediately
 * after this one — do exactly that and fail on it:
 * {@code SetClosedRecordConstructorsRule} ("Could not infer type for variable") and
 * {@code InjectTypeCastForFunctionArgumentsRule} (NPE in the type computer, for a predicate containing
 * {@code switch-case} or one of the {@code if-missing}/{@code if-null} family). Both are latent rather than
 * broken today precisely because every producer of a {@code selectCondition} —
 * {@code PushFilterIntoVectorSearchRule} and {@code PushLimitIntoPrimarySearchRule} — runs in the physical
 * rewrites, after the last rule that would trip over one.
 * <p>
 * What this rule must still do early is rebind the predicate. The index-only branch drops the primary
 * lookup, so the record variable the {@code WHERE} reads ceases to exist and
 * {@code VectorIndexAccessMethod#neutralizeDanglingExpressions} would collapse the predicate to
 * {@code select(missing)} and drop every row. So the branch declares the {@code INCLUDE} columns as outputs
 * of the index-search unnest-map and rewrites the predicate to read those variables instead — after which
 * it is an ordinary predicate over ordinary variables, in an ordinary {@code SELECT}, and every rule between
 * here and the physical rewrites sees a plan it already understands.
 */
public class IntroduceTopKAccessMethodRule extends AbstractIntroduceAccessMethodRule {

    private static final Logger LOGGER = LogManager.getLogger();

    // Operators representing the pattern to be matched
    protected Mutable<ILogicalOperator> limitRef = null;
    protected LimitOperator limitOp = null;
    protected Mutable<ILogicalOperator> orderRef = null;
    protected OrderOperator orderOp = null;
    protected AbstractFunctionCallExpression annDistanceExpr = null;
    protected IVariableTypeEnvironment typeEnvironment = null;
    protected final OptimizableOperatorSubTree subTree = new OptimizableOperatorSubTree();
    protected VectorSimilarityMetric queryDistanceMetric = null;

    // Search effort the ANN hint asks for, which is what a vector plan costs rather than anything the
    // optimizer chooses. Unset until the hint is read.
    protected double queryMinProbeFraction = 0;
    protected int queryKMultiplier = 0;

    // The WHERE between the ORDER and the scan, and how many SELECTs carry it. The pushdown installs one
    // condition on the search, so a subtree holding more than one SELECT is refused an index outright rather
    // than have one of them left above the search; see chooseVectorIndex.
    protected SelectOperator selectOp = null;
    protected int numSelectOps = 0;

    /**
     * Master switch for the index-only ANN plan optimization. Enabled by default: when everything the plan
     * reads above the LIMIT is covered by the index -- the primary keys and the INCLUDE columns -- the plan
     * emits (pk, dist, INCLUDE columns) directly from the secondary VTree and skips the primary BTree lookup
     * + rerank. The plan rewrite handles above-LIMIT query-parameter ASSIGNs (see
     * {@link #isProjectionCoveredByIndex}), and the secondary VTree reconciles anti-matter (delete) tuples
     * itself, so an index-only ANN query over a dataset with deletes returns no deleted rows.
     */
    private static final boolean INDEX_ONLY_ENABLED = true;

    /**
     * Ancestor chain from the rewrite-pre entry point down to (but not including) the matched
     * {@code limitOp}. Populated by {@link #checkAndApplyTopKTransformation} as it recurses; consumed
     * by {@link #isProjectionCoveredByIndex} to gather variables used above the LIMIT.
     */
    protected final List<AbstractLogicalOperator> aboveLimitOps = new ArrayList<>();

    // Register vector index access method
    protected static Map<FunctionIdentifier, List<IAccessMethod>> accessMethods = new HashMap<>();

    static {
        registerAccessMethod(VectorIndexAccessMethod.INSTANCE, accessMethods);
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        clear();
        boolean adviseIndex = context.getIndexAdvisor().getAdvise();
        if (adviseIndex && context.getIndexAdvisor().getFakeIndexProvider() != null) {
            setMetadataIndexDeclarations(context, (IIndexProvider) context.getIndexAdvisor().getFakeIndexProvider());
        } else {
            setMetadataIndexDeclarations(context, (IIndexProvider) context.getMetadataProvider());
        }

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        // Already checked?
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        // Start from root operators
        if (op.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT
                && op.getOperatorTag() != LogicalOperatorTag.SINK
                && op.getOperatorTag() != LogicalOperatorTag.DELEGATE_OPERATOR) {
            return false;
        }

        // Recursively find pattern: LIMIT → ORDER BY ANN_DISTANCE → ... → DATASOURCE_SCAN
        boolean planTransformed = checkAndApplyTopKTransformation(opRef, context);

        if (planTransformed) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }

        return planTransformed;
    }

    @FunctionalInterface
    protected interface TopKMatchAction {
        boolean apply(IOptimizationContext context) throws AlgebricksException;
    }

    /**
     * Recursively checks the plan for LIMIT → ORDER BY ANN_DISTANCE pattern
     * and applies vector index optimization if applicable.
     */
    protected boolean checkAndApplyTopKTransformation(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return matchTopKPattern(opRef, context, this::analyzeAndTransform);
    }

    /**
     * Recursively looks for the LIMIT → ORDER BY ANN_DISTANCE pattern and runs the action on the first
     * match.
     *
     * @param opRef operator to search from
     * @param context the optimization context
     * @param action what to run once the pattern matches
     */
    protected boolean matchTopKPattern(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            TopKMatchAction action) throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        // Check if current operator is LIMIT
        if (op.getOperatorTag() == LogicalOperatorTag.LIMIT) {
            limitRef = opRef;
            limitOp = (LimitOperator) op;

            if (context.checkIfInDontApplySet(this, limitOp)) {
                return false;
            }

            // Find ORDER operator by skipping intermediate operators (ASSIGN, EXCHANGE, nested LIMIT)
            Pair<Mutable<ILogicalOperator>, OrderOperator> orderPair = findOrderOperator(limitOp);
            if (orderPair != null) {
                orderRef = orderPair.first;
                orderOp = orderPair.second;
                // Skip the index transform when the LIMIT carries an OFFSET: the vector search is sized
                // from k alone, so LIMIT k OFFSET o would skip into an undersized candidate pool and return
                // truncated/empty results. Fall through to the exact ORDER BY ... LIMIT plan, which honors
                // OFFSET correctly. (A future enhancement could size the search to (k + offset).)
                if (!limitOp.hasOffset() && matchesAnnDistancePattern()) {
                    return action.apply(context);
                }
            }
        }

        // Recursively check children — push self onto the ancestor chain so that, when we reach a
        // LIMIT below, isProjectionCoveredByIndex() has the full list of operators above it.
        aboveLimitOps.add(op);
        try {
            for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
                boolean matched = matchTopKPattern(inputOpRef, context, action);
                if (matched) {
                    return true;
                }
            }
        } finally {
            aboveLimitOps.remove(aboveLimitOps.size() - 1);
        }

        return false;
    }

    /**
     * Finds the ORDER operator by traversing through intermediate operators.
     * Skips ASSIGN, EXCHANGE, and nested LIMIT operators that are added for
     * distributed execution optimization.
     *
     * The optimizer often adds intermediate operators between LIMIT and ORDER:
     * - ASSIGN: For result projection
     * - EXCHANGE: For data redistribution in distributed execution
     * - LIMIT: For distributed top-k optimization
     *
     * This method traverses through these intermediate operators until it finds
     * the ORDER operator or encounters an operator it doesn't recognize.
     *
     * @param limitOp The LIMIT operator to start from
     * @return Pair of (orderRef, orderOp) if found, null otherwise
     */
    protected Pair<Mutable<ILogicalOperator>, OrderOperator> findOrderOperator(LimitOperator limitOp) {
        if (limitOp.getInputs().isEmpty()) {
            return null;
        }

        Mutable<ILogicalOperator> currentRef = limitOp.getInputs().get(0);
        AbstractLogicalOperator currentOp = (AbstractLogicalOperator) currentRef.getValue();

        // Traverse through intermediate operators until we find ORDER or fail
        // The loop will terminate when:
        // 1. We find ORDER operator (success)
        // 2. We hit an empty input list (no more children)
        // 3. We hit an operator we don't know how to skip through
        while (true) {
            // Check if we found the ORDER operator
            if (currentOp.getOperatorTag() == LogicalOperatorTag.ORDER) {
                return new Pair<>(currentRef, (OrderOperator) currentOp);
            }

            // Skip through known intermediate operators
            if (currentOp.getOperatorTag() == LogicalOperatorTag.ASSIGN
                    || currentOp.getOperatorTag() == LogicalOperatorTag.EXCHANGE
                    || currentOp.getOperatorTag() == LogicalOperatorTag.LIMIT) {

                if (currentOp.getInputs().isEmpty()) {
                    // No more children to traverse
                    return null;
                }
                currentRef = currentOp.getInputs().get(0);
                currentOp = (AbstractLogicalOperator) currentRef.getValue();
            } else {
                // Hit an operator we don't know how to skip through
                // This means LIMIT is not directly above ORDER BY ANN_DISTANCE
                return null;
            }
        }
    }

    /**
     * Checks if ORDER BY pattern exists (has at least one ordering expression).
     * We don't resolve the actual ANN_DISTANCE function here - that happens later
     * after subtree initialization in analyzeAnnDistanceFunction().
     */
    protected boolean matchesAnnDistancePattern() {
        List<Pair<IOrder, Mutable<ILogicalExpression>>> orderExprs = orderOp.getOrderExpressions();

        // Just check that ORDER BY has exactly one expression
        // We'll verify it's ANN_DISTANCE later after subtree init
        return orderExprs.size() == 1;
    }

    /**
     * Analyzes the pattern and attempts to apply vector index transformation.
     */
    protected boolean analyzeAndTransform(IOptimizationContext context) throws AlgebricksException {
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new TreeMap<>();
        List<Pair<IAccessMethod, Index>> chosenIndexes = new ArrayList<>();
        if (!analyzeApplicability(context, analyzedAMs, chosenIndexes)) {
            return false;
        }

        if (chosenIndexes.isEmpty()) {
            // No vector index left: either none exists, none has the required INCLUDE fields, or the
            // cost-based optimizer priced them all above the scan. Fall back to data scan + sort.
            context.addToDontApplySet(this, limitOp);
            return false;
        }

        Index vectorIndex = chosenIndexes.get(0).second;
        AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(VectorIndexAccessMethod.INSTANCE);

        boolean transformed = applyTopKPlanTransformation(vectorIndex, analysisCtx, context);

        // Always mark as processed to avoid re-attempting optimization on this operator
        context.addToDontApplySet(this, limitOp);

        return transformed;
    }

    /**
     * Works out which vector indexes can answer the matched pattern, leaving the plan untouched.
     *
     * @param context the optimization context
     * @param analyzedAMs filled with the access-method analysis backing the chosen indexes
     * @param chosenIndexes filled with the applicable indexes, empty when none apply
     */
    protected boolean analyzeApplicability(IOptimizationContext context,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs, List<Pair<IAccessMethod, Index>> chosenIndexes)
            throws AlgebricksException {
        // 1. Initialize subtree from ORDER down to DATASOURCE_SCAN
        if (!initializeSubTree()) {
            return false;
        }

        // 2. Get type environment for type checking
        typeEnvironment = context.getOutputTypeEnvironment(orderOp);

        // 3. Load dataset metadata (including vector indexes)
        // This MUST be done before extracting filter fields because field-access-by-index
        // needs the record type to resolve field index to field name
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        if (!subTree.setDatasetAndTypeMetadata(metadataProvider)) {
            return false;
        }

        // 4. Find SELECT operator (if any) and extract filter fields.
        // Populates selectOp if a SELECT exists.
        // Must be called AFTER setDatasetAndTypeMetadata so recordType is available.
        findSelectOperatorInSubTree();

        // 5. Analyze ANN_DISTANCE function arguments
        if (!analyzeAnnDistanceFunction(analyzedAMs, context)) {
            return false;
        }

        // 6. Find applicable vector indexes on the dataset
        fillSubTreeIndexExprs(subTree, analyzedAMs, context, false);

        // 7. Choose best vector index (considering INCLUDE fields if filter exists), minus the ones the
        // cost-based optimizer already ruled out.
        chooseVectorIndex(analyzedAMs, chosenIndexes, context);

        return true;
    }

    /**
     * Finds the SELECT operators between ORDER and DATASOURCE_SCAN, setting {@link #selectOp} to the topmost
     * and {@link #numSelectOps} to how many there are.
     *
     * @return The topmost SELECT operator if any, null otherwise
     */
    protected SelectOperator findSelectOperatorInSubTree() {
        selectOp = null;
        numSelectOps = 0;
        if (orderOp.getInputs().isEmpty()) {
            return null;
        }

        // Traverse from ORDER down to DATASOURCE_SCAN
        AbstractLogicalOperator currentOp = (AbstractLogicalOperator) orderOp.getInputs().get(0).getValue();

        while (currentOp != null) {
            if (currentOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                numSelectOps++;
                if (selectOp == null) {
                    selectOp = (SelectOperator) currentOp;
                }
            }

            if (currentOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                break;
            }
            if (currentOp.getInputs().isEmpty()) {
                break;
            }
            currentOp = (AbstractLogicalOperator) currentOp.getInputs().get(0).getValue();
        }

        return selectOp;
    }

    /**
     * Initializes the subtree from ORDER operator's child down to DATASOURCE_SCAN.
     * OptimizableOperatorSubTree doesn't recognize ORDER as a valid top operator,
     * so we start from ORDER's child (usually ASSIGN).
     */
    protected boolean initializeSubTree() throws AlgebricksException {
        if (orderOp.getInputs().isEmpty()) {
            return false;
        }
        subTree.initFromSubTree(orderOp.getInputs().get(0));
        return subTree.hasDataSourceScan();
    }

    /**
     * Analyzes ANN_DISTANCE function and updates analysis context.
     * This is called AFTER subtree initialization, so assigns/unnests are available
     * for resolving variable references.
     */
    protected boolean analyzeAnnDistanceFunction(Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs,
            IOptimizationContext context) throws AlgebricksException {

        // Get ORDER BY expression
        List<Pair<IOrder, Mutable<ILogicalExpression>>> orderExprs = orderOp.getOrderExpressions();
        ILogicalExpression orderExpr = orderExprs.get(0).second.getValue();

        // Resolve to actual ANN_DISTANCE function (handle both direct and variable reference cases)
        annDistanceExpr = resolveAnnDistanceExpr(orderExpr, subTree.getAssignsAndUnnests());

        if (annDistanceExpr == null) {
            // ORDER BY expression is not ANN_DISTANCE
            return false;
        }

        // The top-K cursor returns the nearest K candidates (ascending distance). A DESC order-by asks
        // for the farthest K, which this plan cannot produce, so only match ASC and let DESC fall back
        // to the non-index (full-scan + sort) plan.
        if (orderExprs.get(0).first.getKind() != IOrder.OrderKind.ASC) {
            return false;
        }

        // The distance metric now comes from the ANN hint: the desugared call is a 2-arg distance builtin,
        // so there is no metric argument to read. It is used only for compile-time index selection.
        AnnSearchPreferenceAnnotation annHint = annDistanceExpr.getAnnotation(AnnSearchPreferenceAnnotation.class);
        if (annHint != null) {
            queryDistanceMetric = VectorIndexAccessMethod.resolveQueryMetric(annHint.getMetric());
            queryMinProbeFraction = annHint.getMinProbeFraction();
            queryKMultiplier = annHint.getKMultiplier();
        }

        // Now analyze the ANN_DISTANCE function arguments
        AccessMethodAnalysisContext analysisCtx = new AccessMethodAnalysisContext();

        boolean matchFound = VectorIndexAccessMethod.INSTANCE.analyzeFuncExprArgsAndUpdateAnalysisCtx(annDistanceExpr,
                subTree.getAssignsAndUnnests(), analysisCtx, context, typeEnvironment);

        if (!matchFound) {
            return false;
        }

        analyzedAMs.put(VectorIndexAccessMethod.INSTANCE, analysisCtx);
        return true;
    }

    /**
     * Resolves ORDER BY expression to ANN_DISTANCE function.
     * Handles two cases:
     * 1. Direct function call: ANN_DISTANCE(...)
     * 2. Variable reference: $$var where $$var := ANN_DISTANCE(...) in an ASSIGN
     *
     * This method is called after subtree init, so assigns/unnests are available.
     */
    protected AbstractFunctionCallExpression resolveAnnDistanceExpr(ILogicalExpression orderExpr,
            List<AbstractLogicalOperator> assignsAndUnnests) {

        // Case 1: Direct function call
        AbstractFunctionCallExpression direct = asAnnDistanceCall(orderExpr);
        if (direct != null) {
            return direct;
        }

        // Case 2: Variable reference - trace back through assigns
        if (orderExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            VariableReferenceExpression varRef = (VariableReferenceExpression) orderExpr;
            LogicalVariable orderVar = varRef.getVariableReference();

            // Search assigns/unnests for the variable (similar to InvertedIndexAccessMethod)
            for (AbstractLogicalOperator op : assignsAndUnnests) {
                if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                    AssignOperator assignOp = (AssignOperator) op;
                    List<LogicalVariable> assignVars = assignOp.getVariables();
                    List<Mutable<ILogicalExpression>> assignExprs = assignOp.getExpressions();

                    for (int i = 0; i < assignVars.size(); i++) {
                        if (assignVars.get(i).equals(orderVar)) {
                            // Found the assignment
                            return asAnnDistanceCall(assignExprs.get(i).getValue());
                        }
                    }
                }
            }
        }

        return null;
    }

    /** Returns {@code expr} as an ann-distance function call, or {@code null} if it is not one. */
    private AbstractFunctionCallExpression asAnnDistanceCall(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            if (isAnnDistance(funcExpr)) {
                return funcExpr;
            }
        }
        return null;
    }

    private boolean isAnnDistance(AbstractFunctionCallExpression funcExpr) {
        // A desugared ann_distance is a concrete distance builtin carrying the ANN hint; a plain
        // vector_distance carries the same builtin without the hint and must stay exact (no index).
        return funcExpr.hasAnnotation(AnnSearchPreferenceAnnotation.class);
    }

    private boolean isRuledOutByCost(Index index) {
        SkipSecondaryIndexSearchExpressionAnnotation skipAnnotation =
                annDistanceExpr.getAnnotation(SkipSecondaryIndexSearchExpressionAnnotation.class);
        if (skipAnnotation == null) {
            return false;
        }
        Collection<String> rejected = skipAnnotation.getIndexNames();
        return rejected == null || rejected.contains(index.getIndexName());
    }

    /**
     * Chooses the best vector index from candidates.
     * Considers:
     * 1. INCLUDE fields: If query has filter (WHERE clause), index must have all filter fields in INCLUDE
     * 2. Distance metric: Prefers indexes with matching distance metrics.
     * If the query specifies a constant distance metric that does not match the index metadata, compilation fails.
     */
    protected void chooseVectorIndex(Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs,
            List<Pair<IAccessMethod, Index>> result, IOptimizationContext context) throws AlgebricksException {

        AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(VectorIndexAccessMethod.INSTANCE);
        if (analysisCtx == null) {
            return;
        }

        // A predicate that cannot be moved into the search must not leave a SELECT above it. The search
        // caps candidates at k * k_multiplier, chosen without regard to the predicate, so filtering
        // afterwards answers "the passing rows among the k nearest" instead of "the k nearest passing
        // rows" -- and it can return nothing at all where passing rows exist. A non-functional predicate
        // (random(), and anything else registered with isFunctional = false) cannot be evaluated per
        // candidate in the cursor, so no vector index is offered and the query takes the exact scan.
        if (selectOp != null && !selectOp.getCondition().getValue().isFunctional()) {
            LOGGER.trace("chooseVectorIndex: the WHERE is non-functional and cannot be pushed into the "
                    + "search; not offering a vector index");
            return;
        }
        // The gate and the binding each handle one SELECT, and the search takes one condition. With several,
        // pushing the topmost would leave the others above the search -- the very shape being avoided -- so
        // no index is offered. The select rules consolidate adjacent SELECTs and push each as far down as its
        // variables allow, so this shape is not known to arise; it is refused rather than assumed away.
        if (numSelectOps > 1) {
            LOGGER.trace("chooseVectorIndex: {} SELECTs between the ORDER and the scan, of which the pushdown "
                    + "can install one; not offering a vector index", numSelectOps);
            return;
        }
        // The LIMIT's count reaches the search as k, so everything in between must map one row to one row.
        // An UNNEST does not: its n = 0 case (empty array, absent field, non-array value) drops the row
        // after the search has capped its candidates, exactly as a predicate left above the search would.
        for (AbstractLogicalOperator pipelineOp : subTree.getAssignsAndUnnests()) {
            if (pipelineOp.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
                LOGGER.trace("chooseVectorIndex: {} between the ORDER and the scan does not preserve the row "
                        + "count; not offering a vector index", pipelineOp.getOperatorTag());
                return;
            }
        }

        // Iterate over candidate vector indexes
        Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexIt =
                analysisCtx.getIteratorForIndexExprsAndVars();

        // Every index whose field, metric and INCLUDE fields all match. More than one can qualify, so
        // they are collected rather than settled here: the cheapest is picked once they are costed.
        List<Pair<IAccessMethod, Index>> exactMatches = new ArrayList<>();
        Pair<IAccessMethod, Index> fieldMatch = null; // Index with matching field only (and INCLUDE if needed)

        while (indexIt.hasNext()) {
            Map.Entry<Index, List<Pair<Integer, Integer>>> indexEntry = indexIt.next();
            Index index = indexEntry.getKey();

            if (index.getIndexType() == IndexType.VTREE) {
                if (isRuledOutByCost(index)) {
                    continue;
                }

                // A predicate must be evaluable INSIDE the search, or this index cannot answer the query
                // at all. The search caps candidates at k * k_multiplier, chosen without regard to the
                // predicate, so a predicate left in a SELECT above it answers "the passing rows among the
                // k nearest" instead of "the k nearest passing rows" -- silently wrong, and it can return
                // nothing where passing rows exist. Asking the pushdown itself, rather than a separate
                // name-matching test, is what makes this decision agree with the binding that follows: an
                // index offered here is one PushFilterIntoVectorSearchRule can and will bind, leaving no
                // SELECT above the search in either plan shape.
                if (selectOp != null && !isFilterPushableToInclude(index, context)) {
                    continue;
                }

                if (queryDistanceMetric != null) {
                    VectorSimilarityMetric indexMetric = VectorIndexAccessMethod.getIndexMetric(index);
                    if (queryDistanceMetric == indexMetric) {
                        // Exact match: field name AND distance metric match.
                        exactMatches.add(new Pair<>(VectorIndexAccessMethod.INSTANCE, index));
                    } else if (fieldMatch == null) {
                        // Field matches but metric doesn't - store as fallback only if no exact match.
                        fieldMatch = new Pair<>(VectorIndexAccessMethod.INSTANCE, index);
                    }
                } else {
                    // No query metric available. Do NOT pick an arbitrary index here: without a metric to
                    // compare against, the first VTREE index found could easily answer a cosine query from a
                    // euclidean index — silently wrong results. This should be unreachable (isAnnDistance()
                    // requires the ANN hint and the rewrite visitor always fills in a validated metric), so
                    // bail out to the exact plan rather than guess.
                    LOGGER.warn("No query distance metric available for ANN search; skipping vector index "
                            + "selection and falling back to full scan (KNN).");
                    return;
                }
            }
        }

        // Prefer exact match. If only a field match exists (metric mismatch), fall back to full scan (KNN).
        if (!exactMatches.isEmpty()) {
            result.addAll(exactMatches);
        } else if (fieldMatch != null) {
            Index idx = fieldMatch.second;
            VectorSimilarityMetric indexMetric = VectorIndexAccessMethod.getIndexMetric(idx);
            LOGGER.warn("Distance metric mismatch: query uses '{}' but index '{}' uses '{}'. "
                    + "Falling back to full scan (KNN).", queryDistanceMetric, idx.getIndexName(), indexMetric);
        }
    }

    public record VectorIndexCandidate(Index index, VectorIndexGeometry geometry, double fetchedCard,
            AbstractFunctionCallExpression annDistanceExpr) {
    }

    /**
     * Collects every vector index that could answer a top-k ANN pattern under the given operator,
     * leaving the plan untouched. Lets a cost-based caller price the alternatives before this rule runs
     *
     * @param opRef operator to search from
     * @param context the optimization context
     * @param indexProvider where the dataset's indexes are looked up
     * @param datasetCardinality rows in the dataset; the index holds an entry per row whatever the query filters
     * @param candidates filled with the applicable indexes, left empty when none apply
     */
    public boolean collectVectorIndexCandidates(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            IIndexProvider indexProvider, double datasetCardinality, List<VectorIndexCandidate> candidates)
            throws AlgebricksException {
        clear();
        setMetadataIndexDeclarations(context, indexProvider);
        return matchTopKPattern(opRef, context, ctx -> describeCandidates(ctx, datasetCardinality, candidates));
    }

    /**
     * Analyzes the matched pattern and describes each applicable index in costable terms.
     *
     * @param context the optimization context
     * @param datasetCardinality rows in the dataset; the index holds an entry per row whatever the query filters
     * @param candidates filled with the applicable indexes
     */
    private boolean describeCandidates(IOptimizationContext context, double datasetCardinality,
            List<VectorIndexCandidate> candidates) throws AlgebricksException {
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new TreeMap<>();
        List<Pair<IAccessMethod, Index>> chosenIndexes = new ArrayList<>();
        if (!analyzeApplicability(context, analyzedAMs, chosenIndexes) || chosenIndexes.isEmpty()) {
            return false;
        }

        Long topK = integralConstant(limitOp.getMaxObjects().getValue());
        if (topK == null || queryKMultiplier <= 0) {
            return false;
        }

        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        List<IAType> primaryKeyTypes = KeyFieldTypeUtil.getPartitoningKeyTypes(subTree.getDataset(),
                subTree.getRecordType(), subTree.getMetaRecordType());
        if (primaryKeyTypes == null) {
            return false;
        }
        int numPartitions = metadataProvider.getPartitioningProperties(subTree.getDataset()).getNumberOfPartitions();
        long pageSize = metadataProvider.getStorageProperties().getBufferCachePageSize();

        // The search collects this many candidates either way; an index-only plan then reads the distance
        // off the cursor rather than fetching and reranking them.
        double candidateCard = (double) topK * queryKMultiplier;

        for (Pair<IAccessMethod, Index> candidate : chosenIndexes) {
            // Whether the primary lookup is skipped is per-index: two indexes can differ in whether their
            // INCLUDE list covers this query's WHERE, and that is the difference between fetching every
            // candidate and fetching none.
            double fetchedCard = isProjectionCoveredByIndex(candidate.second, context) ? 0 : candidateCard;
            Index.VectorIndexDetails details = (Index.VectorIndexDetails) candidate.second.getIndexDetails();
            VectorIndexGeometry geometry = new VectorIndexGeometry(details.getVectorParameters(),
                    details.getIncludeFieldTypes(), primaryKeyTypes, datasetCardinality, numPartitions, pageSize,
                    queryMinProbeFraction, candidateCard);
            candidates.add(new VectorIndexCandidate(candidate.second, geometry, fetchedCard, annDistanceExpr));
        }
        return true;
    }

    private static Long integralConstant(ILogicalExpression expr) {
        Long asLong = ConstantExpressionUtil.getLongConstant(expr);
        if (asLong != null) {
            return asLong;
        }
        Integer asInt = ConstantExpressionUtil.getIntConstant(expr);
        return asInt == null ? null : asInt.longValue();
    }

    /**
     * Applies the top-k plan transformation using the chosen vector index.
     *
     * Transforms:
     *   LIMIT k → ORDER BY ANN_DISTANCE(vectorField, qvec, metric) → ... → DATASOURCE_SCAN
     * Into:
     *   LIMIT k → ORDER BY ANN_DISTANCE(vectorField, qvec, metric) → ... → UNNEST-MAP(vector_index_search)
     *
     * Key points:
     * - **ONLY replaces DATASOURCE_SCAN** with vector index search (UNNEST-MAP)
     * - ORDER BY and LIMIT operators **remain unchanged**
     * - Vector index returns candidate tuples (may be > k from multiple partitions)
     * - ORDER BY ANN_DISTANCE computes exact distances on candidates
     * - ORDER BY + LIMIT extract the true top-k results
     * - Similar to B+Tree/R-Tree: keeps top operator (ORDER/SELECT), only replaces data scan
     */
    protected boolean applyTopKPlanTransformation(Index vectorIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context) throws AlgebricksException {

        // Decide whether the index-only plan branch can be taken: when every variable consumed above the
        // LIMIT resolves to a primary key or an INCLUDE column of this index, the assembled record from the
        // primary BTree lookup is never needed, so the vector index emits (pk, dist, INCLUDE columns) and
        // the plan skips the lookup + rerank entirely.
        boolean indexOnly = isProjectionCoveredByIndex(vectorIndex, context);

        // Build the index-search subplan (UNNEST-MAP over vector index). selectOp is passed so the
        // access method can attach a selectCondition for filter pushdown when applicable.
        ILogicalOperator indexSearchOp = VectorIndexAccessMethod.INSTANCE.createIndexSearchPlan(limitRef, orderRef,
                annDistanceExpr, subTree, vectorIndex, analysisCtx, context, selectOp, indexOnly, aboveLimitOps);

        if (indexSearchOp == null) {
            return false;
        }

        // Replace DATASOURCE_SCAN with the vector index search subplan. ORDER BY ANN_DISTANCE
        // remains in place to compute distances on the returned candidates.
        subTree.getDataSourceRef().setValue(indexSearchOp);
        return true;
    }

    /**
     * Detects whether consumers above the matched {@code limitOp} read only what the vector index emits: the
     * dataset's primary keys and the index's INCLUDE columns — meaning the assembled record produced by a
     * downstream primary BTree lookup is never needed and the plan can take the index-only branch (vector
     * index emits {@code (pk, dist, INCLUDE columns)}, SORT on {@code $$dist}, LIMIT, done — no primary
     * lookup, no rerank).
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Identify dataset PK variables and "record" variables (the non-PK variables produced by the
     *       {@code DataSourceScan}: the dataset record and, if present, the meta record).</li>
     *   <li>Collect bindings from every {@code ASSIGN} in the subtree below {@code limitOp}
     *       (variable → defining expression).</li>
     *   <li>Compute the live-out set of {@code limitOp} by collecting variables used by every operator
     *       in {@link #aboveLimitOps}.</li>
     *   <li>For each live-out variable, trace through the bindings: it is covered iff it resolves to a
     *       PK variable, a constant, or a field access whose full path — reassembled through the ASSIGN
     *       chain when the compiler split it — is a primary key of the record it is read from (the dataset
     *       record or the meta record, each against its own keys), or an INCLUDE column of the searched
     *       index read from the dataset record. If any live-out variable fails the trace, the optimization
     *       cannot be safely applied.</li>
     * </ol>
     *
     * <p>Conservatively returns {@code false} for external data sources or unfamiliar plan shapes.
     */
    protected boolean isProjectionCoveredByIndex(Index vectorIndex, IOptimizationContext context)
            throws AlgebricksException {
        // Index-only skips the primary BTree lookup and emits (pk, dist) straight from the secondary VTree.
        // Correct even with deletes: the VTree search cursor reconciles anti-matter (delete) tuples itself,
        // so an index-only ANN query over a dataset with deletes returns no deleted rows. Enabled by default; the
        // gate is a kill-switch, not a correctness guard, so when disabled the optimizer falls back to the
        // legacy lookup-and-rerank plan.
        if (!INDEX_ONLY_ENABLED) {
            return false;
        }
        if (subTree == null || subTree.getDataset() == null || subTree.getDataSourceRef() == null) {
            return false;
        }
        ILogicalOperator dataSourceOpRaw = subTree.getDataSourceRef().getValue();
        if (!(dataSourceOpRaw instanceof AbstractScanOperator)) {
            return false;
        }
        AbstractScanOperator dataSourceOp = (AbstractScanOperator) dataSourceOpRaw;
        List<LogicalVariable> dsVars = dataSourceOp.getVariables();

        // PK field PATHS, split by their key source (data record vs. meta record) so a PK sourced from
        // meta() named "id" is never confused with an unrelated data-record field that happens to share the
        // name. Full paths, so a nested primary key is matched exactly like a top-level one.
        List<List<String>> pkPaths = subTree.getDataset().getPrimaryKeys();
        List<Integer> keySourceIndicators = DatasetUtil.getKeySourceIndicators(subTree.getDataset());
        Set<List<String>> recordPkFieldPaths = new HashSet<>();
        Set<List<String>> metaPkFieldPaths = new HashSet<>();
        for (int i = 0; i < pkPaths.size(); i++) {
            List<String> p = pkPaths.get(i);
            if (p == null || p.isEmpty()) {
                return false;
            }
            boolean fromMeta = keySourceIndicators != null && keySourceIndicators.get(i) == 1;
            (fromMeta ? metaPkFieldPaths : recordPkFieldPaths).add(p);
        }
        int numPK = pkPaths.size();
        if (dsVars.size() < numPK + 1) {
            return false; // need at least PKs + a record variable
        }
        Set<LogicalVariable> pkVars = new HashSet<>(dsVars.subList(0, numPK));
        // Anything after PKs is the dataset record (+ optional meta record). Direct references to these
        // mean we need the assembled record; only field-access for PK names is OK, and only against the
        // record that field's PK is actually sourced from.
        Set<LogicalVariable> recordVars = new HashSet<>(dsVars.subList(numPK, dsVars.size()));
        LogicalVariable dataRecordVar = dsVars.get(numPK);
        LogicalVariable metaRecordVar = dsVars.size() > numPK + 1 ? dsVars.get(numPK + 1) : null;
        // An INCLUDE column is served like a primary key: the search reads it from the secondary tuple and
        // emits it, so the record never has to be assembled to return it.
        Index.VectorIndexDetails details = (Index.VectorIndexDetails) vectorIndex.getIndexDetails();
        List<List<String>> includePaths = details.getIncludeFieldNames();
        CoverageContext coverage = new CoverageContext(pkVars, recordVars, dataRecordVar, metaRecordVar,
                recordPkFieldPaths, metaPkFieldPaths, includePaths == null ? Set.of() : new HashSet<>(includePaths),
                pathContext(subTree.getRecordType(), dataRecordVar == null ? Set.of() : Set.of(dataRecordVar)),
                pathContext(subTree.getMetaRecordType(), metaRecordVar == null ? Set.of() : Set.of(metaRecordVar)));

        // Collect ASSIGN bindings throughout the entire subtree.
        Map<LogicalVariable, ILogicalExpression> bindings =
                VectorIncludeFilterPushdown.collectAssignBindings(subTree.getRoot());

        // Gather variables used above the LIMIT.
        Set<LogicalVariable> liveOut = new HashSet<>();
        for (AbstractLogicalOperator op : aboveLimitOps) {
            try {
                VariableUtilities.getUsedVariables(op, liveOut);
            } catch (AlgebricksException e) {
                LOGGER.trace("isProjectionCoveredByIndex: failed to collect used vars from {}", op.getOperatorTag(), e);
                return false;
            }
        }
        // Trace each live-out variable; fail on the first that the index does not cover.
        Set<LogicalVariable> visiting = new HashSet<>();
        for (LogicalVariable v : liveOut) {
            if (!isVarCovered(v, bindings, coverage, visiting)) {
                LOGGER.trace("isProjectionCoveredByIndex: live-out variable {} is not covered by the index; bailing",
                        v);
                return false;
            }
        }

        // A WHERE below the LIMIT is invisible to the liveOut scan above, which only covers the projection
        // above the LIMIT. It can still be served index-only in two ways: its variables are covered, or
        // the predicate reads nothing but INCLUDE columns of the chosen index, in which case the index-only
        // branch rebinds it onto those columns instead of leaving a SELECT above a dead record variable
        // (which VectorIndexAccessMethod#neutralizeDanglingExpressions would collapse to select(missing),
        // dropping every row). Anything else falls back to lookup-and-rerank.
        Set<LogicalVariable> filterVars = new HashSet<>();
        collectSelectConditionVars(subTree.getRoot(), filterVars);
        for (LogicalVariable v : filterVars) {
            if (!isVarCovered(v, bindings, coverage, visiting)) {
                if (isFilterPushableToInclude(vectorIndex, context)) {
                    return true;
                }
                LOGGER.trace("isProjectionCoveredByIndex: WHERE condition var {} is neither covered nor an INCLUDE "
                        + "field of {}; not index-only", v, vectorIndex.getIndexName());
                return false;
            }
        }
        // Every variable the predicate reads is one the search emits, which is still not enough: the
        // index-only branch installs a predicate by rebinding it onto the INCLUDE columns, and that is its
        // only way to evaluate one -- the record is never assembled. A predicate it cannot rebind leaves it
        // holding a SELECT it cannot install, which job generation then rejects. Two such predicates reach
        // here with every variable "safe": one over primary keys, and one reading no dataset field at all
        // (`WHERE random() > 0.5`), whose variable set is empty so the loop above passes vacuously. Both
        // belong on lookup-and-rerank, where the SELECT can simply stay above the search.
        return selectOp == null || isFilterPushableToInclude(vectorIndex, context);
    }

    /**
     * Whether this subtree's {@code SELECT} predicate can be evaluated entirely from {@code vectorIndex}'s
     * INCLUDE columns, and can therefore be rebound onto them by the index-only branch.
     * <p>
     * The verdict must match what {@code VectorIndexAccessMethod} will actually manage, because by the time
     * that branch runs {@code indexOnly} has already been serialized into the index-search arguments and
     * cannot be withdrawn. Both go through {@link VectorIncludeFilterPushdown} for exactly that reason; this
     * call allocates no variables.
     */
    protected boolean isFilterPushableToInclude(Index vectorIndex, IOptimizationContext context)
            throws AlgebricksException {
        if (selectOp == null || vectorIndex == null || vectorIndex.getIndexType() != IndexType.VTREE) {
            return false;
        }
        VectorIncludeFilterPushdown.IndexContext idx = buildIncludeFilterContext(vectorIndex);
        return idx != null
                && VectorIncludeFilterPushdown.isPushable(selectOp.getCondition().getValue(), selectOp, idx, context);
    }

    /**
     * Assembles the INCLUDE-pushdown inputs from this subtree: the chosen index's INCLUDE list and
     * quantization, the dataset record type, and the record variable(s) the {@code DataSourceScan}
     * produces — the only bases a pushable field access may be rooted at.
     */
    protected VectorIncludeFilterPushdown.IndexContext buildIncludeFilterContext(Index vectorIndex) {
        if (subTree == null || subTree.getDataset() == null || subTree.getDataSourceRef() == null) {
            return null;
        }
        ILogicalOperator dataSourceOpRaw = subTree.getDataSourceRef().getValue();
        if (!(dataSourceOpRaw instanceof AbstractScanOperator)) {
            return null;
        }
        List<LogicalVariable> dsVars = ((AbstractScanOperator) dataSourceOpRaw).getVariables();
        int numPK = subTree.getDataset().getPrimaryKeys().size();
        if (dsVars.size() <= numPK) {
            return null;
        }
        // On a collection with a meta part the scan produces [pk..., record, meta]. Only the record is a
        // pushdown base: the analysis resolves a field access against the record type, so admitting the meta
        // variable would let `WHERE meta(m).year > 0` bind to the record's INCLUDE column of the same name.
        Set<LogicalVariable> recordVars = new HashSet<>();
        for (LogicalVariable dsVar : dsVars.subList(numPK, dsVars.size())) {
            OptimizableOperatorSubTree.RecordTypeSource varType = subTree.getRecordTypeFor(dsVar);
            if (varType != null && varType.sourceIndicator == 0) {
                recordVars.add(dsVar);
            }
        }
        if (recordVars.isEmpty()) {
            return null;
        }

        Index.VectorIndexDetails details = (Index.VectorIndexDetails) vectorIndex.getIndexDetails();
        return new VectorIncludeFilterPushdown.IndexContext(details.getIncludeFieldNames(), subTree.getRecordType(),
                details.getVectorParameters().isQuantized(), numPK, recordVars);
    }

    /**
     * Groups the pieces {@link #isVarCovered} and {@link #isExprCovered} need to decide whether a field
     * access is covered by the index: the data-source PK variables, the combined record/meta "direct use"
     * variables, the data-record and meta-record variables individually, the PK field paths declared on
     * each of those two records (kept separate so a same-named field on the "wrong" record — e.g. a
     * data-record field called "id" next to a {@code meta().id} PK — is never treated as covered), and the
     * searched index's INCLUDE paths.
     */
    private static final class CoverageContext {
        final Set<LogicalVariable> pkVars;
        final Set<LogicalVariable> recordVars;
        final LogicalVariable dataRecordVar;
        final LogicalVariable metaRecordVar;
        final Set<List<String>> recordPkFieldPaths;
        final Set<List<String>> metaPkFieldPaths;
        /** The searched index's INCLUDE columns, which the search emits alongside the primary keys. */
        final Set<List<String>> includePaths;
        /** Resolves a field access on the data record, and on the meta record, to its full path. */
        final VectorIncludeFilterPushdown.IndexContext dataRecordCtx;
        final VectorIncludeFilterPushdown.IndexContext metaRecordCtx;

        CoverageContext(Set<LogicalVariable> pkVars, Set<LogicalVariable> recordVars, LogicalVariable dataRecordVar,
                LogicalVariable metaRecordVar, Set<List<String>> recordPkFieldPaths, Set<List<String>> metaPkFieldPaths,
                Set<List<String>> includePaths, VectorIncludeFilterPushdown.IndexContext dataRecordCtx,
                VectorIncludeFilterPushdown.IndexContext metaRecordCtx) {
            this.pkVars = pkVars;
            this.recordVars = recordVars;
            this.dataRecordVar = dataRecordVar;
            this.metaRecordVar = metaRecordVar;
            this.recordPkFieldPaths = recordPkFieldPaths;
            this.metaPkFieldPaths = metaPkFieldPaths;
            this.includePaths = includePaths;
            this.dataRecordCtx = dataRecordCtx;
            this.metaRecordCtx = metaRecordCtx;
        }
    }

    /**
     * A context that resolves a field access rooted at one of {@code recordVars} to its full path. Only
     * the record type and the accepted bases matter for that, so the index-side fields are left empty.
     */
    private static VectorIncludeFilterPushdown.IndexContext pathContext(ARecordType recordType,
            Set<LogicalVariable> recordVars) {
        return new VectorIncludeFilterPushdown.IndexContext(null, recordType, false, 0, recordVars);
    }

    /**
     * Collect the variables used in every {@code SELECT} condition in the subtree (below the LIMIT). Used by
     * {@link #isProjectionCoveredByIndex} to check the {@code WHERE}, which the live-out scan above the LIMIT
     * does not see.
     */
    private void collectSelectConditionVars(ILogicalOperator op, Set<LogicalVariable> vars) {
        if (op == null) {
            return;
        }
        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            try {
                VariableUtilities.getUsedVariables(op, vars);
            } catch (AlgebricksException e) {
                LOGGER.trace("collectSelectConditionVars: getUsedVariables failed for SELECT", e);
            }
        }
        for (Mutable<ILogicalOperator> input : op.getInputs()) {
            collectSelectConditionVars(input.getValue(), vars);
        }
    }

    private boolean isVarCovered(LogicalVariable v, Map<LogicalVariable, ILogicalExpression> bindings,
            CoverageContext coverage, Set<LogicalVariable> visiting) {
        if (coverage.pkVars.contains(v)) {
            return true;
        }
        if (coverage.recordVars.contains(v)) {
            // Direct use of the dataset record — requires the assembled record. Not safe.
            return false;
        }
        if (!visiting.add(v)) {
            // Cycle in bindings (shouldn't normally happen). Treat as unsafe.
            return false;
        }
        try {
            ILogicalExpression e = bindings.get(v);
            if (e == null) {
                // Variable produced by something other than an ASSIGN (e.g. a downstream operator we
                // don't track). Be conservative.
                return false;
            }
            return isExprCovered(e, bindings, coverage, visiting);
        } finally {
            visiting.remove(v);
        }
    }

    private boolean isExprCovered(ILogicalExpression e, Map<LogicalVariable, ILogicalExpression> bindings,
            CoverageContext coverage, Set<LogicalVariable> visiting) {
        switch (e.getExpressionTag()) {
            case CONSTANT:
                return true;
            case VARIABLE:
                return isVarCovered(((VariableReferenceExpression) e).getVariableReference(), bindings, coverage,
                        visiting);
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) e;
                FunctionIdentifier fid = fce.getFunctionIdentifier();
                // Special-case field access on the record/meta variable: PK fields sourced from that SAME
                // record only. A PK field name that collides with an unrelated field on the OTHER record
                // (e.g. a data-record field "id" next to a meta()-sourced PK also named "id") must not be
                // treated as covered here — only field-access on the record the PK is actually declared on
                // is a safe substitute.
                // A field access on the record is safe when its FULL path is a primary key or, on the data
                // record, an INCLUDE column of the searched index -- both are emitted by the search. Paths
                // rather than names, resolved through the same code the filter pushdown uses, so a nested
                // column and either access form (by name, or by index after
                // ByNameToByIndexFieldAccessRule) are all handled, as is an access the compiler split across
                // ASSIGNs ($$a := m.info; $$b := $$a.year), which resolves through the bindings to its full
                // path. An access that does not itself match falls through to the argument walk below, which
                // reaches a shorter path that may: reading `m.info.year` is servable by an `INCLUDE (info)`
                // column.
                try {
                    List<String> path =
                            VectorIncludeFilterPushdown.resolveRecordFieldPath(fce, coverage.dataRecordCtx, bindings);
                    if (path != null
                            && (coverage.recordPkFieldPaths.contains(path) || coverage.includePaths.contains(path))) {
                        return true;
                    }
                    List<String> metaPath =
                            VectorIncludeFilterPushdown.resolveRecordFieldPath(fce, coverage.metaRecordCtx, bindings);
                    if (metaPath != null && coverage.metaPkFieldPaths.contains(metaPath)) {
                        return true;
                    }
                } catch (AlgebricksException resolveFailure) {
                    LOGGER.trace("isProjectionCoveredByIndex: could not resolve a field access; not index-only",
                            resolveFailure);
                    return false;
                }
                // For any other function call: all argument expressions must be covered.
                for (Mutable<ILogicalExpression> arg : fce.getArguments()) {
                    if (!isExprCovered(arg.getValue(), bindings, coverage, visiting)) {
                        return false;
                    }
                }
                return true;
            }
            default:
                return false;
        }
    }

    /**
     * Clears the state for the next optimization attempt.
     */
    protected void clear() {
        limitRef = null;
        limitOp = null;
        orderRef = null;
        orderOp = null;
        annDistanceExpr = null;
        typeEnvironment = null;
        queryDistanceMetric = null;
        queryMinProbeFraction = 0;
        queryKMultiplier = 0;
        selectOp = null;
        numSelectOps = 0;
        aboveLimitOps.clear();
        subTree.reset();
    }

    @Override
    public Map<FunctionIdentifier, List<IAccessMethod>> getAccessMethods() {
        return accessMethods;
    }
}
