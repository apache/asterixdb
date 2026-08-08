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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.annotations.AbstractExpressionAnnotationWithIndexNames;
import org.apache.asterix.common.annotations.AnnSearchPreferenceAnnotation;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;

/**
 * Access method for vector indexes.
 *
 * This access method is designed specifically for ORDER BY ANN_DISTANCE() LIMIT k queries.
 * It does NOT handle SELECT-based optimizations (WHERE clauses with ANN_DISTANCE).
 *
 * Example query pattern:
 * <pre>
 * SELECT id, title
 * FROM movie
 * WHERE year > 2000  -- Handled by BTreeAccessMethod
 * ORDER BY ANN_DISTANCE(reviewEmbedding, [1.0, 2.0, ...], "Euclidean")  -- Handled by VectorIndexAccessMethod
 * LIMIT 10
 * </pre>
 *
 * - If vector index EXISTS: Optimizer transforms plan to use UNNEST-MAP(vector_index_search)
 *   → Returns candidate tuples (approximate ANN search)
 *   → ORDER BY ANN_DISTANCE computes distances on candidates only
 *   → Faster but approximate results
 *
 * - If vector index DOES NOT EXIST: Optimizer leaves plan unchanged (DATASOURCE_SCAN)
 *   → Returns ALL tuples (exhaustive scan)
 *   → ORDER BY ANN_DISTANCE computes distances on all tuples
 *   → Falls back to exact KNN search (slower but exact results)
 *
 * The same ORDER BY ANN_DISTANCE operator works in both cases - the optimizer just swaps the data source.
 *
 * This is used by IntroduceTopKAccessMethodRule to validate and analyze ANN_DISTANCE function calls
 * in ORDER BY clauses.
 */
public class VectorIndexAccessMethod implements IAccessMethod {

    public static final VectorIndexAccessMethod INSTANCE = new VectorIndexAccessMethod();

    // Optimizable functions for vector index: the concrete distance builtins that ann_distance desugars
    // into. Only those carrying an AnnSearchPreferenceAnnotation are actually optimized (see the gate in
    // analyzeFuncExprArgsAndUpdateAnalysisCtx); a plain vector_distance produces the same builtins WITHOUT
    // the hint and stays an exact full-scan KNN.
    private static final List<Pair<FunctionIdentifier, Boolean>> FUNC_IDENTIFIERS =
            Collections.unmodifiableList(Arrays.asList(new Pair<>(BuiltinFunctions.EUCLIDEAN_DISTANCE, true),
                    new Pair<>(BuiltinFunctions.EUCLIDEAN_SQUARED_DISTANCE, true),
                    new Pair<>(BuiltinFunctions.COSINE_DISTANCE, true),
                    new Pair<>(BuiltinFunctions.DOT_DISTANCE, true)));

    @Override
    public List<Pair<FunctionIdentifier, Boolean>> getOptimizableFunctions() {
        return FUNC_IDENTIFIERS;
    }

    @Override
    public boolean analyzeFuncExprArgsAndUpdateAnalysisCtx(AbstractFunctionCallExpression funcExpr,
            List<AbstractLogicalOperator> assignsAndUnnests, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {

        // The desugared ann_distance is a 2-arg distance builtin carrying the ANN hint. Gate on the hint so
        // a plain vector_distance (same builtin, no hint) is never rewritten into an index search.
        if (!funcExpr.hasAnnotation(AnnSearchPreferenceAnnotation.class)) {
            return false;
        }
        if (funcExpr.getArguments().size() != 2) {
            return false;
        }
        // Validate arg0 = variable/field reference, arg1 = constant (query vector).
        // Populates analysisCtx with information needed for index matching.
        return AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVarAndUpdateAnalysisCtx(funcExpr, analysisCtx,
                context, typeEnvironment, false);
    }

    @Override
    public boolean matchIndexType(IndexType indexType) {
        return indexType == IndexType.VTREE;
    }

    @Override
    public boolean matchAllIndexExprs(Index index) {
        // Vector indexes only have one field, so this is not applicable
        return false;
    }

    @Override
    public boolean matchPrefixIndexExprs(Index index) {
        // Vector indexes don't support prefix matching like composite BTree indexes
        return false;
    }

    @Override
    public boolean applySelectPlanTransformation(List<Mutable<ILogicalOperator>> afterSelectRefs,
            Mutable<ILogicalOperator> selectRef, OptimizableOperatorSubTree subTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context) throws AlgebricksException {
        // NOT IMPLEMENTED: Vector indexes are not used for SELECT-based optimizations
        // If we wanted to support: WHERE ANN_DISTANCE(...) < threshold
        // we would implement this method. For now, only ORDER BY + LIMIT is supported.
        return false;
    }

    @Override
    public ILogicalOperator createIndexSearchPlan(List<Mutable<ILogicalOperator>> afterTopOpRefs,
            Mutable<ILogicalOperator> topOpRef, Mutable<ILogicalExpression> conditionRef,
            List<Mutable<ILogicalOperator>> assignBeforeTheOpRefs, OptimizableOperatorSubTree indexSubTree,
            OptimizableOperatorSubTree probeSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            boolean retainInput, boolean retainNull, boolean requiresBroadcast, IOptimizationContext context,
            LogicalVariable newMissingNullPlaceHolderForLOJ, IAlgebricksConstantValue leftOuterMissingValue,
            List<Pair<LogicalVariable, List<ILogicalExpression>>> optimizableDisjunctionConditions)
            throws AlgebricksException {
        // NOT IMPLEMENTED: Used for SELECT-based plan transformation
        // Vector index top-k search plan is created in IntroduceTopKAccessMethodRule
        return null;
    }

    @Override
    public boolean applyJoinPlanTransformation(List<Mutable<ILogicalOperator>> afterJoinRefs,
            Mutable<ILogicalOperator> joinRef, OptimizableOperatorSubTree leftSubTree,
            OptimizableOperatorSubTree rightSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, boolean isLeftOuterJoin, boolean isLeftOuterJoinWithSpecialGroupBy,
            IAlgebricksConstantValue leftOuterMissingValue) throws AlgebricksException {
        // NOT IMPLEMENTED: Vector indexes are not used for join optimization
        return false;
    }

    /**
     * Creates vector index search plan for ORDER BY ANN_DISTANCE() queries.
     *
     * This method creates a two-stage index search plan:
     * 1. Vector index search: Returns (vector_embedding, pk) for candidate tuples
     * 2. Primary index lookup: Uses PKs to fetch full records
     *
     * Transformation:
     *   LIMIT k → ORDER BY ANN_DISTANCE(vectorField, qvec, metric) → ... → DATASOURCE_SCAN
     * Into:
     *   LIMIT k → ORDER BY ANN_DISTANCE(vectorField, qvec, metric) → PRIMARY_INDEX_UNNEST(pk) → VECTOR_INDEX_UNNEST
     *
     * Data flow:
     * - VECTOR_INDEX_UNNEST: Returns top-k candidates from index (vector_embedding + pk)
     * - PRIMARY_INDEX_UNNEST: Uses PK to fetch full record with all fields
     * - ORDER BY: Computes exact distances on full records
     * - LIMIT: Extracts final top-k results
     *
     * TODO: Add index-only plan optimization (skip primary lookup when only PK is needed)
     *
     * @param limitRef Reference to LIMIT operator
     * @param orderRef Reference to ORDER operator
     * @param annDistanceExpr The ANN_DISTANCE function expression from ORDER BY
     * @param subTree The subtree containing the data source
     * @param chosenIndex The vector index to use
     * @param analysisCtx Analysis context with index information
     * @param context Optimization context
     * @return The transformed plan with vector index search + primary lookup, or null if transformation fails
     */
    public ILogicalOperator createIndexSearchPlan(Mutable<ILogicalOperator> limitRef,
            Mutable<ILogicalOperator> orderRef, AbstractFunctionCallExpression annDistanceExpr,
            OptimizableOperatorSubTree subTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, SelectOperator selectOp, boolean indexOnly,
            List<AbstractLogicalOperator> aboveLimitOps) throws AlgebricksException {

        // Get dataset metadata
        Dataset dataset = subTree.getDataset();
        ARecordType recordType = subTree.getRecordType();
        ARecordType metaRecordType = subTree.getMetaRecordType();
        AbstractDataSourceOperator dataSourceOp = (AbstractDataSourceOperator) subTree.getDataSourceRef().getValue();

        // The desugared ann_distance call is <distance-builtin>(vectorField, queryVector) carrying an
        // AnnSearchPreferenceAnnotation with {metric, min_probe_fraction, k_multiplier}.
        // arg0 = vectorField (variable reference), arg1 = queryVector (constant or variable).
        AnnSearchPreferenceAnnotation annHint = annDistanceExpr.getAnnotation(AnnSearchPreferenceAnnotation.class);
        ILogicalExpression queryVectorExpr = annDistanceExpr.getArguments().get(1).getValue();

        // Extract k value from LIMIT operator
        LimitOperator limitOp = (LimitOperator) limitRef.getValue();
        ILogicalExpression kValueExpr = limitOp.getMaxObjects().getValue();

        // Create variables to hold query parameters in the positional layout the runtime search expects:
        // [query_vector, k_value, distance_metric, min_probe_fraction, k_multiplier].
        ArrayList<LogicalVariable> queryVarList = new ArrayList<>();
        ArrayList<Mutable<ILogicalExpression>> queryExprList = new ArrayList<>();

        // Add query vector variable
        LogicalVariable queryVectorVar = context.newVar();
        queryVarList.add(queryVectorVar);
        queryExprList.add(new MutableObject<>(queryVectorExpr.cloneExpression()));

        // Add k value variable
        LogicalVariable kValueVar = context.newVar();
        queryVarList.add(kValueVar);
        queryExprList.add(new MutableObject<>(kValueExpr.cloneExpression()));

        // Add distance metric variable (slot 2). The runtime scan never reads it (the effective metric is
        // baked into the index metadata); it is retained for the positional contract and rebuilt from the
        // ANN hint's metric string.
        LogicalVariable distanceMetricVar = context.newVar();
        queryVarList.add(distanceMetricVar);
        queryExprList.add(new MutableObject<>(
                new ConstantExpression(new AsterixConstantValue(new AString(annHint.getMetric())))));

        // Add min_probe_fraction variable (slot 3) from the ANN hint (default/validation applied at rewrite).
        // Fraction of leaf clusters to probe (0.0-1.0); nprobe = max(1, floor(totalLeafClusters * fraction)).
        LogicalVariable minProbeFractionVar = context.newVar();
        queryVarList.add(minProbeFractionVar);
        queryExprList.add(new MutableObject<>(
                new ConstantExpression(new AsterixConstantValue(new ADouble(annHint.getMinProbeFraction())))));

        // Add k_multiplier variable (slot 4) from the ANN hint. K * kMultiplier candidates are collected
        // for reranking.
        LogicalVariable kMultiplierVar = context.newVar();
        queryVarList.add(kMultiplierVar);
        queryExprList.add(new MutableObject<>(
                new ConstantExpression(new AsterixConstantValue(new AInt32(annHint.getKMultiplier())))));

        // Create ASSIGN operator to hold query parameters
        AssignOperator assignSearchKeys = new AssignOperator(queryVarList, queryExprList);
        assignSearchKeys.setSourceLocation(dataSourceOp.getSourceLocation());
        assignSearchKeys.getInputs().add(
                new MutableObject<>(OperatorManipulationUtil.deepCopy(dataSourceOp.getInputs().get(0).getValue())));
        assignSearchKeys.setExecutionMode(dataSourceOp.getExecutionMode());
        context.computeAndSetTypeEnvironmentForOperator(assignSearchKeys);

        // Create VectorJobGenParams to pass parameters to Hyracks runtime
        VectorJobGenParams jobGenParams = new VectorJobGenParams(chosenIndex.getIndexName(), IndexType.VTREE,
                chosenIndex.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName(), false, // retainInput - not needed for simple case
                false // requiresBroadcast
        );
        jobGenParams.setQueryVarList(queryVarList);

        // Index-only opportunity detected upstream in IntroduceTopKAccessMethodRule (PK-only
        // projection above LIMIT). When set, the secondary UnnestMap emits an extra $$dist field, the
        // ORDER BY rebinds to it, and the primary BTree lookup + rerank are skipped (see branch below).
        //
        // Every precondition must be settled HERE, before the params are handed to
        // AccessMethodUtils.createSecondaryIndexUnnestMap(): that call serializes jobGenParams into the
        // index-search function arguments, so clearing indexOnly afterwards does not change the emitted plan
        // — the runtime would still emit [pk..., dist] while the plan declares only the PK variables, an
        // output arity mismatch. The ORDER shape the branch below relies on is checkable now.
        boolean isIndexOnlyPlan = indexOnly;
        if (isIndexOnlyPlan) {
            ILogicalOperator orderCandidate = orderRef.getValue();
            isIndexOnlyPlan = orderCandidate.getOperatorTag() == LogicalOperatorTag.ORDER
                    && ((OrderOperator) orderCandidate).getOrderExpressions().size() == 1;
        }
        jobGenParams.setIndexOnly(isIndexOnlyPlan);

        // Create UNNEST-MAP operator for vector index search
        // This returns: <pk> from the index (vector embeddings skipped to save memory)
        ILogicalOperator secondaryIndexUnnestOp = AccessMethodUtils.createSecondaryIndexUnnestMap(dataset, recordType,
                metaRecordType, chosenIndex, assignSearchKeys, jobGenParams, context, false, false, null);

        // INCLUDE-field filter pushdown is intentionally deferred to PushFilterIntoVectorSearchRule (a
        // physical rewrite): the field-access predicate references the record variable, which is not in this
        // unnest's input type environment. Just register the variables the vector index search produces.
        context.computeAndSetTypeEnvironmentForOperator(secondaryIndexUnnestOp);

        if (isIndexOnlyPlan) {
            // ============================================================================
            // INDEX-ONLY BRANCH
            // ============================================================================
            // The projection above LIMIT only references PK columns (verified by
            // IntroduceTopKAccessMethodRule.isProjectionPkOnly). We can therefore:
            //   1. Add a new $$dist ADOUBLE output variable to the secondary UnnestMap so the runtime
            //      emits  [ pk_0, ..., pk_{n-1}, dist ]  per candidate.
            //   2. Rewrite the upstream ORDER BY ann_distance(...) to ORDER BY $$dist (a cheap scalar
            //      sort, no record assembly, no field-access, no rerank).
            //   3. Substitute old PK variables (from the original DataSourceScan) with the new PK
            //      variables produced by the secondary UnnestMap — the optimizer is about to replace
            //      the DataSourceScan, so any operator that still references the old PK vars would
            //      dangle.
            //   4. Rewrite field-access($$rec, "<pk_field>") expressions to VarRef($$pk_new) so ASSIGNs
            //      like  $$id := field-access($$rec, "id")  become  $$id := $$pk_new.
            //   5. Skip createRestOfIndexSearchPlan — no primary BTree lookup, no record assembly. The
            //      runtime reads D(q,x) from the cursor and appends it.
            if (!(secondaryIndexUnnestOp instanceof UnnestMapOperator)) {
                // Not recoverable: indexOnly=true is already baked into the index-search arguments above, so
                // there is no falling back to lookup-and-rerank from here. retainInput/retainNull are both
                // false, which is exactly the case where createSecondaryIndexUnnestMap returns an
                // UnnestMapOperator, so this is a broken invariant rather than an unsupported plan shape.
                throw new AlgebricksException("Vector index-only plan: expected an UnnestMapOperator from the "
                        + "secondary index search but got " + secondaryIndexUnnestOp.getOperatorTag());
            } else {
                UnnestMapOperator unnestMap = (UnnestMapOperator) secondaryIndexUnnestOp;
                LogicalVariable distVar = context.newVar();
                unnestMap.getVariables().add(distVar);
                unnestMap.getVariableTypes().add(BuiltinType.ADOUBLE);

                // Rewrite ORDER BY: replace the ann_distance call with VarRef($$dist). isIndexOnlyPlan was
                // only left true if orderOp has exactly one expression (the ann_distance call); keep the same
                // direction.
                OrderOperator orderOp = (OrderOperator) orderRef.getValue();
                VariableReferenceExpression distVarRef = new VariableReferenceExpression(distVar);
                distVarRef.setSourceLocation(orderOp.getSourceLocation());
                orderOp.getOrderExpressions().get(0).second.setValue(distVarRef);

                // Build the variable substitution map: old PK vars → new PK vars. The secondary
                // UnnestMap allocates fresh context.newVar() PK variables, so anything that
                // referenced the old PK variable from the DataSourceScan needs to be redirected.
                List<LogicalVariable> oldVars = dataSourceOp.getVariables();
                int numPK = dataset.getPrimaryKeys().size();
                List<LogicalVariable> newPkVars = unnestMap.getVariables().subList(0, numPK);
                Map<LogicalVariable, LogicalVariable> pkSubstitution = new java.util.HashMap<>();
                for (int i = 0; i < numPK; i++) {
                    pkSubstitution.put(oldVars.get(i), newPkVars.get(i));
                }
                // The record variable (and meta record var, if present) — we rewrite
                // field-access($$rec, "<pk_field>") to VarRef($$pk_new) below.
                LogicalVariable oldRecVar = numPK < oldVars.size() ? oldVars.get(numPK) : null;

                // Map PK field name -> new PK variable.
                Map<String, LogicalVariable> pkFieldToNewVar = new java.util.HashMap<>();
                List<List<String>> pkPaths = dataset.getPrimaryKeys();
                for (int i = 0; i < numPK; i++) {
                    List<String> p = pkPaths.get(i);
                    if (p != null && p.size() == 1) {
                        pkFieldToNewVar.put(p.get(0), newPkVars.get(i));
                    }
                }

                // The rewrites below must cover the operators ABOVE the LIMIT too — the result
                // projection (e.g. SELECT VALUE m.idx) lives above LIMIT and references the old
                // record var via field-access($$rec, "idx"). Walking only from limitRef would leave
                // it dangling ("Could not infer type"). aboveLimitOps[0] is the outermost ancestor
                // (e.g. DistributeResult); walking from it reaches the projection, the LIMIT, the
                // ORDER, and the ASSIGNs below in a single descent.
                ILogicalOperator rewriteRoot = (aboveLimitOps != null && !aboveLimitOps.isEmpty())
                        ? aboveLimitOps.get(0) : (ILogicalOperator) limitRef.getValue();

                // 1. Substitute old PK var refs throughout the plan (above and below LIMIT).
                org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities
                        .substituteVariablesInDescendantsAndSelf(rewriteRoot, pkSubstitution, context);

                // 2. Rewrite field-access on the old record var to direct PK VarRefs (e.g. the
                //    SELECT VALUE m.idx projection above LIMIT).
                if (oldRecVar != null) {
                    rewriteRecordFieldAccessToPk(rewriteRoot, oldRecVar, pkFieldToNewVar);
                }

                // 3. Any remaining ASSIGN expression that still references oldRecVar (transitively) —
                // for example $$x := $$m.getField("embedding") hoisted by the SQL++ compiler out of
                // ann_distance(m.embedding, ...) — is now structurally orphaned: no operator above
                // LIMIT consumes it (we wouldn't have entered the index-only branch otherwise) but the
                // type system still walks it. Replace each such expression with MISSING so it types
                // cleanly. None of these values is ever read at runtime.
                if (oldRecVar != null) {
                    neutralizeDanglingExpressions(rewriteRoot, oldRecVar);
                }

                // Recompute type env bottom-up over every operator whose schema/expressions changed:
                // the new secondary UnnestMap, the LIMIT chain, then each above-LIMIT ancestor from the
                // one closest to LIMIT up to the root (aboveLimitOps is ordered root-first).
                context.computeAndSetTypeEnvironmentForOperator(secondaryIndexUnnestOp);
                org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil.typeOpRec(limitRef, context);
                if (aboveLimitOps != null) {
                    for (int ai = aboveLimitOps.size() - 1; ai >= 0; ai--) {
                        context.computeAndSetTypeEnvironmentForOperator(aboveLimitOps.get(ai));
                    }
                }

                // Cross-pollination dedup (index-only branch): when cross_pollination_m > 1 the secondary
                // cursor emits up to M (pk..., dist) copies per record. The primary-lookup path dedups via a
                // DistinctOperator above its primary UNNEST-MAP (see createRestOfIndexSearchPlan below), but the
                // index-only branch skips that path, so it must splice its OWN Distinct — keyed on the new
                // secondary-UnnestMap PK vars — directly above the secondary UNNEST-MAP and below ORDER BY/LIMIT,
                // so LIMIT applies to DISTINCT PKs. DistinctOperator propagates all input vars, so $$dist
                // survives for the ORDER BY $$dist above it. For M == 1 we return the bare UNNEST-MAP so the
                // plan stays byte-identical to the legacy path.
                if (extractCrossPollinationM(chosenIndex) > 1 && numPK > 0) {
                    List<Mutable<ILogicalExpression>> dedupExprs = new ArrayList<>(numPK);
                    for (LogicalVariable pkVar : newPkVars) {
                        VariableReferenceExpression pkRef = new VariableReferenceExpression(pkVar);
                        pkRef.setSourceLocation(secondaryIndexUnnestOp.getSourceLocation());
                        dedupExprs.add(new MutableObject<>(pkRef));
                    }
                    DistinctOperator dedupOp = new DistinctOperator(dedupExprs);
                    dedupOp.setSourceLocation(secondaryIndexUnnestOp.getSourceLocation());
                    dedupOp.getInputs().add(new MutableObject<>(secondaryIndexUnnestOp));
                    dedupOp.setExecutionMode(secondaryIndexUnnestOp.getExecutionMode());
                    context.computeAndSetTypeEnvironmentForOperator(dedupOp);
                    return dedupOp;
                }
                return secondaryIndexUnnestOp;
            }
        }

        // Add primary index lookup to get full record
        // This uses the PKs returned from vector index to fetch complete records
        ILogicalOperator primaryIndexUnnestOp = AccessMethodUtils.createRestOfIndexSearchPlan(null, // afterTopOpRefs - not needed for ORDER BY case
                null, // topOpRef - not needed for ORDER BY case
                null, // conditionRef - no WHERE condition to push down
                null, // assignsBeforeTopOpRef - query params already in assignSearchKeys
                dataSourceOp, dataset, recordType, metaRecordType, secondaryIndexUnnestOp, // inputOp: vector index search results
                context, true, // sortPrimaryKeys
                false, // retainInput
                false, // retainMissing
                false, // requiresBroadcast
                chosenIndex, // secondaryIndex
                analysisCtx, subTree, // indexSubTree
                null, // probeSubTree - not a join
                null, // newMissingPlaceHolderForLOJ
                null, // leftOuterMissingValue
                false, // anyRealTypeConvertedToIntegerType
                null // optimizableDisjunctionConditions — vector index doesn't use OR-disjunction CBO
        );

        // Cross-pollination dedup: each base record surfaces as up to M (distance, centroidId, pk) copies from
        // the vector cursor when cross_pollination_m > 1. Splice a DistinctOperator keyed on the PK variables
        // ABOVE the primary-index lookup. We can't put it BETWEEN the two UNNEST-MAPs because
        // createRestOfIndexSearchPlan internally casts its inputOp to AbstractUnnestMapOperator. Cost is one
        // primary-key lookup per duplicate, bounded by M * nprobe_candidates — cheap in practice. For M == 1
        // we skip dedup so the plan is byte-identical to the legacy path.
        //
        // Dedup keys are the data-source operator's PK vars (head of dataSourceOp.getVariables()), NOT the
        // secondary UNNEST-MAP's PK vars: createRestOfIndexSearchPlan emits the primary UNNEST-MAP using the
        // data source's variable IDs, so those are the vars actually in scope above primaryIndexUnnestOp. Using
        // the secondary's PK vars instead causes "Could not infer type for variable" because they are only
        // INPUT search keys to the primary unnest, not part of its output type environment.
        ILogicalOperator topOfIndexPlan = primaryIndexUnnestOp;
        int crossPollinationM = extractCrossPollinationM(chosenIndex);
        if (crossPollinationM > 1) {
            List<LogicalVariable> dsVars = dataSourceOp.getVariables();
            int numPkVars = dataset.getPrimaryKeys().size();
            if (numPkVars > 0 && dsVars.size() >= numPkVars) {
                List<LogicalVariable> dedupPkVars = new ArrayList<>(dsVars.subList(0, numPkVars));
                List<Mutable<ILogicalExpression>> distinctExprs = new ArrayList<>(dedupPkVars.size());
                for (LogicalVariable pkVar : dedupPkVars) {
                    VariableReferenceExpression pkRef = new VariableReferenceExpression(pkVar);
                    pkRef.setSourceLocation(primaryIndexUnnestOp.getSourceLocation());
                    distinctExprs.add(new MutableObject<>(pkRef));
                }
                DistinctOperator dedupOp = new DistinctOperator(distinctExprs);
                dedupOp.setSourceLocation(primaryIndexUnnestOp.getSourceLocation());
                dedupOp.getInputs().add(new MutableObject<>(primaryIndexUnnestOp));
                dedupOp.setExecutionMode(primaryIndexUnnestOp.getExecutionMode());
                context.computeAndSetTypeEnvironmentForOperator(dedupOp);
                topOfIndexPlan = dedupOp;
            }
        }

        return topOfIndexPlan;
    }

    /**
     * Reads {@code cross_pollination_m} from the chosen vector index's WITH-object. Returns 1 (no
     * cross-pollination) if the index is not a {@code VectorIndexDetails} or the field is absent.
     */
    private static int extractCrossPollinationM(Index chosenIndex) {
        if (chosenIndex == null) {
            return 1;
        }
        Index.IIndexDetails details = chosenIndex.getIndexDetails();
        if (!(details instanceof Index.VectorIndexDetails)) {
            return 1;
        }
        AdmObjectNode withObj = ((Index.VectorIndexDetails) details).getWithObjectNode();
        if (withObj == null) {
            return 1;
        }
        return Math.max(1, withObj.getOptionalInt("cross_pollination_m", 1));
    }

    /**
     * Checks if this ann_distance expression can use the given vector index (by field-name match).
     * Query/index distance-metric compatibility is handled separately in
     * {@link IntroduceTopKAccessMethodRule#chooseVectorIndex}.
     *
     * @param index The vector index to check
     * @param optFuncExpr The optimizable function expression
     * @param checkApplicableOnly Whether to only check applicability
     * @return true if the index can be used, false otherwise
     */
    @Override
    public boolean exprIsOptimizable(Index index, IOptimizableFuncExpr optFuncExpr, boolean checkApplicableOnly)
            throws AlgebricksException {
        if (index.getIndexType() != IndexType.VTREE) {
            return false;
        }
        // Get the field name from the ann_distance call (arg0).
        List<String> fieldName = optFuncExpr.getFieldName(0);
        if (fieldName == null || fieldName.isEmpty()) {
            return false;
        }
        // A vector index has exactly one key field; the query field must match it.
        Index.VectorIndexDetails vectorDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        List<List<String>> indexKeyFieldNames = vectorDetails.getKeyFieldNames();
        if (indexKeyFieldNames.size() != 1) {
            return false;
        }
        return indexKeyFieldNames.get(0).equals(fieldName);
    }

    @Override
    public AbstractExpressionAnnotationWithIndexNames getSecondaryIndexAnnotation(IOptimizableFuncExpr optFuncExpr) {
        // Not used for vector indexes
        return null;
    }

    @Override
    public String getName() {
        return "VECTOR_INDEX";
    }

    @Override
    public boolean acceptsFunction(AbstractFunctionCallExpression functionExpr, Index index, IAType indexedFieldType,
            boolean defaultNull, boolean finalStep) throws AlgebricksException {
        // Check if this function can be optimized with this index type

        // Vector fields should be arrays of numbers
        ATypeTag typeTag = indexedFieldType.getTypeTag();
        return typeTag == ATypeTag.ARRAY || typeTag == ATypeTag.MULTISET;
    }

    @Override
    public int compareTo(IAccessMethod o) {
        return this.getName().compareTo(o.getName());
    }

    /**
     * Normalizes a distance metric string to its canonical form.
     * Handles aliases and case-insensitive matching.
     *
     * Supported metrics and aliases:
     * - "euclidean", "l2" → "euclidean"
     * - "euclidean_squared", "l2_squared" → "euclidean_squared"
     * - "cosine", "cosine similarity" → "cosine"
     * - "dot" → "dot"
     *
     * @param metric The distance metric string (may be null or empty)
     * @return Normalized canonical metric name, or "euclidean" as default
     */
    public static String normalizeDistanceMetric(String metric) {
        if (metric == null || metric.trim().isEmpty()) {
            return VectorSimilarityMetric.EUCLIDEAN.canonical(); // Default metric
        }
        VectorSimilarityMetric resolved = VectorSimilarityMetric.fromAlias(metric);
        // Unknown metric: return normalized as-is (the distance function handles/rejects it).
        return (resolved != null) ? resolved.canonical() : metric.toLowerCase().trim();
    }

    /**
     * Extracts and normalizes the distance metric from a vector index.
     *
     * @param index The vector index
     * @return Normalized distance metric string, or "euclidean" as default
     */
    public static String getIndexDistanceMetric(Index index) {
        if (index.getIndexType() != IndexType.VTREE) {
            return ""; // Default for non-vector indexes
        }

        Index.VectorIndexDetails vectorDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        AdmObjectNode withObjectNode = vectorDetails.getWithObjectNode();

        String indexMetric = (withObjectNode != null) ? withObjectNode.getOptionalString("similarity", "") : "";

        return normalizeDistanceMetric(indexMetric);
    }

    /**
     * Ensures an integer constant expression uses AInt32 instead of AInt64.
     *
     * SQL++ parser creates AInt64 (8 bytes) for all integer literals, but the runtime
     * uses IntegerPointable.getInteger() which reads only 4 bytes. Reading the first
     * 4 bytes of an 8-byte big-endian AInt64 for small values yields 0.
     *
     * This method converts AInt64 constants to AInt32 at compile time so the runtime
     * can correctly read them with IntegerPointable.
     *
     * @param expr The expression to check and potentially convert
     * @return The original expression if not an AInt64 constant, or a new AInt32 constant expression
     */

    /**
     * Index-only support: walk {@code op} and its descendants and replace every
     * {@code field-access-by-name(VarRef(recVar), "fieldName")} where {@code fieldName} is a PK column
     * with a direct {@code VarRef(pkVar)}. Used to repair ASSIGNs like
     * {@code $$id := field-access($$rec, "id")} after the DataSourceScan has been replaced by the
     * secondary UnnestMap (which no longer produces the record variable).
     */
    private static void rewriteRecordFieldAccessToPk(ILogicalOperator op, LogicalVariable recVar,
            Map<String, LogicalVariable> pkFieldToVar) {
        if (op == null) {
            return;
        }
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator a = (AssignOperator) op;
            for (Mutable<ILogicalExpression> exprRef : a.getExpressions()) {
                rewriteRecordFieldAccessExpr(exprRef, recVar, pkFieldToVar);
            }
        }
        // Other operator types could in principle hold expressions referencing $$rec (e.g. SELECT,
        // ORDER, ASSIGNs further down). Walk all inputs and apply uniformly.
        for (Mutable<ILogicalOperator> input : op.getInputs()) {
            rewriteRecordFieldAccessToPk(input.getValue(), recVar, pkFieldToVar);
        }
    }

    private static void rewriteRecordFieldAccessExpr(Mutable<ILogicalExpression> exprRef, LogicalVariable recVar,
            Map<String, LogicalVariable> pkFieldToVar) {
        ILogicalExpression e = exprRef.getValue();
        if (e.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) e;
        if (BuiltinFunctions.FIELD_ACCESS_BY_NAME.equals(fce.getFunctionIdentifier())
                && fce.getArguments().size() == 2) {
            ILogicalExpression a0 = fce.getArguments().get(0).getValue();
            ILogicalExpression a1 = fce.getArguments().get(1).getValue();
            if (a0.getExpressionTag() == LogicalExpressionTag.VARIABLE
                    && a1.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                LogicalVariable v0 = ((VariableReferenceExpression) a0).getVariableReference();
                if (recVar.equals(v0)) {
                    String fieldName = extractStringFromConstant(a1);
                    if (fieldName != null && pkFieldToVar.containsKey(fieldName)) {
                        VariableReferenceExpression pkRef =
                                new VariableReferenceExpression(pkFieldToVar.get(fieldName));
                        pkRef.setSourceLocation(e.getSourceLocation());
                        exprRef.setValue(pkRef);
                        return;
                    }
                }
            }
        }
        for (Mutable<ILogicalExpression> arg : fce.getArguments()) {
            rewriteRecordFieldAccessExpr(arg, recVar, pkFieldToVar);
        }
    }

    /**
     * Replace any ASSIGN expression whose subtree (transitively) mentions a dead variable with a MISSING
     * constant. Seeded with the now-gone dataset record variable; the closure grows to include every
     * variable whose defining ASSIGN we neutralize, so chains such as
     * {@code $$237 := field-access($$rec, "embedding")} → {@code $$dist := ann_distance($$237, ...)} are
     * fully repaired (the second ASSIGN references {@code $$237}, not {@code $$rec} directly, and would
     * otherwise dangle and fail type inference). The live-out check guarantees nothing above LIMIT depends
     * on these values, but the type system still walks them.
     */
    private static void neutralizeDanglingExpressions(ILogicalOperator root, LogicalVariable oldRecVar) {
        Set<LogicalVariable> dead = new java.util.HashSet<>();
        dead.add(oldRecVar);
        // Iterate to a fixpoint: each pass may neutralize an ASSIGN whose variable then feeds the next.
        while (neutralizeDeadPass(root, dead)) {
            // keep going until no new variable becomes dead
        }
    }

    private static boolean neutralizeDeadPass(ILogicalOperator op, Set<LogicalVariable> dead) {
        if (op == null) {
            return false;
        }
        boolean changed = false;
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator a = (AssignOperator) op;
            List<LogicalVariable> vars = a.getVariables();
            List<Mutable<ILogicalExpression>> exprs = a.getExpressions();
            for (int i = 0; i < exprs.size(); i++) {
                Mutable<ILogicalExpression> exprRef = exprs.get(i);
                ILogicalExpression e = exprRef.getValue();
                if (e.getExpressionTag() != LogicalExpressionTag.CONSTANT && expressionReferencesAny(e, dead)) {
                    ConstantExpression missing = new ConstantExpression(new AsterixConstantValue(AMissing.MISSING));
                    missing.setSourceLocation(e.getSourceLocation());
                    exprRef.setValue(missing);
                    if (dead.add(vars.get(i))) {
                        changed = true;
                    }
                }
            }
        }
        for (Mutable<ILogicalOperator> input : op.getInputs()) {
            changed |= neutralizeDeadPass(input.getValue(), dead);
        }
        return changed;
    }

    private static boolean expressionReferencesAny(ILogicalExpression e, Set<LogicalVariable> vars) {
        if (e == null) {
            return false;
        }
        if (e.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            return vars.contains(((VariableReferenceExpression) e).getVariableReference());
        }
        if (e.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            for (Mutable<ILogicalExpression> arg : ((AbstractFunctionCallExpression) e).getArguments()) {
                if (expressionReferencesAny(arg.getValue(), vars)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static String extractStringFromConstant(ILogicalExpression e) {
        if (e.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return null;
        }
        ConstantExpression ce = (ConstantExpression) e;
        IAlgebricksConstantValue v = ce.getValue();
        if (!(v instanceof AsterixConstantValue)) {
            return null;
        }
        IAObject obj = ((AsterixConstantValue) v).getObject();
        return (obj instanceof AString) ? ((AString) obj).getStringValue() : null;
    }

}
