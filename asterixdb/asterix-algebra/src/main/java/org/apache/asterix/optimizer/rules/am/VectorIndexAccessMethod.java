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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.annotations.AbstractExpressionAnnotationWithIndexNames;
import org.apache.asterix.common.annotations.AnnSearchPreferenceAnnotation;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.VectorIncludeFilterPushdown;
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
import org.apache.hyracks.util.annotations.AiProvenance;

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
 * WHERE year > 2000  -- Evaluated inside the search when `year` is an INCLUDE column of the index
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
     * With {@code indexOnly} set there is no primary lookup: the search emits the primary keys, the distance
     * and the INCLUDE columns, the ORDER BY sorts on that distance, and the plan above is rebound to those
     * outputs.
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
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED, notes = "Splice the index-only search into the plan before retyping the operators above it")
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
        AnnSearchPreferenceAnnotation annHint = annDistanceExpr.getAnnotation(AnnSearchPreferenceAnnotation.class);
        ILogicalExpression queryVectorExpr = getQueryVectorExpr(annDistanceExpr, analysisCtx);

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

        // Output order is [pk..., distance?, INCLUDE columns...]. The distance goes on first because the
        // INCLUDE columns must be the trailing outputs: job generation lines the emitted tuple up with the
        // output record descriptor by taking them from the end of the variable list.
        LogicalVariable distVar = null;
        VectorIncludeFilterPushdown.IncludeColumns includeColumns = null;
        if (secondaryIndexUnnestOp instanceof UnnestMapOperator) {
            UnnestMapOperator searchUnnest = (UnnestMapOperator) secondaryIndexUnnestOp;
            if (isIndexOnlyPlan) {
                distVar = context.newVar();
                searchUnnest.getVariables().add(distVar);
                searchUnnest.getVariableTypes().add(BuiltinType.ADOUBLE);
            }
            // Every INCLUDE column of the index is declared, for both plan shapes. One mapping then serves
            // whatever reads them -- a predicate pushed into the search, a projection returning them, or
            // neither -- and the runtime emits exactly what is declared.
            includeColumns = VectorIncludeFilterPushdown.declareIncludeColumns(searchUnnest,
                    includeContext(chosenIndex, dataset, recordType, dataSourceOp), context::newVar);
        }
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
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        "the vector index-only plan expected an UnnestMapOperator from the secondary index "
                                + "search but got " + secondaryIndexUnnestOp.getOperatorTag());
            } else {
                UnnestMapOperator unnestMap = (UnnestMapOperator) secondaryIndexUnnestOp;

                // Rewrite ORDER BY: replace the ann_distance call with VarRef($$dist). isIndexOnlyPlan was
                // only left true if orderOp has exactly one expression (the ann_distance call); keep the same
                // direction.
                OrderOperator orderOp = (OrderOperator) orderRef.getValue();
                VariableReferenceExpression distVarRef = new VariableReferenceExpression(distVar);
                distVarRef.setSourceLocation(orderOp.getSourceLocation());
                orderOp.getOrderExpressions().get(0).second.setValue(distVarRef);

                // Rebind the WHERE onto the index's INCLUDE columns. There is no primary lookup here to
                // evaluate it against, and the record variable is about to be neutralized, so a predicate
                // left reading the record would collapse to select(missing) and drop every row.
                bindIncludeFilterToSearch(selectOp, chosenIndex, dataset, recordType, dataSourceOp, includeColumns,
                        context);

                // Build the variable substitution map: old PK vars → new PK vars. The secondary
                // UnnestMap allocates fresh context.newVar() PK variables, so anything that
                // referenced the old PK variable from the DataSourceScan needs to be redirected.
                List<LogicalVariable> oldVars = dataSourceOp.getVariables();
                int numPK = dataset.getPrimaryKeys().size();
                List<LogicalVariable> newPkVars = unnestMap.getVariables().subList(0, numPK);
                Map<LogicalVariable, LogicalVariable> pkSubstitution = new HashMap<>();
                for (int i = 0; i < numPK; i++) {
                    pkSubstitution.put(oldVars.get(i), newPkVars.get(i));
                }
                // The record variable and, if present, the meta record variable — we rewrite
                // field-access($$rec, "<pk_field>") / field-access($$meta, "<pk_field>") to
                // VarRef($$pk_new) below, keeping the two separate so a PK field name that collides
                // with an unrelated field on the OTHER record (e.g. a data-record field "id" next to a
                // meta()-sourced PK also named "id") is never substituted on the wrong record.
                LogicalVariable oldRecVar = numPK < oldVars.size() ? oldVars.get(numPK) : null;
                LogicalVariable oldMetaVar = numPK + 1 < oldVars.size() ? oldVars.get(numPK + 1) : null;

                // Map PK field PATH -> new PK variable, split by the record the PK field is actually
                // sourced from (0 = data record, 1 = meta record) so a PK sourced from meta() named "id" is
                // never confused with an unrelated data-record field of that name. Paths, not names: a
                // nested primary key binds exactly as a top-level one does.
                List<Integer> keySourceIndicators = DatasetUtil.getKeySourceIndicators(dataset);
                Map<List<String>, LogicalVariable> recordPkPathToNewVar = new LinkedHashMap<>();
                Map<List<String>, LogicalVariable> metaPkPathToNewVar = new LinkedHashMap<>();
                List<List<String>> pkPaths = dataset.getPrimaryKeys();
                for (int i = 0; i < numPK; i++) {
                    List<String> p = pkPaths.get(i);
                    if (p != null && !p.isEmpty()) {
                        boolean fromMeta = keySourceIndicators != null && keySourceIndicators.get(i) == 1;
                        (fromMeta ? metaPkPathToNewVar : recordPkPathToNewVar).put(p, newPkVars.get(i));
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

                // 2. Rewrite field-access on the old record/meta vars to direct PK VarRefs (e.g. the
                //    SELECT VALUE m.idx projection above LIMIT), each against only its own PK fields.
                // Both the primary keys and the INCLUDE columns are outputs of the search now, so pointing
                // the plan above at them is one operation over full paths, applied to the record the path is
                // actually declared on.
                // Through the plan's ASSIGN bindings, so an access the compiler split across ASSIGNs binds
                // as the one path it is -- the same resolution the gate accepted it by. Collected before any
                // rewrite, so each variable still maps to the access it was defined as.
                Map<LogicalVariable, ILogicalExpression> bindings =
                        VectorIncludeFilterPushdown.collectAssignBindings(rewriteRoot);
                if (oldRecVar != null) {
                    VectorIncludeFilterPushdown.IndexContext recordCtx =
                            includeContext(chosenIndex, dataset, recordType, Set.of(oldRecVar));
                    if (includeColumns != null) {
                        bindPathsInDescendants(rewriteRoot, recordCtx, includeColumns.pathToVar(), bindings);
                    }
                    bindPathsInDescendants(rewriteRoot, recordCtx, recordPkPathToNewVar, bindings);
                }
                if (oldMetaVar != null) {
                    bindPathsInDescendants(rewriteRoot,
                            includeContext(chosenIndex, dataset, metaRecordType, Set.of(oldMetaVar)),
                            metaPkPathToNewVar, bindings);
                }

                // 3. Any remaining ASSIGN expression that still references oldRecVar/oldMetaVar
                // (transitively) — for example $$x := $$m.getField("embedding") hoisted by the SQL++
                // compiler out of ann_distance(m.embedding, ...) — is now structurally orphaned: no
                // operator above LIMIT consumes it (we wouldn't have entered the index-only branch
                // otherwise) but the type system still walks it. Replace each such expression with
                // MISSING so it types cleanly. None of these values is ever read at runtime.
                List<LogicalVariable> deadRecordVars = new ArrayList<>();
                if (oldRecVar != null) {
                    deadRecordVars.add(oldRecVar);
                }
                if (oldMetaVar != null) {
                    deadRecordVars.add(oldMetaVar);
                }
                if (!deadRecordVars.isEmpty()) {
                    neutralizeDanglingExpressions(rewriteRoot, deadRecordVars);
                }

                // Cross-pollination dedup (index-only branch): when cross_pollination_m > 1 the secondary
                // cursor emits up to M (pk..., dist) copies per record. The primary-lookup path dedups via a
                // DistinctOperator above its primary UNNEST-MAP (see createRestOfIndexSearchPlan below), but the
                // index-only branch skips that path, so it must splice its OWN Distinct — keyed on the new
                // secondary-UnnestMap PK vars — directly above the secondary UNNEST-MAP and below ORDER BY/LIMIT,
                // so LIMIT applies to DISTINCT PKs. DistinctOperator propagates all input vars, so $$dist
                // survives for the ORDER BY $$dist above it. For M == 1 we return the bare UNNEST-MAP so the
                // plan stays byte-identical to the legacy path.
                ILogicalOperator indexOnlyPlan = secondaryIndexUnnestOp;
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
                    indexOnlyPlan = dedupOp;
                }

                // The rewritten operators above now read variables only the search produces, so the search
                // must be in the tree before any of them is retyped. Typed while the scan it replaces is still
                // there, those variables have no producer: a bare reference merely types as unknown, but a
                // function over one of them -- `m.year + 1`, `lower(m.title)`, a nested access on an INCLUDE
                // column -- has no input type to compute from and fails. The caller replaces this same
                // reference with what is returned here, which is harmless.
                subTree.getDataSourceRef().setValue(indexOnlyPlan);

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
                return indexOnlyPlan;
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
     * Returns the query vector of {@code annDistanceExpr}: whichever argument the matcher did not take as
     * the indexed field. A distance is symmetric in its two vectors, so the field may be written on either
     * side, and reading argument 1 positionally picks the field itself in the swapped form.
     *
     * @param annDistanceExpr the desugared 2-arg ann_distance call
     * @param analysisCtx the analysis context the matcher populated for that call
     * @return the query vector expression
     * @throws CompilationException if the call has no matched function expression in {@code analysisCtx}
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Resolve the ann_distance query vector by matched argument rather than by position")
    private static ILogicalExpression getQueryVectorExpr(AbstractFunctionCallExpression annDistanceExpr,
            AccessMethodAnalysisContext analysisCtx) throws CompilationException {
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.getMatchedFuncExprs()) {
            if (optFuncExpr.getFuncExpr() == annDistanceExpr) {
                return optFuncExpr.getConstantExpr(0);
            }
        }
        // The index was chosen from this very analysis context, so the call is always among its matched
        // expressions; a miss is a broken invariant, not a plan shape we can decline.
        throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, annDistanceExpr.getSourceLocation(),
                "no matched function expression for the ann_distance call");
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
        return ((Index.VectorIndexDetails) details).getVectorParameters().getCrossPollinationM();
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
     * Resolves the metric written in the query to its enum, or {@code null} if it is not a metric.
     * <p>
     * Index selection compares the result against the index's own metric, so {@code null} matches
     * no index.
     *
     * @param metric the metric as written in the query (may be null or blank)
     * @return the metric, or {@code null} when absent or unrecognized
     */
    public static VectorSimilarityMetric resolveQueryMetric(String metric) {
        if (metric == null || metric.isBlank()) {
            return null;
        }
        return VectorSimilarityMetric.fromAlias(metric);
    }

    /**
     * The metric an index was built with, or {@code null} for a non-vector index.
     */
    public static VectorSimilarityMetric getIndexMetric(Index index) {
        if (index.getIndexType() != IndexType.VTREE) {
            return null;
        }
        Index.VectorIndexDetails vectorDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        return vectorDetails.getVectorParameters().getSimilarity();
    }

    /**
     * Bind a {@code WHERE} over the index's {@code INCLUDE} columns to the vector search of an index-only
     * plan: declare the columns it reads as outputs of the index-search unnest-map and rewrite the
     * predicate to read them, so the cursor can filter candidates itself and no primary lookup is needed to
     * evaluate it. The predicate stays in its {@code SELECT} until the physical rewrites.
     * <p>
     * A no-op when the query has no {@code WHERE}. Otherwise the predicate must be fully bindable:
     * {@code IntroduceTopKAccessMethodRule.isProjectionPkOnly} only admits the index-only plan when
     * {@link VectorIncludeFilterPushdown} says it is, and by the time we get here {@code indexOnly} has
     * already been serialized into the index-search arguments and cannot be withdrawn. A decline at this
     * point is therefore the two having drifted apart, which is a broken invariant rather than an
     * unsupported plan shape — fail loudly instead of emitting a plan that silently drops every row.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Index-only ANN plans bind their INCLUDE-field predicate")
    private static void bindIncludeFilterToSearch(SelectOperator selectOp, Index chosenIndex, Dataset dataset,
            ARecordType recordType, AbstractDataSourceOperator dataSourceOp,
            VectorIncludeFilterPushdown.IncludeColumns includeColumns, IOptimizationContext context)
            throws AlgebricksException {
        if (selectOp == null) {
            return;
        }
        ILogicalExpression bound = VectorIncludeFilterPushdown.bindPredicate(selectOp.getCondition().getValue(),
                selectOp, includeContext(chosenIndex, dataset, recordType, dataSourceOp), context, includeColumns);
        if (bound == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "the vector index-only plan was chosen for a WHERE that cannot be pushed into the "
                            + "INCLUDE fields of index " + chosenIndex.getIndexName());
        }
        // Rebind the SELECT to the INCLUDE columns instead of moving the predicate into the unnest-map's
        // select condition. The record variable it used to read is about to be neutralized, so it cannot
        // stay as it is -- but once it reads variables the unnest-map produces it is an ordinary predicate
        // over ordinary variables, which every rule between here and the physical rewrites can handle.
        // PushFilterIntoVectorSearchRule moves it into the select condition at the same point it does for
        // the lookup-and-rerank plan, and fails the compilation if by then it cannot; a predicate the rules
        // in between prove true is simply removed, and nothing has to arrive.
        selectOp.getCondition().setValue(bound);
    }

    /**
     * The index-side inputs {@link VectorIncludeFilterPushdown} needs, with the record variable(s) a field
     * access must be rooted at to be a candidate.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5_1, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED, notes = "Root pushdown at the data record only, never the meta record")
    private static VectorIncludeFilterPushdown.IndexContext includeContext(Index chosenIndex, Dataset dataset,
            ARecordType recordType, AbstractDataSourceOperator dataSourceOp) {
        int numPK = dataset.getPrimaryKeys().size();
        List<LogicalVariable> dsVars = dataSourceOp.getVariables();
        // The scan produces [pk..., record, meta?]. Only the record is a base: INCLUDE paths are resolved
        // against the record type, so admitting the meta variable would let `meta(m).year` bind to a record
        // column of the same name. IntroduceTopKAccessMethodRule's gate makes the same choice, and must: its
        // verdict is what admits a predicate to the binding done here.
        Set<LogicalVariable> recordVars = numPK < dsVars.size() ? Set.of(dsVars.get(numPK)) : Set.of();
        return includeContext(chosenIndex, dataset, recordType, recordVars);
    }

    private static VectorIncludeFilterPushdown.IndexContext includeContext(Index chosenIndex, Dataset dataset,
            ARecordType recordType, Set<LogicalVariable> recordVars) {
        Index.VectorIndexDetails details = (Index.VectorIndexDetails) chosenIndex.getIndexDetails();
        return new VectorIncludeFilterPushdown.IndexContext(details.getIncludeFieldNames(), recordType,
                details.getVectorParameters().isQuantized(), dataset.getPrimaryKeys().size(), recordVars);
    }

    /**
     * Point every reference to one of {@code pathToVar}'s paths, in {@code op} and its descendants, at the
     * search output carrying that column, so the plan above no longer needs the assembled record.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    private static void bindPathsInDescendants(ILogicalOperator op, VectorIncludeFilterPushdown.IndexContext idx,
            Map<List<String>, LogicalVariable> pathToVar, Map<LogicalVariable, ILogicalExpression> bindings)
            throws AlgebricksException {
        if (op == null || pathToVar == null || pathToVar.isEmpty()) {
            return;
        }
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            for (Mutable<ILogicalExpression> exprRef : ((AssignOperator) op).getExpressions()) {
                VectorIncludeFilterPushdown.bindPaths(exprRef, idx, pathToVar, bindings);
            }
        }
        for (Mutable<ILogicalOperator> input : op.getInputs()) {
            bindPathsInDescendants(input.getValue(), idx, pathToVar, bindings);
        }
    }

    /**
     * Replace any ASSIGN expression whose subtree (transitively) mentions a dead variable with a MISSING
     * constant. Seeded with the now-gone dataset record variable(s) (data record and, if present, meta
     * record); the closure grows to include every variable whose defining ASSIGN we neutralize, so chains
     * such as {@code $$237 := field-access($$rec, "embedding")} → {@code $$dist := ann_distance($$237, ...)}
     * are fully repaired (the second ASSIGN references {@code $$237}, not {@code $$rec} directly, and would
     * otherwise dangle and fail type inference). The live-out check guarantees nothing above LIMIT depends
     * on these values, but the type system still walks them.
     */
    private static void neutralizeDanglingExpressions(ILogicalOperator root, List<LogicalVariable> oldRecordVars) {
        Set<LogicalVariable> dead = new HashSet<>(oldRecordVars);
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
}
