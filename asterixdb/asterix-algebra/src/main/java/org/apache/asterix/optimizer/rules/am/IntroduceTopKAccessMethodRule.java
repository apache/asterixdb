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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.asterix.common.annotations.AnnSearchPreferenceAnnotation;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.metadata.declared.IIndexProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
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
    protected String queryDistanceMetric = null; // Distance metric from the query (e.g., "euclidean", "cosine")

    // SELECT operator info for filter pushdown
    protected SelectOperator selectOp = null;
    protected Set<List<String>> filterFieldNames = null; // Field names referenced in WHERE clause

    /**
     * Master switch for the index-only ANN plan optimization. Enabled by default: when the projection above
     * the LIMIT is PK-only, the plan emits (pk, dist) directly from the secondary VTree and skips the
     * primary BTree lookup + rerank. The plan rewrite handles above-LIMIT query-parameter ASSIGNs (see
     * {@link #isProjectionPkOnly}), and the secondary VTree reconciles anti-matter (delete) tuples itself,
     * so a PK-only ANN query over a dataset with deletes returns no deleted PKs.
     */
    private static final boolean INDEX_ONLY_ENABLED = true;

    /**
     * Ancestor chain from the rewrite-pre entry point down to (but not including) the matched
     * {@code limitOp}. Populated by {@link #checkAndApplyTopKTransformation} as it recurses; consumed
     * by {@link #isProjectionPkOnly} to gather variables used above the LIMIT.
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
        setMetadataIndexDeclarations(context, (IIndexProvider) context.getMetadataProvider());

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

    /**
     * Recursively checks the plan for LIMIT → ORDER BY ANN_DISTANCE pattern
     * and applies vector index optimization if applicable.
     */
    protected boolean checkAndApplyTopKTransformation(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

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
                    return analyzeAndTransform(context);
                }
            }
        }

        // Recursively check children — push self onto the ancestor chain so that, when we reach a
        // LIMIT below, isProjectionPkOnly() has the full list of operators above it.
        aboveLimitOps.add(op);
        try {
            for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
                boolean transformed = checkAndApplyTopKTransformation(inputOpRef, context);
                if (transformed) {
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
        // Populates selectOp and filterFieldNames if a SELECT exists.
        // Must be called AFTER setDatasetAndTypeMetadata so recordType is available.
        findSelectOperatorInSubTree();

        // 5. Analyze ANN_DISTANCE function arguments
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new TreeMap<>();
        if (!analyzeAnnDistanceFunction(analyzedAMs, context)) {
            return false;
        }

        // 6. Find applicable vector indexes on the dataset
        fillSubTreeIndexExprs(subTree, analyzedAMs, context, false);

        // 7. Choose best vector index (considering INCLUDE fields if filter exists)
        List<Pair<IAccessMethod, Index>> chosenIndexes = new ArrayList<>();
        chooseVectorIndex(analyzedAMs, chosenIndexes);

        if (chosenIndexes.isEmpty()) {
            // No vector index available (either none exists, or none has required INCLUDE fields).
            // Fall back to data scan + sort.
            context.addToDontApplySet(this, limitOp);
            return false;
        }

        // 8. Apply plan transformation
        Index vectorIndex = chosenIndexes.get(0).second;
        AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(VectorIndexAccessMethod.INSTANCE);

        boolean transformed = applyTopKPlanTransformation(vectorIndex, analysisCtx, context);

        // Always mark as processed to avoid re-attempting optimization on this operator
        context.addToDontApplySet(this, limitOp);

        return transformed;
    }

    /**
     * Finds SELECT operator between ORDER and DATASOURCE_SCAN.
     * Also extracts filter field names from the SELECT condition.
     *
     * @return The SELECT operator if found, null otherwise
     */
    protected SelectOperator findSelectOperatorInSubTree() {
        if (orderOp.getInputs().isEmpty()) {
            return null;
        }

        // Traverse from ORDER down to DATASOURCE_SCAN
        AbstractLogicalOperator currentOp = (AbstractLogicalOperator) orderOp.getInputs().get(0).getValue();

        while (currentOp != null) {
            if (currentOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                selectOp = (SelectOperator) currentOp;
                filterFieldNames = extractFilterFieldsFromCondition(selectOp.getCondition().getValue());
                return selectOp;
            }

            if (currentOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                break;
            }
            if (currentOp.getInputs().isEmpty()) {
                break;
            }
            currentOp = (AbstractLogicalOperator) currentOp.getInputs().get(0).getValue();
        }

        return null;
    }

    /**
     * Extracts field names referenced in a filter condition expression.
     * Traverses function call expressions to find field access operations.
     *
     * @param condition The filter condition expression
     * @return Set of field names (as List<String> for nested field paths)
     */
    protected Set<List<String>> extractFilterFieldsFromCondition(ILogicalExpression condition) {
        Set<List<String>> fields = new HashSet<>();
        extractFieldsFromExpressionRecursive(condition, fields);
        return fields;
    }

    /**
     * Recursively extracts field names from an expression.
     * Handles function calls (AND, OR, comparison operators, field-access).
     */
    private void extractFieldsFromExpressionRecursive(ILogicalExpression expr, Set<List<String>> fields) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

            // Check if this is a field access function (e.g., field-access-by-name)
            if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)
                    || funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
                // Extract field name from field access function
                List<String> fieldPath = extractFieldPathFromFieldAccess(funcExpr);
                if (fieldPath != null && !fieldPath.isEmpty()) {
                    fields.add(fieldPath);
                }
            } else {
                // Recursively process arguments for other functions (AND, OR, GT, LT, etc.)
                for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                    extractFieldsFromExpressionRecursive(arg.getValue(), fields);
                }
            }
        } else if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            // Variable reference - trace back through assigns to find field access.
            // First check subTree assigns; if not found, scan from ORDER down to DATASOURCE_SCAN.
            VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
            LogicalVariable var = varRef.getVariableReference();
            boolean found = findFieldFromAssigns(var, subTree.getAssignsAndUnnests(), fields);
            if (!found && orderOp != null) {
                searchAssignsInPlan(var, orderOp, fields);
            }
        }
    }

    /**
     * Searches for variable definition in a list of assigns and extracts field info.
     */
    private boolean findFieldFromAssigns(LogicalVariable var, List<AbstractLogicalOperator> assigns,
            Set<List<String>> fields) {
        for (AbstractLogicalOperator op : assigns) {
            if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assignOp = (AssignOperator) op;
                List<LogicalVariable> assignVars = assignOp.getVariables();
                List<Mutable<ILogicalExpression>> assignExprs = assignOp.getExpressions();

                for (int i = 0; i < assignVars.size(); i++) {
                    if (assignVars.get(i).equals(var)) {
                        extractFieldsFromExpressionRecursive(assignExprs.get(i).getValue(), fields);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Searches all operators in the plan from the given operator downward for variable definition.
     */
    private boolean searchAssignsInPlan(LogicalVariable var, ILogicalOperator startOp, Set<List<String>> fields) {
        ILogicalOperator currentOp = startOp;

        while (currentOp != null) {
            if (currentOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assignOp = (AssignOperator) currentOp;
                List<LogicalVariable> assignVars = assignOp.getVariables();
                List<Mutable<ILogicalExpression>> assignExprs = assignOp.getExpressions();

                for (int i = 0; i < assignVars.size(); i++) {
                    if (assignVars.get(i).equals(var)) {
                        // Found the assignment - recursively extract fields
                        extractFieldsFromExpressionRecursive(assignExprs.get(i).getValue(), fields);
                        return true;
                    }
                }
            }

            // Move to next operator
            if (currentOp.getInputs().isEmpty()) {
                break;
            }
            currentOp = currentOp.getInputs().get(0).getValue();
        }
        return false;
    }

    /**
     * Extracts field path from a field-access function expression.
     * For nested fields like row.nested.field, returns ["nested", "field"].
     */
    private List<String> extractFieldPathFromFieldAccess(AbstractFunctionCallExpression funcExpr) {
        List<String> fieldPath = new ArrayList<>();
        extractFieldPathRecursive(funcExpr, fieldPath);
        return fieldPath;
    }

    /**
     * Recursively builds field path from nested field access expressions.
     */
    private void extractFieldPathRecursive(ILogicalExpression expr, List<String> fieldPath) {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

        if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
            // First argument is the record, second argument is the field name
            if (funcExpr.getArguments().size() >= 2) {
                // Recursively process the record argument (for nested field access)
                extractFieldPathRecursive(funcExpr.getArguments().get(0).getValue(), fieldPath);

                // Extract field name from second argument
                ILogicalExpression fieldNameExpr = funcExpr.getArguments().get(1).getValue();
                String fieldName = AccessMethodUtils.getStringConstant(new MutableObject<>(fieldNameExpr));
                if (fieldName != null) {
                    fieldPath.add(fieldName);
                }
            }
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
            // First argument is the record, second argument is the field index
            if (funcExpr.getArguments().size() >= 2) {
                // Recursively process the record argument (for nested field access)
                extractFieldPathRecursive(funcExpr.getArguments().get(0).getValue(), fieldPath);

                // Extract field index from second argument and look up field name
                ILogicalExpression fieldIndexExpr = funcExpr.getArguments().get(1).getValue();
                Integer fieldIndex = AccessMethodUtils.getInt32Constant(new MutableObject<>(fieldIndexExpr));
                if (fieldIndex != null && subTree.getRecordType() != null) {
                    String[] fieldNames = subTree.getRecordType().getFieldNames();
                    if (fieldIndex >= 0 && fieldIndex < fieldNames.length) {
                        fieldPath.add(fieldNames[fieldIndex]);
                    }
                }
            }
        }
    }

    /**
     * Checks if a vector index has all required filter fields in its INCLUDE fields.
     *
     * @param index The vector index to check
     * @param filterFields The set of field names referenced in the filter
     * @return true if all filter fields are in the index's INCLUDE fields
     */
    protected boolean indexHasIncludeFields(Index index, Set<List<String>> filterFields) {
        if (filterFields == null || filterFields.isEmpty()) {
            // No filter fields - index can be used
            return true;
        }

        if (index.getIndexType() != IndexType.VTREE) {
            return false;
        }

        Index.VectorIndexDetails vectorDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        List<List<String>> includeFieldNames = vectorDetails.getIncludeFieldNames();

        if (includeFieldNames == null || includeFieldNames.isEmpty()) {
            return false;
        }

        // Check if all filter fields are present in INCLUDE fields.
        for (List<String> filterField : filterFields) {
            boolean found = false;
            for (List<String> includeField : includeFieldNames) {
                if (includeField.equals(filterField)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
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
            queryDistanceMetric = VectorIndexAccessMethod.normalizeDistanceMetric(annHint.getMetric());
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

    /**
     * Chooses the best vector index from candidates.
     * Considers:
     * 1. INCLUDE fields: If query has filter (WHERE clause), index must have all filter fields in INCLUDE
     * 2. Distance metric: Prefers indexes with matching distance metrics.
     * If the query specifies a constant distance metric that does not match the index metadata, compilation fails.
     */
    protected void chooseVectorIndex(Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs,
            List<Pair<IAccessMethod, Index>> result) throws AlgebricksException {

        AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(VectorIndexAccessMethod.INSTANCE);
        if (analysisCtx == null) {
            return;
        }

        // Check if query has filter predicates
        boolean hasFilter = selectOp != null && filterFieldNames != null && !filterFieldNames.isEmpty();

        // Iterate over candidate vector indexes
        Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexIt =
                analysisCtx.getIteratorForIndexExprsAndVars();

        Pair<IAccessMethod, Index> exactMatch = null; // Index with matching field, metric, AND INCLUDE fields
        Pair<IAccessMethod, Index> fieldMatch = null; // Index with matching field only (and INCLUDE if needed)

        while (indexIt.hasNext()) {
            Map.Entry<Index, List<Pair<Integer, Integer>>> indexEntry = indexIt.next();
            Index index = indexEntry.getKey();

            if (index.getIndexType() == IndexType.VTREE) {
                // If query has filter, index must have all filter fields in its INCLUDE list.
                if (hasFilter && !indexHasIncludeFields(index, filterFieldNames)) {
                    continue;
                }

                if (queryDistanceMetric != null && !queryDistanceMetric.isEmpty()) {
                    String indexMetric = VectorIndexAccessMethod.getIndexDistanceMetric(index);
                    if (queryDistanceMetric.equals(indexMetric)) {
                        // Exact match: field name AND distance metric match.
                        exactMatch = new Pair<>(VectorIndexAccessMethod.INSTANCE, index);
                        break;
                    } else if (fieldMatch == null) {
                        // Field matches but metric doesn't - store as fallback only if no exact match.
                        fieldMatch = new Pair<>(VectorIndexAccessMethod.INSTANCE, index);
                    }
                } else {
                    // No query metric available - use first field match (backward compatibility).
                    result.add(new Pair<>(VectorIndexAccessMethod.INSTANCE, index));
                    break;
                }
            }
        }

        // Prefer exact match. If only a field match exists (metric mismatch), fall back to full scan (KNN).
        if (exactMatch != null) {
            result.add(exactMatch);
        } else if (fieldMatch != null) {
            Index idx = fieldMatch.second;
            String indexMetric = VectorIndexAccessMethod.getIndexDistanceMetric(idx);
            LOGGER.warn("Distance metric mismatch: query uses '{}' but index '{}' uses '{}'. "
                    + "Falling back to full scan (KNN).", queryDistanceMetric, idx.getIndexName(), indexMetric);
        }
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

        // Decide whether the index-only plan branch can be taken (PK-only projection above LIMIT):
        // when every variable consumed above the LIMIT is PK-derived, the assembled record from the
        // primary BTree lookup is never needed, so the vector index emits (pk, dist) and the plan
        // skips the lookup + rerank entirely.
        boolean indexOnly = isProjectionPkOnly();

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
     * Detects whether consumers above the matched {@code limitOp} only reference primary-key columns
     * of the dataset — meaning the assembled record produced by a downstream primary BTree lookup is
     * never needed and the plan can take the index-only branch (vector index emits {@code (pk, dist)},
     * SORT on {@code $$dist}, LIMIT, done — no primary lookup, no rerank).
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Identify dataset PK variables and "record" variables (the non-PK variables produced by the
     *       {@code DataSourceScan}: the dataset record and, if present, the meta record).</li>
     *   <li>Collect bindings from every {@code ASSIGN} in the subtree below {@code limitOp}
     *       (variable → defining expression).</li>
     *   <li>Compute the live-out set of {@code limitOp} by collecting variables used by every operator
     *       in {@link #aboveLimitOps}.</li>
     *   <li>For each live-out variable, trace through the bindings: it is PK-safe iff it resolves to a
     *       PK variable, a constant, or a {@code field-access-by-name($$rec, f)} where {@code f} is the
     *       (top-level, single-segment) name of a PK column. If any live-out variable fails the trace,
     *       the optimization cannot be safely applied.</li>
     * </ol>
     *
     * <p>Conservatively returns {@code false} for any dataset with a meta record (currently out of
     * scope), composite PK paths, external data sources, or unfamiliar plan shapes.
     */
    protected boolean isProjectionPkOnly() {
        // Index-only skips the primary BTree lookup and emits (pk, dist) straight from the secondary VTree.
        // Correct even with deletes: the VTree search cursor reconciles anti-matter (delete) tuples itself,
        // so a PK-only ANN query over a dataset with deletes returns no deleted PKs. Enabled by default; the
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

        // PK fields: only support top-level single-segment paths (e.g. ["id"]); composite or nested PK
        // paths are out of scope for the initial activation.
        List<List<String>> pkPaths = subTree.getDataset().getPrimaryKeys();
        Set<String> pkFieldNames = new HashSet<>();
        for (List<String> p : pkPaths) {
            if (p == null || p.size() != 1) {
                return false;
            }
            pkFieldNames.add(p.get(0));
        }
        int numPK = pkPaths.size();
        if (dsVars.size() < numPK + 1) {
            return false; // need at least PKs + a record variable
        }
        Set<LogicalVariable> pkVars = new HashSet<>(dsVars.subList(0, numPK));
        // Anything after PKs is the dataset record (+ optional meta record). Direct references to these
        // mean we need the assembled record; only field-access for PK names is OK.
        Set<LogicalVariable> recordVars = new HashSet<>(dsVars.subList(numPK, dsVars.size()));

        // Collect ASSIGN bindings throughout the entire subtree.
        Map<LogicalVariable, ILogicalExpression> bindings = new HashMap<>();
        collectAssignBindings(subTree.getRoot(), bindings);

        // Gather variables used above the LIMIT.
        Set<LogicalVariable> liveOut = new HashSet<>();
        for (AbstractLogicalOperator op : aboveLimitOps) {
            try {
                VariableUtilities.getUsedVariables(op, liveOut);
            } catch (AlgebricksException e) {
                LOGGER.trace("isProjectionPkOnly: failed to collect used vars from {}", op.getOperatorTag(), e);
                return false;
            }
        }
        // Trace each live-out variable; fail on the first non-PK-derived one.
        Set<LogicalVariable> visiting = new HashSet<>();
        for (LogicalVariable v : liveOut) {
            if (!isVarPkSafe(v, bindings, pkVars, recordVars, pkFieldNames, visiting)) {
                LOGGER.trace("isProjectionPkOnly: live-out variable {} is not PK-derived; bailing", v);
                return false;
            }
        }

        // A WHERE below the LIMIT that needs a non-PK record field (e.g. an INCLUDE-field filter such as
        // WHERE m.year > 2000) cannot be served by the index-only plan: the record variable is dead, so
        // VectorIndexAccessMethod#neutralizeDanglingExpressions collapses the predicate to select(missing)
        // and drops every row. The liveOut scan above only covers the projection above the LIMIT, so a
        // SELECT below the LIMIT is invisible to it. Require every SELECT-condition variable in the subtree
        // to be PK-safe; otherwise fall back to lookup-and-rerank, which materializes the record and lets
        // PushFilterIntoVectorSearchRule push the INCLUDE filter into the vector cursor correctly.
        Set<LogicalVariable> filterVars = new HashSet<>();
        collectSelectConditionVars(subTree.getRoot(), filterVars);
        for (LogicalVariable v : filterVars) {
            if (!isVarPkSafe(v, bindings, pkVars, recordVars, pkFieldNames, visiting)) {
                LOGGER.trace("isProjectionPkOnly: WHERE condition var {} is not PK-safe; not index-only", v);
                return false;
            }
        }
        return true;
    }

    /**
     * Collect the variables used in every {@code SELECT} condition in the subtree (below the LIMIT). Used by
     * {@link #isProjectionPkOnly} to reject the index-only plan when a {@code WHERE} needs a non-PK record
     * field (e.g. a filter on an INCLUDE field), which the index-only plan cannot serve.
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

    /**
     * Walk {@code op} and its inputs collecting {@code variable → expression} bindings from every
     * {@code ASSIGN} operator. Used by {@link #isProjectionPkOnly} to trace derived live-out variables
     * back to their definitions.
     */
    private void collectAssignBindings(ILogicalOperator op, Map<LogicalVariable, ILogicalExpression> bindings) {
        if (op == null) {
            return;
        }
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator a = (AssignOperator) op;
            List<LogicalVariable> vars = a.getVariables();
            List<Mutable<ILogicalExpression>> exprs = a.getExpressions();
            for (int i = 0; i < vars.size(); i++) {
                bindings.put(vars.get(i), exprs.get(i).getValue());
            }
        }
        for (Mutable<ILogicalOperator> input : op.getInputs()) {
            collectAssignBindings(input.getValue(), bindings);
        }
    }

    private boolean isVarPkSafe(LogicalVariable v, Map<LogicalVariable, ILogicalExpression> bindings,
            Set<LogicalVariable> pkVars, Set<LogicalVariable> recordVars, Set<String> pkFieldNames,
            Set<LogicalVariable> visiting) {
        if (pkVars.contains(v)) {
            return true;
        }
        if (recordVars.contains(v)) {
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
            return isExprPkSafe(e, bindings, pkVars, recordVars, pkFieldNames, visiting);
        } finally {
            visiting.remove(v);
        }
    }

    private boolean isExprPkSafe(ILogicalExpression e, Map<LogicalVariable, ILogicalExpression> bindings,
            Set<LogicalVariable> pkVars, Set<LogicalVariable> recordVars, Set<String> pkFieldNames,
            Set<LogicalVariable> visiting) {
        switch (e.getExpressionTag()) {
            case CONSTANT:
                return true;
            case VARIABLE:
                return isVarPkSafe(((VariableReferenceExpression) e).getVariableReference(), bindings, pkVars,
                        recordVars, pkFieldNames, visiting);
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) e;
                FunctionIdentifier fid = fce.getFunctionIdentifier();
                // Special-case field access on the record variable: PK fields only.
                if (BuiltinFunctions.FIELD_ACCESS_BY_NAME.equals(fid) && fce.getArguments().size() == 2) {
                    ILogicalExpression a0 = fce.getArguments().get(0).getValue();
                    ILogicalExpression a1 = fce.getArguments().get(1).getValue();
                    if (a0.getExpressionTag() == LogicalExpressionTag.VARIABLE
                            && a1.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                        LogicalVariable target = ((VariableReferenceExpression) a0).getVariableReference();
                        if (recordVars.contains(target)) {
                            String fieldName = extractStringConstant(a1);
                            return fieldName != null && pkFieldNames.contains(fieldName);
                        }
                    }
                }
                // For any other function call: all argument expressions must be PK-safe.
                for (Mutable<ILogicalExpression> arg : fce.getArguments()) {
                    if (!isExprPkSafe(arg.getValue(), bindings, pkVars, recordVars, pkFieldNames, visiting)) {
                        return false;
                    }
                }
                return true;
            }
            default:
                return false;
        }
    }

    private static String extractStringConstant(ILogicalExpression e) {
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
        selectOp = null;
        filterFieldNames = null;
        aboveLimitOps.clear();
        subTree.reset();
    }

    @Override
    public Map<FunctionIdentifier, List<IAccessMethod>> getAccessMethods() {
        return accessMethods;
    }
}
