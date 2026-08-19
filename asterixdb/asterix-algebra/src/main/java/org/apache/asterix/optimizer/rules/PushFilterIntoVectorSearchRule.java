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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import org.apache.asterix.optimizer.rules.am.AccessMethodUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.rules.InlineVariablesRule;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Pushes filter conditions into a vector index search when the filter only references
 * INCLUDE fields of that index. Must run in physicalRewritesTopLevel (after
 * SetClosedRecordConstructorsRule) so that record constructors have already been closed.
 *
 * Pattern:
 * <pre>
 *   SELECT (condition on INCLUDE fields)
 *     └── ASSIGN* (optional)
 *           └── PRIMARY_INDEX_UNNEST
 *                 └── ...
 *                       └── VECTOR_INDEX_UNNEST [$pk]
 * </pre>
 *
 * Transforms to:
 * <pre>
 *   ASSIGN* (optional, SELECT removed)
 *     └── PRIMARY_INDEX_UNNEST
 *           └── ...
 *                 └── VECTOR_INDEX_UNNEST [$pk, $includeField1, ...]
 *                       selectCondition: (rewritten to use $includeField1, ...)
 * </pre>
 *
 * New variables are created for the INCLUDE fields produced by VECTOR_INDEX_UNNEST, and
 * field-access expressions in the filter are rewritten to reference those variables.
 */
public class PushFilterIntoVectorSearchRule implements IAlgebraicRewriteRule {

    /**
     * Annotation key for filter variable to physical field index mapping.
     * Value type: Map&lt;LogicalVariable, Integer&gt;
     */
    public static final String VECTOR_FILTER_VAR_MAPPING = "VECTOR_FILTER_VAR_MAPPING";

    /**
     * Annotation key for filter variable to type mapping.
     * Value type: Map&lt;LogicalVariable, IAType&gt;
     */
    public static final String VECTOR_FILTER_VAR_TYPES = "VECTOR_FILTER_VAR_TYPES";

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();

        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        SelectOperator selectOp = (SelectOperator) op;

        VectorSearchInfo searchInfo = findVectorIndexUnnest(selectOp, context);
        if (searchInfo == null) {
            return false;
        }
        if (searchInfo.includeFieldNames() == null || searchInfo.includeFieldNames().isEmpty()) {
            return false;
        }

        // Inline ASSIGNs into the select condition so that filter references hit field-access
        // expressions on the source record directly.
        ILogicalExpression selectCondition = selectOp.getCondition().getValue();
        MutableObject<ILogicalExpression> conditionRef = new MutableObject<>(selectCondition.cloneExpression());

        Set<LogicalVariable> usedVariables = new HashSet<>();
        conditionRef.getValue().getUsedVariables(usedVariables);

        InlineVariablesRule.InlineVariablesVisitor inlineVisitor = null;
        Map<LogicalVariable, ILogicalExpression> varAssignRhs = new HashMap<>();

        for (ILogicalOperator child = selectOp.getInputs().get(0).getValue(); child
                .getOperatorTag() == LogicalOperatorTag.ASSIGN; child = child.getInputs().get(0).getValue()) {
            varAssignRhs.clear();
            AssignOperator assignOp = (AssignOperator) child;
            extractInlinableVariables(assignOp, usedVariables, varAssignRhs);

            if (!varAssignRhs.isEmpty()) {
                if (inlineVisitor == null) {
                    inlineVisitor = new InlineVariablesRule.InlineVariablesVisitor(varAssignRhs, null);
                    inlineVisitor.setContext(context);
                    inlineVisitor.setOperator(selectOp);
                }
                if (!inlineVisitor.transform(conditionRef)) {
                    break;
                }
                usedVariables.clear();
                conditionRef.getValue().getUsedVariables(usedVariables);
            }
        }

        // Collect the FULL field paths the filter reads, resolved against the record variable(s) this vector
        // search feeds. Matching on full paths (not leaf names) is what keeps `WHERE m.b.year > 2000` from
        // being pushed against an `INCLUDE (a.year)` column, and restricting the base to this subtree's
        // record variable(s) is what keeps a second record (from a join or a LET) from being redirected into
        // the index's INCLUDE field.
        Set<List<String>> filterFieldPaths = new HashSet<>();
        extractFieldPaths(conditionRef.getValue(), searchInfo, filterFieldPaths);

        if (filterFieldPaths.isEmpty()) {
            return false;
        }

        // Bail out if the filter references any field that is not in the index's INCLUDE list.
        Map<List<String>, Integer> includeFieldIndex = buildIncludeFieldIndex(searchInfo.includeFieldNames());
        // Two identical INCLUDE paths would collapse the map and bind the filter to whichever was inserted
        // last; bail rather than risk pushing a predicate that resolves to the wrong physical field.
        if (includeFieldIndex.size() < searchInfo.includeFieldNames().size()) {
            return false;
        }
        for (List<String> fieldPath : filterFieldPaths) {
            if (!includeFieldIndex.containsKey(fieldPath)) {
                return false;
            }
        }

        // Create fresh logical variables for INCLUDE fields referenced by the filter. These variables
        // are produced by VECTOR_INDEX_UNNEST solely for filter evaluation.
        Map<List<String>, LogicalVariable> fieldToNewVar = new HashMap<>();
        Map<LogicalVariable, Integer> filterVarToFieldIndex = new HashMap<>();
        Map<LogicalVariable, IAType> filterVarToType = new HashMap<>();

        // Physical tuple format depends on quantization:
        // Non-quantized: [distance, centroidId, pk_0..pk_{N-1}, include_fields...]
        // Quantized:     [distance, centroidId, qDist, qEmbed, pk_0..pk_{N-1}, include_fields...]
        // INCLUDE fields start after the secondary keys and ALL primary key columns.
        int numSecondaryKeys = searchInfo.isQuantized() ? VTreeDataTupleAccessor.Q_NUM_SECONDARY_FIELDS
                : VTreeDataTupleAccessor.NQ_NUM_SECONDARY_FIELDS;
        int includeFieldPhysicalIndex = numSecondaryKeys + searchInfo.numPrimaryKeys();

        for (List<String> fieldPath : searchInfo.includeFieldNames()) {
            if (filterFieldPaths.contains(fieldPath)) {
                LogicalVariable newVar = context.newVar();
                fieldToNewVar.put(fieldPath, newVar);
                filterVarToFieldIndex.put(newVar, includeFieldPhysicalIndex);

                // For open-schema fields not in the type declaration, getSubFieldType returns null;
                // default to ANY so the type environment can still resolve the variable type.
                IAType fieldType = searchInfo.recordType().getSubFieldType(fieldPath);
                if (fieldType == null) {
                    fieldType = BuiltinType.ANY;
                }
                filterVarToType.put(newVar, fieldType);
            }

            includeFieldPhysicalIndex++;
        }

        // Rewrite field-access expressions in the condition to reference the new INCLUDE variables.
        ILogicalExpression rewrittenCondition = rewriteFieldAccess(conditionRef.getValue(), fieldToNewVar, searchInfo);

        // Completeness guard: the pushed condition must reference ONLY the freshly-created INCLUDE
        // variables. The earlier checks act on field names that extractFieldNames recognizes; an
        // access pattern it does not recognize, or a non-inlinable ASSIGN output, can leave a
        // reference to the source record (produced ABOVE the vector unnest-map) in the condition.
        // Embedding such a condition would reference a variable that is not in scope at the unnest --
        // an invalid plan. Bail and leave the SELECT untouched rather than emit a broken plan.
        Set<LogicalVariable> rewrittenUsed = new HashSet<>();
        rewrittenCondition.getUsedVariables(rewrittenUsed);
        if (!new HashSet<>(fieldToNewVar.values()).containsAll(rewrittenUsed)) {
            return false;
        }

        // Hand the mappings to VectorSearchPOperator so it can construct the filter schema.
        searchInfo.vectorUnnest().getAnnotations().put(VECTOR_FILTER_VAR_MAPPING, filterVarToFieldIndex);
        searchInfo.vectorUnnest().getAnnotations().put(VECTOR_FILTER_VAR_TYPES, filterVarToType);

        // Register filter variables as produced by the unnest-map so sanity checks recognize them.
        for (Map.Entry<List<String>, LogicalVariable> entry : fieldToNewVar.entrySet()) {
            LogicalVariable var = entry.getValue();
            IAType type = filterVarToType.get(var);
            searchInfo.vectorUnnest().getVariables().add(var);
            searchInfo.vectorUnnest().getVariableTypes().add(type);
        }

        searchInfo.vectorUnnest().setSelectCondition(new MutableObject<>(rewrittenCondition));

        // Drop the SELECT now that the predicate has been embedded in the vector index search.
        opRef.setValue(selectOp.getInputs().get(0).getValue());

        context.addToDontApplySet(this, op);

        context.computeAndSetTypeEnvironmentForOperator(searchInfo.vectorUnnest());
        OperatorPropertiesUtil.typeOpRec(opRef, context);

        return true;
    }

    /**
     * Information about a vector index search found in the plan.
     *
     * @param recordVars the variables produced between the SELECT and the vector search — i.e. the record
     *                   (and PK) variables of THIS search's primary-index lookup. A field access must be
     *                   rooted at one of these to be a candidate for pushdown.
     */
    private record VectorSearchInfo(UnnestMapOperator vectorUnnest, List<List<String>> includeFieldNames,
            ARecordType recordType, boolean isQuantized, int numPrimaryKeys, Set<LogicalVariable> recordVars) {
    }

    /**
     * Finds VECTOR_INDEX_UNNEST below the SELECT operator, skipping intervening ASSIGNs.
     */
    private VectorSearchInfo findVectorIndexUnnest(SelectOperator selectOp, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator current = selectOp.getInputs().get(0).getValue();
        while (current.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            current = current.getInputs().get(0).getValue();
        }
        return searchForVectorUnnest(current, context, new HashSet<>());
    }

    /**
     * Recursively searches for a VECTOR_INDEX_UNNEST under the given operator, accumulating on the way down
     * the variables produced by the non-vector unnest-maps it passes through — the primary-index lookup that
     * materializes the dataset record the filter reads.
     */
    private VectorSearchInfo searchForVectorUnnest(ILogicalOperator op, IOptimizationContext context,
            Set<LogicalVariable> recordVars) throws AlgebricksException {

        if (op.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            UnnestMapOperator unnest = (UnnestMapOperator) op;
            ILogicalExpression expr = unnest.getExpressionRef().getValue();

            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

                if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INDEX_SEARCH)) {
                    AccessMethodJobGenParams params = new AccessMethodJobGenParams();
                    params.readFromFuncArgs(funcExpr.getArguments());

                    if (params.getIndexType() == IndexType.VTREE) {
                        if (unnest.getSelectCondition() != null) {
                            return null;
                        }
                        return buildSearchInfo(unnest, params, context, recordVars);
                    }
                }
            }
            recordVars.addAll(unnest.getVariables());
        }

        for (Mutable<ILogicalOperator> inputRef : op.getInputs()) {
            VectorSearchInfo result = searchForVectorUnnest(inputRef.getValue(), context, recordVars);
            if (result != null) {
                return result;
            }
        }

        return null;
    }

    /**
     * Builds VectorSearchInfo from the found vector index.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    private VectorSearchInfo buildSearchInfo(UnnestMapOperator unnest, AccessMethodJobGenParams params,
            IOptimizationContext context, Set<LogicalVariable> recordVars) throws AlgebricksException {

        MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();

        Dataset dataset = mp.findDataset(params.getDatabaseName(), params.getDataverseName(), params.getDatasetName());
        if (dataset == null) {
            return null;
        }

        Index index = mp.getIndex(params.getDatabaseName(), params.getDataverseName(), params.getDatasetName(),
                params.getIndexName());
        if (index == null || index.getIndexType() != IndexType.VTREE) {
            return null;
        }

        Index.VectorIndexDetails details = (Index.VectorIndexDetails) index.getIndexDetails();

        ARecordType recordType = (ARecordType) mp.findType(dataset.getItemTypeDatabaseName(),
                dataset.getItemTypeDataverseName(), dataset.getItemTypeName());

        return new VectorSearchInfo(unnest, details.getIncludeFieldNames(), recordType,
                details.getVectorParameters().isQuantized(), dataset.getPrimaryKeys().size(), recordVars);
    }

    /**
     * Extracts variables from ASSIGN that can be inlined.
     */
    private void extractInlinableVariables(AssignOperator assignOp, Set<LogicalVariable> targetVars,
            Map<LogicalVariable, ILogicalExpression> outMap) {
        List<LogicalVariable> vars = assignOp.getVariables();
        List<Mutable<ILogicalExpression>> exprs = assignOp.getExpressions();

        for (int i = 0; i < vars.size(); i++) {
            LogicalVariable var = vars.get(i);
            if (targetVars.contains(var)) {
                ILogicalExpression expr = exprs.get(i).getValue();
                if (expr.isFunctional()) {
                    outMap.put(var, expr);
                }
            }
        }
    }

    /**
     * Builds a map from an INCLUDE field's full path to its index in the INCLUDE list.
     * <p>
     * Keyed by the whole path, not the leaf name: {@code INCLUDE (a.year)} must not match a filter on
     * {@code b.year}. {@link List} equality gives exactly path equality.
     */
    private Map<List<String>, Integer> buildIncludeFieldIndex(List<List<String>> includeFieldNames) {
        Map<List<String>, Integer> result = new HashMap<>();
        for (int i = 0; i < includeFieldNames.size(); i++) {
            result.put(includeFieldNames.get(i), i);
        }
        return result;
    }

    /**
     * Collects the full field paths the filter reads from this vector search's record variable(s).
     * <p>
     * Only accesses rooted at one of {@code searchInfo.recordVars()} are collected. An access on any other
     * record is deliberately ignored here, so it survives the rewrite and trips the completeness guard in
     * {@link #rewritePre} — the rule then leaves the SELECT alone instead of silently evaluating someone
     * else's predicate against an INCLUDE column.
     */
    private void extractFieldPaths(ILogicalExpression expr, VectorSearchInfo searchInfo, Set<List<String>> fieldPaths)
            throws AlgebricksException {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        List<String> path = resolveFieldPath(funcExpr, searchInfo);
        if (path != null) {
            fieldPaths.add(path);
            // Do not recurse: the nested accesses under this one are the prefix of the path just added.
            return;
        }

        // Recurse into arguments
        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            extractFieldPaths(arg.getValue(), searchInfo, fieldPaths);
        }
    }

    /**
     * Resolve a (possibly nested) field-access expression to its full path relative to one of this vector
     * search's record variables, or {@code null} if it is not such an access.
     * <p>
     * {@code FIELD_ACCESS_BY_INDEX}'s integer is an index into the field names of the type of ITS OWN base
     * expression, so for a nested access the base's type has to be resolved first (descending the record
     * type as the recursion descends) — resolving every level against the top-level record type produces a
     * bogus name, which in the worst case collides with a real INCLUDE path. Note that
     * {@code ByNameToByIndexFieldAccessRule} has already converted accesses on a declared type to
     * {@code FIELD_ACCESS_BY_INDEX} by the time this rule runs, so the nested case is the common one.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
    private List<String> resolveFieldPath(AbstractFunctionCallExpression funcExpr, VectorSearchInfo searchInfo)
            throws AlgebricksException {
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();
        boolean byName = fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME);
        boolean byIndex = fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX);
        if ((!byName && !byIndex) || funcExpr.getArguments().size() < 2) {
            return null;
        }

        // Resolve the base: either this search's record variable (empty prefix) or an enclosing field access.
        ILogicalExpression base = funcExpr.getArguments().get(0).getValue();
        List<String> prefix;
        if (base.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            LogicalVariable baseVar = ((VariableReferenceExpression) base).getVariableReference();
            if (!searchInfo.recordVars().contains(baseVar)) {
                return null;
            }
            prefix = List.of();
        } else if (base.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            prefix = resolveFieldPath((AbstractFunctionCallExpression) base, searchInfo);
            if (prefix == null) {
                return null;
            }
        } else {
            return null;
        }

        String fieldName;
        if (byName) {
            fieldName = AccessMethodUtils.getStringConstant(funcExpr.getArguments().get(1));
        } else {
            Integer idx = AccessMethodUtils.getInt32Constant(funcExpr.getArguments().get(1));
            ARecordType baseType = resolveRecordTypeAt(searchInfo.recordType(), prefix);
            if (idx == null || baseType == null) {
                return null;
            }
            String[] names = baseType.getFieldNames();
            fieldName = idx >= 0 && idx < names.length ? names[idx] : null;
        }
        if (fieldName == null) {
            return null;
        }

        List<String> path = new ArrayList<>(prefix);
        path.add(fieldName);
        return path;
    }

    /**
     * The record type reached by following {@code path} from {@code recordType}, or {@code null} if the path
     * does not resolve to a record type (open field, non-record type, unknown path).
     */
    private ARecordType resolveRecordTypeAt(ARecordType recordType, List<String> path) throws AlgebricksException {
        if (recordType == null) {
            return null;
        }
        if (path.isEmpty()) {
            return recordType;
        }
        IAType type = recordType.getSubFieldType(path);
        if (type != null && type.getTypeTag() == ATypeTag.OBJECT) {
            return (ARecordType) type;
        }
        return null;
    }

    /**
     * Rewrites field-access expressions to use new INCLUDE field variables.
     * Example: gt($row.getField(2), 2000) -> gt($year, 2000)
     */
    private ILogicalExpression rewriteFieldAccess(ILogicalExpression expr,
            Map<List<String>, LogicalVariable> fieldToVar, VectorSearchInfo searchInfo) throws AlgebricksException {

        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return expr;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

        // Replace a field access with a variable reference only when its FULL path — resolved against this
        // search's record variable — is one of the INCLUDE fields we created a variable for.
        List<String> path = resolveFieldPath(funcExpr, searchInfo);
        if (path != null && fieldToVar.containsKey(path)) {
            LogicalVariable newVar = fieldToVar.get(path);
            VariableReferenceExpression varRef = new VariableReferenceExpression(newVar);
            varRef.setSourceLocation(funcExpr.getSourceLocation());
            return varRef;
        }

        // Recurse into arguments
        List<Mutable<ILogicalExpression>> newArgs = new ArrayList<>();
        boolean changed = false;

        for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
            ILogicalExpression newArg = rewriteFieldAccess(argRef.getValue(), fieldToVar, searchInfo);
            newArgs.add(new MutableObject<>(newArg));
            if (newArg != argRef.getValue()) {
                changed = true;
            }
        }

        if (changed) {
            ScalarFunctionCallExpression newFunc =
                    new ScalarFunctionCallExpression(funcExpr.getFunctionInfo(), newArgs);
            newFunc.setSourceLocation(funcExpr.getSourceLocation());
            return newFunc;
        }

        return expr;
    }
}
